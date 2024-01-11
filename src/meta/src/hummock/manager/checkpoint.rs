// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound::{Excluded, Included};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;

use function_name::named;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    object_size_map, summarize_group_deltas,
};
use risingwave_pb::hummock::hummock_version_checkpoint::StaleObjects;
use risingwave_pb::hummock::{HummockVersion, HummockVersionArchive, HummockVersionCheckpoint};

use crate::hummock::error::Result;
use crate::hummock::manager::{read_lock, write_lock};
use crate::hummock::metrics_utils::trigger_gc_stat;
use crate::hummock::HummockManager;
use crate::storage::{MetaStore, MetaStoreError, DEFAULT_COLUMN_FAMILY};

const HUMMOCK_INIT_FLAG_KEY: &[u8] = b"hummock_init_flag";

/// A hummock version checkpoint compacts previous hummock version delta logs, and stores stale
/// objects from those delta logs.
impl HummockManager {
    /// # Panics
    /// if checkpoint is not found.
    pub async fn read_checkpoint(&self) -> Result<HummockVersionCheckpoint> {
        use prost::Message;
        let data = match self
            .object_store
            .read(&self.version_checkpoint_path, ..)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                if e.is_object_not_found_error() {
                    panic!(
                        "Hummock version checkpoints do not exist in object store, path: {}",
                        self.version_checkpoint_path
                    );
                }
                return Err(e.into());
            }
        };
        let ckpt = HummockVersionCheckpoint::decode(data).map_err(|e| anyhow::anyhow!(e))?;
        Ok(ckpt)
    }

    pub(super) async fn write_checkpoint(
        &self,
        checkpoint: &HummockVersionCheckpoint,
    ) -> Result<()> {
        use prost::Message;
        let buf = checkpoint.encode_to_vec();
        self.object_store
            .upload(&self.version_checkpoint_path, buf.into())
            .await?;
        Ok(())
    }

    pub(super) async fn write_version_archive(
        &self,
        archive: &HummockVersionArchive,
    ) -> Result<()> {
        use prost::Message;
        let buf = archive.encode_to_vec();
        let archive_path = format!(
            "{}/{}",
            self.version_archive_dir,
            archive.version.as_ref().unwrap().id
        );
        self.object_store.upload(&archive_path, buf.into()).await?;
        Ok(())
    }

    /// Creates a hummock version checkpoint.
    /// Returns the diff between new and old checkpoint id.
    /// Note that this method must not be called concurrently, because internally it doesn't hold
    /// lock throughout the method.
    #[named]
    pub async fn create_version_checkpoint(&self, min_delta_log_num: u64) -> Result<u64> {
        let timer = self.metrics.version_checkpoint_latency.start_timer();
        // 1. hold read lock and create new checkpoint
        let versioning_guard = read_lock!(self, versioning).await;
        let versioning = versioning_guard.deref();
        let current_version = &versioning.current_version;
        let old_checkpoint = &versioning.checkpoint;
        let new_checkpoint_id = current_version.id;
        let old_checkpoint_id = old_checkpoint.version.as_ref().unwrap().id;
        if new_checkpoint_id < old_checkpoint_id + min_delta_log_num {
            return Ok(0);
        }
        let mut stale_objects = old_checkpoint.stale_objects.clone();
        let mut archive: Option<HummockVersionArchive> = None;
        if !self.env.opts.enable_hummock_data_archive {
            // `object_sizes` is used to calculate size of stale objects.
            let mut object_sizes = object_size_map(old_checkpoint.version.as_ref().unwrap());
            for (_, version_delta) in versioning
                .hummock_version_deltas
                .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
            {
                for group_deltas in version_delta.group_deltas.values() {
                    let summary = summarize_group_deltas(group_deltas);
                    object_sizes.extend(
                        summary
                            .insert_table_infos
                            .iter()
                            .map(|t| (t.object_id, t.file_size)),
                    );
                }
                let removed_object_ids = version_delta.gc_object_ids.clone();
                if removed_object_ids.is_empty() {
                    continue;
                }
                let total_file_size = removed_object_ids
                    .iter()
                    .map(|t| object_sizes.get(t).copied().unwrap())
                    .sum::<u64>();
                stale_objects.insert(
                    version_delta.id,
                    StaleObjects {
                        id: removed_object_ids,
                        total_file_size,
                    },
                );
            }
        } else {
            archive = Some(HummockVersionArchive {
                version: old_checkpoint.version.clone(),
                version_deltas: versioning
                    .hummock_version_deltas
                    .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
                    .map(|(_, version_delta)| version_delta.clone())
                    .collect(),
            });
        }
        let new_checkpoint = HummockVersionCheckpoint {
            version: Some(current_version.clone()),
            stale_objects,
        };

        drop(versioning_guard);
        // 2. persist the new checkpoint without holding lock
        self.write_checkpoint(&new_checkpoint).await?;
        if let Some(archive) = archive {
            if let Err(e) = self.write_version_archive(&archive).await {
                tracing::warn!(
                    "failed to write version archive {}, {e}",
                    archive.version.as_ref().unwrap().id
                );
            }
        }
        // 3. hold write lock and update in memory state
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        assert!(
            versioning.checkpoint.version.is_none()
                || new_checkpoint.version.as_ref().unwrap().id
                    >= versioning.checkpoint.version.as_ref().unwrap().id
        );
        versioning.checkpoint = new_checkpoint;
        versioning.mark_objects_for_deletion();

        let min_pinned_version_id = versioning.min_pinned_version_id();
        trigger_gc_stat(&self.metrics, &versioning.checkpoint, min_pinned_version_id);
        drop(versioning_guard);
        timer.observe_duration();
        self.metrics
            .checkpoint_version_id
            .set(new_checkpoint_id as i64);

        Ok(new_checkpoint_id - old_checkpoint_id)
    }

    pub(super) async fn need_init(&self) -> Result<bool> {
        match self
            .env
            .meta_store()
            .get_cf(DEFAULT_COLUMN_FAMILY, HUMMOCK_INIT_FLAG_KEY)
            .await
        {
            Ok(_) => Ok(false),
            Err(MetaStoreError::ItemNotFound(_)) => Ok(true),
            Err(e) => Err(e.into()),
        }
    }

    pub(super) async fn mark_init(&self) -> Result<()> {
        self.env
            .meta_store()
            .put_cf(
                DEFAULT_COLUMN_FAMILY,
                HUMMOCK_INIT_FLAG_KEY.to_vec(),
                memcomparable::to_vec(&0).unwrap(),
            )
            .await
            .map_err(Into::into)
    }

    pub fn pause_version_checkpoint(&self) {
        self.pause_version_checkpoint.store(true, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is paused.");
    }

    pub fn resume_version_checkpoint(&self) {
        self.pause_version_checkpoint
            .store(false, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is resumed.");
    }

    pub fn is_version_checkpoint_paused(&self) -> bool {
        self.pause_version_checkpoint.load(Ordering::Relaxed)
    }

    #[named]
    pub async fn get_checkpoint_version(&self) -> HummockVersion {
        let versioning_guard = read_lock!(self, versioning).await;
        versioning_guard
            .checkpoint
            .version
            .as_ref()
            .unwrap()
            .clone()
    }
}
