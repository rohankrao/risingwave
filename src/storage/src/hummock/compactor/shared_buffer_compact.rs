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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::try_join_all;
use futures::{stream, StreamExt, TryFutureExt};
use itertools::Itertools;
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{FullKey, PointRange, TableKey, UserKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{CompactionGroupId, EpochWithGap, HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::compact_task;
use tracing::error;

use crate::filter_key_extractor::{FilterKeyExtractorImpl, FilterKeyExtractorManager};
use crate::hummock::compactor::compaction_filter::DummyCompactionFilter;
use crate::hummock::compactor::context::CompactorContext;
use crate::hummock::compactor::{CompactOutput, Compactor};
use crate::hummock::event_handler::uploader::UploadTaskPayload;
use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::iterator::{
    Forward, ForwardMergeRangeIterator, HummockIterator, OrderedMergeIteratorInner,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchInner, SharedBufferVersionedEntry,
};
use crate::hummock::utils::MemoryTracker;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BlockedXor16FilterBuilder, CachePolicy, CompactionDeleteRangeIterator, GetObjectId,
    HummockError, HummockResult, MonotonicDeleteEvent, SstableBuilderOptions,
    SstableObjectIdManagerRef,
};
use crate::mem_table::ImmutableMemtable;

const GC_DELETE_KEYS_FOR_FLUSH: bool = false;
const GC_WATERMARK_FOR_FLUSH: u64 = 0;

/// Flush shared buffer to level0. Resulted SSTs are grouped by compaction group.
pub async fn compact(
    context: CompactorContext,
    sstable_object_id_manager: SstableObjectIdManagerRef,
    payload: UploadTaskPayload,
    compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
    filter_key_extractor_manager: FilterKeyExtractorManager,
) -> HummockResult<Vec<LocalSstableInfo>> {
    let mut grouped_payload: HashMap<CompactionGroupId, UploadTaskPayload> = HashMap::new();
    for imm in payload {
        let compaction_group_id = match compaction_group_index.get(&imm.table_id) {
            // compaction group id is used only as a hint for grouping different data.
            // If the compaction group id is not found for the table id, we can assign a
            // default compaction group id for the batch.
            //
            // On meta side, when we commit a new epoch, it is acceptable that the
            // compaction group id provided from CN does not match the latest compaction
            // group config.
            None => StaticCompactionGroupId::StateDefault as CompactionGroupId,
            Some(group_id) => *group_id,
        };
        grouped_payload
            .entry(compaction_group_id)
            .or_default()
            .push(imm);
    }

    let mut futures = vec![];
    for (id, group_payload) in grouped_payload {
        let id_copy = id;
        futures.push(
            compact_shared_buffer(
                context.clone(),
                sstable_object_id_manager.clone(),
                filter_key_extractor_manager.clone(),
                group_payload,
            )
            .map_ok(move |results| {
                results
                    .into_iter()
                    .map(move |mut result| {
                        result.compaction_group_id = id_copy;
                        result
                    })
                    .collect_vec()
            }),
        );
    }
    // Note that the output is reordered compared with input `payload`.
    let result = try_join_all(futures)
        .await?
        .into_iter()
        .flatten()
        .collect_vec();
    Ok(result)
}

/// For compaction from shared buffer to level 0, this is the only function gets called.
async fn compact_shared_buffer(
    context: CompactorContext,
    sstable_object_id_manager: SstableObjectIdManagerRef,
    filter_key_extractor_manager: FilterKeyExtractorManager,
    mut payload: UploadTaskPayload,
) -> HummockResult<Vec<LocalSstableInfo>> {
    // Local memory compaction looks at all key ranges.

    let mut existing_table_ids: HashSet<u32> = payload
        .iter()
        .map(|imm| imm.table_id.table_id)
        .dedup()
        .collect();

    assert!(!existing_table_ids.is_empty());

    let multi_filter_key_extractor = filter_key_extractor_manager
        .acquire(existing_table_ids.clone())
        .await?;
    if let FilterKeyExtractorImpl::Multi(multi) = &multi_filter_key_extractor {
        existing_table_ids = multi.get_existing_table_ids();
    }
    let multi_filter_key_extractor = Arc::new(multi_filter_key_extractor);

    let mut size_and_start_user_keys = vec![];
    let mut compact_data_size = 0;
    payload.retain(|imm| {
        let ret = existing_table_ids.contains(&imm.table_id.table_id);
        if !ret {
            error!(
                "can not find table {:?}, it may be removed by meta-service",
                imm.table_id
            );
        }
        ret
    });
    let mut total_key_count = 0;
    for imm in &payload {
        total_key_count += imm.kv_count();
        let data_size = {
            // calculate encoded bytes of key var length
            (imm.kv_count() * 8 + imm.size()) as u64
        };
        compact_data_size += data_size;
        size_and_start_user_keys.push((data_size, imm.start_user_key()));
    }
    size_and_start_user_keys.sort();
    let mut splits = Vec::with_capacity(size_and_start_user_keys.len());
    splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
    let mut key_split_append = |key_before_last: &Bytes| {
        splits.last_mut().unwrap().right = key_before_last.clone();
        splits.push(KeyRange::new(key_before_last.clone(), Bytes::new()));
    };
    let sstable_size = (context.storage_opts.sstable_size_mb as u64) << 20;
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;
    let parallelism = std::cmp::min(
        context.storage_opts.share_buffers_sync_parallelism as u64,
        size_and_start_user_keys.len() as u64,
    );
    let sub_compaction_data_size = if compact_data_size > parallel_compact_size && parallelism > 1 {
        compact_data_size / parallelism
    } else {
        compact_data_size
    };
    // mul 1.2 for other extra memory usage.
    let mut sub_compaction_sstable_size =
        std::cmp::min(sstable_size, sub_compaction_data_size * 6 / 5);
    let mut split_weight_by_vnode = 0;
    if existing_table_ids.len() > 1 {
        if parallelism > 1 && compact_data_size > sstable_size {
            let mut last_buffer_size = 0;
            let mut last_user_key = UserKey::default();
            for (data_size, user_key) in size_and_start_user_keys {
                if last_buffer_size >= sub_compaction_data_size
                    && last_user_key.as_ref() != user_key
                {
                    last_user_key.set(user_key);
                    key_split_append(
                        &FullKey {
                            user_key,
                            epoch_with_gap: EpochWithGap::new_max_epoch(),
                        }
                        .encode()
                        .into(),
                    );
                    last_buffer_size = data_size;
                } else {
                    last_user_key.set(user_key);
                    last_buffer_size += data_size;
                }
            }
        }
    } else {
        let mut vnodes = vec![];
        for imm in &payload {
            vnodes.extend(imm.collect_vnodes());
        }
        vnodes.sort();
        vnodes.dedup();
        const MIN_SSTABLE_SIZE: u64 = 16 * 1024 * 1024;
        if compact_data_size >= MIN_SSTABLE_SIZE && !vnodes.is_empty() {
            let mut avg_vnode_size = compact_data_size / (vnodes.len() as u64);
            split_weight_by_vnode = VirtualNode::COUNT;
            while avg_vnode_size < MIN_SSTABLE_SIZE && split_weight_by_vnode > 0 {
                split_weight_by_vnode /= 2;
                avg_vnode_size *= 2;
            }
            sub_compaction_sstable_size = compact_data_size;
        }
    }

    let parallelism = splits.len();
    let mut compact_success = true;
    let mut output_ssts = Vec::with_capacity(parallelism);
    let mut compaction_futures = vec![];
    let use_block_based_filter = BlockedXor16FilterBuilder::is_kv_count_too_large(total_key_count);

    let table_vnode_partition = if existing_table_ids.len() == 1 {
        let table_id = existing_table_ids.iter().next().unwrap();
        vec![(*table_id, split_weight_by_vnode as u32)]
            .into_iter()
            .collect()
    } else {
        BTreeMap::default()
    };
    for (split_index, key_range) in splits.into_iter().enumerate() {
        let compactor = SharedBufferCompactRunner::new(
            split_index,
            key_range,
            context.clone(),
            sub_compaction_sstable_size as usize,
            table_vnode_partition.clone(),
            use_block_based_filter,
            Box::new(sstable_object_id_manager.clone()),
        );
        let mut forward_iters = Vec::with_capacity(payload.len());
        let mut del_iter = ForwardMergeRangeIterator::new(HummockEpoch::MAX);
        for imm in &payload {
            forward_iters.push(imm.clone().into_forward_iter());
            del_iter.add_batch_iter(imm.delete_range_iter());
        }
        let compaction_executor = context.compaction_executor.clone();
        let multi_filter_key_extractor = multi_filter_key_extractor.clone();
        let handle = compaction_executor.spawn(async move {
            compactor
                .run(
                    OrderedMergeIteratorInner::new(forward_iters),
                    multi_filter_key_extractor,
                    CompactionDeleteRangeIterator::new(del_iter),
                )
                .await
        });
        compaction_futures.push(handle);
    }

    let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
    let mut err = None;
    while let Some(future_result) = buffered.next().await {
        match future_result {
            Ok(Ok((split_index, ssts, table_stats_map))) => {
                output_ssts.push((split_index, ssts, table_stats_map));
            }
            Ok(Err(e)) => {
                compact_success = false;
                tracing::warn!("Shared Buffer Compaction failed with error: {:#?}", e);
                err = Some(e);
            }
            Err(e) => {
                compact_success = false;
                tracing::warn!(
                    "Shared Buffer Compaction failed with future error: {:#?}",
                    e
                );
                err = Some(HummockError::compaction_executor(
                    "failed while execute in tokio",
                ));
            }
        }
    }

    // Sort by split/key range index.
    output_ssts.sort_by_key(|(split_index, ..)| *split_index);

    if compact_success {
        let mut level0 = Vec::with_capacity(parallelism);
        for (_, ssts, _) in output_ssts {
            for sst_info in &ssts {
                context
                    .compactor_metrics
                    .write_build_l0_bytes
                    .inc_by(sst_info.file_size());
            }
            level0.extend(ssts);
        }
        Ok(level0)
    } else {
        Err(err.unwrap())
    }
}

/// Merge multiple batches into a larger one
pub async fn merge_imms_in_memory(
    table_id: TableId,
    instance_id: LocalInstanceId,
    imms: Vec<ImmutableMemtable>,
    memory_tracker: Option<MemoryTracker>,
) -> HummockResult<ImmutableMemtable> {
    let mut kv_count = 0;
    let mut epochs = vec![];
    let mut merged_size = 0;
    let mut merged_imm_ids = Vec::with_capacity(imms.len());

    let mut smallest_table_key = BytesMut::new();
    let mut smallest_empty = true;
    let mut largest_table_key = Bound::Included(Bytes::new());

    let mut imm_iters = Vec::with_capacity(imms.len());
    let mut del_iter = ForwardMergeRangeIterator::new(HummockEpoch::MAX);
    let table_version = if imms.is_empty() {
        None
    } else {
        imms[0].table_version
    };
    for imm in imms {
        assert!(
            imm.kv_count() > 0 || imm.has_range_tombstone(),
            "imm should not be empty"
        );
        assert_eq!(
            table_id,
            imm.table_id(),
            "should only merge data belonging to the same table"
        );
        assert_eq!(
            table_version, imm.table_version,
            "table version of imms differ, {:?}, {:?}",
            table_version, imm.table_version
        );

        merged_imm_ids.push(imm.batch_id());
        epochs.push(imm.min_epoch());
        kv_count += imm.kv_count();
        merged_size += imm.size();
        del_iter.add_batch_iter(imm.delete_range_iter());

        if smallest_empty || smallest_table_key.as_ref().gt(imm.raw_smallest_key()) {
            smallest_table_key.clear();
            smallest_table_key.extend_from_slice(imm.raw_smallest_key());
            smallest_empty = false;
        }
        let imm_raw_largest_key = imm.raw_largest_key();
        if match (&largest_table_key, imm_raw_largest_key) {
            (_, Bound::Unbounded) => true,
            (Bound::Included(x), Bound::Included(y)) | (Bound::Included(x), Bound::Excluded(y)) => {
                x < y
            }
            (Bound::Excluded(x), Bound::Included(y)) | (Bound::Excluded(x), Bound::Excluded(y)) => {
                x <= y
            }
            (Bound::Unbounded, _) => false,
        } {
            largest_table_key = imm_raw_largest_key.as_ref().cloned();
        }

        imm_iters.push(imm.into_forward_iter());
    }
    let mut del_iter = CompactionDeleteRangeIterator::new(del_iter);
    del_iter.rewind().await?;
    epochs.sort();

    // use merge iterator to merge input imms
    let mut mi = OrderedMergeIteratorInner::new(imm_iters);
    mi.rewind().await?;
    let mut items = Vec::with_capacity(kv_count);
    while mi.is_valid() {
        let (key, (epoch, value)) = mi.current_item();
        items.push(((key, value), epoch));
        mi.next().await?;
    }

    let mut merged_payload: Vec<SharedBufferVersionedEntry> = Vec::new();
    let mut pivot = items
        .first()
        .map(|((k, _), _)| k.clone())
        .unwrap_or_default();
    let mut monotonic_tombstone_events = vec![];
    let target_extended_user_key =
        PointRange::from_user_key(UserKey::new(table_id, TableKey(pivot.as_ref())), false);
    while del_iter.is_valid() && del_iter.key().le(&target_extended_user_key) {
        let event_key = del_iter.key().to_vec();
        del_iter.next().await?;
        monotonic_tombstone_events.push(MonotonicDeleteEvent {
            event_key,
            new_epoch: del_iter.earliest_epoch(),
        });
    }

    let mut versions: Vec<(EpochWithGap, HummockValue<Bytes>)> = Vec::new();

    let mut pivot_last_delete_epoch = HummockEpoch::MAX;

    for ((key, value), epoch) in items {
        assert!(key >= pivot, "key should be in ascending order");
        if key != pivot {
            merged_payload.push((pivot, versions));
            pivot = key;
            pivot_last_delete_epoch = HummockEpoch::MAX;
            versions = vec![];
            let target_extended_user_key =
                PointRange::from_user_key(UserKey::new(table_id, TableKey(pivot.as_ref())), false);
            while del_iter.is_valid() && del_iter.key().le(&target_extended_user_key) {
                let event_key = del_iter.key().to_vec();
                del_iter.next().await?;
                monotonic_tombstone_events.push(MonotonicDeleteEvent {
                    event_key,
                    new_epoch: del_iter.earliest_epoch(),
                });
            }
        }
        let earliest_range_delete_which_can_see_key =
            del_iter.earliest_delete_since(epoch.pure_epoch());
        if value.is_delete() {
            pivot_last_delete_epoch = epoch.pure_epoch();
        } else if earliest_range_delete_which_can_see_key < pivot_last_delete_epoch {
            debug_assert!(
                epoch.pure_epoch() < earliest_range_delete_which_can_see_key
                    && earliest_range_delete_which_can_see_key < pivot_last_delete_epoch
            );
            pivot_last_delete_epoch = earliest_range_delete_which_can_see_key;
            // In each merged immutable memtable, since a union set of delete ranges is constructed
            // and thus original delete ranges are replaced with the union set and not
            // used in read, we lose exact information about whether a key is deleted by
            // a delete range in the merged imm which it belongs to. Therefore we need
            // to construct a corresponding delete key to represent this.
            versions.push((
                EpochWithGap::new_from_epoch(earliest_range_delete_which_can_see_key),
                HummockValue::Delete,
            ));
        }
        versions.push((epoch, value));
    }
    while del_iter.is_valid() {
        let event_key = del_iter.key().to_vec();
        del_iter.next().await?;
        monotonic_tombstone_events.push(MonotonicDeleteEvent {
            event_key,
            new_epoch: del_iter.earliest_epoch(),
        });
    }

    // process the last key
    if !versions.is_empty() {
        merged_payload.push((pivot, versions));
    }

    drop(del_iter);

    Ok(SharedBufferBatch {
        inner: Arc::new(SharedBufferBatchInner::new_with_multi_epoch_batches(
            epochs,
            merged_payload,
            smallest_table_key.freeze(),
            largest_table_key,
            kv_count,
            merged_imm_ids,
            monotonic_tombstone_events,
            merged_size,
            memory_tracker,
        )),
        table_id,
        instance_id,
        table_version,
    })
}

pub struct SharedBufferCompactRunner {
    compactor: Compactor,
    split_index: usize,
}

impl SharedBufferCompactRunner {
    pub fn new(
        split_index: usize,
        key_range: KeyRange,
        context: CompactorContext,
        sub_compaction_sstable_size: usize,
        table_vnode_partition: BTreeMap<u32, u32>,
        use_block_based_filter: bool,
        object_id_getter: Box<dyn GetObjectId>,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        options.capacity = sub_compaction_sstable_size;
        let compactor = Compactor::new(
            context,
            options,
            super::TaskConfig {
                key_range,
                cache_policy: CachePolicy::Fill(CachePriority::High),
                gc_delete_keys: GC_DELETE_KEYS_FOR_FLUSH,
                watermark: GC_WATERMARK_FOR_FLUSH,
                stats_target_table_ids: None,
                task_type: compact_task::TaskType::SharedBuffer,
                is_target_l0_or_lbase: true,
                table_vnode_partition,
                use_block_based_filter,
            },
            object_id_getter,
        );
        Self {
            compactor,
            split_index,
        }
    }

    pub async fn run(
        self,
        iter: impl HummockIterator<Direction = Forward>,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        del_iter: CompactionDeleteRangeIterator,
    ) -> HummockResult<CompactOutput> {
        let dummy_compaction_filter = DummyCompactionFilter {};
        let (ssts, table_stats_map) = self
            .compactor
            .compact_key_range(
                iter,
                dummy_compaction_filter,
                del_iter,
                filter_key_extractor,
                None,
                None,
                None,
            )
            .await?;
        Ok((self.split_index, ssts, table_stats_map))
    }
}
