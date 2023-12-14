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

use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound::{Included, Unbounded};
use std::ops::{Bound, RangeBounds};

use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::estimate_size::{EstimateSize, KvSize};
use risingwave_hummock_sdk::key::{FullKey, TableKey, TableKeyRange};
use thiserror::Error;

use crate::error::{StorageError, StorageResult};
use crate::hummock::iterator::{FromRustIterator, RustIteratorBuilder};
use crate::hummock::shared_buffer::shared_buffer_batch::{SharedBufferBatch, SharedBufferBatchId};
use crate::hummock::utils::{
    cmp_delete_range_left_bounds, do_delete_sanity_check, do_insert_sanity_check,
    do_update_sanity_check, filter_with_delete_range, ENABLE_SANITY_CHECK,
};
use crate::hummock::value::HummockValue;
use crate::row_serde::value_serde::ValueRowSerde;
use crate::storage_value::StorageValue;
use crate::store::*;
pub type ImmutableMemtable = SharedBufferBatch;

pub type ImmId = SharedBufferBatchId;

#[derive(Clone, Debug, EstimateSize)]
pub enum KeyOp {
    Insert(Bytes),
    Delete(Bytes),
    /// (old_value, new_value)
    Update((Bytes, Bytes)),
}

/// `MemTable` is a buffer for modify operations without encoding
#[derive(Clone)]
pub struct MemTable {
    pub(crate) buffer: MemTableStore,
    pub(crate) is_consistent_op: bool,
    pub(crate) kv_size: KvSize,
}

#[derive(Error, Debug)]
pub enum MemTableError {
    #[error("Inconsistent operation")]
    InconsistentOperation {
        key: TableKey<Bytes>,
        prev: KeyOp,
        new: KeyOp,
    },
}

type Result<T> = std::result::Result<T, Box<MemTableError>>;

pub type MemTableStore = BTreeMap<TableKey<Bytes>, KeyOp>;
pub struct MemTableIteratorBuilder;

fn map_to_hummock_value<'a>(
    (key, op): (&'a TableKey<Bytes>, &'a KeyOp),
) -> (TableKey<&'a [u8]>, HummockValue<&'a [u8]>) {
    (
        TableKey(key.0.as_ref()),
        match op {
            KeyOp::Insert(value) | KeyOp::Update((_, value)) => HummockValue::Put(value),
            KeyOp::Delete(_) => HummockValue::Delete,
        },
    )
}

impl RustIteratorBuilder for MemTableIteratorBuilder {
    type Iterable = MemTableStore;

    type RewindIter<'a> =
        impl Iterator<Item = (TableKey<&'a [u8]>, HummockValue<&'a [u8]>)> + Send + 'a;
    type SeekIter<'a> =
        impl Iterator<Item = (TableKey<&'a [u8]>, HummockValue<&'a [u8]>)> + Send + 'a;

    fn seek<'a>(iterable: &'a Self::Iterable, seek_key: TableKey<&[u8]>) -> Self::SeekIter<'a> {
        iterable
            .range::<[u8], _>((Included(seek_key.0), Unbounded))
            .map(map_to_hummock_value)
    }

    fn rewind(iterable: &Self::Iterable) -> Self::RewindIter<'_> {
        iterable.iter().map(map_to_hummock_value)
    }
}

pub type MemTableHummockIterator<'a> = FromRustIterator<'a, MemTableIteratorBuilder>;

impl MemTable {
    pub fn new(is_consistent_op: bool) -> Self {
        Self {
            buffer: BTreeMap::new(),
            is_consistent_op,
            kv_size: KvSize::new(),
        }
    }

    pub fn drain(&mut self) -> Self {
        self.kv_size.set(0);
        std::mem::replace(self, Self::new(self.is_consistent_op))
    }

    pub fn is_dirty(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// write methods
    pub fn insert(&mut self, pk: TableKey<Bytes>, value: Bytes) -> Result<()> {
        if !self.is_consistent_op {
            let key_len = std::mem::size_of::<Bytes>() + pk.len();
            let insert_value = KeyOp::Insert(value);
            self.kv_size.add(&pk, &insert_value);
            let origin_value = self.buffer.insert(pk, insert_value);
            self.sub_origin_size(origin_value, key_len);

            return Ok(());
        }
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                let insert_value = KeyOp::Insert(value);
                self.kv_size.add(e.key(), &insert_value);
                e.insert(insert_value);
                Ok(())
            }
            Entry::Occupied(mut e) => {
                let origin_value = e.get_mut();
                self.kv_size.sub_val(origin_value);
                match origin_value {
                    KeyOp::Delete(ref mut old_value) => {
                        let old_val = std::mem::take(old_value);
                        let update_value = KeyOp::Update((old_val, value));
                        self.kv_size.add_val(&update_value);
                        e.insert(update_value);
                        Ok(())
                    }
                    KeyOp::Insert(_) | KeyOp::Update(_) => {
                        Err(MemTableError::InconsistentOperation {
                            key: e.key().clone(),
                            prev: e.get().clone(),
                            new: KeyOp::Insert(value),
                        }
                        .into())
                    }
                }
            }
        }
    }

    pub fn delete(&mut self, pk: TableKey<Bytes>, old_value: Bytes) -> Result<()> {
        let key_len = std::mem::size_of::<Bytes>() + pk.len();
        if !self.is_consistent_op {
            let delete_value = KeyOp::Delete(old_value);
            self.kv_size.add(&pk, &delete_value);
            let origin_value = self.buffer.insert(pk, delete_value);
            self.sub_origin_size(origin_value, key_len);
            return Ok(());
        }
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                let delete_value = KeyOp::Delete(old_value);
                self.kv_size.add(e.key(), &delete_value);
                e.insert(delete_value);
                Ok(())
            }
            Entry::Occupied(mut e) => {
                let origin_value = e.get_mut();
                self.kv_size.sub_val(origin_value);
                match origin_value {
                    KeyOp::Insert(original_value) => {
                        if ENABLE_SANITY_CHECK && original_value != &old_value {
                            return Err(Box::new(MemTableError::InconsistentOperation {
                                key: e.key().clone(),
                                prev: e.get().clone(),
                                new: KeyOp::Delete(old_value),
                            }));
                        }

                        self.kv_size.sub_size(key_len);
                        e.remove();

                        Ok(())
                    }
                    KeyOp::Delete(_) => Err(MemTableError::InconsistentOperation {
                        key: e.key().clone(),
                        prev: e.get().clone(),
                        new: KeyOp::Delete(old_value),
                    }
                    .into()),
                    KeyOp::Update(value) => {
                        let (original_old_value, original_new_value) = std::mem::take(value);
                        if ENABLE_SANITY_CHECK && original_new_value != old_value {
                            return Err(Box::new(MemTableError::InconsistentOperation {
                                key: e.key().clone(),
                                prev: e.get().clone(),
                                new: KeyOp::Delete(old_value),
                            }));
                        }
                        let delete_value = KeyOp::Delete(original_old_value);
                        self.kv_size.add_val(&delete_value);
                        e.insert(delete_value);
                        Ok(())
                    }
                }
            }
        }
    }

    pub fn update(
        &mut self,
        pk: TableKey<Bytes>,
        old_value: Bytes,
        new_value: Bytes,
    ) -> Result<()> {
        if !self.is_consistent_op {
            let key_len = std::mem::size_of::<Bytes>() + pk.len();

            let update_value = KeyOp::Update((old_value, new_value));
            self.kv_size.add(&pk, &update_value);
            let origin_value = self.buffer.insert(pk, update_value);
            self.sub_origin_size(origin_value, key_len);
            return Ok(());
        }
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                let update_value = KeyOp::Update((old_value, new_value));
                self.kv_size.add(e.key(), &update_value);
                e.insert(update_value);
                Ok(())
            }
            Entry::Occupied(mut e) => {
                let origin_value = e.get_mut();
                self.kv_size.sub_val(origin_value);
                match origin_value {
                    KeyOp::Insert(original_new_value) => {
                        if ENABLE_SANITY_CHECK && original_new_value != &old_value {
                            return Err(Box::new(MemTableError::InconsistentOperation {
                                key: e.key().clone(),
                                prev: e.get().clone(),
                                new: KeyOp::Update((old_value, new_value)),
                            }));
                        }
                        let new_key_op = KeyOp::Insert(new_value);
                        self.kv_size.add_val(&new_key_op);
                        e.insert(new_key_op);
                        Ok(())
                    }
                    KeyOp::Update((origin_old_value, original_new_value)) => {
                        if ENABLE_SANITY_CHECK && original_new_value != &old_value {
                            return Err(Box::new(MemTableError::InconsistentOperation {
                                key: e.key().clone(),
                                prev: e.get().clone(),
                                new: KeyOp::Update((old_value, new_value)),
                            }));
                        }
                        let old_value = std::mem::take(origin_old_value);
                        let new_key_op = KeyOp::Update((old_value, new_value));
                        self.kv_size.add_val(&new_key_op);
                        e.insert(new_key_op);
                        Ok(())
                    }
                    KeyOp::Delete(_) => Err(MemTableError::InconsistentOperation {
                        key: e.key().clone(),
                        prev: e.get().clone(),
                        new: KeyOp::Update((old_value, new_value)),
                    }
                    .into()),
                }
            }
        }
    }

    pub fn into_parts(self) -> BTreeMap<TableKey<Bytes>, KeyOp> {
        self.buffer
    }

    pub fn iter<'a, R>(
        &'a self,
        key_range: R,
    ) -> impl Iterator<Item = (&'a TableKey<Bytes>, &'a KeyOp)>
    where
        R: RangeBounds<TableKey<Bytes>> + 'a,
    {
        self.buffer.range(key_range)
    }

    fn sub_origin_size(&mut self, origin_value: Option<KeyOp>, key_len: usize) {
        if let Some(origin_value) = origin_value {
            self.kv_size.sub_val(&origin_value);
            self.kv_size.sub_size(key_len);
        }
    }
}

impl KeyOp {
    /// Print as debug string with decoded data.
    ///
    /// # Panics
    ///
    /// The function will panic if it failed to decode the bytes with provided data types.
    pub fn debug_fmt(&self, row_deserializer: &impl ValueRowSerde) -> String {
        match self {
            Self::Insert(after) => {
                let after = row_deserializer.deserialize(after.as_ref());
                format!("Insert({:?})", &after)
            }
            Self::Delete(before) => {
                let before = row_deserializer.deserialize(before.as_ref());
                format!("Delete({:?})", &before)
            }
            Self::Update((before, after)) => {
                let after = row_deserializer.deserialize(after.as_ref());
                let before = row_deserializer.deserialize(before.as_ref());
                format!("Update({:?}, {:?})", &before, &after)
            }
        }
    }
}

#[try_stream(ok = StateStoreIterItem, error = StorageError)]
pub(crate) async fn merge_stream<'a>(
    mem_table_iter: impl Iterator<Item = (&'a TableKey<Bytes>, &'a KeyOp)> + 'a,
    inner_stream: impl StateStoreReadIterStream,
    table_id: TableId,
    epoch: u64,
) {
    let inner_stream = inner_stream.peekable();
    pin_mut!(inner_stream);

    let mut mem_table_iter = mem_table_iter.fuse().peekable();

    loop {
        match (inner_stream.as_mut().peek().await, mem_table_iter.peek()) {
            (None, None) => break,
            // The mem table side has come to an end, return data from the shared storage.
            (Some(_), None) => {
                let (key, value) = inner_stream.next().await.unwrap()?;
                yield (key, value)
            }
            // The stream side has come to an end, return data from the mem table.
            (None, Some(_)) => {
                let (key, key_op) = mem_table_iter.next().unwrap();
                match key_op {
                    KeyOp::Insert(value) | KeyOp::Update((_, value)) => {
                        yield (FullKey::new(table_id, key.clone(), epoch), value.clone())
                    }
                    _ => {}
                }
            }
            (Some(Ok((inner_key, _))), Some((mem_table_key, _))) => {
                debug_assert_eq!(inner_key.user_key.table_id, table_id);
                match inner_key.user_key.table_key.cmp(mem_table_key) {
                    Ordering::Less => {
                        // yield data from storage
                        let (key, value) = inner_stream.next().await.unwrap()?;
                        yield (key, value);
                    }
                    Ordering::Equal => {
                        // both memtable and storage contain the key, so we advance both
                        // iterators and return the data in memory.

                        let (_, key_op) = mem_table_iter.next().unwrap();
                        let (key, old_value_in_inner) = inner_stream.next().await.unwrap()?;
                        match key_op {
                            KeyOp::Insert(value) => {
                                yield (key.clone(), value.clone());
                            }
                            KeyOp::Delete(_) => {}
                            KeyOp::Update((old_value, new_value)) => {
                                debug_assert!(old_value == &old_value_in_inner);

                                yield (key, new_value.clone());
                            }
                        }
                    }
                    Ordering::Greater => {
                        // yield data from mem table
                        let (key, key_op) = mem_table_iter.next().unwrap();

                        match key_op {
                            KeyOp::Insert(value) => {
                                yield (FullKey::new(table_id, key.clone(), epoch), value.clone());
                            }
                            KeyOp::Delete(_) => {}
                            KeyOp::Update(_) => unreachable!(
                                "memtable update should always be paired with a storage key"
                            ),
                        }
                    }
                }
            }
            (Some(Err(_)), Some(_)) => {
                // Throw the error.
                return Err(inner_stream.next().await.unwrap().unwrap_err());
            }
        }
    }
}

pub struct MemtableLocalStateStore<S: StateStoreWrite + StateStoreRead> {
    mem_table: MemTable,
    inner: S,

    epoch: Option<u64>,

    table_id: TableId,
    is_consistent_op: bool,
    table_option: TableOption,
}

impl<S: StateStoreWrite + StateStoreRead> MemtableLocalStateStore<S> {
    pub fn new(inner: S, option: NewLocalOptions) -> Self {
        Self {
            inner,
            mem_table: MemTable::new(option.is_consistent_op),
            epoch: None,
            table_id: option.table_id,
            is_consistent_op: option.is_consistent_op,
            table_option: option.table_option,
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: StateStoreWrite + StateStoreRead> LocalStateStore for MemtableLocalStateStore<S> {
    type IterStream<'a> = impl StateStoreIterItemStream + 'a;

    #[allow(clippy::unused_async)]
    async fn may_exist(
        &self,
        _key_range: TableKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<bool> {
        Ok(true)
    }

    async fn get(
        &self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        match self.mem_table.buffer.get(&key) {
            None => self.inner.get(key, self.epoch(), read_options).await,
            Some(op) => match op {
                KeyOp::Insert(value) | KeyOp::Update((_, value)) => Ok(Some(value.clone())),
                KeyOp::Delete(_) => Ok(None),
            },
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::IterStream<'_>>> + Send + '_ {
        async move {
            let stream = self
                .inner
                .iter(key_range.clone(), self.epoch(), read_options)
                .await?;
            Ok(merge_stream(
                self.mem_table.iter(key_range),
                stream,
                self.table_id,
                self.epoch(),
            ))
        }
    }

    fn insert(
        &mut self,
        key: TableKey<Bytes>,
        new_val: Bytes,
        old_val: Option<Bytes>,
    ) -> StorageResult<()> {
        match old_val {
            None => self.mem_table.insert(key, new_val)?,
            Some(old_val) => self.mem_table.update(key, old_val, new_val)?,
        };
        Ok(())
    }

    fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
        Ok(self.mem_table.delete(key, old_val)?)
    }

    async fn flush(
        &mut self,
        delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
    ) -> StorageResult<usize> {
        debug_assert!(delete_ranges
            .iter()
            .map(|(key, _)| key)
            .is_sorted_by(|a, b| Some(cmp_delete_range_left_bounds(a.as_ref(), b.as_ref()))));
        let buffer = self.mem_table.drain().into_parts();
        let mut kv_pairs = Vec::with_capacity(buffer.len());
        for (key, key_op) in filter_with_delete_range(buffer.into_iter(), delete_ranges.iter()) {
            match key_op {
                // Currently, some executors do not strictly comply with these semantics. As
                // a workaround you may call disable the check by initializing the
                // state store with `is_consistent_op=false`.
                KeyOp::Insert(value) => {
                    if ENABLE_SANITY_CHECK && self.is_consistent_op {
                        do_insert_sanity_check(
                            key.clone(),
                            value.clone(),
                            &self.inner,
                            self.epoch(),
                            self.table_id,
                            self.table_option,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_put(value)));
                }
                KeyOp::Delete(old_value) => {
                    if ENABLE_SANITY_CHECK && self.is_consistent_op {
                        do_delete_sanity_check(
                            key.clone(),
                            old_value,
                            &self.inner,
                            self.epoch(),
                            self.table_id,
                            self.table_option,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_delete()));
                }
                KeyOp::Update((old_value, new_value)) => {
                    if ENABLE_SANITY_CHECK && self.is_consistent_op {
                        do_update_sanity_check(
                            key.clone(),
                            old_value,
                            new_value.clone(),
                            &self.inner,
                            self.epoch(),
                            self.table_id,
                            self.table_option,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_put(new_value)));
                }
            }
        }
        self.inner
            .ingest_batch(
                kv_pairs,
                delete_ranges,
                WriteOptions {
                    epoch: self.epoch(),
                    table_id: self.table_id,
                },
            )
            .await
    }

    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch")
    }

    fn is_dirty(&self) -> bool {
        self.mem_table.is_dirty()
    }

    #[allow(clippy::unused_async)]
    async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
        assert!(
            self.epoch.replace(options.epoch.curr).is_none(),
            "local state store of table id {:?} is init for more than once",
            self.table_id
        );
        Ok(())
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, _opts: SealCurrentEpochOptions) {
        assert!(!self.is_dirty());
        let prev_epoch = self
            .epoch
            .replace(next_epoch)
            .expect("should have init epoch before seal the first epoch");
        assert!(
            next_epoch > prev_epoch,
            "new epoch {} should be greater than current epoch: {}",
            next_epoch,
            prev_epoch
        );
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rand::{thread_rng, Rng};
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};
    use risingwave_hummock_sdk::EpochWithGap;

    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::value::HummockValue;
    use crate::mem_table::{KeyOp, MemTable, MemTableHummockIterator};

    #[tokio::test]
    async fn test_mem_table_memory_size() {
        let mut mem_table = MemTable::new(true);
        assert_eq!(mem_table.kv_size.size(), 0);

        mem_table
            .insert(TableKey("key1".into()), "value1".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key1").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value1").len()
        );

        // delete
        mem_table.drain();
        assert_eq!(mem_table.kv_size.size(), 0);
        mem_table
            .delete(TableKey("key2".into()), "value2".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key2").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value2").len()
        );
        mem_table
            .insert(TableKey("key2".into()), "value22".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key2").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value22").len()
                + Bytes::from("value2").len()
        );

        mem_table
            .delete(TableKey("key2".into()), "value22".into())
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key2").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value2").len()
        );

        // update
        mem_table.drain();
        assert_eq!(mem_table.kv_size.size(), 0);
        mem_table
            .insert(TableKey("key3".into()), "value3".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key3").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value3").len()
        );

        // update-> insert
        mem_table
            .update(TableKey("key3".into()), "value3".into(), "value333".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key3").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value333").len()
        );

        mem_table.drain();
        mem_table
            .update(TableKey("key4".into()), "value4".into(), "value44".into())
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key4").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value4").len()
                + Bytes::from("value44").len()
        );
        mem_table
            .update(
                TableKey("key4".into()),
                "value44".into(),
                "value4444".into(),
            )
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key4").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value4").len()
                + Bytes::from("value4444").len()
        );
    }

    #[tokio::test]
    async fn test_mem_table_memory_size_not_consistent_op() {
        let mut mem_table = MemTable::new(false);
        assert_eq!(mem_table.kv_size.size(), 0);

        mem_table
            .insert(TableKey("key1".into()), "value1".into())
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key1").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value1").len()
        );

        mem_table
            .insert(TableKey("key1".into()), "value111".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key1").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value111").len()
        );
        mem_table.drain();

        mem_table
            .update(TableKey("key4".into()), "value4".into(), "value44".into())
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key4").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value4").len()
                + Bytes::from("value44").len()
        );
        mem_table
            .update(
                TableKey("key4".into()),
                "value44".into(),
                "value4444".into(),
            )
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key4").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value44").len()
                + Bytes::from("value4444").len()
        );
    }

    #[tokio::test]
    async fn test_mem_table_hummock_iterator() {
        let mut rng = thread_rng();

        fn get_key(i: usize) -> TableKey<Bytes> {
            let mut bytes = BytesMut::new();
            bytes.put(&VirtualNode::ZERO.to_be_bytes()[..]);
            bytes.put(format!("key_{:20}", i).as_bytes());
            TableKey(bytes.freeze())
        }

        let ordered_test_data = (0..10000)
            .map(|i| {
                let key_op = match rng.gen::<usize>() % 3 {
                    0 => KeyOp::Insert(Bytes::from("insert")),
                    1 => KeyOp::Delete(Bytes::from("delete")),
                    2 => KeyOp::Update((Bytes::from("old_value"), Bytes::from("new_value"))),
                    _ => unreachable!(),
                };
                (get_key(i), key_op)
            })
            .collect_vec();

        let mut test_data = ordered_test_data.clone();

        test_data.shuffle(&mut rng);
        let mut mem_table = MemTable::new(true);
        for (key, op) in test_data {
            match op {
                KeyOp::Insert(value) => {
                    mem_table.insert(key, value).unwrap();
                }
                KeyOp::Delete(value) => mem_table.delete(key, value).unwrap(),
                KeyOp::Update((old_value, new_value)) => {
                    mem_table.update(key, old_value, new_value).unwrap();
                }
            }
        }

        const TEST_TABLE_ID: TableId = TableId::new(233);
        const TEST_EPOCH: u64 = 10;

        async fn check_data(
            iter: &mut MemTableHummockIterator<'_>,
            test_data: &[(TableKey<Bytes>, KeyOp)],
        ) {
            let mut idx = 0;
            while iter.is_valid() {
                let key = iter.key();
                let value = iter.value();

                let (expected_key, expected_value) = test_data[idx].clone();
                assert_eq!(key.epoch_with_gap, EpochWithGap::new_from_epoch(TEST_EPOCH));
                assert_eq!(key.user_key.table_id, TEST_TABLE_ID);
                assert_eq!(key.user_key.table_key.0, expected_key.0.as_ref());
                match expected_value {
                    KeyOp::Insert(expected_value) | KeyOp::Update((_, expected_value)) => {
                        assert_eq!(value, HummockValue::Put(expected_value.as_ref()));
                    }
                    KeyOp::Delete(_) => {
                        assert_eq!(value, HummockValue::Delete);
                    }
                }

                idx += 1;
                iter.next().await.unwrap();
            }
            assert_eq!(idx, test_data.len());
        }

        let mut iter = MemTableHummockIterator::new(
            &mem_table.buffer,
            EpochWithGap::new_from_epoch(TEST_EPOCH),
            TEST_TABLE_ID,
            None,
        );

        // Test rewind
        iter.rewind().await.unwrap();
        check_data(&mut iter, &ordered_test_data).await;

        // Test seek with a later epoch, the first key is not skipped
        let later_epoch = EpochWithGap::new_from_epoch(TEST_EPOCH + 1);
        let seek_idx = 500;
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(seek_idx)),
            },
            epoch_with_gap: later_epoch,
        })
        .await
        .unwrap();
        check_data(&mut iter, &ordered_test_data[seek_idx..]).await;

        // Test seek with a earlier epoch, the first key is skipped
        let early_epoch = EpochWithGap::new_from_epoch(TEST_EPOCH - 1);
        let seek_idx = 500;
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(seek_idx)),
            },
            epoch_with_gap: early_epoch,
        })
        .await
        .unwrap();
        check_data(&mut iter, &ordered_test_data[seek_idx + 1..]).await;

        // Test seek to over the end
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(ordered_test_data.len() + 10)),
            },
            epoch_with_gap: EpochWithGap::new_from_epoch(TEST_EPOCH),
        })
        .await
        .unwrap();
        check_data(&mut iter, &[]).await;

        // Test seek with a smaller table id
        let smaller_table_id = TableId::new(TEST_TABLE_ID.table_id() - 1);
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: smaller_table_id,
                table_key: TableKey(&get_key(ordered_test_data.len() + 10)),
            },
            epoch_with_gap: EpochWithGap::new_from_epoch(TEST_EPOCH),
        })
        .await
        .unwrap();
        check_data(&mut iter, &ordered_test_data).await;

        // Test seek with a greater table id
        let greater_table_id = TableId::new(TEST_TABLE_ID.table_id() + 1);
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: greater_table_id,
                table_key: TableKey(&get_key(0)),
            },
            epoch_with_gap: EpochWithGap::new_from_epoch(TEST_EPOCH),
        })
        .await
        .unwrap();
        check_data(&mut iter, &[]).await;
    }
}
