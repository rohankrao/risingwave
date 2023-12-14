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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::{ArrayRef, Op};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::row::{self, CompactedRow, OwnedRow, Row, RowExt};
use risingwave_common::types::{ScalarImpl, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_storage::StateStore;

use super::{AggCall, GroupKey};
use crate::cache::{new_unbounded, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTable;
use crate::executor::{ActorContextRef, StreamExecutorResult};

type DedupCache = ManagedLruCache<CompactedRow, Box<[i64]>>;

/// Deduplicater for one distinct column.
struct ColumnDeduplicater<S: StateStore> {
    cache: DedupCache,
    _phantom: PhantomData<S>,
}

impl<S: StateStore> ColumnDeduplicater<S> {
    fn new(watermark_epoch: Arc<AtomicU64>, metrics_info: MetricsInfo) -> Self {
        Self {
            cache: new_unbounded(watermark_epoch, metrics_info),
            _phantom: PhantomData,
        }
    }

    async fn dedup(
        &mut self,
        ops: &[Op],
        column: &ArrayRef,
        mut visibilities: Vec<&mut Bitmap>,
        dedup_table: &mut StateTable<S>,
        group_key: Option<&GroupKey>,
        ctx: ActorContextRef,
    ) -> StreamExecutorResult<()> {
        let n_calls = visibilities.len();

        let mut prev_counts_map = HashMap::new(); // also serves as changeset

        // inverted masks for visibilities, 1 means hidden, 0 means visible
        let mut vis_masks_inv = (0..visibilities.len())
            .map(|_| BitmapBuilder::zeroed(column.len()))
            .collect_vec();
        let actor_id_str = ctx.id.to_string();
        let fragment_id_str = ctx.fragment_id.to_string();
        let table_id_str = dedup_table.table_id().to_string();
        for (datum_idx, (op, datum)) in ops.iter().zip_eq_fast(column.iter()).enumerate() {
            // skip if this item is hidden to all agg calls (this is likely to happen)
            if !visibilities.iter().any(|vis| vis.is_set(datum_idx)) {
                continue;
            }

            // get counts of the distinct key of all agg calls that distinct on this column
            let row_prefix = group_key.map(GroupKey::table_row).chain(row::once(datum));
            let table_pk = group_key.map(GroupKey::table_pk).chain(row::once(datum));
            let cache_key =
                CompactedRow::from(group_key.map(GroupKey::cache_key).chain(row::once(datum)));

            ctx.streaming_metrics
                .agg_distinct_total_cache_count
                .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                .inc();
            // TODO(yuhao): avoid this `contains`.
            // https://github.com/risingwavelabs/risingwave/issues/9233
            let mut counts = if self.cache.contains(&cache_key) {
                self.cache.get_mut(&cache_key).unwrap()
            } else {
                ctx.streaming_metrics
                    .agg_distinct_cache_miss_count
                    .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                    .inc();
                // load from table into the cache
                let counts = if let Some(counts_row) =
                    dedup_table.get_row(&table_pk).await? as Option<OwnedRow>
                {
                    counts_row
                        .iter()
                        .map(|v| v.map_or(0, ScalarRefImpl::into_int64))
                        .collect()
                } else {
                    // ensure there is a row in the dedup table for this distinct key
                    dedup_table.insert(
                        (&row_prefix).chain(row::repeat_n(Some(ScalarImpl::from(0i64)), n_calls)),
                    );
                    vec![0; n_calls].into_boxed_slice()
                };
                self.cache.put(cache_key.clone(), counts); // TODO(rc): can we avoid this clone?

                self.cache.get_mut(&cache_key).unwrap()
            };
            debug_assert_eq!(counts.len(), visibilities.len());

            // snapshot the counts as prev counts when first time seeing this distinct key
            prev_counts_map
                .entry(datum)
                .or_insert_with(|| counts.to_owned());

            match op {
                Op::Insert | Op::UpdateInsert => {
                    // iterate over vis of each distinct agg call, count up for visible datum
                    for (i, vis) in visibilities.iter().enumerate() {
                        if vis.is_set(datum_idx) {
                            counts[i] += 1;
                            if counts[i] > 1 {
                                // duplicate, hide this one
                                vis_masks_inv[i].set(datum_idx, true);
                            }
                        }
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    // iterate over vis of each distinct agg call, count down for visible datum
                    for (i, vis) in visibilities.iter().enumerate() {
                        if vis.is_set(datum_idx) {
                            counts[i] -= 1;
                            debug_assert!(counts[i] >= 0);
                            if counts[i] > 0 {
                                // still exists at least one duplicate, hide this one
                                vis_masks_inv[i].set(datum_idx, true);
                            }
                        }
                    }
                }
            }
        }

        // flush changes to dedup table
        prev_counts_map
            .into_iter()
            .for_each(|(datum, prev_counts)| {
                let row_prefix = group_key.map(GroupKey::table_row).chain(row::once(datum));
                let cache_key =
                    CompactedRow::from(group_key.map(GroupKey::cache_key).chain(row::once(datum)));
                let new_counts = OwnedRow::new(
                    self.cache
                        .get(&cache_key)
                        .expect("distinct key in `prev_counts_map` must also exist in `self.cache`")
                        .iter()
                        .map(|&v| Some(v.into()))
                        .collect(),
                );
                let old_counts =
                    OwnedRow::new(prev_counts.iter().map(|&v| Some(v.into())).collect());
                dedup_table.update(row_prefix.chain(old_counts), row_prefix.chain(new_counts));
            });

        for (vis, vis_mask_inv) in visibilities.iter_mut().zip_eq(vis_masks_inv.into_iter()) {
            // update visibility
            **vis &= !vis_mask_inv.finish();
        }

        // if we determine to flush to the table when processing every chunk instead of barrier
        // coming, we can evict all including current epoch data.
        self.cache.evict();

        Ok(())
    }

    /// Flush the deduplication table.
    fn flush(&mut self, dedup_table: &StateTable<S>, ctx: ActorContextRef) {
        // TODO(rc): now we flush the table in `dedup` method.
        // WARN: if you want to change to batching the write to table. please remember to change
        // `self.cache.evict()` too.
        self.cache.evict();

        let actor_id_str = ctx.id.to_string();
        let fragment_id_str = ctx.fragment_id.to_string();
        let table_id_str = dedup_table.table_id().to_string();
        ctx.streaming_metrics
            .agg_distinct_cached_entry_count
            .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
            .set(self.cache.len() as i64);
    }
}

/// # Safety
///
/// There must not be duplicate items in `indices`.
unsafe fn get_many_mut_from_slice<'a, T>(slice: &'a mut [T], indices: &[usize]) -> Vec<&'a mut T> {
    let mut res = Vec::with_capacity(indices.len());
    let ptr = slice.as_mut_ptr();
    for &idx in indices {
        res.push(&mut *ptr.add(idx));
    }
    res
}

pub struct DistinctDeduplicater<S: StateStore> {
    /// Key: distinct column index;
    /// Value: (agg call indices that distinct on the column, deduplicater for the column).
    deduplicaters: HashMap<usize, (Box<[usize]>, ColumnDeduplicater<S>)>,
    ctx: ActorContextRef,
}

impl<S: StateStore> DistinctDeduplicater<S> {
    pub fn new(
        agg_calls: &[AggCall],
        watermark_epoch: Arc<AtomicU64>,
        distinct_dedup_tables: &HashMap<usize, StateTable<S>>,
        ctx: ActorContextRef,
    ) -> Self {
        let actor_id = ctx.id;
        let deduplicaters: HashMap<_, _> = agg_calls
            .iter()
            .enumerate()
            .filter(|(_, call)| call.distinct) // only distinct agg calls need dedup table
            .into_group_map_by(|(_, call)| call.args.val_indices()[0])
            .into_iter()
            .map(|(distinct_col, indices_and_calls)| {
                let table_id = distinct_dedup_tables.get(&distinct_col).unwrap().table_id();
                let metrics_info = MetricsInfo::new(
                    ctx.streaming_metrics.clone(),
                    table_id,
                    actor_id,
                    "distinct dedup",
                );
                let call_indices: Box<[_]> = indices_and_calls.into_iter().map(|v| v.0).collect();
                let deduplicater = ColumnDeduplicater::new(watermark_epoch.clone(), metrics_info);
                (distinct_col, (call_indices, deduplicater))
            })
            .collect();
        Self { deduplicaters, ctx }
    }

    pub fn dedup_caches_mut(&mut self) -> impl Iterator<Item = &mut DedupCache> {
        self.deduplicaters
            .values_mut()
            .map(|(_, deduplicater)| &mut deduplicater.cache)
    }

    /// Deduplicate the chunk for each agg call, by returning new visibilities
    /// that hide duplicate rows.
    pub async fn dedup_chunk(
        &mut self,
        ops: &[Op],
        columns: &[ArrayRef],
        mut visibilities: Vec<Bitmap>,
        dedup_tables: &mut HashMap<usize, StateTable<S>>,
        group_key: Option<&GroupKey>,
    ) -> StreamExecutorResult<Vec<Bitmap>> {
        for (distinct_col, (ref call_indices, deduplicater)) in &mut self.deduplicaters {
            let column = &columns[*distinct_col];
            let dedup_table = dedup_tables.get_mut(distinct_col).unwrap();
            // Select visibilities (as mutable references) of distinct agg calls that distinct on
            // `distinct_col` so that `Deduplicater` doesn't need to care about index mapping.
            // SAFETY: all items in `agg_call_indices` are unique by nature, see `new`.
            let visibilities = unsafe { get_many_mut_from_slice(&mut visibilities, call_indices) };
            deduplicater
                .dedup(
                    ops,
                    column,
                    visibilities,
                    dedup_table,
                    group_key,
                    self.ctx.clone(),
                )
                .await?;
        }
        Ok(visibilities)
    }

    /// Flush dedup state caches to dedup tables.
    pub fn flush(
        &mut self,
        dedup_tables: &mut HashMap<usize, StateTable<S>>,
    ) -> StreamExecutorResult<()> {
        for (distinct_col, (_, deduplicater)) in &mut self.deduplicaters {
            let dedup_table = dedup_tables.get_mut(distinct_col).unwrap();
            deduplicater.flush(dedup_table, self.ctx.clone());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::ActorContext;

    async fn infer_dedup_tables<S: StateStore>(
        agg_calls: &[AggCall],
        group_key_types: &[DataType],
        store: S,
    ) -> HashMap<usize, StateTable<S>> {
        // corresponding to `Agg::infer_distinct_dedup_table` in frontend
        let mut dedup_tables = HashMap::new();

        for (distinct_col, indices_and_calls) in agg_calls
            .iter()
            .enumerate()
            .filter(|(_, call)| call.distinct) // only distinct agg calls need dedup table
            .into_group_map_by(|(_, call)| call.args.val_indices()[0])
        {
            let mut columns = vec![];
            let mut order_types = vec![];

            let mut next_column_id = 0;
            let mut add_column_desc = |data_type: DataType| {
                columns.push(ColumnDesc::unnamed(
                    ColumnId::new(next_column_id),
                    data_type,
                ));
                next_column_id += 1;
            };

            // group key columns
            for data_type in group_key_types {
                add_column_desc(data_type.clone());
                order_types.push(OrderType::ascending());
            }

            // distinct key column
            add_column_desc(indices_and_calls[0].1.args.arg_types()[0].clone());
            order_types.push(OrderType::ascending());

            // count columns
            for (_, _) in indices_and_calls {
                add_column_desc(DataType::Int64);
            }

            let n_columns = columns.len();
            let table = StateTable::new_without_distribution_with_value_indices(
                store.clone(),
                TableId::new(2333 + distinct_col as u32),
                None,
                columns,
                order_types,
                (0..(group_key_types.len() + 1)).collect(),
                ((group_key_types.len() + 1)..n_columns).collect(),
            )
            .await;
            dedup_tables.insert(distinct_col, table);
        }

        dedup_tables
    }

    #[tokio::test]
    async fn test_distinct_deduplicater() {
        // Schema:
        // a: int, b int, c int
        // Agg calls:
        // count(a), count(distinct a), sum(distinct a), count(distinct b)
        // Group keys:
        // empty

        let agg_calls = [
            AggCall::from_pretty("(count:int8 $0:int8)"), // count(a)
            AggCall::from_pretty("(count:int8 $0:int8 distinct)"), // count(distinct a)
            AggCall::from_pretty("(  sum:int8 $0:int8 distinct)"), // sum(distinct a)
            AggCall::from_pretty("(count:int8 $1:int8 distinct)"), // count(distinct b)
        ];

        let store = MemoryStateStore::new();
        let mut epoch = EpochPair::new_test_epoch(1);
        let mut dedup_tables = infer_dedup_tables(&agg_calls, &[], store).await;
        dedup_tables
            .values_mut()
            .for_each(|table| table.init_epoch(epoch));

        let mut deduplicater = DistinctDeduplicater::new(
            &agg_calls,
            Arc::new(AtomicU64::new(0)),
            &dedup_tables,
            ActorContext::create(0),
        );

        // --- chunk 1 ---

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            + 1  10   100
            + 1  11   101",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(&ops, &columns, visibilities, &mut dedup_tables, None)
            .await
            .unwrap();
        assert_eq!(
            visibilities[0].iter().collect_vec(),
            vec![true, true] // same as original chunk
        );
        assert_eq!(
            visibilities[1].iter().collect_vec(),
            vec![true, false] // distinct on a
        );
        assert_eq!(
            visibilities[2].iter().collect_vec(),
            vec![true, false] // distinct on a, same as above
        );
        assert_eq!(
            visibilities[3].iter().collect_vec(),
            vec![true, true] // distinct on b
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }

        // --- chunk 2 ---

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            + 1  11  -102
            + 2  12   103  D
            + 2  12  -104",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(&ops, &columns, visibilities, &mut dedup_tables, None)
            .await
            .unwrap();
        assert_eq!(
            visibilities[0].iter().collect_vec(),
            vec![true, false, true] // same as original chunk
        );
        assert_eq!(
            visibilities[1].iter().collect_vec(),
            vec![false, false, true] // distinct on a
        );
        assert_eq!(
            visibilities[2].iter().collect_vec(),
            vec![false, false, true] // distinct on a, same as above
        );
        assert_eq!(
            visibilities[3].iter().collect_vec(),
            vec![false, false, true] // distinct on b
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }

        drop(deduplicater);

        // test recovery
        let mut deduplicater = DistinctDeduplicater::new(
            &agg_calls,
            Arc::new(AtomicU64::new(0)),
            &dedup_tables,
            ActorContext::create(0),
        );

        // --- chunk 3 ---

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            - 1  10   100  D
            - 1  11   101
            - 1  11  -102",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(&ops, &columns, visibilities, &mut dedup_tables, None)
            .await
            .unwrap();
        assert_eq!(
            visibilities[0].iter().collect_vec(),
            vec![false, true, true] // same as original chunk
        );
        assert_eq!(
            visibilities[1].iter().collect_vec(),
            // distinct on a
            vec![
                false, // hidden in original chunk
                false, // not the last one
                false, // not the last one
            ]
        );
        assert_eq!(
            visibilities[2].iter().collect_vec(),
            // distinct on a, same as above
            vec![
                false, // hidden in original chunk
                false, // not the last one
                false, // not the last one
            ]
        );
        assert_eq!(
            visibilities[3].iter().collect_vec(),
            // distinct on b
            vec![
                false, // hidden in original chunk
                false, // not the last one
                true,  // is the last one
            ]
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_distinct_deduplicater_with_group() {
        // Schema:
        // a: int, b int, c int
        // Agg calls:
        // count(a), count(distinct a), count(distinct b)
        // Group keys:
        // c

        let agg_calls = [
            AggCall::from_pretty("(count:int8 $0:int8)"), // count(a)
            AggCall::from_pretty("(count:int8 $0:int8 distinct)"), // count(distinct a)
            AggCall::from_pretty("(count:int8 $1:int8 distinct)"), // count(distinct b)
        ];

        let group_key_types = [DataType::Int64];
        let group_key = GroupKey::new(OwnedRow::new(vec![Some(100.into())]), None);

        let store = MemoryStateStore::new();
        let mut epoch = EpochPair::new_test_epoch(1);
        let mut dedup_tables = infer_dedup_tables(&agg_calls, &group_key_types, store).await;
        dedup_tables
            .values_mut()
            .for_each(|table| table.init_epoch(epoch));

        let mut deduplicater = DistinctDeduplicater::new(
            &agg_calls,
            Arc::new(AtomicU64::new(0)),
            &dedup_tables,
            ActorContext::create(0),
        );

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            + 1  10   100
            + 1  11   100
            + 1  11   100
            + 2  12   200  D
            + 2  12   100",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(
                &ops,
                &columns,
                visibilities,
                &mut dedup_tables,
                Some(&group_key),
            )
            .await
            .unwrap();
        assert_eq!(
            visibilities[0].iter().collect_vec(),
            vec![true, true, true, false, true] // same as original chunk
        );
        assert_eq!(
            visibilities[1].iter().collect_vec(),
            vec![true, false, false, false, true] // distinct on a
        );
        assert_eq!(
            visibilities[2].iter().collect_vec(),
            vec![true, true, false, false, true] // distinct on b
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            - 1  10   100  D
            - 1  11   100
            - 1  11   100",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(
                &ops,
                &columns,
                visibilities,
                &mut dedup_tables,
                Some(&group_key),
            )
            .await
            .unwrap();
        assert_eq!(
            visibilities[0].iter().collect_vec(),
            vec![false, true, true] // same as original chunk
        );
        assert_eq!(
            visibilities[1].iter().collect_vec(),
            // distinct on a
            vec![
                false, // hidden in original chunk
                false, // not the last one
                false, // not the last one
            ]
        );
        assert_eq!(
            visibilities[2].iter().collect_vec(),
            // distinct on b
            vec![
                false, // hidden in original chunk
                false, // not the last one
                true,  // is the last one
            ]
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }
    }
}
