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

use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::RowRef;
use risingwave_common::catalog::{ColumnDesc, Schema};
use risingwave_common::estimate_size::VecWithKvSize;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use super::sides::{stream_lookup_arrange_prev_epoch, stream_lookup_arrange_this_epoch};
use crate::cache::cache_may_stale;
use crate::common::metrics::MetricsInfo;
use crate::common::JoinStreamChunkBuilder;
use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::lookup::cache::LookupCache;
use crate::executor::lookup::sides::{ArrangeJoinSide, ArrangeMessage, StreamJoinSide};
use crate::executor::lookup::LookupExecutor;
use crate::executor::{ActorContextRef, Barrier, Executor, ExecutorInfo, Message};
use crate::task::AtomicU64Ref;

/// Parameters for [`LookupExecutor`].
pub struct LookupExecutorParams<S: StateStore> {
    pub ctx: ActorContextRef,
    pub info: ExecutorInfo,

    /// The side for arrangement. Currently, it should be a
    /// `MaterializeExecutor`.
    pub arrangement: Box<dyn Executor>,

    /// The side for stream. It can be any stream, but it will generally be a
    /// `MaterializeExecutor`.
    pub stream: Box<dyn Executor>,

    /// Should be the same as [`ColumnDesc`] in the arrangement.
    ///
    /// From the perspective of arrangements, `arrangement_col_descs` include all columns of the
    /// `MaterializeExecutor`. For example, if we already have a table with 3 columns: `a, b,
    /// _row_id`, and we create an arrangement with join key `a` on it. `arrangement_col_descs`
    /// should contain all 3 columns.
    pub arrangement_col_descs: Vec<ColumnDesc>,

    /// Should only contain [`ColumnOrder`] for arrange in the arrangement.
    ///
    /// Still using the above `a, b, _row_id` example. If we create an arrangement with join key
    /// `a`, there will be 3 elements in `arrangement_col_descs`, and only 1 element in
    /// `arrangement_order_rules`.
    ///
    /// * The only element is the order rule for `a`, which is the join key. Join keys should
    ///   always come first.
    ///
    /// For the MV pk, they will only be contained in `arrangement_col_descs`, without being part
    /// of this `arrangement_order_rules`.
    pub arrangement_order_rules: Vec<ColumnOrder>,

    /// By default, the output of [`LookupExecutor`] is `stream columns + arrangement columns`.
    /// The executor will do a reorder of columns before producing output, so that data can be
    /// consistent among A lookup B and B lookup A.
    pub column_mapping: Vec<usize>,

    /// Whether to use the current epoch of the arrangement to lookup.
    ///
    /// [`LookupExecutor`] will act differently on whether `use_current_epoch` is set to true. In a
    /// nutshell, lookup in the previous epoch (this option set to false) will be more efficient.
    /// Therefore, the optimizer should choose an optimal order for lookup executors.
    ///
    /// See [`stream_lookup_arrange_this_epoch`] and [`stream_lookup_arrange_prev_epoch`] for more
    /// information.
    pub use_current_epoch: bool,

    /// The join keys on the stream side.
    pub stream_join_key_indices: Vec<usize>,

    /// The join keys on the arrangement side.
    pub arrange_join_key_indices: Vec<usize>,

    pub storage_table: StorageTable<S>,

    pub watermark_epoch: AtomicU64Ref,

    pub chunk_size: usize,
}

impl<S: StateStore> LookupExecutor<S> {
    pub fn new(params: LookupExecutorParams<S>) -> Self {
        let LookupExecutorParams {
            ctx,
            info,
            arrangement,
            stream,
            arrangement_col_descs,
            arrangement_order_rules,
            use_current_epoch,
            stream_join_key_indices,
            arrange_join_key_indices,
            column_mapping,
            storage_table,
            watermark_epoch,
            chunk_size,
        } = params;

        let output_column_length = stream.schema().len() + arrangement.schema().len();

        // internal output schema: | stream | arrange |
        // will be rewritten using `column_mapping`.

        let schema_fields = stream
            .schema()
            .fields
            .iter()
            .chain(arrangement.schema().fields.iter())
            .cloned()
            .collect_vec();

        assert_eq!(schema_fields.len(), output_column_length);

        let schema = Schema::new(schema_fields);

        let chunk_data_types = schema.data_types();
        let arrangement_data_types = arrangement.schema().data_types();
        let stream_data_types = stream.schema().data_types();

        let arrangement_pk_indices = arrangement.pk_indices().to_vec();
        let stream_pk_indices = stream.pk_indices().to_vec();

        // check if arrange join key is exactly the same as order rules
        {
            let mut arrange_join_key_indices = arrange_join_key_indices.clone();
            arrange_join_key_indices.sort_unstable();
            let mut arrangement_order_types_indices = arrangement_order_rules
                .iter()
                .map(|x| x.column_index)
                .collect_vec();
            arrangement_order_types_indices.sort_unstable();
            assert_eq!(
                arrange_join_key_indices,
                &arrangement_order_types_indices[0..arrange_join_key_indices.len()],
                "invalid join key: arrange_join_key_indices = {:?}, order_rules: {:?}",
                arrange_join_key_indices,
                arrangement_order_rules
            );
        }

        // check whether join keys are of the same length.
        assert_eq!(
            stream_join_key_indices.len(),
            arrange_join_key_indices.len()
        );

        // resolve mapping from join keys in stream row -> joins keys for arrangement.
        let key_indices_mapping = arrangement_order_rules
            .iter()
            .map(|x| x.column_index) // the required column idx in this position
            .filter_map(|x| arrange_join_key_indices.iter().position(|y| *y == x)) // the position of the item in join keys
            .map(|x| stream_join_key_indices[x]) // the actual column idx in stream
            .collect_vec();

        // check the inferred schema is really the same as the output schema of the lookup executor.
        assert_eq!(
            info.schema
                .fields
                .iter()
                .map(|x| x.data_type())
                .collect_vec(),
            column_mapping
                .iter()
                .map(|x| schema.fields[*x].data_type())
                .collect_vec(),
            "mismatched output schema"
        );

        let metrics_info = MetricsInfo::new(
            ctx.streaming_metrics.clone(),
            storage_table.table_id().table_id(),
            ctx.id,
            "Lookup",
        );

        Self {
            ctx,
            info,
            chunk_data_types,
            last_barrier: None,
            stream_executor: Some(stream),
            arrangement_executor: Some(arrangement),
            stream: StreamJoinSide {
                key_indices: stream_join_key_indices,
                pk_indices: stream_pk_indices,
                col_types: stream_data_types,
            },
            arrangement: ArrangeJoinSide {
                pk_indices: arrangement_pk_indices,
                col_types: arrangement_data_types,
                col_descs: arrangement_col_descs,
                order_rules: arrangement_order_rules,
                key_indices: arrange_join_key_indices,
                use_current_epoch,
                storage_table,
            },
            column_mapping,
            key_indices_mapping,
            lookup_cache: LookupCache::new(watermark_epoch, metrics_info),
            chunk_size,
        }
    }

    /// Try produce one stream message from [`LookupExecutor`]. If there's no message to produce, it
    /// will return `None`, and the `next` function of [`LookupExecutor`] will continuously polling
    /// messages until there's one.
    ///
    /// If we can use `async_stream` to write this part, things could be easier.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn execute_inner(mut self: Box<Self>) {
        let input = if self.arrangement.use_current_epoch {
            stream_lookup_arrange_this_epoch(
                // 这个两个是上游数据
                self.stream_executor.take().unwrap(),
                self.arrangement_executor.take().unwrap(),
            )
            .boxed()
        } else {
            stream_lookup_arrange_prev_epoch(
                self.stream_executor.take().unwrap(),
                self.arrangement_executor.take().unwrap(),
            )
            .boxed()
        };

        // 问题根源在这里，这个拿到的 arrange_to_output 有问题，需要invest
        let (stream_to_output, arrange_to_output) = JoinStreamChunkBuilder::get_i2o_mapping(
            &self.column_mapping,
            self.stream.col_types.len(),
            self.arrangement.col_types.len(),
        );

        let reorder_chunk_data_types = self
            .column_mapping
            .iter()
            .map(|x| self.chunk_data_types[*x].clone())
            .collect_vec();

        #[for_await]
        // 上游两个stream变成chunk，看一看chunk
        for msg in input {
            let msg = msg?;
            self.lookup_cache.evict();
            match msg {
                ArrangeMessage::Barrier(barrier) => {
                    self.process_barrier(&barrier);

                    if self.arrangement.use_current_epoch {
                        // When lookup this epoch, stream side barrier always come after arrangement
                        // ready, so we can forward barrier now.
                        yield Message::Barrier(barrier);
                    }
                }
                ArrangeMessage::ArrangeReady(arrangement_chunks, barrier) => {
                    // The arrangement is ready, and we will receive a bunch of stream messages for
                    // the next poll.
                    // TODO: apply chunk as soon as we receive them, instead of batching.

                    for chunk in arrangement_chunks {
                        self.lookup_cache
                            .apply_batch(chunk, &self.arrangement.key_indices)
                    }

                    if !self.arrangement.use_current_epoch {
                        // When look prev epoch, arrange ready will always come after stream
                        // barrier. So we yield barrier now.
                        yield Message::Barrier(barrier);
                    }
                }
                ArrangeMessage::Stream(chunk) => {
                    let chunk = chunk.compact();
                    let (chunk, ops) = chunk.into_parts();
                    
                    // 怀疑这个builder有问题，这个里面的 arrange_to_output 有问题
                    let mut builder = JoinStreamChunkBuilder::new(
                        self.chunk_size,
                        //print 判断对应的schema
                        reorder_chunk_data_types.clone(),
                        stream_to_output.clone(),
                        arrange_to_output.clone(),
                    );

                    for (op, row) in ops.iter().zip_eq_debug(chunk.rows()) {
                        for matched_row in self
                            .lookup_one_row(&row, self.last_barrier.as_ref().unwrap().epoch)
                            .await?
                        {
                            tracing::info!(target: "events::stream::lookup::put", "{:?} {:?}", row, matched_row);
                            tracing::info!("Yufan row: {:?}, matched_row {:?}", row, matched_row);
                            // 这里面穿进去的row是有问题的,导致出现了int16的数据，理论上不应该
                            // row: [Some(Decimal(Normalized(1))), Some(Int32(4)), Some(Serial(Serial(362824135644807168)))]
                            // matched_row OwnedRow([Some(Decimal(Normalized(1))), Some(Int16(2)), Some(Int32(4)), Some(Serial(Serial(362824135644807168)))])
                            // match row里面的数据出问题了，这里面有int16的数据，然后在下面映射出错误了
                            if let Some(chunk) = builder.append_row(*op, row, &matched_row) {
                                yield Message::Chunk(chunk);
                            }
                        }
                        // TODO: support outer join (return null if no rows are matched)
                    }

                    if let Some(chunk) = builder.take() {
                        yield Message::Chunk(chunk);
                    }
                }
            }
        }
    }

    /// Process the barrier and apply changes if necessary.
    fn process_barrier(&mut self, barrier: &Barrier) {
        if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
            let previous_vnode_bitmap = self
                .arrangement
                .storage_table
                .update_vnode_bitmap(vnode_bitmap.clone());

            // Manipulate the cache if necessary.
            if cache_may_stale(&previous_vnode_bitmap, &vnode_bitmap) {
                self.lookup_cache.clear();
            }
        }

        // Use the new stream barrier epoch as new cache epoch
        self.lookup_cache.update_epoch(barrier.epoch.curr);
        self.last_barrier = Some(barrier.clone());
    }

    /// Lookup all rows corresponding to a join key in shared buffer.
    async fn lookup_one_row(
        &mut self,
        stream_row: &RowRef<'_>,
        epoch_pair: EpochPair,
    ) -> StreamExecutorResult<Vec<OwnedRow>> {
        // stream_row is the row from stream side, we need to transform into the correct order of
        // the arrangement side.
        let lookup_row = stream_row
            .project(&self.key_indices_mapping)
            .into_owned_row();
                let table_id_str = self.arrangement.storage_table.table_id().to_string();
        let actor_id_str = self.ctx.id.to_string();
        let fragment_id_str = self.ctx.fragment_id.to_string();
        self.ctx
            .streaming_metrics
            .lookup_total_query_cache_count
            .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
            .inc();
        if let Some(result) = self.lookup_cache.lookup(&lookup_row) {
            return Ok(result.iter().cloned().collect_vec());
        }

        // cache miss
        self.ctx
            .streaming_metrics
            .lookup_cache_miss_count
            .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
            .inc();

        tracing::debug!(target: "events::stream::lookup::lookup_row", "{:?}", lookup_row);

        let mut all_rows = VecWithKvSize::new();
        // Drop the stream.
        {
            let all_data_iter = match self.arrangement.use_current_epoch {
                true => {
                    self.arrangement
                        .storage_table
                        .batch_iter_with_pk_bounds(
                            HummockReadEpoch::NoWait(epoch_pair.curr),
                            &lookup_row,
                            ..,
                            false,
                            PrefetchOptions::default(),
                        )
                        .await?
                }
                false => {
                    self.arrangement
                        .storage_table
                        .batch_iter_with_pk_bounds(
                            HummockReadEpoch::NoWait(epoch_pair.prev),
                            &lookup_row,
                            ..,
                            false,
                            PrefetchOptions::default(),
                        )
                        .await?
                }
            };

            pin_mut!(all_data_iter);
            while let Some(row) = all_data_iter.next_row().await? {
                // Only need value (include storage pk).
                                all_rows.push(row);
            }
        }

        tracing::debug!(target: "events::stream::lookup::result", "{:?} => {:?}", lookup_row, all_rows.inner());

        self.lookup_cache.batch_update(lookup_row, all_rows.clone());

        self.ctx
            .streaming_metrics
            .lookup_cached_entry_count
            .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
            .set(self.lookup_cache.len() as i64);

        Ok(all_rows.into_inner())
    }
}
