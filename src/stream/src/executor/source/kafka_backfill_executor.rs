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

use std::assert_matches::assert_matches;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::pin::pin;
use std::time::Duration;

use anyhow::anyhow;
use either::Either;
use futures::stream::{select_with_strategy, AbortHandle, Abortable};
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::row::Row;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_connector::source::{
    BoxSourceWithStateStream, ConnectorState, SourceContext, SourceCtrlOpts, SplitId,
    SplitMetaData, StreamChunkWithState,
};
use risingwave_connector::ConnectorParams;
use risingwave_source::source_desc::{SourceDesc, SourceDescBuilder};
use risingwave_storage::StateStore;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

use super::executor_core::StreamSourceCore;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::executor::*;

type ExecutorSplitId = String;

/// A constant to multiply when calculating the maximum time to wait for a barrier. This is due to
/// some latencies in network and cost in meta.
const WAIT_BARRIER_MULTIPLE_TIMES: u128 = 5;

pub struct KafkaBackfillExecutorWrapper<S: StateStore> {
    inner: KafkaBackfillExecutor<S>,
    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    input: Box<dyn Executor>,
}

pub struct KafkaBackfillExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    // /// Receiver of barrier channel.
    // barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// System parameter reader to read barrier interval
    system_params: SystemParamsReaderRef,

    // control options for connector level
    source_ctrl_opts: SourceCtrlOpts,

    // config for the connector node
    connector_params: ConnectorParams,
}

impl<S: StateStore> KafkaBackfillExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        info: ExecutorInfo,
        stream_source_core: Option<StreamSourceCore<S>>,
        metrics: Arc<StreamingMetrics>,
        // barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        source_ctrl_opts: SourceCtrlOpts,
        connector_params: ConnectorParams,
    ) -> Self {
        Self {
            actor_ctx,
            info,
            stream_source_core,
            metrics,
            // barrier_receiver: Some(barrier_receiver),
            system_params,
            source_ctrl_opts,
            connector_params,
        }
    }

    async fn build_stream_source_reader(
        &self,
        source_desc: &SourceDesc,
        state: ConnectorState,
    ) -> StreamExecutorResult<(
        BoxSourceWithStateStream,
        HashMap<ExecutorSplitId, AbortHandle>,
    )> {
        let column_ids = source_desc
            .columns
            .iter()
            .map(|column_desc| column_desc.column_id)
            .collect_vec();
        let source_ctx = SourceContext::new_with_suppressor(
            self.actor_ctx.id,
            self.stream_source_core.as_ref().unwrap().source_id,
            self.actor_ctx.fragment_id,
            source_desc.metrics.clone(),
            self.source_ctrl_opts.clone(),
            self.connector_params.connector_client.clone(),
            self.actor_ctx.error_suppressor.clone(),
        );
        let source_ctx = Arc::new(source_ctx);
        // Unlike SourceExecutor, which creates a stream_reader with all splits,
        // we create a separate stream_reader for each split here, because we
        // want to abort early for each split after the split's backfilling is finished.
        match state {
            Some(splits) => {
                let mut abort_handles = HashMap::new();
                let mut streams = vec![];
                for split in splits {
                    let split_id = split.id().to_string();
                    let reader = source_desc
                        .source
                        .stream_reader(Some(vec![split]), column_ids.clone(), source_ctx.clone())
                        .await
                        .map_err(StreamExecutorError::connector_error)?;
                    let (abort_handle, abort_registration) = AbortHandle::new_pair();
                    let stream = Abortable::new(reader, abort_registration);
                    abort_handles.insert(split_id, abort_handle);
                    streams.push(stream);
                }
                return Ok((futures::stream::select_all(streams).boxed(), abort_handles));
            }
            None => return Ok((futures::stream::pending().boxed(), HashMap::new())),
        }
    }

    async fn apply_split_change<const BIASED: bool>(
        &mut self,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunkWithState>,
        split_assignment: &HashMap<ActorId, Vec<SplitImpl>>,
    ) -> StreamExecutorResult<Option<Vec<SplitImpl>>> {
        // self.metrics
        //     .source_split_change_count
        //     .with_label_values(
        //         &self
        //             .get_metric_labels()
        //             .iter()
        //             .map(AsRef::as_ref)
        //             .collect::<Vec<&str>>(),
        //     )
        //     .inc();
        if let Some(target_splits) = split_assignment.get(&self.actor_ctx.id).cloned() {
            if let Some(target_state) = self.update_state_if_changed(Some(target_splits)).await? {
                tracing::info!(
                    actor_id = self.actor_ctx.id,
                    state = ?target_state,
                    "apply split change"
                );

                self.replace_stream_reader_with_target_state(
                    source_desc,
                    stream,
                    target_state.clone(),
                )
                .await?;

                return Ok(Some(target_state));
            }
        }

        Ok(None)
    }

    // Note: `update_state_if_changed` will modify `state_cache`
    async fn update_state_if_changed(
        &mut self,
        state: ConnectorState,
    ) -> StreamExecutorResult<ConnectorState> {
        let core = self.stream_source_core.as_mut().unwrap();

        let target_splits: HashMap<_, _> = state
            .unwrap()
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        let mut target_state: Vec<SplitImpl> = Vec::with_capacity(target_splits.len());

        let mut split_changed = false;

        for (split_id, split) in &target_splits {
            if let Some(s) = core.state_cache.get(split_id) {
                // existing split, no change, clone from cache
                target_state.push(s.clone())
            } else {
                split_changed = true;
                // write new assigned split to state cache. snapshot is base on cache.

                let initial_state = if let Some(recover_state) = core
                    .split_state_store
                    .try_recover_from_state_store(split)
                    .await?
                {
                    recover_state
                } else {
                    split.clone()
                };

                core.state_cache
                    .entry(split.id())
                    .or_insert_with(|| initial_state.clone());

                target_state.push(initial_state);
            }
        }

        // state cache may be stale
        for existing_split_id in core.stream_source_splits.keys() {
            if !target_splits.contains_key(existing_split_id) {
                tracing::info!("split dropping detected: {}", existing_split_id);
                split_changed = true;
            }
        }

        Ok(split_changed.then_some(target_state))
    }

    /// Rebuild stream if there is a err in stream
    async fn rebuild_stream_reader_from_error<const BIASED: bool>(
        &mut self,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunkWithState>,
        split_info: &mut [SplitImpl],
        e: StreamExecutorError,
    ) -> StreamExecutorResult<()> {
        let core = self.stream_source_core.as_mut().unwrap();
        tracing::warn!(
            "stream source reader error, actor: {:?}, source: {:?}",
            self.actor_ctx.id,
            core.source_id,
        );
        GLOBAL_ERROR_METRICS.user_source_reader_error.report([
            "SourceReaderError".to_owned(),
            e.to_report_string(),
            "KafkaBackfillExecutor".to_owned(),
            self.actor_ctx.id.to_string(),
            core.source_id.to_string(),
        ]);
        // fetch the newest offset, either it's in cache (before barrier)
        // or in state table (just after barrier)
        let target_state = if core.state_cache.is_empty() {
            for ele in &mut *split_info {
                if let Some(recover_state) = core
                    .split_state_store
                    .try_recover_from_state_store(ele)
                    .await?
                {
                    *ele = recover_state;
                }
            }
            split_info.to_owned()
        } else {
            core.state_cache
                .values()
                .map(|split_impl| split_impl.to_owned())
                .collect_vec()
        };

        self.replace_stream_reader_with_target_state(source_desc, stream, target_state)
            .await
    }

    async fn replace_stream_reader_with_target_state<const BIASED: bool>(
        &mut self,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunkWithState>,
        target_state: Vec<SplitImpl>,
    ) -> StreamExecutorResult<()> {
        tracing::info!(
            "actor {:?} apply source split change to {:?}",
            self.actor_ctx.id,
            target_state
        );

        // Replace the source reader with a new one of the new state.

        let (reader, abort_handles) = self
            .build_stream_source_reader(source_desc, Some(target_state.clone()))
            .await?;

        stream.replace_data_stream(reader);

        Ok(())
    }

    async fn take_snapshot_and_clear_cache(
        &mut self,
        epoch: EpochPair,
        target_state: Option<Vec<SplitImpl>>,
        should_trim_state: bool,
    ) -> StreamExecutorResult<()> {
        let core = self.stream_source_core.as_mut().unwrap();

        let mut cache = core
            .state_cache
            .values()
            .map(|split_impl| split_impl.to_owned())
            .collect_vec();

        if let Some(target_splits) = target_state {
            let target_split_ids: HashSet<_> =
                target_splits.iter().map(|split| split.id()).collect();

            cache.retain(|split| target_split_ids.contains(&split.id()));

            let dropped_splits = core
                .stream_source_splits
                .extract_if(|split_id, _| !target_split_ids.contains(split_id))
                .map(|(_, split)| split)
                .collect_vec();

            if should_trim_state && !dropped_splits.is_empty() {
                // trim dropped splits' state
                core.split_state_store.trim_state(&dropped_splits).await?;
            }

            core.stream_source_splits = target_splits
                .into_iter()
                .map(|split| (split.id(), split))
                .collect();
        }

        if !cache.is_empty() {
            tracing::debug!(actor_id = self.actor_ctx.id, state = ?cache, "take snapshot");
            core.split_state_store.take_snapshot(cache).await?
        }
        // commit anyway, even if no message saved
        core.split_state_store.state_store.commit(epoch).await?;

        core.state_cache.clear();

        Ok(())
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        let core = self.stream_source_core.as_mut().unwrap();
        core.split_state_store.state_store.try_flush().await?;

        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(mut self, input: BoxedExecutor) {
        // TODO: these can be inferred in frontend by checking additional_column_type
        let split_column_idx = 1;
        let offset_column_idx = 2;

        let mut input = input.execute();

        // Poll the upstream to get the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;

        let mut core = self.stream_source_core.unwrap();

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();
        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        let mut boot_state = Vec::default();
        if let Some(mutation) = barrier.mutation.as_ref() {
            match mutation.as_ref() {
                Mutation::Add(AddMutation { splits, .. })
                | Mutation::Update(UpdateMutation {
                    actor_splits: splits,
                    ..
                }) => {
                    if let Some(splits) = splits.get(&self.actor_ctx.id) {
                        tracing::info!(
                            "source exector: actor {:?} boot with splits: {:?}",
                            self.actor_ctx.id,
                            splits
                        );
                        boot_state = splits.clone();
                    }
                }
                _ => {}
            }
        }
        let mut latest_split_info = boot_state.clone();

        core.split_state_store.init_epoch(barrier.epoch);

        for ele in &mut boot_state {
            if let Some(recover_state) = core
                .split_state_store
                .try_recover_from_state_store(ele)
                .await?
            {
                *ele = recover_state;
            }
        }

        // init in-memory split states with persisted state if any
        core.init_split_state(boot_state.clone());

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = Some(core);

        let recover_state: ConnectorState = (!boot_state.is_empty()).then_some(boot_state);
        tracing::info!(actor_id = self.actor_ctx.id, state = ?recover_state, "start with state");
        let (source_chunk_reader, abort_handles) = self
            .build_stream_source_reader(&source_desc, recover_state)
            .instrument_await("source_build_reader")
            .await?;
        let source_chunk_reader = pin!(source_chunk_reader);

        // // Merge the chunks from source and the barriers into a single stream. We prioritize
        // // barriers over source data chunks here.
        // let mut stream =
        //     StreamReaderWithPause::<true, StreamChunkWithState>::new(input, source_chunk_reader);

        // If the first barrier requires us to pause on startup, pause the stream.
        if barrier.is_pause_on_startup() {
            // stream.pause_stream();
            todo!()
        }

        yield Message::Barrier(barrier);

        // XXX:
        // - What's the best poll strategy?
        // - Should we also add a barrier stream for backfill executor?
        // - TODO: support pause source chunk
        let mut backfill_stream = select_with_strategy(
            input.by_ref().map(Either::Left),
            source_chunk_reader.map(Either::Right),
            |_: &mut ()| futures::stream::PollNext::Left,
        );

        #[derive(Debug)]
        enum BackfillState {
            /// `None` means not started yet. It's the initial state.
            Backfilling(Option<String>),
            /// Backfill is stopped at this offset. Source needs to filter out messages before this offset.
            SourceCachingUp(String),
            Finished,
        }

        let split_ids = abort_handles.keys();
        // TODO: recover from state store
        let mut backfill_state: HashMap<ExecutorSplitId, BackfillState> = split_ids
            .map(|k| (k.clone(), BackfillState::Backfilling(None)))
            .collect();
        let need_backfill = backfill_state
            .values()
            .any(|state| !matches!(state, BackfillState::Finished));

        if need_backfill {
            #[for_await]
            'backfill_loop: for either in &mut backfill_stream {
                match either {
                    // Upstream
                    Either::Left(msg) => {
                        match msg? {
                            Message::Barrier(barrier) => {
                                // TODO: handle split change etc. & Persist progress.
                                yield Message::Barrier(barrier);
                            }
                            Message::Chunk(chunk) => {
                                // We need to iterate over all rows because there might be multiple splits in a chunk.
                                // Note: We assume offset from the source is monotonically increasing for the algorithm to work correctly.
                                let mut new_vis = BitmapBuilder::zeroed(chunk.visibility().len());
                                for (i, (_, row)) in chunk.rows().enumerate() {
                                    let split =
                                        row.datum_at(split_column_idx).unwrap().into_int64();
                                    let offset =
                                        row.datum_at(offset_column_idx).unwrap().into_int64();
                                    let backfill_state =
                                        backfill_state.get_mut(&split.to_string()).unwrap();
                                    match backfill_state {
                                        BackfillState::Backfilling(backfill_offset) => {
                                            new_vis.set(i, false);
                                            match compare_kafka_offset(
                                                backfill_offset.as_ref(),
                                                offset,
                                            ) {
                                                Ordering::Less => {
                                                    // continue backfilling. Ignore this row
                                                }
                                                Ordering::Equal => {
                                                    // backfilling for this split is finished just right.
                                                    *backfill_state = BackfillState::Finished;
                                                    abort_handles
                                                        .get(&split.to_string())
                                                        .unwrap()
                                                        .abort();
                                                }
                                                Ordering::Greater => {
                                                    // backfilling for this split produced more data.
                                                    *backfill_state =
                                                        BackfillState::SourceCachingUp(
                                                            offset.to_string(),
                                                        );
                                                    abort_handles
                                                        .get(&split.to_string())
                                                        .unwrap()
                                                        .abort();
                                                }
                                            }
                                        }
                                        BackfillState::SourceCachingUp(backfill_offset) => {
                                            new_vis.set(i, false);
                                            match compare_kafka_offset(
                                                Some(backfill_offset),
                                                offset,
                                            ) {
                                                Ordering::Less => {
                                                    // XXX: Is this possible? i.e., Source doesn't contain the
                                                    // last backfilled row.
                                                    new_vis.set(i, true);
                                                    *backfill_state = BackfillState::Finished;
                                                }
                                                Ordering::Equal => {
                                                    // Source just caught up with backfilling.
                                                    *backfill_state = BackfillState::Finished;
                                                }
                                                Ordering::Greater => {
                                                    // Source is still behind backfilling.
                                                    *backfill_offset = offset.to_string();
                                                }
                                            }
                                        }
                                        BackfillState::Finished => {
                                            new_vis.set(i, true);
                                            // This split's backfilling is finisehd, we are waiting for other splits
                                        }
                                    }
                                }
                                // emit chunk if vis is not empty. i.e., some splits finished backfilling.
                                let new_vis = new_vis.finish();
                                if new_vis.count_ones() != 0 {
                                    let new_chunk = chunk.clone_with_vis(new_vis);
                                    yield Message::Chunk(new_chunk);
                                }
                                // TODO: maybe use a counter to optimize this
                                if backfill_state
                                    .values()
                                    .all(|state| matches!(state, BackfillState::Finished))
                                {
                                    // all splits finished backfilling
                                    break 'backfill_loop;
                                }
                            }
                            Message::Watermark(_) => {
                                // Ignore watermark during backfill. (?)
                            }
                        }
                    }
                    // backfill
                    Either::Right(msg) => {
                        let StreamChunkWithState {
                            chunk,
                            split_offset_mapping,
                        } = msg.map_err(StreamExecutorError::connector_error)?;
                        let split_offset_mapping =
                            split_offset_mapping.expect("kafka source should have offsets");

                        let state: HashMap<_, _> = split_offset_mapping
                            .iter()
                            .flat_map(|(split_id, offset)| {
                                let origin_split_impl = self
                                    .stream_source_core
                                    .as_mut()
                                    .unwrap()
                                    .stream_source_splits
                                    .get_mut(split_id);

                                // update backfill progress
                                let prev_state = backfill_state.insert(
                                    split_id.to_string(),
                                    BackfillState::Backfilling(Some(offset.to_string())),
                                );
                                // abort_handles should prevents other cases happening
                                assert_matches!(
                                    prev_state,
                                    Some(BackfillState::Backfilling(_)),
                                    "Unexpected backfilling state, split_id: {split_id}"
                                );

                                origin_split_impl.map(|split_impl| {
                                    split_impl.update_in_place(offset.clone())?;
                                    Ok::<_, anyhow::Error>((split_id.clone(), split_impl.clone()))
                                })
                            })
                            .try_collect()?;

                        self.stream_source_core
                            .as_mut()
                            .unwrap()
                            .state_cache
                            .extend(state);

                        yield Message::Chunk(chunk);
                        self.try_flush_data().await?;
                    }
                }
            }
        }

        // All splits finished backfilling. Now we only forward the source data.
        #[for_await]
        for msg in input {
            match msg? {
                Message::Barrier(barrier) => {
                    // TODO: How to handle a split change here?
                    // We might need to persist its state. Is is possible that we need to backfill?
                    yield Message::Barrier(barrier);
                }
                Message::Chunk(chunk) => {
                    yield Message::Chunk(chunk);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}

fn compare_kafka_offset(a: Option<&String>, b: i64) -> Ordering {
    match a {
        Some(a) => {
            let a = a.parse::<i64>().unwrap();
            a.cmp(&b)
        }
        None => Ordering::Less,
    }
}

impl<S: StateStore> Executor for KafkaBackfillExecutorWrapper<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.inner.execute(self.input).boxed()
    }

    fn schema(&self) -> &Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }
}

impl<S: StateStore> Debug for KafkaBackfillExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("KafkaBackfillExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .field("pk_indices", &self.info.pk_indices)
                .finish()
        } else {
            f.debug_struct("KafkaBackfillExecutor").finish()
        }
    }
}
