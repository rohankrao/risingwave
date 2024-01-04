// Copyright 2024 RisingWave Labs
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
use std::future::Future;
use std::iter::once;
use std::ops::Sub;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use prometheus::HistogramTimer;
use risingwave_common::util::pending_on_none;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_storage::store::SyncResult;
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use thiserror_ext::AsReport;

use super::progress::BackfillState;
use super::CollectResult;
use crate::error::StreamResult;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::Barrier;
use crate::task::ActorId;

/// The state machine of local barrier manager.
#[derive(Debug)]
enum ManagedBarrierStateInner {
    /// Barriers from some actors have been collected and stashed, however no `send_barrier`
    /// request from the meta service is issued.
    Stashed {
        /// Actor ids we've collected and stashed.
        collected_actors: HashSet<ActorId>,
    },

    /// Meta service has issued a `send_barrier` request. We're collecting barriers now.
    Issued {
        /// Actor ids remaining to be collected.
        remaining_actors: HashSet<ActorId>,

        barrier_inflight_latency: HistogramTimer,
    },
}

#[derive(Debug)]
pub(super) struct BarrierState {
    prev_epoch: u64,
    inner: ManagedBarrierStateInner,
    kind: BarrierKind,
}

type EpochCompletedFuture = impl Future<Output = (u64, StreamResult<CollectResult>)>;

pub(super) struct ManagedBarrierState {
    /// Record barrier state for each epoch of concurrent checkpoints.
    ///
    /// The key is curr_epoch, and the first value is prev_epoch
    epoch_barrier_state_map: BTreeMap<u64, BarrierState>,

    /// Record the progress updates of creating mviews for each epoch of concurrent checkpoints.
    pub(super) create_mview_progress: HashMap<u64, HashMap<ActorId, BackfillState>>,

    pub(super) state_store: StateStoreImpl,

    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    syncing_epoch_futures: FuturesOrdered<EpochCompletedFuture>,
}

impl ManagedBarrierState {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(
            StateStoreImpl::for_test(),
            Arc::new(StreamingMetrics::unused()),
        )
    }

    /// Create a barrier manager state. This will be called only once.
    pub(super) fn new(
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            epoch_barrier_state_map: BTreeMap::default(),
            create_mview_progress: Default::default(),
            state_store,
            streaming_metrics,
            syncing_epoch_futures: FuturesOrdered::new(),
        }
    }

    /// Notify if we have collected barriers from all actor ids. The state must be `Issued`.
    fn may_notify(&mut self, curr_epoch: u64) {
        // Report if there's progress on the earliest in-flight barrier.
        if self.epoch_barrier_state_map.keys().next() == Some(&curr_epoch) {
            self.streaming_metrics.barrier_manager_progress.inc();
        }

        while let Some(entry) = self.epoch_barrier_state_map.first_entry() {
            let (epoch, barrier_state) = match &entry.get().inner {
                ManagedBarrierStateInner::Issued {
                    remaining_actors, ..
                } if remaining_actors.is_empty() => entry.remove_entry(),
                _ => {
                    break;
                }
            };

            match barrier_state.inner {
                ManagedBarrierStateInner::Stashed { .. } => unreachable!(),
                ManagedBarrierStateInner::Issued {
                    barrier_inflight_latency: timer,
                    ..
                } => {
                    timer.observe_duration();
                }
            }

            let create_mview_progress = self
                .create_mview_progress
                .remove(&epoch)
                .unwrap_or_default()
                .into_iter()
                .map(|(actor, state)| CreateMviewProgress {
                    backfill_actor_id: actor,
                    done: matches!(state, BackfillState::Done(_)),
                    consumed_epoch: match state {
                        BackfillState::ConsumingUpstream(consumed_epoch, _) => consumed_epoch,
                        BackfillState::Done(_) => epoch,
                    },
                    consumed_rows: match state {
                        BackfillState::ConsumingUpstream(_, consumed_rows) => consumed_rows,
                        BackfillState::Done(consumed_rows) => consumed_rows,
                    },
                })
                .collect();

            let kind = barrier_state.kind;
            match kind {
                BarrierKind::Unspecified => unreachable!(),
                BarrierKind::Initial => tracing::info!(
                    epoch = barrier_state.prev_epoch,
                    "ignore sealing data for the first barrier"
                ),
                BarrierKind::Barrier | BarrierKind::Checkpoint => {
                    dispatch_state_store!(&self.state_store, state_store, {
                        state_store.seal_epoch(barrier_state.prev_epoch, kind.is_checkpoint());
                    });
                }
            }

            let barrier_sync_latency = self.streaming_metrics.barrier_sync_latency.clone();
            let state_store = self.state_store.clone();

            self.syncing_epoch_futures.push_back(async move {
                let sync_result = match kind {
                    BarrierKind::Unspecified => unreachable!(),
                    BarrierKind::Initial => {
                        if let Some(hummock) = state_store.as_hummock() {
                            let mce = hummock.get_pinned_version().max_committed_epoch();
                            assert_eq!(
                                mce, epoch,
                                "first epoch should match with the current version",
                            );
                        }
                        tracing::info!(?epoch, "ignored syncing data for the first barrier");
                        SyncResult {
                            sync_size: 0,
                            uncommitted_ssts: Vec::new(),
                            table_watermarks: HashMap::new(),
                        }
                    }
                    BarrierKind::Barrier => SyncResult {
                        sync_size: 0,
                        uncommitted_ssts: Vec::new(),
                        table_watermarks: HashMap::new(),
                    },
                    BarrierKind::Checkpoint => {
                        let timer = barrier_sync_latency.start_timer();
                        match dispatch_state_store!(state_store, store, {
                            store
                                .sync(epoch)
                                .instrument_await(format!("sync_epoch (epoch {})", epoch))
                                .await
                                .inspect_err(|e| {
                                    tracing::error!(
                                        epoch,
                                        error = %e.as_report(),
                                        "Failed to sync state store",
                                    );
                                })
                        }) {
                            Ok(sync_result) => {
                                timer.observe_duration();
                                sync_result
                            }
                            Err(err) => {
                                return (epoch, Err(err.into()));
                            }
                        }
                    }
                };
                (
                    epoch,
                    Ok(CollectResult {
                        sync_result,
                        create_mview_progress,
                    }),
                )
            });
        }
    }

    pub(crate) fn has_epoch(&self, epoch: u64) -> bool {
        self.epoch_barrier_state_map.contains_key(&epoch)
    }

    /// Returns an iterator on epochs that is awaiting on `actor_id`.
    pub(crate) fn epochs_await_on_actor(
        &self,
        actor_id: ActorId,
    ) -> impl Iterator<Item = u64> + '_ {
        self.epoch_barrier_state_map
            .iter()
            .filter_map(move |(epoch, barrier_state)| {
                #[allow(clippy::single_match)]
                match barrier_state.inner {
                    ManagedBarrierStateInner::Issued {
                        ref remaining_actors,
                        ..
                    } => {
                        if remaining_actors.contains(&actor_id) {
                            Some(*epoch)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
    }

    /// Collect a `barrier` from the actor with `actor_id`.
    pub(super) fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        tracing::debug!(
            target: "events::stream::barrier::manager::collect",
            epoch = barrier.epoch.curr, actor_id, state = ?self.epoch_barrier_state_map,
            "collect_barrier",
        );

        match self.epoch_barrier_state_map.get_mut(&barrier.epoch.curr) {
            Some(&mut BarrierState {
                prev_epoch,
                inner:
                    ManagedBarrierStateInner::Stashed {
                        ref mut collected_actors,
                    },
                ..
            }) => {
                let new = collected_actors.insert(actor_id);
                assert!(new);
                assert_eq!(prev_epoch, barrier.epoch.prev);
            }
            Some(&mut BarrierState {
                prev_epoch,
                inner:
                    ManagedBarrierStateInner::Issued {
                        ref mut remaining_actors,
                        ..
                    },
                ..
            }) => {
                let exist = remaining_actors.remove(&actor_id);
                assert!(
                    exist,
                    "the actor doesn't exist. actor_id: {:?}, curr_epoch: {:?}",
                    actor_id, barrier.epoch.curr
                );
                assert_eq!(prev_epoch, barrier.epoch.prev);
                self.may_notify(barrier.epoch.curr);
            }
            None => {
                self.epoch_barrier_state_map.insert(
                    barrier.epoch.curr,
                    BarrierState {
                        prev_epoch: barrier.epoch.prev,
                        inner: ManagedBarrierStateInner::Stashed {
                            collected_actors: once(actor_id).collect(),
                        },
                        kind: barrier.kind,
                    },
                );
            }
        }
    }

    /// When the meta service issues a `send_barrier` request, call this function to transform to
    /// `Issued` and start to collect or to notify.
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: HashSet<ActorId>,
    ) {
        let timer = self
            .streaming_metrics
            .barrier_inflight_latency
            .start_timer();
        let inner = match self.epoch_barrier_state_map.get_mut(&barrier.epoch.curr) {
            Some(&mut BarrierState {
                inner:
                    ManagedBarrierStateInner::Stashed {
                        ref mut collected_actors,
                    },
                ..
            }) => {
                assert!(
                    actor_ids_to_collect.is_superset(collected_actors),
                    "to_collect: {:?}, collected: {:?}",
                    actor_ids_to_collect,
                    collected_actors
                );
                let remaining_actors: HashSet<ActorId> = actor_ids_to_collect.sub(collected_actors);
                ManagedBarrierStateInner::Issued {
                    remaining_actors,
                    barrier_inflight_latency: timer,
                }
            }
            Some(&mut BarrierState {
                inner: ManagedBarrierStateInner::Issued { .. },
                ..
            }) => {
                panic!(
                    "barrier epochs{:?} state has already been `Issued`",
                    barrier.epoch
                );
            }
            None => ManagedBarrierStateInner::Issued {
                remaining_actors: actor_ids_to_collect,
                barrier_inflight_latency: timer,
            },
        };
        self.epoch_barrier_state_map.insert(
            barrier.epoch.curr,
            BarrierState {
                prev_epoch: barrier.epoch.prev,
                inner,
                kind: barrier.kind,
            },
        );
        self.may_notify(barrier.epoch.curr);
    }

    pub(crate) async fn next_complete_epoch(&mut self) -> (u64, StreamResult<CollectResult>) {
        pending_on_none(self.syncing_epoch_futures.next()).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::executor::Barrier;
    use crate::task::barrier_manager::managed_state::ManagedBarrierState;

    #[tokio::test]
    async fn test_managed_state_add_actor() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(1);
        let barrier2 = Barrier::new_test_barrier(2);
        let barrier3 = Barrier::new_test_barrier(3);
        let actor_ids_to_collect1 = HashSet::from([1, 2]);
        let actor_ids_to_collect2 = HashSet::from([1, 2]);
        let actor_ids_to_collect3 = HashSet::from([1, 2, 3]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1);
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2);
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3);
        managed_barrier_state.collect(1, &barrier1);
        managed_barrier_state.collect(2, &barrier1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
        );
        managed_barrier_state.collect(1, &barrier2);
        managed_barrier_state.collect(1, &barrier3);
        managed_barrier_state.collect(2, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &3
        );
        managed_barrier_state.collect(2, &barrier3);
        managed_barrier_state.collect(3, &barrier3);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_stop_actor() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(1);
        let barrier2 = Barrier::new_test_barrier(2);
        let barrier3 = Barrier::new_test_barrier(3);
        let actor_ids_to_collect1 = HashSet::from([1, 2, 3, 4]);
        let actor_ids_to_collect2 = HashSet::from([1, 2, 3]);
        let actor_ids_to_collect3 = HashSet::from([1, 2]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1);
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2);
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3);

        managed_barrier_state.collect(1, &barrier1);
        managed_barrier_state.collect(1, &barrier2);
        managed_barrier_state.collect(1, &barrier3);
        managed_barrier_state.collect(2, &barrier1);
        managed_barrier_state.collect(2, &barrier2);
        managed_barrier_state.collect(2, &barrier3);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &1
        );
        managed_barrier_state.collect(3, &barrier1);
        managed_barrier_state.collect(3, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &1
        );
        managed_barrier_state.collect(4, &barrier1);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_issued_after_collect() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(1);
        let barrier2 = Barrier::new_test_barrier(2);
        let barrier3 = Barrier::new_test_barrier(3);
        let actor_ids_to_collect1 = HashSet::from([1, 2]);
        let actor_ids_to_collect2 = HashSet::from([1, 2, 3]);
        let actor_ids_to_collect3 = HashSet::from([1, 2, 3]);

        managed_barrier_state.collect(1, &barrier3);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &3
        );
        managed_barrier_state.collect(1, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
        );
        managed_barrier_state.collect(1, &barrier1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &1
        );
        managed_barrier_state.collect(2, &barrier1);
        managed_barrier_state.collect(2, &barrier2);
        managed_barrier_state.collect(2, &barrier3);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
        );
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2);
        managed_barrier_state.collect(3, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &3
        );
        managed_barrier_state.collect(3, &barrier3);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &3
        );
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }
}
