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

use std::sync::Arc;

use futures::StreamExt;
use risingwave_common::catalog::Schema;

use super::*;

mod epoch_check;
mod epoch_provide;
mod schema_check;
mod trace;
mod update_check;

/// [`WrapperExecutor`] will do some sanity checks and logging for the wrapped executor.
pub struct WrapperExecutor {
    input: BoxedExecutor,

    actor_ctx: ActorContextRef,

    enable_executor_row_count: bool,
}

impl WrapperExecutor {
    pub fn new(
        input: BoxedExecutor,
        actor_ctx: ActorContextRef,
        enable_executor_row_count: bool,
    ) -> Self {
        Self {
            input,
            actor_ctx,
            enable_executor_row_count,
        }
    }

    #[allow(clippy::let_and_return)]
    fn wrap_debug(
        info: Arc<ExecutorInfo>,
        stream: impl MessageStream + 'static,
    ) -> impl MessageStream + 'static {
        // Update check
        let stream = update_check::update_check(info, stream);

        stream
    }

    fn wrap(
        enable_executor_row_count: bool,
        info: Arc<ExecutorInfo>,
        actor_ctx: ActorContextRef,
        stream: impl MessageStream + 'static,
    ) -> BoxedMessageStream {
        // -- Shared wrappers --

        // Await tree
        let stream = trace::instrument_await_tree(info.clone(), actor_ctx.id, stream);

        // Schema check
        let stream = schema_check::schema_check(info.clone(), stream);
        // Epoch check
        let stream = epoch_check::epoch_check(info.clone(), stream);

        // Epoch provide
        let stream = epoch_provide::epoch_provide(stream);

        // Trace
        let stream = trace::trace(enable_executor_row_count, info.clone(), actor_ctx, stream);

        if cfg!(debug_assertions) {
            Self::wrap_debug(info, stream).boxed()
        } else {
            stream.boxed()
        }
    }
}

impl Executor for WrapperExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let info = Arc::new(self.input.info());
        Self::wrap(
            self.enable_executor_row_count,
            info,
            self.actor_ctx,
            self.input.execute(),
        )
        .boxed()
    }

    fn execute_with_epoch(self: Box<Self>, epoch: u64) -> BoxedMessageStream {
        let info = Arc::new(self.input.info());
        Self::wrap(
            self.enable_executor_row_count,
            info,
            self.actor_ctx,
            self.input.execute_with_epoch(epoch),
        )
        .boxed()
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        self.input.pk_indices()
    }

    fn identity(&self) -> &str {
        self.input.identity()
    }
}
