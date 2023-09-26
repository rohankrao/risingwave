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

use risingwave_pb::stream_plan::StreamFsFetchNode;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::BoxedExecutor;
use crate::from_proto::ExecutorBuilder;
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct FsFetchExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for FsFetchExecutorBuilder {
    type Node = StreamFsFetchNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        todo!()
    }
}
