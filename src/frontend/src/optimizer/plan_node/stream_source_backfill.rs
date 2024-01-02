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

use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::stream_plan::stream_node::{NodeBody, PbNodeBody};
use risingwave_pb::stream_plan::{PbStreamNode, PbStreamSource, SourceNode};

use super::stream::prelude::*;
use super::{PlanBase, PlanRef, PlanTreeNodeUnary};
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{childless_record, Distill};
use crate::optimizer::plan_node::{generic, ExprRewritable, StreamNode};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::Explain;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSourceBackfill {
    pub base: PlanBase<Stream>,
    core: generic::Source,
}

impl_plan_tree_node_for_leaf! { StreamSourceBackfill }

impl StreamSourceBackfill {
    pub fn new(source: generic::Source) -> Self {
        let base = PlanBase::new_stream_with_core(
            &source,
            Distribution::SomeShard,
            source.catalog.as_ref().map_or(true, |s| s.append_only),
            false,
            FixedBitSet::with_capacity(source.column_catalog.len()),
        );

        Self { base, core: source }
    }

    fn get_columns(&self) -> Vec<&str> {
        self.core
            .column_catalog
            .iter()
            .map(|column| column.name())
            .collect()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn adhoc_to_stream_prost(&self, state: &mut BuildFragmentGraphState) -> PbStreamNode {
        use risingwave_pb::stream_plan::*;

        let stream_key = self
            .stream_key()
            .unwrap_or_else(|| {
                panic!(
                    "should always have a stream key in the stream plan but not, sub plan: {}",
                    PlanRef::from(self.clone()).explain_to_string()
                )
            })
            .iter()
            .map(|x| *x as u32)
            .collect_vec();

        let source_catalog = self.source_catalog();
        let source_inner = source_catalog.map(|source_catalog| PbStreamSource {
            source_id: source_catalog.id,
            source_name: source_catalog.name.clone(),
            // FIXME: this is not the same as source.
            state_table: Some(
                generic::Source::infer_internal_table_catalog()
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            info: Some(source_catalog.info.clone()),
            row_id_index: self.core.row_id_index.map(|index| index as _),
            columns: self
                .core
                .column_catalog
                .iter()
                .map(|c| c.to_protobuf())
                .collect_vec(),
            with_properties: source_catalog.with_properties.clone().into_iter().collect(),
            rate_limit: self.base.ctx().overwrite_options().streaming_rate_limit,
        });

        let stream_scan_body = PbNodeBody::SourceBackfill(SourceNode { source_inner });

        let fields = self.schema().to_prost();
        // plan: merge -> backfill
        PbStreamNode {
            fields: fields.clone(),
            input: vec![
                // The merge node body will be filled by the `ActorBuilder` on the meta service.
                PbStreamNode {
                    node_body: Some(PbNodeBody::Merge(Default::default())),
                    identity: "Upstream".into(),
                    fields,
                    stream_key: vec![], // not used
                    ..Default::default()
                },
            ],
            node_body: Some(stream_scan_body),
            stream_key,
            operator_id: self.base.id().0 as u64,
            identity: self.distill_to_string(),
            append_only: self.append_only(),
        }
    }
}

impl Distill for StreamSourceBackfill {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let columns = self
            .get_columns()
            .iter()
            .map(|ele| Pretty::from(ele.to_string()))
            .collect();
        let col = Pretty::Array(columns);
        childless_record("StreamSourceBackfill", vec![("columns", col)])
    }
}

impl ExprRewritable for StreamSourceBackfill {}

impl ExprVisitable for StreamSourceBackfill {}

impl StreamNode for StreamSourceBackfill {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        unreachable!("stream source backfill cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}
