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
use std::rc::Rc;
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::bail;
use risingwave_common::catalog::{ConnectionId, DatabaseId, SchemaId, UserId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkFormatDesc};
use risingwave_connector::sink::{
    CONNECTOR_TYPE_KEY, SINK_TYPE_OPTION, SINK_USER_FORCE_APPEND_ONLY_OPTION,
};
use risingwave_pb::catalog::{PbSource, Table};
use risingwave_pb::ddl_service::ReplaceTableChange;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::stream_node::NodeBody::Merge;
use risingwave_pb::stream_plan::{DispatcherType, MergeNode, StreamFragmentGraph, StreamNode};
use risingwave_sqlparser::ast::{
    ColumnDef, ConnectorSchema, CreateSink, CreateSinkStatement, EmitMode, Encode, Format,
    ObjectName, Query, Select, SelectItem, SetExpr, SourceWatermark, Statement, TableConstraint,
    TableFactor, TableWithJoins,
};
use risingwave_sqlparser::parser::Parser;

use super::create_mv::get_column_names;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::expr::{ExprImpl, InputRef, Literal};
use crate::handler::create_table::ColumnIdGenerator;
use crate::handler::privilege::resolve_query_privileges;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{
    generic, Explain, PlanTreeNodeUnary, StreamExchange, StreamProject,
};
use crate::optimizer::property::Distribution::HashShard;
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, RelationCollectorVisitor};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
use crate::utils::resolve_privatelink_in_with_option;
use crate::{Planner, TableCatalog, WithOptions};

pub fn gen_sink_query_from_name(from_name: ObjectName) -> Result<Query> {
    let table_factor = TableFactor::Table {
        name: from_name,
        alias: None,
        for_system_time_as_of_proctime: false,
    };
    let from = vec![TableWithJoins {
        relation: table_factor,
        joins: vec![],
    }];
    let select = Select {
        from,
        projection: vec![SelectItem::Wildcard(None)],
        ..Default::default()
    };
    let body = SetExpr::Select(Box::new(select));
    Ok(Query {
        with: None,
        body,
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    })
}

pub fn gen_sink_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: CreateSinkStatement,
    inject_exchange: Option<Vec<usize>>,
) -> Result<(Box<Query>, PlanRef, SinkCatalog)> {
    let db_name = session.database();
    let (sink_schema_name, sink_table_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.sink_name.clone())?;

    // Used for debezium's table name
    let sink_from_table_name;
    let query = match stmt.sink_from {
        CreateSink::From(from_name) => {
            sink_from_table_name = from_name.0.last().unwrap().real_value();
            Box::new(gen_sink_query_from_name(from_name)?)
        }
        CreateSink::AsQuery(query) => {
            sink_from_table_name = sink_table_name.clone();
            query
        }
    };

    let sink_into_table_name = stmt.into_table_name.map(|name| name.real_value());

    let (sink_database_id, sink_schema_id) =
        session.get_database_and_schema_id_for_create(sink_schema_name.clone())?;

    let definition = context.normalized_sql().to_owned();

    let (dependent_relations, bound) = {
        let mut binder = Binder::new_for_stream(session);
        let bound = binder.bind_query(*query.clone())?;
        (binder.included_relations(), bound)
    };

    let check_items = resolve_query_privileges(&bound);
    session.check_privileges(&check_items)?;

    // If column names not specified, use the name in materialized view.
    let col_names = get_column_names(&bound, session, stmt.columns)?;

    let mut with_options = context.with_options().clone();

    if sink_into_table_name.is_some() {
        let prev = with_options
            .inner_mut()
            .insert(CONNECTOR_TYPE_KEY.to_string(), "table".to_string());

        if let Some(prev) = prev {
            tracing::warn!("connector {} replaced by table", prev);
        }
    }

    let connection_id = {
        let conn_id =
            resolve_privatelink_in_with_option(&mut with_options, &sink_schema_name, session)?;
        conn_id.map(ConnectionId)
    };

    let emit_on_window_close = stmt.emit_mode == Some(EmitMode::OnWindowClose);
    if emit_on_window_close {
        context.warn_to_user("EMIT ON WINDOW CLOSE is currently an experimental feature. Please use it with caution.");
    }

    let connector = with_options
        .get(CONNECTOR_TYPE_KEY)
        .cloned()
        .ok_or_else(|| ErrorCode::BindError(format!("missing field '{CONNECTOR_TYPE_KEY}'")))?;

    let format_desc = match stmt.sink_schema {
        // Case A: new syntax `format ... encode ...`
        Some(f) => {
            validate_compatibility(&connector, &f)?;
            Some(bind_sink_format_desc(f)?)
        }
        None => match with_options.get(SINK_TYPE_OPTION) {
            // Case B: old syntax `type = '...'`
            Some(t) => SinkFormatDesc::from_legacy_type(&connector, t)?.map(|mut f| {
                session.notice_to_user("Consider using the newer syntax `FORMAT ... ENCODE ...` instead of `type = '...'`.");
                if let Some(v) = with_options.get(SINK_USER_FORCE_APPEND_ONLY_OPTION) {
                    f.options.insert(SINK_USER_FORCE_APPEND_ONLY_OPTION.into(), v.into());
                }
                f
            }),
            // Case C: no format + encode required
            None => None,
        },
    };

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    if let Some(col_names) = col_names {
        plan_root.set_out_names(col_names)?;
    };

    let sink_plan = plan_root.gen_sink_plan(
        sink_table_name,
        definition,
        with_options,
        emit_on_window_close,
        db_name.to_owned(),
        sink_from_table_name,
        format_desc,
        sink_into_table_name,
    )?;
    let sink_desc = sink_plan.sink_desc().clone();

    let sink_plan: PlanRef = if let Some(idxes) = inject_exchange {
        let exchange = StreamExchange::new(sink_plan.input(), HashShard(idxes)).into();
        sink_plan.clone_with_input(exchange).into()
    } else {
        sink_plan.into()
    };

    let ctx = sink_plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Sink:");
        ctx.trace(sink_plan.explain_to_string());
    }

    let dependent_relations =
        RelationCollectorVisitor::collect_with(dependent_relations, sink_plan.clone());

    let sink_catalog = sink_desc.into_catalog(
        SchemaId::new(sink_schema_id),
        DatabaseId::new(sink_database_id),
        UserId::new(session.user_id()),
        connection_id,
        dependent_relations.into_iter().collect_vec(),
    );

    Ok((query, sink_plan, sink_catalog))
}

pub async fn handle_create_sink(
    handle_args: HandlerArgs,
    stmt: CreateSinkStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    session.check_relation_name_duplicated(stmt.sink_name.clone())?;

    let target_table = stmt.into_table_name.clone();

    let mut target_table_change = None;
    if let Some(table_name) = target_table {
        let db_name = session.database();
        let (schema_name, real_table_name) =
            Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
        let search_path = session.config().get_search_path();
        let user_name = &session.auth_context().user_name;

        let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

        let original_catalog = {
            let reader = session.env().catalog_reader().read_guard();
            let (table, schema_name) =
                reader.get_table_by_name(db_name, schema_path, &real_table_name)?;

            match table.table_type() {
                TableType::Table => {}

                _ => Err(ErrorCode::InvalidInputSyntax(format!(
                    "\"{table_name}\" is not a table or cannot be altered"
                )))?,
            }

            session.check_privilege_for_drop_alter(schema_name, &**table)?;

            table.clone()
        };

        // Retrieve the original table definition and parse it to AST.
        let [mut definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
            .context("unable to parse original table definition")?
            .try_into()
            .unwrap();
        let Statement::CreateTable { source_schema, .. } = &mut definition else {
            panic!("unexpected statement: {:?}", definition);
        };
        let source_schema = source_schema
            .clone()
            .map(|source_schema| source_schema.into_source_schema_v2().0);

        // Create handler args as if we're creating a new table with the altered definition.
        let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;
        let col_id_gen = ColumnIdGenerator::new_alter(&original_catalog);
        let Statement::CreateTable {
            columns,
            constraints,
            source_watermarks,
            append_only,
            ..
        } = definition
        else {
            panic!("unexpected statement type: {:?}", definition);
        };

        let (mut graph, table, source) = regenerate_table(
            &session,
            table_name,
            &original_catalog,
            source_schema,
            handler_args,
            col_id_gen,
            columns,
            constraints,
            source_watermarks,
            append_only,
            1,
        )
        .await?;

        fn modify_source_to_merge(node: &mut StreamNode) {
            let n2 = node.clone();
            if let Some(NodeBody::Union(_u)) = &mut node.node_body {
                for input in &mut node.input {
                    if let Some(NodeBody::Project(_p)) = &mut input.node_body {
                        if let Ok(x) = input.input.iter_mut().exactly_one() {
                            if let Some(NodeBody::Source(s)) = &mut x.node_body && s.source_inner.is_none() {
                                println!("rewriting");
                                input.node_body = Some(Merge(MergeNode{
                                    upstream_actor_id: vec![],
                                    upstream_fragment_id: 0,
                                    upstream_dispatcher_type: DispatcherType::Hash as _,
                                    fields: n2.fields.clone(),
                                }));
                                input.identity = "merge".to_string();
                            }
                        }
                    } else if let Some(NodeBody::Source(s)) = &mut input.node_body && s.source_inner.is_none() {
                        println!("rewriting");
                        input.node_body = Some(Merge(MergeNode{
                            upstream_actor_id: vec![],
                            upstream_fragment_id: 0,
                            upstream_dispatcher_type: DispatcherType::Hash as _,
                            fields: n2.fields.clone(),
                        }));
                        input.identity = "merge".to_string();
                    };
                }

                return;
            }

            for input in &mut node.input {
                modify_source_to_merge(input);
            }
        }

        for fragment in graph.fragments.values_mut() {
            if let Some(node) = &mut fragment.node {
                modify_source_to_merge(node);
            }
        }

        // Calculate the mapping from the original columns to the new columns.
        let col_index_mapping = ColIndexMapping::new(
            original_catalog
                .columns()
                .iter()
                .map(|old_c| {
                    table.columns.iter().position(|new_c| {
                        new_c.get_column_desc().unwrap().column_id == old_c.column_id().get_id()
                    })
                })
                .collect(),
            table.columns.len(),
        );

        target_table_change = Some(ReplaceTableChange {
            source,
            table: Some(table.clone()),
            fragment_graph: Some(graph),
            table_col_index_mapping: Some(col_index_mapping.to_protobuf()),
        });
    }

    let (sink, graph) = {
        let context = Rc::new(OptimizerContext::from_handler_args(handle_args));

        let (query, plan, sink) = if let Some(x) = &target_table_change {
            let table1 = x.table.clone().unwrap();

            let xx = table1
                .pk
                .iter()
                .map(|x| x.column_index as usize)
                .collect_vec();
            let (query, plan, sink) = gen_sink_plan(&session, context.clone(), stmt, Some(xx))?;

            println!("columns {:#?}", sink.full_columns());

            println!("table {:#?}", table1.columns);

            for col in sink.full_columns() {
                println!("col {:?} is gen {}", col, col.is_generated());
            }

            #[derive(PartialEq, Debug, Copy, Clone)]
            enum PrimaryKeyKind {
                UserDefinedPrimaryKey,
                RowIdAsPrimaryKey,
                AppendOnly,
            }

            let kind = if table1.append_only {
                PrimaryKeyKind::AppendOnly
            } else if let Some(_row_id_index) = table1.row_id_index {
                PrimaryKeyKind::RowIdAsPrimaryKey
            } else {
                PrimaryKeyKind::UserDefinedPrimaryKey
            };

            let plan: PlanRef = match kind {
                PrimaryKeyKind::UserDefinedPrimaryKey
                | PrimaryKeyKind::RowIdAsPrimaryKey
                | PrimaryKeyKind::AppendOnly => {
                    let mut exprs = vec![];

                    let table_columns = table1
                        .get_columns()
                        .iter()
                        .map(|col| {
                            let col_desc = col.get_column_desc().unwrap();
                            let data_type = col_desc.get_column_type().unwrap();
                            DataType::from(data_type)
                        })
                        .collect_vec();

                    let sink_visable_columns = sink
                        .full_columns()
                        .iter()
                        .enumerate()
                        .filter(|(_i, c)| !c.is_hidden())
                        .collect_vec();

                    for (idx, data_type) in table_columns.iter().enumerate() {
                        if idx < sink_visable_columns.len() {
                            let (sink_col_idx, sink_column) = sink_visable_columns[idx];

                            let sink_col_type = sink_column.data_type();
                            if data_type != sink_col_type {
                                bail!(
                                    "column type mismatch: {:?} vs {:?}",
                                    data_type,
                                    sink_col_type
                                );
                            } else {
                                exprs.push(ExprImpl::InputRef(Box::new(InputRef::new(
                                    sink_col_idx,
                                    data_type.clone(),
                                ))));
                            }
                        } else {
                            exprs.push(ExprImpl::Literal(Box::new(Literal::new(
                                None,
                                data_type.clone(),
                            ))));
                        };
                    }

                    let logical_project = generic::Project::new(exprs, plan);

                    StreamProject::new(logical_project).into()
                }
            };

            (query, plan, sink)
        } else {
            gen_sink_plan(&session, context.clone(), stmt, None)?
        };

        let has_order_by = !query.order_by.is_empty();
        if has_order_by {
            context.warn_to_user(
                r#"The ORDER BY clause in the CREATE SINK statement has no effect at all."#
                    .to_string(),
            );
        }

        let mut graph = build_graph(plan);

        if let Some(x) = &target_table_change {
            graph.parallelism = x.fragment_graph.clone().unwrap().parallelism.clone();
        } else {
            graph.parallelism = session
                .config()
                .get_streaming_parallelism()
                .map(|parallelism| Parallelism { parallelism });
        }

        (sink, graph)
    };

    let _job_guard =
        session
            .env()
            .creating_streaming_job_tracker()
            .guard(CreatingStreamingJobInfo::new(
                session.session_id(),
                sink.database_id.database_id,
                sink.schema_id.schema_id,
                sink.name.clone(),
            ));

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_sink(sink.to_proto(), graph, target_table_change)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn regenerate_table(
    session: &Arc<SessionImpl>,
    table_name: ObjectName,
    original_catalog: &Arc<TableCatalog>,
    source_schema: Option<ConnectorSchema>,
    handler_args: HandlerArgs,
    col_id_gen: ColumnIdGenerator,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    with_external_sink: i32,
) -> Result<(StreamFragmentGraph, Table, Option<PbSource>)> {
    use risingwave_pb::catalog::table::OptionalAssociatedSourceId;

    use super::create_table::gen_create_table_plan;
    use crate::handler::create_table::gen_create_table_plan_with_source;

    let context = OptimizerContext::from_handler_args(handler_args);
    let (plan, source, table) = match source_schema {
        Some(source_schema) => {
            gen_create_table_plan_with_source(
                context,
                table_name,
                columns,
                constraints,
                source_schema,
                source_watermarks,
                col_id_gen,
                append_only,
                with_external_sink,
            )
            .await?
        }
        None => gen_create_table_plan(
            context,
            table_name,
            columns,
            constraints,
            col_id_gen,
            source_watermarks,
            append_only,
            with_external_sink,
        )?,
    };

    // TODO: avoid this backward conversion.
    if TableCatalog::from(&table).pk_column_ids() != original_catalog.pk_column_ids() {
        Err(ErrorCode::InvalidInputSyntax(
            "alter primary key of table is not supported".to_owned(),
        ))?
    }

    let graph = StreamFragmentGraph {
        parallelism: session
            .config()
            .get_streaming_parallelism()
            .map(|parallelism| Parallelism { parallelism }),
        ..build_graph(plan)
    };

    // Fill the original table ID.
    let table = Table {
        id: original_catalog.id().table_id(),
        optional_associated_source_id: original_catalog
            .associated_source_id()
            .map(|source_id| OptionalAssociatedSourceId::AssociatedSourceId(source_id.into())),
        ..table
    };

    Ok((graph, table, source))
}

/// Transforms the (format, encode, options) from sqlparser AST into an internal struct `SinkFormatDesc`.
/// This is an analogy to (part of) [`crate::handler::create_source::bind_columns_from_source`]
/// which transforms sqlparser AST `SourceSchemaV2` into `StreamSourceInfo`.
fn bind_sink_format_desc(value: ConnectorSchema) -> Result<SinkFormatDesc> {
    use risingwave_connector::sink::catalog::{SinkEncode, SinkFormat};
    use risingwave_connector::sink::encoder::TimestamptzHandlingMode;
    use risingwave_sqlparser::ast::{Encode as E, Format as F};

    let format = match value.format {
        F::Plain => SinkFormat::AppendOnly,
        F::Upsert => SinkFormat::Upsert,
        F::Debezium => SinkFormat::Debezium,
        f @ (F::Native | F::DebeziumMongo | F::Maxwell | F::Canal) => {
            return Err(ErrorCode::BindError(format!("sink format unsupported: {f}")).into());
        }
    };
    let encode = match value.row_encode {
        E::Json => SinkEncode::Json,
        E::Protobuf => SinkEncode::Protobuf,
        E::Avro => SinkEncode::Avro,
        E::Template => SinkEncode::Template,
        e @ (E::Native | E::Csv | E::Bytes) => {
            return Err(ErrorCode::BindError(format!("sink encode unsupported: {e}")).into());
        }
    };
    let mut options = WithOptions::try_from(value.row_options.as_slice())?.into_inner();

    options
        .entry(TimestamptzHandlingMode::OPTION_KEY.to_owned())
        .or_insert(TimestamptzHandlingMode::FRONTEND_DEFAULT.to_owned());

    Ok(SinkFormatDesc {
        format,
        encode,
        options,
    })
}

static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, HashMap<Format, Vec<Encode>>>> =
    LazyLock::new(|| {
        use risingwave_connector::sink::kafka::KafkaSink;
        use risingwave_connector::sink::kinesis::KinesisSink;
        use risingwave_connector::sink::pulsar::PulsarSink;
        use risingwave_connector::sink::redis::RedisSink;
        use risingwave_connector::sink::Sink as _;

        convert_args!(hashmap!(
                KafkaSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                ),
                KinesisSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                PulsarSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                RedisSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json,Encode::Template],
                    Format::Upsert => vec![Encode::Json,Encode::Template],
                ),
        ))
    });
pub fn validate_compatibility(connector: &str, format_desc: &ConnectorSchema) -> Result<()> {
    let compatible_formats = CONNECTORS_COMPATIBLE_FORMATS
        .get(connector)
        .ok_or_else(|| {
            ErrorCode::BindError(format!(
                "connector {} is not supported by FORMAT ... ENCODE ... syntax",
                connector
            ))
        })?;
    let compatible_encodes = compatible_formats.get(&format_desc.format).ok_or_else(|| {
        ErrorCode::BindError(format!(
            "connector {} does not support format {:?}",
            connector, format_desc.format
        ))
    })?;
    if !compatible_encodes.contains(&format_desc.row_encode) {
        return Err(ErrorCode::BindError(format!(
            "connector {} does not support format {:?} with encode {:?}",
            connector, format_desc.format, format_desc.row_encode
        ))
        .into());
    }
    Ok(())
}

/// For `planner_test` crate so that it does not depend directly on `connector` crate just for `SinkFormatDesc`.
impl TryFrom<&WithOptions> for Option<SinkFormatDesc> {
    type Error = risingwave_connector::sink::SinkError;

    fn try_from(value: &WithOptions) -> std::result::Result<Self, Self::Error> {
        let connector = value.get(CONNECTOR_TYPE_KEY);
        let r#type = value.get(SINK_TYPE_OPTION);
        match (connector, r#type) {
            (Some(c), Some(t)) => SinkFormatDesc::from_legacy_type(c, t),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_sink_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 as select t1.country from t1;";
        frontend.run_sql(sql).await.unwrap();

        let sql = r#"CREATE SINK snk1 FROM mv1
                    WITH (connector = 'jdbc', mysql.endpoint = '127.0.0.1:3306', mysql.table =
                        '<table_name>', mysql.database = '<database_name>', mysql.user = '<user_name>',
                        mysql.password = '<password>', type = 'append-only', force_append_only = 'true');"#.to_string();
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t1")
            .unwrap();
        assert_eq!(source.name, "t1");

        // Check table exists.
        let (table, schema_name) = catalog_reader
            .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv1")
            .unwrap();
        assert_eq!(table.name(), "mv1");

        // Check sink exists.
        let (sink, _) = catalog_reader
            .get_sink_by_name(DEFAULT_DATABASE_NAME, SchemaPath::Name(schema_name), "snk1")
            .unwrap();
        assert_eq!(sink.name, "snk1");
    }
}
