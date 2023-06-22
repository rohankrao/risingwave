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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::TableOption;
use risingwave_meta_model::MetadataModel;
use risingwave_meta_storage::MetaStore;
use risingwave_pb::catalog::{
    Connection, Database, Function, Index, Schema, Sink, Source, Table, View,
};

use super::{ConnectionId, DatabaseId, FunctionId, RelationId, SchemaId, SinkId, SourceId, ViewId};
use crate::manager::{IndexId, MetaSrvEnv, TableId};
use crate::{MetaError, MetaResult};

pub type Catalog = (
    Vec<Database>,
    Vec<Schema>,
    Vec<Table>,
    Vec<Source>,
    Vec<Sink>,
    Vec<Index>,
    Vec<View>,
    Vec<Function>,
    Vec<Connection>,
);

type DatabaseKey = String;
type SchemaKey = (DatabaseId, String);
type RelationKey = (DatabaseId, SchemaId, String);

/// [`DatabaseManager`] caches meta catalog information and maintains dependent relationship
/// between tables.
pub struct DatabaseManager {
    /// Cached database information.
    pub(super) databases: BTreeMap<DatabaseId, Database>,
    /// Cached schema information.
    pub(super) schemas: BTreeMap<SchemaId, Schema>,
    /// Cached source information.
    pub(super) sources: BTreeMap<SourceId, Source>,
    /// Cached sink information.
    pub(super) sinks: BTreeMap<SinkId, Sink>,
    /// Cached index information.
    pub(super) indexes: BTreeMap<IndexId, Index>,
    /// Cached table information.
    pub(super) tables: BTreeMap<TableId, Table>,
    /// Cached view information.
    pub(super) views: BTreeMap<ViewId, View>,
    /// Cached function information.
    pub(super) functions: BTreeMap<FunctionId, Function>,
    /// Cached connection information.
    pub(super) connections: BTreeMap<ConnectionId, Connection>,

    /// Relation reference count mapping.
    // TODO(zehua): avoid key conflicts after distinguishing table's and source's id generator.
    pub(super) relation_ref_count: HashMap<RelationId, usize>,
    // In-progress creation tracker.
    pub(super) in_progress_creation_tracker: HashSet<RelationKey>,
    // In-progress creating streaming job tracker: this is a temporary workaround to avoid clean up
    // creating streaming jobs.
    pub(super) in_progress_creation_streaming_job: HashMap<TableId, RelationKey>,
    // In-progress creating tables, including internal tables.
    pub(super) in_progress_creating_tables: HashMap<TableId, Table>,
}

impl DatabaseManager {
    pub async fn new<S: MetaStore>(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let databases = Database::list(env.meta_store()).await?;
        let schemas = Schema::list(env.meta_store()).await?;
        let sources = Source::list(env.meta_store()).await?;
        let sinks = Sink::list(env.meta_store()).await?;
        let tables = Table::list(env.meta_store()).await?;
        let indexes = Index::list(env.meta_store()).await?;
        let views = View::list(env.meta_store()).await?;
        let functions = Function::list(env.meta_store()).await?;
        let connections = Connection::list(env.meta_store()).await?;

        let mut relation_ref_count = HashMap::new();

        let databases = BTreeMap::from_iter(
            databases
                .into_iter()
                .map(|database| (database.id, database)),
        );
        let schemas = BTreeMap::from_iter(schemas.into_iter().map(|schema| (schema.id, schema)));
        let sources = BTreeMap::from_iter(sources.into_iter().map(|source| {
            // TODO(weili): wait for yezizp to refactor ref cnt
            if let Some(connection_id) = source.connection_id {
                *relation_ref_count.entry(connection_id).or_default() += 1;
            }
            (source.id, source)
        }));
        let sinks = BTreeMap::from_iter(sinks.into_iter().map(|sink| {
            for depend_relation_id in &sink.dependent_relations {
                *relation_ref_count.entry(*depend_relation_id).or_default() += 1;
            }
            (sink.id, sink)
        }));
        let indexes = BTreeMap::from_iter(indexes.into_iter().map(|index| (index.id, index)));
        let tables = BTreeMap::from_iter(tables.into_iter().map(|table| {
            for depend_relation_id in &table.dependent_relations {
                *relation_ref_count.entry(*depend_relation_id).or_default() += 1;
            }
            (table.id, table)
        }));
        let views = BTreeMap::from_iter(views.into_iter().map(|view| {
            for depend_relation_id in &view.dependent_relations {
                *relation_ref_count.entry(*depend_relation_id).or_default() += 1;
            }
            (view.id, view)
        }));
        let functions = BTreeMap::from_iter(functions.into_iter().map(|f| (f.id, f)));
        let connections = BTreeMap::from_iter(connections.into_iter().map(|c| (c.id, c)));

        Ok(Self {
            databases,
            schemas,
            sources,
            sinks,
            views,
            tables,
            indexes,
            functions,
            connections,
            relation_ref_count,
            in_progress_creation_tracker: HashSet::default(),
            in_progress_creation_streaming_job: HashMap::default(),
            in_progress_creating_tables: HashMap::default(),
        })
    }

    pub fn get_catalog(&self) -> Catalog {
        (
            self.databases.values().cloned().collect_vec(),
            self.schemas.values().cloned().collect_vec(),
            self.tables.values().cloned().collect_vec(),
            self.sources.values().cloned().collect_vec(),
            self.sinks.values().cloned().collect_vec(),
            self.indexes.values().cloned().collect_vec(),
            self.views.values().cloned().collect_vec(),
            self.functions.values().cloned().collect_vec(),
            self.connections.values().cloned().collect_vec(),
        )
    }

    pub fn check_relation_name_duplicated(&self, relation_key: &RelationKey) -> MetaResult<()> {
        if self.tables.values().any(|x| {
            x.database_id == relation_key.0
                && x.schema_id == relation_key.1
                && x.name.eq(&relation_key.2)
        }) {
            Err(MetaError::catalog_duplicated("table", &relation_key.2))
        } else if self.sources.values().any(|x| {
            x.database_id == relation_key.0
                && x.schema_id == relation_key.1
                && x.name.eq(&relation_key.2)
        }) {
            Err(MetaError::catalog_duplicated("source", &relation_key.2))
        } else if self.indexes.values().any(|x| {
            x.database_id == relation_key.0
                && x.schema_id == relation_key.1
                && x.name.eq(&relation_key.2)
        }) {
            Err(MetaError::catalog_duplicated("index", &relation_key.2))
        } else if self.sinks.values().any(|x| {
            x.database_id == relation_key.0
                && x.schema_id == relation_key.1
                && x.name.eq(&relation_key.2)
        }) {
            Err(MetaError::catalog_duplicated("sink", &relation_key.2))
        } else if self.views.values().any(|x| {
            x.database_id == relation_key.0
                && x.schema_id == relation_key.1
                && x.name.eq(&relation_key.2)
        }) {
            Err(MetaError::catalog_duplicated("view", &relation_key.2))
        } else {
            Ok(())
        }
    }

    pub fn check_function_duplicated(&self, function: &Function) -> MetaResult<()> {
        if self.functions.values().any(|x| {
            x.database_id == function.database_id
                && x.schema_id == function.schema_id
                && x.name.eq(&function.name)
                && x.arg_types == function.arg_types
        }) {
            Err(MetaError::catalog_duplicated("function", &function.name))
        } else {
            Ok(())
        }
    }

    pub fn check_connection_name_duplicated(&self, relation_key: &RelationKey) -> MetaResult<()> {
        if self.connections.values().any(|conn| {
            conn.database_id == relation_key.0
                && conn.schema_id == relation_key.1
                && conn.name.eq(&relation_key.2)
        }) {
            Err(MetaError::catalog_duplicated("connection", &relation_key.2))
        } else {
            Ok(())
        }
    }

    pub fn list_databases(&self) -> Vec<Database> {
        self.databases.values().cloned().collect_vec()
    }

    pub fn list_creating_tables(&self) -> Vec<Table> {
        self.in_progress_creating_tables
            .values()
            .cloned()
            .collect_vec()
    }

    pub fn list_tables(&self) -> Vec<Table> {
        self.tables.values().cloned().collect_vec()
    }

    pub fn get_table(&self, table_id: TableId) -> Option<&Table> {
        self.tables.get(&table_id)
    }

    pub fn get_all_table_options(&self) -> HashMap<TableId, TableOption> {
        self.tables
            .iter()
            .map(|(id, table)| (*id, TableOption::build_table_option(&table.properties)))
            .collect()
    }

    pub fn list_table_ids(&self, schema_id: SchemaId) -> Vec<TableId> {
        self.tables
            .values()
            .filter(|table| table.schema_id == schema_id)
            .map(|table| table.id)
            .collect_vec()
    }

    pub fn list_sources(&self) -> Vec<Source> {
        self.sources.values().cloned().collect_vec()
    }

    pub fn list_source_ids(&self, schema_id: SchemaId) -> Vec<SourceId> {
        self.sources
            .values()
            .filter(|&s| s.schema_id == schema_id)
            .map(|s| s.id)
            .collect_vec()
    }

    pub fn get_connection(&self, connection_id: ConnectionId) -> Option<&Connection> {
        self.connections.get(&connection_id)
    }

    pub fn list_connections(&self) -> Vec<Connection> {
        self.connections.values().cloned().collect()
    }

    pub fn list_stream_job_ids(&self) -> impl Iterator<Item = RelationId> + '_ {
        self.tables
            .keys()
            .copied()
            .chain(self.sinks.keys().copied())
            .chain(self.indexes.keys().copied())
    }

    pub fn check_database_duplicated(&self, database_key: &DatabaseKey) -> MetaResult<()> {
        if self.databases.values().any(|x| x.name.eq(database_key)) {
            Err(MetaError::catalog_duplicated("database", database_key))
        } else {
            Ok(())
        }
    }

    pub fn check_schema_duplicated(&self, schema_key: &SchemaKey) -> MetaResult<()> {
        if self
            .schemas
            .values()
            .any(|x| x.database_id == schema_key.0 && x.name.eq(&schema_key.1))
        {
            Err(MetaError::catalog_duplicated("schema", &schema_key.1))
        } else {
            Ok(())
        }
    }

    pub fn schema_is_empty(&self, schema_id: SchemaId) -> bool {
        self.tables.values().all(|t| t.schema_id != schema_id)
            && self.sources.values().all(|s| s.schema_id != schema_id)
            && self.sinks.values().all(|s| s.schema_id != schema_id)
            && self.indexes.values().all(|i| i.schema_id != schema_id)
            && self.views.values().all(|v| v.schema_id != schema_id)
    }

    pub fn increase_ref_count(&mut self, relation_id: RelationId) {
        *self.relation_ref_count.entry(relation_id).or_insert(0) += 1;
    }

    pub fn decrease_ref_count(&mut self, relation_id: RelationId) {
        match self.relation_ref_count.entry(relation_id) {
            Entry::Occupied(mut o) => {
                *o.get_mut() -= 1;
                if *o.get() == 0 {
                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }

    pub fn has_creation_in_database(&self, database_id: DatabaseId) -> bool {
        self.in_progress_creation_tracker
            .iter()
            .any(|relation_key| relation_key.0 == database_id)
    }

    pub fn has_creation_in_schema(&self, schema_id: SchemaId) -> bool {
        self.in_progress_creation_tracker
            .iter()
            .any(|relation_key| relation_key.1 == schema_id)
    }

    pub fn has_in_progress_creation(&self, relation: &RelationKey) -> bool {
        self.in_progress_creation_tracker
            .contains(&relation.clone())
    }

    pub fn mark_creating(&mut self, relation: &RelationKey) {
        self.in_progress_creation_tracker.insert(relation.clone());
    }

    pub fn mark_creating_streaming_job(&mut self, table_id: TableId, key: RelationKey) {
        self.in_progress_creation_streaming_job
            .insert(table_id, key);
    }

    pub fn unmark_creating(&mut self, relation: &RelationKey) {
        self.in_progress_creation_tracker.remove(&relation.clone());
    }

    pub fn unmark_creating_streaming_job(&mut self, table_id: TableId) {
        self.in_progress_creation_streaming_job.remove(&table_id);
    }

    pub fn find_creating_streaming_job_id(&self, key: &RelationKey) -> Option<TableId> {
        self.in_progress_creation_streaming_job
            .iter()
            .find(|(_, v)| *v == key)
            .map(|(k, _)| *k)
    }

    pub fn all_creating_streaming_jobs(&self) -> impl Iterator<Item = TableId> + '_ {
        self.in_progress_creation_streaming_job.keys().cloned()
    }

    pub fn mark_creating_tables(&mut self, tables: &[Table]) {
        self.in_progress_creating_tables
            .extend(tables.iter().map(|t| (t.id, t.clone())));
    }

    pub fn unmark_creating_tables(&mut self, table_ids: &[TableId]) {
        for id in table_ids {
            self.in_progress_creating_tables.remove(id);
        }
    }

    pub fn ensure_database_id(&self, database_id: DatabaseId) -> MetaResult<()> {
        if self.databases.contains_key(&database_id) {
            Ok(())
        } else {
            Err(MetaError::catalog_id_not_found("database", database_id))
        }
    }

    pub fn ensure_schema_id(&self, schema_id: SchemaId) -> MetaResult<()> {
        if self.schemas.contains_key(&schema_id) {
            Ok(())
        } else {
            Err(MetaError::catalog_id_not_found("schema", schema_id))
        }
    }

    pub fn ensure_view_id(&self, view_id: ViewId) -> MetaResult<()> {
        if self.views.contains_key(&view_id) {
            Ok(())
        } else {
            Err(MetaError::catalog_id_not_found("view", view_id))
        }
    }

    pub fn ensure_table_id(&self, table_id: TableId) -> MetaResult<()> {
        if self.tables.contains_key(&table_id) {
            Ok(())
        } else {
            Err(MetaError::catalog_id_not_found("table", table_id))
        }
    }

    pub fn ensure_source_id(&self, source_id: SourceId) -> MetaResult<()> {
        if self.sources.contains_key(&source_id) {
            Ok(())
        } else {
            Err(MetaError::catalog_id_not_found("source", source_id))
        }
    }

    pub fn ensure_sink_id(&self, sink_id: SinkId) -> MetaResult<()> {
        if self.sinks.contains_key(&sink_id) {
            Ok(())
        } else {
            Err(MetaError::catalog_id_not_found("sink", sink_id))
        }
    }

    pub fn ensure_index_id(&self, index_id: IndexId) -> MetaResult<()> {
        if self.indexes.contains_key(&index_id) {
            Ok(())
        } else {
            Err(MetaError::catalog_id_not_found("index", index_id))
        }
    }

    pub fn ensure_connection_id(&self, connection_id: ConnectionId) -> MetaResult<()> {
        if self.connections.contains_key(&connection_id) {
            Ok(())
        } else {
            Err(MetaError::catalog_id_not_found("connection", connection_id))
        }
    }

    // TODO(zehua): refactor when using SourceId.
    pub fn ensure_table_view_or_source_id(&self, table_id: &TableId) -> MetaResult<()> {
        if self.tables.contains_key(table_id)
            || self.sources.contains_key(table_id)
            || self.views.contains_key(table_id)
        {
            Ok(())
        } else {
            Err(MetaError::catalog_id_not_found(
                "table, view or source",
                *table_id,
            ))
        }
    }
}
