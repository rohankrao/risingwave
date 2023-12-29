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

use std::collections::BTreeMap;

use risingwave_pb::catalog::PbSchemaRegistryNameStrategy;

use super::schema_registry::{
    get_subject_by_strategy, handle_sr_list, name_strategy_from_str, Client, Subject,
};
use super::SchemaFetchError;

const MESSAGE_NAME_KEY: &str = "message";
const KEY_MESSAGE_NAME_KEY: &str = "key.message";
const SCHEMA_LOCATION_KEY: &str = "schema.location";
const SCHEMA_REGISTRY_KEY: &str = "schema.registry";
const NAME_STRATEGY_KEY: &str = "schema.registry.name.strategy";

pub struct SchemaLoader {
    pub client: Client,
    pub name_strategy: PbSchemaRegistryNameStrategy,
    pub topic: String,
    pub key_record_name: Option<String>,
    pub val_record_name: Option<String>,
}

impl SchemaLoader {
    pub fn from_format_options(
        topic: &str,
        format_options: &BTreeMap<String, String>,
    ) -> Result<Self, SchemaFetchError> {
        let schema_location = format_options
            .get(SCHEMA_REGISTRY_KEY)
            .ok_or_else(|| SchemaFetchError(format!("{SCHEMA_REGISTRY_KEY} required")))?;
        let client_config = format_options.into();
        let urls = handle_sr_list(schema_location)?;
        let client = Client::new(urls, &client_config)?;

        let name_strategy = format_options
            .get(NAME_STRATEGY_KEY)
            .map(|s| {
                name_strategy_from_str(s)
                    .ok_or_else(|| SchemaFetchError(format!("unrecognized strategy {s}")))
            })
            .transpose()?
            .unwrap_or_default();
        let key_record_name = format_options.get(KEY_MESSAGE_NAME_KEY).cloned();
        let val_record_name = format_options.get(MESSAGE_NAME_KEY).cloned();

        Ok(Self {
            client,
            name_strategy,
            topic: topic.into(),
            key_record_name,
            val_record_name,
        })
    }

    pub async fn load_key_schema<O>(&self) -> Result<(i32, O), SchemaFetchError>
    where
        // O: TryFrom<(Subject, Vec<Subject>), Error = SchemaFetchError>,
        O: LoadedSchema,
    {
        let subject = get_subject_by_strategy(
            &self.name_strategy,
            &self.topic,
            self.key_record_name.as_deref(),
            true,
        )?;
        // let loaded = self.client.get_schema_by_subject(&subject).await?;
        let (primary_subject, dependency_subjects) =
            self.client.get_subject_and_references(&subject).await?;
        let schema_id = primary_subject.schema.id;
        let o = O::compile(primary_subject, dependency_subjects)?;
        Ok((schema_id, o))
    }
}

pub trait LoadedSchema: Sized {
    fn compile(primary: Subject, references: Vec<Subject>) -> Result<Self, SchemaFetchError>;
}

// load key: returns none for url or pb, some for sr-avro
// load value:

// post-fetch enc-specific parsing
// * url: avro::parse_str or pb::decode
// * sr: avro::parse_str or compile + pb::decode
