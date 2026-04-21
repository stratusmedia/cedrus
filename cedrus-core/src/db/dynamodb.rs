use std::collections::HashMap;

use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest};
use uuid::Uuid;

use cedrus_cedar::{Entity, EntityUid, Policy, PolicyId, Schema, Template, TemplateLink};

use crate::{
    PageHash, PageList, Query, Selector,
    core::{
        self, IdentitySource,
        project::{ApiKey, Project},
    },
};

use super::{Database, DatabaseError};

const PK: &str = "PK";
const SK: &str = "SK";

const GSI1: &str = "GSI1";
const GSI1_PK: &str = "GSI1PK";

const PROJECT_TYPE: &str = "P";
const PROJECT_APIKEY_TYPE: &str = "PAK";
const PROJECT_IDENTITY_SOURCE_TYPE: &str = "PIS";
const PROJECT_SCHEMA_TYPE: &str = "PS";
const PROJECT_ENTITY_TYPE: &str = "PE";
const PROJECT_POLICY_TYPE: &str = "PP";
const PROJECT_TEMPLATE_TYPE: &str = "PT";
const PROJECT_TEMPLATE_LINK_TYPE: &str = "PTL";

/*
Types of items in the table:

Project:
PK: "P#[PROJECT_UUID]"
SK: "P#[PROJECT_UUID]"
GSI1PK: "P"

API Key:
PK: "P#[PROJECT_UUID]"
SK: "P#[PROJECT_UUID]#PAK#[API_KEY_UUID]"
GSI1PK: "PAK"

Identity Source:
PK: "P#[PROJECT_UUID]"
SK: "P#[PROJECT_UUID]#PIS"
GSI1PK: "PI"

Schema:
PK: "P#[PROJECT_UUID]"
SK: "P#[PROJECT_UUID]#PS"
GSI1PK: "PS"

Entity:
PK: "P#[PROJECT_UUID]"
SK: "P#[PROJECT_UUID]#PE#[ENTITY_UID]"
GSI1PK: "PE"

Policy:
PK: "P#[PROJECT_UUID]"
SK: "P#[PROJECT_UUID]#PP#[POLICY_ID]"
GSI1PK: "PP"

Template:
PK: "P#[PROJECT_UUID]"
SK: "P#[PROJECT_UUID]#PT#[TEMPLATE_ID]"
GSI1PK: "PT"

Template Link:
PK: "P#[PROJECT_UUID]"
SK: "P#[PROJECT_UUID]#PTL#[TEMPLATE_LINK_ID]"
GSI1PK: "PTL"
*/

const DEFAULT_ATT: &str = "__DEFAULT__";
const SCHEMA_ATT: &str = "schema";
const CREATED_AT_ATT: &str = "createdAt";
const UPDATED_AT_ATT: &str = "updatedAt";

#[derive(Debug)]
pub struct FilterExpression {
    pub expression: Option<String>,
    pub names: HashMap<String, String>,
    pub values: HashMap<String, AttributeValue>,
    pub limit: Option<i32>,
    pub start_key: Option<HashMap<String, AttributeValue>>,
}

impl Default for FilterExpression {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterExpression {
    pub fn new() -> FilterExpression {
        FilterExpression {
            expression: None,
            names: HashMap::new(),
            values: HashMap::new(),
            limit: None,
            start_key: None,
        }
    }

    pub fn new_with_query(query: &Query) -> Result<Self, DatabaseError> {
        let mut filter = FilterExpression::new();

        if let Some(selector) = query.selector.clone() {
            let mut expression = String::new();
            Self::selector_to_filter("".to_string(), selector, &mut expression, &mut filter);
            if !expression.trim().is_empty() {
                filter.expression = Some(expression);
            }
        }

        if query.limit > 0 {
            filter.limit = Some(query.limit as i32);
        }
        if let Some(start_key) = query.start_key.clone() {
            let key: serde_json::Value = serde_json::from_str(&start_key)?;
            let key: HashMap<String, AttributeValue> = serde_dynamo::to_item(key)?;
            filter.start_key = Some(key);
        }

        Ok(filter)
    }

    pub fn add_eq(&mut self, name: &str, value: AttributeValue) -> String {
        let x = self.names.len();
        let att_name = format!("#n{x}");
        self.names.insert(att_name.clone(), name.to_string());

        let x = self.values.len();
        let att_val = format!(":v{x}");
        self.values.insert(att_val.clone(), value);

        format!("{att_name} = {att_val}")
    }

    pub fn add_begins_with(&mut self, name: &str, value: AttributeValue) -> String {
        let x = self.names.len();
        let att_name = format!("#n{x}");
        self.names.insert(att_name.clone(), name.to_string());

        let x = self.values.len();
        let att_val = format!(":v{x}");
        self.values.insert(att_val.clone(), value);

        format!("begins_with({att_name}, {att_val})")
    }

    fn selector_to_filter(
        path: String,
        expr: Selector,
        expression: &mut String,
        filter: &mut FilterExpression,
    ) {
        match expr {
            Selector::And(val) => {
                let left = val[0].clone();
                Self::selector_to_filter(path.clone(), left, expression, filter);
                expression.push_str(" AND ");
                let right = val[1].clone();
                Self::selector_to_filter(path, right, expression, filter);
            }
            Selector::Or(val) => {
                let left = val[0].clone();
                Self::selector_to_filter(path.clone(), left, expression, filter);
                expression.push_str(" OR ");
                let right = val[1].clone();
                Self::selector_to_filter(path, right, expression, filter);
            }
            Selector::Eq(val) => {
                expression.push_str(&path);
                expression.push_str(" = ");
                Self::selector_to_filter(path, *val, expression, filter);
            }
            Selector::Neq(val) => {
                expression.push_str(&path);
                expression.push_str(" != ");
                Self::selector_to_filter(path, *val, expression, filter);
            }
            Selector::Gt(val) => {
                expression.push_str(&path);
                expression.push_str(" > ");
                Self::selector_to_filter(path, *val, expression, filter);
            }
            Selector::Gte(val) => {
                expression.push_str(&path);
                expression.push_str(" >= ");
                Self::selector_to_filter(path, *val, expression, filter);
            }
            Selector::Lt(val) => {
                expression.push_str(&path);
                expression.push_str(" < ");
                Self::selector_to_filter(path, *val, expression, filter);
            }
            Selector::Lte(val) => {
                expression.push_str(&path);
                expression.push_str(" <= ");
                Self::selector_to_filter(path, *val, expression, filter);
            }
            Selector::Exists(val) => {
                let str = if val {
                    format!("attribute_exists({path})")
                } else {
                    format!("attribute_not_exists({path})")
                };

                expression.push_str(&str);
            }
            Selector::In(_items) => {}
            Selector::Nin(_items) => {}
            Selector::Record(map) => {
                for (key, val) in map {
                    let x = filter.names.len();
                    let att_name = format!("#n{x}");
                    filter.names.insert(att_name.clone(), key);

                    let path = if path.is_empty() {
                        att_name
                    } else {
                        format!("{}.{}", path, att_name)
                    };

                    match val {
                        Selector::String(_) => {
                            expression.push_str(&path);
                            expression.push_str(" = ");
                        }
                        Selector::Number(_) => {
                            expression.push_str(&path);
                            expression.push_str(" = ");
                        }
                        Selector::Boolean(_) => {
                            expression.push_str(&path);
                            expression.push_str(" = ");
                        }
                        _ => {}
                    }

                    Self::selector_to_filter(path, val, expression, filter);
                }
            }
            Selector::String(val) => {
                let x = filter.values.len();
                let att_val = format!(":v{x}");
                expression.push_str(&att_val);
                filter.values.insert(att_val, AttributeValue::S(val));
            }
            Selector::Number(val) => {
                let x = filter.values.len();
                let att_val = format!(":v{x}");
                expression.push_str(&att_val);
                filter
                    .values
                    .insert(att_val, AttributeValue::N(val.to_string()));
            }
            Selector::Boolean(val) => {
                let x = filter.values.len();
                let att_val = format!(":v{x}");
                expression.push_str(&att_val);
                filter.values.insert(att_val, AttributeValue::Bool(val));
            }
        }
    }
}

pub struct DynamoDb {
    table_name: String,
    client: aws_sdk_dynamodb::Client,
}

impl DynamoDb {
    pub async fn new(conf: &core::DynamoDBConfig) -> Result<Self, DatabaseError> {
        let client = if let Some(endpoint_url) = &conf.endpoint_url {
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                //.test_credentials()
                // DynamoDB run locally uses port 8000 by default.
                .endpoint_url(endpoint_url)
                .load()
                .await;
            let dynamodb_local_config = aws_sdk_dynamodb::config::Builder::from(&config).build();
            aws_sdk_dynamodb::Client::from_conf(dynamodb_local_config)
        } else {
            let mut config = aws_config::from_env();
            if std::env::var("CEDRUS_IPV6").is_ok() {
                config = config.use_dual_stack(true);
            }
            aws_sdk_dynamodb::Client::new(&config.load().await)
        };

        let db = DynamoDb {
            table_name: conf.table_name.clone(),
            client,
        };

        Ok(db)
    }

    pub async fn init(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self
            .client
            .describe_table()
            .table_name(&self.table_name)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }

        let table = self
            .client
            .create_table()
            .table_name(&self.table_name)
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name(PK)
                    .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                    .build()?,
            )
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name(SK)
                    .key_type(aws_sdk_dynamodb::types::KeyType::Range)
                    .build()?,
            )
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name(PK)
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()?,
            )
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name(SK)
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()?,
            )
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name(GSI1_PK)
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()?,
            )
            .global_secondary_indexes(
                aws_sdk_dynamodb::types::GlobalSecondaryIndex::builder()
                    .index_name(GSI1)
                    .key_schema(
                        aws_sdk_dynamodb::types::KeySchemaElement::builder()
                            .attribute_name(GSI1_PK)
                            .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                            .build()?,
                    )
                    .projection(
                        aws_sdk_dynamodb::types::Projection::builder()
                            .projection_type(aws_sdk_dynamodb::types::ProjectionType::All)
                            .build(),
                    )
                    .build()?,
            )
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest);

        table.send().await?;

        Ok(())
    }

    pub fn empty_namespace_to_default(mut schema: Schema) -> Schema {
        if let Some(namespace) = schema.0.get("") {
            schema.0.insert(DEFAULT_ATT.to_string(), namespace.clone());
            schema.0.remove("");
        }
        schema
    }

    pub fn default_namespace_to_empty(mut schema: Schema) -> Schema {
        if let Some(namespace) = schema.0.get(DEFAULT_ATT) {
            schema.0.insert("".to_string(), namespace.clone());
            schema.0.remove(DEFAULT_ATT);
        }
        schema
    }

    fn add_indexes_to_item(
        &self,
        item: &mut HashMap<String, AttributeValue>,
        pk: &str,
        sk: &str,
        entity_type_pk: &str,
    ) {
        item.insert(PK.to_string(), AttributeValue::S(pk.to_string()));
        item.insert(SK.to_string(), AttributeValue::S(sk.to_string()));
        item.insert(
            GSI1_PK.to_string(),
            AttributeValue::S(entity_type_pk.to_string()),
        );
    }

    fn project_to_item(
        &self,
        project: &Project,
    ) -> Result<HashMap<String, AttributeValue>, DatabaseError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(project)?;

        item.insert(
            CREATED_AT_ATT.to_string(),
            AttributeValue::N(project.created_at.timestamp_millis().to_string()),
        );
        item.insert(
            UPDATED_AT_ATT.to_string(),
            AttributeValue::N(project.updated_at.timestamp_millis().to_string()),
        );

        let pk = format!("{}#{}", PROJECT_TYPE, project.id);
        self.add_indexes_to_item(&mut item, &pk, &pk, PROJECT_TYPE);

        Ok(item)
    }

    fn project_from_item(
        &self,
        item: &mut HashMap<String, AttributeValue>,
    ) -> Result<Project, DatabaseError> {
        let Some(created_at_att) = item.get(CREATED_AT_ATT) else {
            return Err(DatabaseError::MissingAttribute(CREATED_AT_ATT.to_string()));
        };
        let Some(updated_at_att) = item.get(UPDATED_AT_ATT) else {
            return Err(DatabaseError::MissingAttribute(UPDATED_AT_ATT.to_string()));
        };
        let Ok(created_at_val) = created_at_att.as_n() else {
            return Err(DatabaseError::InvalidAttribute(CREATED_AT_ATT.to_string()));
        };
        let Ok(updated_at_val) = updated_at_att.as_n() else {
            return Err(DatabaseError::InvalidAttribute(UPDATED_AT_ATT.to_string()));
        };
        let Ok(created_at_int) = created_at_val.parse::<i64>() else {
            return Err(DatabaseError::InvalidAttribute(CREATED_AT_ATT.to_string()));
        };
        let Ok(updated_at_int) = updated_at_val.parse::<i64>() else {
            return Err(DatabaseError::InvalidAttribute(UPDATED_AT_ATT.to_string()));
        };
        let Some(created_at) = chrono::DateTime::from_timestamp_millis(created_at_int) else {
            return Err(DatabaseError::InvalidAttribute(CREATED_AT_ATT.to_string()));
        };
        let Some(updated_at) = chrono::DateTime::from_timestamp_millis(updated_at_int) else {
            return Err(DatabaseError::InvalidAttribute(UPDATED_AT_ATT.to_string()));
        };

        item.insert(
            CREATED_AT_ATT.to_string(),
            AttributeValue::S(created_at.to_rfc3339()),
        );
        item.insert(
            UPDATED_AT_ATT.to_string(),
            AttributeValue::S(updated_at.to_rfc3339()),
        );

        Ok(serde_dynamo::from_item(item.clone())?)
    }

    fn project_schema_to_item(
        &self,
        project_id: &Uuid,
        schema: &Schema,
    ) -> Result<HashMap<String, AttributeValue>, DatabaseError> {
        let schema = DynamoDb::empty_namespace_to_default(schema.clone());

        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert(
            SCHEMA_ATT.to_string(),
            AttributeValue::M(serde_dynamo::to_item(schema)?),
        );

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_SCHEMA_TYPE);
        self.add_indexes_to_item(&mut item, &pk, &sk, PROJECT_SCHEMA_TYPE);

        Ok(item)
    }

    fn project_schema_from_item(
        &self,
        item: &HashMap<String, AttributeValue>,
    ) -> Result<Schema, DatabaseError> {
        let Some(schema_att) = item.get(SCHEMA_ATT) else {
            return Err(DatabaseError::MissingAttribute(SCHEMA_ATT.to_string()));
        };
        let Ok(schema) = schema_att.as_m() else {
            return Err(DatabaseError::InvalidAttribute(SCHEMA_ATT.to_string()));
        };
        Ok(DynamoDb::default_namespace_to_empty(
            serde_dynamo::from_item(schema.clone())?,
        ))
    }

    fn project_apikey_to_item(
        &self,
        project_id: &Uuid,
        apikey: &ApiKey,
    ) -> Result<HashMap<String, AttributeValue>, DatabaseError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(apikey)?;

        item.insert(
            CREATED_AT_ATT.to_string(),
            AttributeValue::N(apikey.created_at.timestamp_millis().to_string()),
        );
        item.insert(
            UPDATED_AT_ATT.to_string(),
            AttributeValue::N(apikey.updated_at.timestamp_millis().to_string()),
        );

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#{}", pk, PROJECT_APIKEY_TYPE, apikey.id);

        self.add_indexes_to_item(&mut item, &pk, &sk, PROJECT_APIKEY_TYPE);

        Ok(item)
    }

    fn project_apikey_from_item(
        &self,
        item: &mut HashMap<String, AttributeValue>,
    ) -> Result<ApiKey, DatabaseError> {
        let Some(created_at_att) = item.get(CREATED_AT_ATT) else {
            return Err(DatabaseError::MissingAttribute(CREATED_AT_ATT.to_string()));
        };
        let Some(updated_at_att) = item.get(UPDATED_AT_ATT) else {
            return Err(DatabaseError::MissingAttribute(UPDATED_AT_ATT.to_string()));
        };
        let Ok(created_at_val) = created_at_att.as_n() else {
            return Err(DatabaseError::InvalidAttribute(CREATED_AT_ATT.to_string()));
        };
        let Ok(updated_at_val) = updated_at_att.as_n() else {
            return Err(DatabaseError::InvalidAttribute(UPDATED_AT_ATT.to_string()));
        };
        let Ok(created_at_int) = created_at_val.parse::<i64>() else {
            return Err(DatabaseError::InvalidAttribute(CREATED_AT_ATT.to_string()));
        };
        let Ok(updated_at_int) = updated_at_val.parse::<i64>() else {
            return Err(DatabaseError::InvalidAttribute(UPDATED_AT_ATT.to_string()));
        };
        let Some(created_at) = chrono::DateTime::from_timestamp_millis(created_at_int) else {
            return Err(DatabaseError::InvalidAttribute(CREATED_AT_ATT.to_string()));
        };
        let Some(updated_at) = chrono::DateTime::from_timestamp_millis(updated_at_int) else {
            return Err(DatabaseError::InvalidAttribute(UPDATED_AT_ATT.to_string()));
        };

        item.insert(
            CREATED_AT_ATT.to_string(),
            AttributeValue::S(created_at.to_rfc3339()),
        );
        item.insert(
            UPDATED_AT_ATT.to_string(),
            AttributeValue::S(updated_at.to_rfc3339()),
        );

        Ok(serde_dynamo::from_item(item.clone())?)
    }

    fn project_identity_source_to_item(
        &self,
        project_id: &Uuid,
        identity_source: &IdentitySource,
    ) -> Result<HashMap<String, AttributeValue>, DatabaseError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(identity_source)?;

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_IDENTITY_SOURCE_TYPE);
        self.add_indexes_to_item(&mut item, &pk, &sk, PROJECT_IDENTITY_SOURCE_TYPE);

        Ok(item)
    }

    fn project_identity_source_from_item(
        &self,
        item: &HashMap<String, AttributeValue>,
    ) -> Result<IdentitySource, DatabaseError> {
        Ok(serde_dynamo::from_item(item.clone())?)
    }

    fn project_entity_to_item(
        &self,
        project_id: &Uuid,
        entity: &Entity,
    ) -> Result<HashMap<String, AttributeValue>, DatabaseError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(entity)?;

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#{}", pk, PROJECT_ENTITY_TYPE, entity.uid());
        self.add_indexes_to_item(&mut item, &pk, &sk, PROJECT_ENTITY_TYPE);

        Ok(item)
    }

    fn project_entity_from_item(
        &self,
        item: &HashMap<String, AttributeValue>,
    ) -> Result<Entity, DatabaseError> {
        Ok(serde_dynamo::from_item(item.clone())?)
    }

    fn project_policy_to_item(
        &self,
        project_id: &Uuid,
        policy_id: &PolicyId,
        policy: &Policy,
    ) -> Result<HashMap<String, AttributeValue>, DatabaseError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(policy)?;

        item.insert(
            "policyId".to_string(),
            AttributeValue::S(policy_id.to_string()),
        );

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#{}", pk, PROJECT_POLICY_TYPE, policy_id);
        self.add_indexes_to_item(&mut item, &pk, &sk, PROJECT_POLICY_TYPE);

        Ok(item)
    }

    fn project_policy_from_item(
        &self,
        item: &HashMap<String, AttributeValue>,
    ) -> Result<Policy, DatabaseError> {
        Ok(serde_dynamo::from_item(item.clone())?)
    }

    fn project_template_to_item(
        &self,
        project_id: &Uuid,
        policy_id: &PolicyId,
        template: &Template,
    ) -> Result<HashMap<String, AttributeValue>, DatabaseError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(template)?;

        item.insert(
            "policyId".to_string(),
            AttributeValue::S(policy_id.to_string()),
        );

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#{}", pk, PROJECT_TEMPLATE_TYPE, policy_id);
        self.add_indexes_to_item(&mut item, &pk, &sk, PROJECT_TEMPLATE_TYPE);

        Ok(item)
    }

    fn project_template_from_item(
        &self,
        item: &HashMap<String, AttributeValue>,
    ) -> Result<Template, DatabaseError> {
        Ok(serde_dynamo::from_item(item.clone())?)
    }

    fn project_template_link_to_item(
        &self,
        project_id: &Uuid,
        link: &TemplateLink,
    ) -> Result<HashMap<String, AttributeValue>, DatabaseError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(link)?;

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#{}", pk, PROJECT_TEMPLATE_LINK_TYPE, link.new_id);
        self.add_indexes_to_item(&mut item, &pk, &sk, PROJECT_TEMPLATE_LINK_TYPE);

        Ok(item)
    }

    fn project_template_link_from_item(
        &self,
        item: &HashMap<String, AttributeValue>,
    ) -> Result<TemplateLink, DatabaseError> {
        Ok(serde_dynamo::from_item(item.clone())?)
    }

    async fn batch_write_item(
        &self,
        request_items: Vec<WriteRequest>,
    ) -> Result<(), DatabaseError> {
        for chunk in request_items.chunks(25) {
            self.client
                .batch_write_item()
                .request_items(&self.table_name, chunk.to_vec())
                .send()
                .await
                .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Database for DynamoDb {
    async fn projects_load(&self, query: &Query) -> Result<PageList<Project>, DatabaseError> {
        let mut filter = FilterExpression::new_with_query(query)?;

        let condition = filter.add_eq("GSI1PK", AttributeValue::S(PROJECT_TYPE.to_string()));

        let mut stream = self
            .client
            .query()
            .table_name(&self.table_name)
            .index_name(GSI1)
            .key_condition_expression(condition)
            .set_filter_expression(filter.expression)
            .set_expression_attribute_names(Some(filter.names))
            .set_expression_attribute_values(Some(filter.values))
            .set_limit(filter.limit)
            .set_exclusive_start_key(filter.start_key)
            .into_paginator()
            .send();

        let mut last_key = None;
        let mut datas = Vec::new();
        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;
            for mut item in page.items.unwrap_or_default() {
                datas.push(Self::project_from_item(&self, &mut item)?);
            }
            if let Some(key) = page.last_evaluated_key {
                let value: serde_json::Value = serde_dynamo::from_item(key)?;
                last_key = Some(
                    serde_json::to_string(&value)
                        .map_err(|e| DatabaseError::SerializationError(e.to_string()))?,
                );
            }
        }

        Ok(PageList::new(datas, last_key))
    }

    async fn project_load(&self, id: &Uuid) -> Result<Option<Project>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, id);

        let Some(mut item) = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(PK, AttributeValue::S(pk.clone()))
            .key(SK, AttributeValue::S(pk))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?
            .item
        else {
            return Ok(None);
        };

        let project = Self::project_from_item(&self, &mut item).ok();
        Ok(project)
    }

    async fn project_save(&self, project: &Project) -> Result<(), DatabaseError> {
        let item = self.project_to_item(project)?;
        self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;

        Ok(())
    }

    async fn project_remove(&self, id: &Uuid) -> Result<(), DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, id);

        let mut request_items = Vec::new();

        let mut stream = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("#PK = :PK")
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_values(":PK", AttributeValue::S(pk))
            .into_paginator()
            .send();

        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;
            for item in &page.items.unwrap_or_default() {
                let pk = item.get("PK").ok_or_else(|| {
                    tracing::error!("Missing PK in item: {:?}", item);
                    DatabaseError::MissingAttribute("PK".to_string())
                })?;
                let sk = item.get("SK").ok_or_else(|| {
                    tracing::error!("Missing SK in item: {:?}", item);
                    DatabaseError::MissingAttribute("SK".to_string())
                })?;

                let request = WriteRequest::builder()
                    .delete_request(
                        DeleteRequest::builder()
                            .key("PK", pk.clone())
                            .key("SK", sk.clone())
                            .build()
                            .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                    )
                    .build();
                request_items.push(request);
            }
        }

        let pk = format!("{}#{}", PROJECT_TYPE, Uuid::nil());
        let sk = format!("{}#{}#{}", pk, PROJECT_TEMPLATE_LINK_TYPE, id);
        let mut stream = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("#PK = :PK AND begins_with(#SK, :SK)")
            .expression_attribute_names("#PK", "PK")
            .expression_attribute_names("#SK", "SK")
            .expression_attribute_values(
                ":PK",
                aws_sdk_dynamodb::types::AttributeValue::S(pk.clone()),
            )
            .expression_attribute_values(":SK", aws_sdk_dynamodb::types::AttributeValue::S(sk))
            .into_paginator()
            .send();

        while let Some(page) = stream.next().await {
            let page = page.map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?;
            for item in &page.items.unwrap_or_default() {
                let pk = item.get("PK").ok_or_else(|| {
                    tracing::error!("Missing PK in item: {:?}", item);
                    DatabaseError::MissingAttribute("PK".to_string())
                })?;
                let sk = item.get("SK").ok_or_else(|| {
                    tracing::error!("Missing SK in item: {:?}", item);
                    DatabaseError::MissingAttribute("SK".to_string())
                })?;

                let request = WriteRequest::builder()
                    .delete_request(
                        DeleteRequest::builder()
                            .key("PK", pk.clone())
                            .key("SK", sk.clone())
                            .build()
                            .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                    )
                    .build();
                request_items.push(request);
            }
        }

        self.batch_write_item(request_items).await?;

        let uid = EntityUid::new(
            crate::core::project::PROJECT_ENTITY_TYPE.to_string(),
            id.to_string(),
        );

        let pk = format!("{}#{}", PROJECT_TYPE, Uuid::nil());
        let sk = format!("{}#{}#{}", pk, PROJECT_ENTITY_TYPE, uid.to_string());

        self.client
            .delete_item()
            .table_name(&self.table_name)
            .key(PK, AttributeValue::S(pk))
            .key(SK, AttributeValue::S(sk))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;

        Ok(())
    }

    async fn project_apikeys_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<ApiKey>, DatabaseError> {
        let mut filter = FilterExpression::new_with_query(query)?;

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_APIKEY_TYPE);

        let att1 = filter.add_eq("PK", AttributeValue::S(pk));
        let att2 = filter.add_begins_with("SK", AttributeValue::S(sk));
        let condition = format!("{} AND {}", att1, att2);

        let mut stream = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression(condition)
            .set_filter_expression(filter.expression)
            .set_expression_attribute_names(Some(filter.names))
            .set_expression_attribute_values(Some(filter.values))
            .set_limit(filter.limit)
            .set_exclusive_start_key(filter.start_key)
            .into_paginator()
            .send();

        let mut last_key = None;
        let mut datas = Vec::new();
        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;
            for item in page.items.unwrap_or_default() {
                datas.push(Self::project_apikey_from_item(&self, &mut item.clone())?);
            }
            if let Some(key) = page.last_evaluated_key {
                let value: serde_json::Value = serde_dynamo::from_item(key)?;
                last_key = Some(
                    serde_json::to_string(&value)
                        .map_err(|e| DatabaseError::SerializationError(e.to_string()))?,
                );
            }
        }

        Ok(PageList::new(datas, last_key))
    }

    async fn project_apikeys_save(
        &self,
        project_id: &Uuid,
        apikeys: &Vec<ApiKey>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for apikey in apikeys {
            let item = self.project_apikey_to_item(project_id, apikey)?;
            let request = WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();
            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_apikeys_remove(
        &self,
        project_id: &Uuid,
        ids: &Vec<Uuid>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for id in ids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_APIKEY_TYPE, id);

            let key_map = vec![
                (PK.to_string(), AttributeValue::S(pk)),
                (SK.to_string(), AttributeValue::S(sk)),
            ]
            .into_iter()
            .collect();

            let request = WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .set_key(Some(key_map))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();
            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_identity_source_load(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<IdentitySource>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_IDENTITY_SOURCE_TYPE);

        let Some(item) = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(PK, AttributeValue::S(pk))
            .key(SK, AttributeValue::S(sk))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?
            .item
        else {
            return Ok(None);
        };

        let identity_source = Self::project_identity_source_from_item(&self, &item).ok();
        Ok(identity_source)
    }

    async fn project_identity_source_save(
        &self,
        project_id: &Uuid,
        identity_source: &IdentitySource,
    ) -> Result<(), DatabaseError> {
        let item = self.project_identity_source_to_item(project_id, identity_source)?;
        self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;

        Ok(())
    }

    async fn project_identity_source_remove(&self, project_id: &Uuid) -> Result<(), DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_IDENTITY_SOURCE_TYPE);

        self.client
            .delete_item()
            .table_name(&self.table_name)
            .key(PK, AttributeValue::S(pk))
            .key(SK, AttributeValue::S(sk))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;

        Ok(())
    }

    async fn project_schema_load(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<Schema>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_SCHEMA_TYPE);

        let Some(item) = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(PK, AttributeValue::S(pk))
            .key(SK, AttributeValue::S(sk))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?
            .item
        else {
            return Ok(None);
        };

        let schema = Self::project_schema_from_item(&self, &item)?;

        Ok(Some(schema))
    }

    async fn project_schema_save(
        &self,
        project_id: &Uuid,
        schema: &Schema,
    ) -> Result<(), DatabaseError> {
        let item = self.project_schema_to_item(project_id, schema)?;
        self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;

        Ok(())
    }

    async fn project_schema_remove(&self, project_id: &Uuid) -> Result<(), DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_SCHEMA_TYPE);

        self.client
            .delete_item()
            .table_name(&self.table_name)
            .key(PK, AttributeValue::S(pk))
            .key(SK, AttributeValue::S(sk))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;

        Ok(())
    }

    async fn project_entities_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<Entity>, DatabaseError> {
        let mut filter = FilterExpression::new_with_query(query)?;

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_ENTITY_TYPE);

        let att1 = filter.add_eq("PK", AttributeValue::S(pk));
        let att2 = filter.add_begins_with("SK", AttributeValue::S(sk));
        let condition = format!("{} AND {}", att1, att2);

        let mut stream = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression(condition)
            .set_filter_expression(filter.expression)
            .set_expression_attribute_names(Some(filter.names))
            .set_expression_attribute_values(Some(filter.values))
            .set_limit(filter.limit)
            .set_exclusive_start_key(filter.start_key)
            .into_paginator()
            .send();

        let mut last_key = None;
        let mut datas = Vec::new();
        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;
            for item in page.items.unwrap_or_default() {
                datas.push(Self::project_entity_from_item(&self, &item)?);
            }
            if let Some(key) = page.last_evaluated_key {
                let value: serde_json::Value = serde_dynamo::from_item(key)?;
                last_key = Some(
                    serde_json::to_string(&value)
                        .map_err(|e| DatabaseError::SerializationError(e.to_string()))?,
                );
            }
        }

        Ok(PageList::new(datas, last_key))
    }

    async fn project_entities_save(
        &self,
        project_id: &Uuid,
        entities: &Vec<Entity>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for entity in entities {
            let item = self.project_entity_to_item(project_id, entity)?;

            let request = WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_entities_remove(
        &self,
        project_id: &Uuid,
        entity_uids: &Vec<EntityUid>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for uid in entity_uids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_ENTITY_TYPE, uid);

            let request = WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .key(PK, AttributeValue::S(pk))
                        .key(SK, AttributeValue::S(sk))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_policies_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageHash<PolicyId, Policy>, DatabaseError> {
        let mut filter = FilterExpression::new_with_query(query)?;

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_POLICY_TYPE);

        let att1 = filter.add_eq("PK", AttributeValue::S(pk));
        let att2 = filter.add_begins_with("SK", AttributeValue::S(sk));
        let condition = format!("{} AND {}", att1, att2);

        let mut stream = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression(condition)
            .set_filter_expression(filter.expression)
            .set_expression_attribute_names(Some(filter.names))
            .set_expression_attribute_values(Some(filter.values))
            .set_limit(filter.limit)
            .set_exclusive_start_key(filter.start_key)
            .into_paginator()
            .send();

        let mut last_key = None;
        let mut datas: HashMap<PolicyId, Policy> = HashMap::new();
        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;
            for item in page.items.unwrap_or_default() {
                let Some(policy_id_attr) = item.get("policyId") else {
                    continue;
                };
                let Ok(policy_id_str) = policy_id_attr.as_s() else {
                    continue;
                };
                let policy_id = policy_id_str.to_string().into();

                datas.insert(policy_id, Self::project_policy_from_item(&self, &item)?);
            }

            if let Some(key) = page.last_evaluated_key {
                let value: serde_json::Value = serde_dynamo::from_item(key)?;
                last_key = Some(
                    serde_json::to_string(&value)
                        .map_err(|e| DatabaseError::SerializationError(e.to_string()))?,
                );
            }
        }

        Ok(PageHash::new(datas, last_key))
    }

    async fn project_policies_save(
        &self,
        project_id: &Uuid,
        policies: &HashMap<PolicyId, Policy>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for (policy_id, policy) in policies {
            let item = self.project_policy_to_item(project_id, policy_id, policy)?;

            let request = WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_policies_remove(
        &self,
        project_id: &Uuid,
        policy_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for policy_id in policy_ids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_POLICY_TYPE, policy_id);

            let request = WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .key(PK, AttributeValue::S(pk))
                        .key(SK, AttributeValue::S(sk))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_templates_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageHash<PolicyId, Template>, DatabaseError> {
        let mut filter = FilterExpression::new_with_query(query)?;

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_TEMPLATE_TYPE);

        let att1 = filter.add_eq("PK", AttributeValue::S(pk));
        let att2 = filter.add_begins_with("SK", AttributeValue::S(sk));
        let condition = format!("{} AND {}", att1, att2);

        let mut stream = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression(condition)
            .set_filter_expression(filter.expression)
            .set_expression_attribute_names(Some(filter.names))
            .set_expression_attribute_values(Some(filter.values))
            .set_limit(filter.limit)
            .set_exclusive_start_key(filter.start_key)
            .into_paginator()
            .send();

        let mut last_key = None;
        let mut datas: HashMap<PolicyId, Template> = HashMap::new();
        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;
            for item in page.items.unwrap_or_default() {
                let Some(policy_id_attr) = item.get("policyId") else {
                    continue;
                };
                let Ok(policy_id_str) = policy_id_attr.as_s() else {
                    continue;
                };
                let policy_id = policy_id_str.to_string().into();

                datas.insert(policy_id, Self::project_template_from_item(&self, &item)?);
            }

            if let Some(key) = page.last_evaluated_key {
                let value: serde_json::Value = serde_dynamo::from_item(key)?;
                last_key = Some(
                    serde_json::to_string(&value)
                        .map_err(|e| DatabaseError::SerializationError(e.to_string()))?,
                );
            }
        }

        Ok(PageHash::new(datas, last_key))
    }

    async fn project_templates_save(
        &self,
        project_id: &Uuid,
        templates: &HashMap<PolicyId, Template>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for (policy_id, template) in templates {
            let item = self.project_template_to_item(project_id, policy_id, template)?;

            let request = WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_templates_remove(
        &self,
        project_id: &Uuid,
        template_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for template_id in template_ids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_TEMPLATE_TYPE, template_id);

            let request = WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .key(PK, AttributeValue::S(pk))
                        .key(SK, AttributeValue::S(sk))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_template_links_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<TemplateLink>, DatabaseError> {
        let mut filter = FilterExpression::new_with_query(query)?;

        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_TEMPLATE_LINK_TYPE);

        let att1 = filter.add_eq("PK", AttributeValue::S(pk));
        let att2 = filter.add_begins_with("SK", AttributeValue::S(sk));
        let condition = format!("{} AND {}", att1, att2);

        let mut stream = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression(condition)
            .set_filter_expression(filter.expression)
            .set_expression_attribute_names(Some(filter.names))
            .set_expression_attribute_values(Some(filter.values))
            .set_limit(filter.limit)
            .set_exclusive_start_key(filter.start_key)
            .into_paginator()
            .send();

        let mut last_key = None;
        let mut datas = Vec::new();
        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;
            for item in page.items.unwrap_or_default() {
                datas.push(Self::project_template_link_from_item(&self, &item)?);
            }
            if let Some(key) = page.last_evaluated_key {
                let value: serde_json::Value = serde_dynamo::from_item(key)?;
                last_key = Some(
                    serde_json::to_string(&value)
                        .map_err(|e| DatabaseError::SerializationError(e.to_string()))?,
                );
            }
        }

        Ok(PageList::new(datas, last_key))
    }

    async fn project_template_links_save(
        &self,
        project_id: &Uuid,
        template_links: &Vec<TemplateLink>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for template_link in template_links {
            let item = self.project_template_link_to_item(project_id, template_link)?;

            let request = WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_template_links_remove(
        &self,
        project_id: &Uuid,
        link_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError> {
        let mut request_items = Vec::new();

        for new_id in link_ids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_TEMPLATE_LINK_TYPE, new_id);

            let request = WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .key(PK, AttributeValue::S(pk))
                        .key(SK, AttributeValue::S(sk))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            request_items.push(request);
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // TODO: Create tests
}
