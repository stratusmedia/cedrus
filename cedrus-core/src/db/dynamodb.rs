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
SK: "P#[PROJECT_UUID]#PT#[POLICY_ID]"
GSI1PK: "PT"

Template Link:
PK: "P#[PROJECT_UUID]"
SK: "P#[PROJECT_UUID]#PTL#[POLICY_ID]"
GSI1PK: "PTL"
*/

const DEFAULT_ATT: &str = "__DEFAULT__";
const SCHEMA_ATT: &str = "schema";
const CREATED_AT_ATT: &str = "createdAt";
const UPDATED_AT_ATT: &str = "updatedAt";

#[derive(Debug)]
pub struct QueryFilter {
    pub condition: String,
    pub index: Option<String>,
    pub filter: Option<String>,
    pub names: HashMap<String, String>,
    pub values: HashMap<String, AttributeValue>,
    pub limit: Option<i32>,
    pub start_key: Option<HashMap<String, AttributeValue>>,
}

impl Default for QueryFilter {
    fn default() -> Self {
        Self::new("#PK = :PK")
    }
}

impl QueryFilter {
    pub fn new(condition: &str) -> QueryFilter {
        QueryFilter {
            condition: condition.to_string(),
            index: None,
            filter: None,
            names: HashMap::new(),
            values: HashMap::new(),
            limit: None,
            start_key: None,
        }
    }

    pub fn new_with_query(query: &Query, condition: &str) -> Result<Self, DatabaseError> {
        let mut filter = QueryFilter::new(condition);

        if let Some(selector) = query.selector.clone() {
            let mut expression = String::new();
            Self::selector_to_filter("".to_string(), selector, &mut expression, &mut filter);
            if !expression.trim().is_empty() {
                filter.filter = Some(expression);
            }
        }

        if let Some(limit) = query.limit
            && limit > 0
        {
            filter.limit = Some(limit as i32);
        }

        if let Some(start_key) = query.start_key.clone() {
            let key: serde_json::Value = serde_json::from_str(&start_key)?;
            let key: HashMap<String, AttributeValue> = serde_dynamo::to_item(key)?;
            filter.start_key = Some(key);
        }

        Ok(filter)
    }

    pub fn index(&self) -> Option<String> {
        self.index.clone()
    }

    pub fn condition(&self) -> String {
        self.condition.clone()
    }

    pub fn filter(&self) -> Option<String> {
        self.filter.clone()
    }

    pub fn names(&self) -> Option<HashMap<String, String>> {
        if self.names.is_empty() {
            None
        } else {
            Some(self.names.clone())
        }
    }

    pub fn values(&self) -> Option<HashMap<String, AttributeValue>> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.values.clone())
        }
    }

    pub fn add_name(&mut self, name: &str, value: &str) {
        self.names.insert(name.to_string(), value.to_string());
    }

    pub fn add_value(&mut self, name: &str, value: AttributeValue) {
        self.values.insert(name.to_string(), value);
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
        filter: &mut QueryFilter,
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

pub struct DynamoDBPage {
    pub items: Vec<HashMap<String, AttributeValue>>,
    pub last_key: Option<String>,
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
        let created_at_millis_str = item
            .get(CREATED_AT_ATT)
            .ok_or_else(|| DatabaseError::MissingAttribute(CREATED_AT_ATT.to_string()))?
            .as_n()
            .map_err(|_| DatabaseError::InvalidAttribute(CREATED_AT_ATT.to_string()))?;
        let created_at_millis: i64 = created_at_millis_str
            .parse()
            .map_err(|_| DatabaseError::InvalidAttribute(CREATED_AT_ATT.to_string()))?;
        let created_at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(created_at_millis)
            .ok_or_else(|| DatabaseError::InvalidAttribute(CREATED_AT_ATT.to_string()))?;

        let updated_at_millis_str = item
            .get(UPDATED_AT_ATT)
            .ok_or_else(|| DatabaseError::MissingAttribute(UPDATED_AT_ATT.to_string()))?
            .as_n()
            .map_err(|_| DatabaseError::InvalidAttribute(UPDATED_AT_ATT.to_string()))?;
        let updated_at_millis: i64 = updated_at_millis_str
            .parse()
            .map_err(|_| DatabaseError::InvalidAttribute(UPDATED_AT_ATT.to_string()))?;
        let updated_at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(updated_at_millis)
            .ok_or_else(|| DatabaseError::InvalidAttribute(UPDATED_AT_ATT.to_string()))?;

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
    ) -> Result<(PolicyId, Policy), DatabaseError> {
        let policy_id_str = item
            .get("policyId")
            .ok_or(DatabaseError::SerializationError(
                "policyId is missing".to_string(),
            ))?
            .as_s()
            .map_err(|_| {
                DatabaseError::SerializationError("policyId is not a string".to_string())
            })?;

        let policy_id: PolicyId = policy_id_str.to_string().into();
        let policy: Policy = serde_dynamo::from_item(item.clone())?;
        Ok((policy_id, policy))
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
    ) -> Result<(PolicyId, Template), DatabaseError> {
        let policy_id_str = item
            .get("policyId")
            .ok_or(DatabaseError::SerializationError(
                "policyId is missing".to_string(),
            ))?
            .as_s()
            .map_err(|_| {
                DatabaseError::SerializationError("policyId is not a string".to_string())
            })?;

        let policy_id: PolicyId = policy_id_str.to_string().into();
        let template: Template = serde_dynamo::from_item(item.clone())?;
        Ok((policy_id, template))
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

    pub async fn put_item(
        &self,
        item: HashMap<String, AttributeValue>,
    ) -> Result<(), DatabaseError> {
        self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?;
        Ok(())
    }

    pub async fn get_item(
        &self,
        pk: &str,
        sk: &str,
    ) -> Result<Option<HashMap<String, AttributeValue>>, DatabaseError> {
        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(PK.to_string(), AttributeValue::S(pk.to_string()))
            .key(SK.to_string(), AttributeValue::S(sk.to_string()))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?;

        Ok(response.item)
    }

    pub async fn delete_item(&self, pk: &str, sk: &str) -> Result<(), DatabaseError> {
        self.client
            .delete_item()
            .table_name(&self.table_name)
            .key(PK.to_string(), AttributeValue::S(pk.to_string()))
            .key(SK.to_string(), AttributeValue::S(sk.to_string()))
            .send()
            .await
            .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?;
        Ok(())
    }

    async fn put_items(
        &self,
        items: Vec<HashMap<String, AttributeValue>>,
    ) -> Result<(), DatabaseError> {
        let mut requests = vec![];
        for item in items {
            let request = WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            requests.push(request);
        }

        self.batch_write_item(requests).await
    }

    async fn delete_items(&self, items: Vec<(String, String)>) -> Result<(), DatabaseError> {
        let mut requests = vec![];
        for (pk, sk) in items {
            let request = WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .key(PK.to_string(), AttributeValue::S(pk))
                        .key(SK.to_string(), AttributeValue::S(sk))
                        .build()
                        .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                )
                .build();

            requests.push(request);
        }

        self.batch_write_item(requests).await
    }

    pub async fn query(&self, filter: &QueryFilter) -> Result<DynamoDBPage, DatabaseError> {
        let limit = filter.limit;

        let response = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression(filter.condition())
            .set_index_name(filter.index())
            .set_filter_expression(filter.filter())
            .set_expression_attribute_names(filter.names())
            .set_expression_attribute_values(filter.values())
            .set_limit(limit)
            .set_exclusive_start_key(filter.start_key.clone())
            .send()
            .await
            .unwrap();

        let mut items: Vec<HashMap<String, AttributeValue>> = vec![];
        items.extend(response.items().to_vec());
        let mut next_key = response.last_evaluated_key().map(ToOwned::to_owned);
        let mut prev_key = next_key.clone();

        while let Some(last_key) = next_key {
            let len = limit.unwrap_or(0);
            if len > 0 && (items.len() as i32) >= len {
                break;
            }

            let mut new_limit = None;
            if len != 0 && (items.len() as i32) < len {
                new_limit = Some(len - items.len() as i32);
            }

            let response = self
                .client
                .query()
                .table_name(&self.table_name)
                .key_condition_expression(filter.condition())
                .set_index_name(filter.index())
                .set_filter_expression(filter.filter())
                .set_expression_attribute_names(filter.names())
                .set_expression_attribute_values(filter.values())
                .set_limit(new_limit)
                .set_exclusive_start_key(Some(last_key))
                .send()
                .await
                .unwrap();

            items.extend(response.items().to_vec());
            next_key = response.last_evaluated_key().map(ToOwned::to_owned);

            // if the next key is the same as the previous key break the loop
            if prev_key == next_key {
                break;
            }
            prev_key = next_key.clone();
        }

        let prev_key = match prev_key {
            Some(key) => {
                let value: serde_json::Value = serde_dynamo::from_item(key)?;
                Some(serde_json::to_string(&value)?)
            }
            None => None,
        };

        Ok(DynamoDBPage {
            items,
            last_key: prev_key,
        })
    }
}

#[async_trait::async_trait]
impl Database for DynamoDb {
    async fn projects_load(&self, query: &Query) -> Result<PageList<Project>, DatabaseError> {
        let mut filter = QueryFilter::new_with_query(query, "#GSI1_PK = :GSI1_PK")?;
        filter.add_name("#GSI1_PK", GSI1_PK);
        filter.add_value(":GSI1_PK", AttributeValue::S(PROJECT_TYPE.to_string()));

        filter.index = Some(GSI1.to_string());

        let page = self.query(&filter).await?;

        let mut datas = Vec::new();
        for mut item in page.items {
            datas.push(Self::project_from_item(&self, &mut item)?);
        }

        Ok(PageList::new(datas, page.last_key))
    }

    async fn project_load(&self, id: &Uuid) -> Result<Option<Project>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, id);
        let item = self.get_item(&pk, &pk).await?;
        let project = item
            .map(|mut i| self.project_from_item(&mut i))
            .transpose()?;
        Ok(project)
    }

    async fn project_save(&self, project: &Project) -> Result<(), DatabaseError> {
        let item = self.project_to_item(project)?;
        self.put_item(item).await
    }

    async fn project_remove(&self, id: &Uuid) -> Result<(), DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, id);

        let mut stream = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("#PK = :PK")
            .expression_attribute_names("#PK", PK)
            .expression_attribute_values(":PK", AttributeValue::S(pk))
            .into_paginator()
            .send();

        let mut request_items = Vec::new();
        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| DatabaseError::AwsSdkError(format!("{:?}", e.raw_response())))?;
            for item in &page.items.unwrap_or_default() {
                let pk = item
                    .get(PK)
                    .ok_or_else(|| DatabaseError::MissingAttribute(PK.to_string()))?;
                let sk = item
                    .get(SK)
                    .ok_or_else(|| DatabaseError::MissingAttribute(SK.to_string()))?;

                let request = WriteRequest::builder()
                    .delete_request(
                        DeleteRequest::builder()
                            .key(PK, pk.clone())
                            .key(SK, sk.clone())
                            .build()
                            .map_err(|e| DatabaseError::AwsSdkError(e.to_string()))?,
                    )
                    .build();
                request_items.push(request);
            }
        }

        self.batch_write_item(request_items).await?;

        Ok(())
    }

    async fn project_apikeys_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<ApiKey>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_APIKEY_TYPE);

        let mut filter = QueryFilter::new_with_query(query, "#PK = :PK AND begins_with(#SK, :SK)")?;
        filter.add_name("#PK", PK);
        filter.add_name("#SK", SK);
        filter.add_value(":PK", AttributeValue::S(pk));
        filter.add_value(":SK", AttributeValue::S(sk));

        let page = self.query(&filter).await?;

        let mut datas = Vec::new();
        for mut item in page.items {
            datas.push(Self::project_apikey_from_item(&self, &mut item)?);
        }

        Ok(PageList::new(datas, page.last_key))
    }

    async fn project_apikeys_save(
        &self,
        project_id: &Uuid,
        apikeys: &Vec<ApiKey>,
    ) -> Result<(), DatabaseError> {
        let mut items = Vec::new();
        for apikey in apikeys {
            let item = self.project_apikey_to_item(project_id, apikey)?;
            items.push(item)
        }

        self.put_items(items).await
    }

    async fn project_apikeys_remove(
        &self,
        project_id: &Uuid,
        ids: &Vec<Uuid>,
    ) -> Result<(), DatabaseError> {
        let mut keys = Vec::new();
        for id in ids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_APIKEY_TYPE, id);

            keys.push((pk, sk));
        }

        self.delete_items(keys).await?;

        Ok(())
    }

    async fn project_identity_source_load(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<IdentitySource>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_IDENTITY_SOURCE_TYPE);

        let item = self.get_item(&pk, &sk).await?;
        let identity_source = item
            .map(|i| self.project_identity_source_from_item(&i))
            .transpose()?;
        Ok(identity_source)
    }

    async fn project_identity_source_save(
        &self,
        project_id: &Uuid,
        identity_source: &IdentitySource,
    ) -> Result<(), DatabaseError> {
        let item = self.project_identity_source_to_item(project_id, identity_source)?;
        self.put_item(item).await
    }

    async fn project_identity_source_remove(&self, project_id: &Uuid) -> Result<(), DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_IDENTITY_SOURCE_TYPE);

        self.delete_item(&pk, &sk).await
    }

    async fn project_schema_load(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<Schema>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_SCHEMA_TYPE);

        let item = self.get_item(&pk, &sk).await?;
        let schema = item
            .map(|i| self.project_schema_from_item(&i))
            .transpose()?;

        Ok(schema)
    }

    async fn project_schema_save(
        &self,
        project_id: &Uuid,
        schema: &Schema,
    ) -> Result<(), DatabaseError> {
        let item = self.project_schema_to_item(project_id, schema)?;
        self.put_item(item).await
    }

    async fn project_schema_remove(&self, project_id: &Uuid) -> Result<(), DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}", pk, PROJECT_SCHEMA_TYPE);

        self.delete_item(&pk, &sk).await
    }

    async fn project_entities_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<Entity>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_ENTITY_TYPE);

        let mut filter = QueryFilter::new_with_query(query, "#PK = :PK AND begins_with(#SK, :SK)")?;
        filter.add_name("#PK", PK);
        filter.add_name("#SK", SK);
        filter.add_value(":PK", AttributeValue::S(pk));
        filter.add_value(":SK", AttributeValue::S(sk));

        let page = self.query(&filter).await?;

        let mut datas = Vec::new();
        for item in page.items {
            datas.push(self.project_entity_from_item(&item)?);
        }

        Ok(PageList::new(datas, page.last_key))
    }

    async fn project_entities_save(
        &self,
        project_id: &Uuid,
        entities: &Vec<Entity>,
    ) -> Result<(), DatabaseError> {
        let mut items = Vec::new();
        for entity in entities {
            let item = self.project_entity_to_item(project_id, entity)?;
            items.push(item);
        }

        self.put_items(items).await
    }

    async fn project_entities_remove(
        &self,
        project_id: &Uuid,
        entity_uids: &Vec<EntityUid>,
    ) -> Result<(), DatabaseError> {
        let mut keys = Vec::new();
        for uid in entity_uids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_ENTITY_TYPE, uid);

            keys.push((pk, sk));
        }

        self.delete_items(keys).await
    }

    async fn project_policies_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageHash<PolicyId, Policy>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_POLICY_TYPE);

        let mut filter = QueryFilter::new_with_query(query, "#PK = :PK AND begins_with(#SK, :SK)")?;
        filter.add_name("#PK", PK);
        filter.add_name("#SK", SK);
        filter.add_value(":PK", AttributeValue::S(pk));
        filter.add_value(":SK", AttributeValue::S(sk));

        let page = self.query(&filter).await?;

        let mut datas: HashMap<PolicyId, Policy> = HashMap::new();
        for item in page.items {
            let (policy_id, policy) = self.project_policy_from_item(&item)?;
            datas.insert(policy_id, policy);
        }

        Ok(PageHash::new(datas, page.last_key))
    }

    async fn project_policies_save(
        &self,
        project_id: &Uuid,
        policies: &HashMap<PolicyId, Policy>,
    ) -> Result<(), DatabaseError> {
        let mut items = Vec::new();
        for (policy_id, policy) in policies {
            let item = self.project_policy_to_item(project_id, policy_id, policy)?;
            items.push(item);
        }

        self.put_items(items).await
    }

    async fn project_policies_remove(
        &self,
        project_id: &Uuid,
        policy_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError> {
        let mut keys = Vec::new();
        for policy_id in policy_ids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_POLICY_TYPE, policy_id);

            keys.push((pk, sk));
        }

        self.delete_items(keys).await
    }

    async fn project_templates_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageHash<PolicyId, Template>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_TEMPLATE_TYPE);

        let mut filter = QueryFilter::new_with_query(query, "#PK = :PK AND begins_with(#SK, :SK)")?;
        filter.add_name("#PK", PK);
        filter.add_name("#SK", SK);
        filter.add_value(":PK", AttributeValue::S(pk));
        filter.add_value(":SK", AttributeValue::S(sk));

        let page = self.query(&filter).await?;

        let mut datas: HashMap<PolicyId, Template> = HashMap::new();
        for item in page.items {
            let (policy_id, template) = self.project_template_from_item(&item)?;
            datas.insert(policy_id, template);
        }

        Ok(PageHash::new(datas, page.last_key))
    }

    async fn project_templates_save(
        &self,
        project_id: &Uuid,
        templates: &HashMap<PolicyId, Template>,
    ) -> Result<(), DatabaseError> {
        let mut items = Vec::new();

        for (policy_id, template) in templates {
            let item = self.project_template_to_item(project_id, policy_id, template)?;

            items.push(item);
        }

        self.put_items(items).await
    }

    async fn project_templates_remove(
        &self,
        project_id: &Uuid,
        template_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError> {
        let mut keys = Vec::new();

        for template_id in template_ids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_TEMPLATE_TYPE, template_id);

            keys.push((pk, sk));
        }

        self.delete_items(keys).await
    }

    async fn project_template_links_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<TemplateLink>, DatabaseError> {
        let pk = format!("{}#{}", PROJECT_TYPE, project_id);
        let sk = format!("{}#{}#", pk, PROJECT_TEMPLATE_LINK_TYPE);

        let mut filter = QueryFilter::new_with_query(query, "#PK = :PK AND begins_with(#SK, :SK)")?;
        filter.add_name("#PK", PK);
        filter.add_name("#SK", SK);
        filter.add_value(":PK", AttributeValue::S(pk));
        filter.add_value(":SK", AttributeValue::S(sk));

        let page = self.query(&filter).await?;

        let mut datas = Vec::new();
        for item in page.items {
            datas.push(self.project_template_link_from_item(&item)?);
        }

        Ok(PageList::new(datas, page.last_key))
    }

    async fn project_template_links_save(
        &self,
        project_id: &Uuid,
        template_links: &Vec<TemplateLink>,
    ) -> Result<(), DatabaseError> {
        let mut items = Vec::new();

        for template_link in template_links {
            let item = self.project_template_link_to_item(project_id, template_link)?;

            items.push(item);
        }

        self.put_items(items).await
    }

    async fn project_template_links_remove(
        &self,
        project_id: &Uuid,
        link_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError> {
        let mut keys = Vec::new();

        for new_id in link_ids {
            let pk = format!("{}#{}", PROJECT_TYPE, project_id);
            let sk = format!("{}#{}#{}", pk, PROJECT_TEMPLATE_LINK_TYPE, new_id);

            keys.push((pk, sk));
        }

        self.delete_items(keys).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Query;
    use cedrus_cedar::{Entity, EntityUid, Policy, PolicyId, Schema, Template, TemplateLink};
    use std::collections::{HashMap, HashSet};
    use uuid::Uuid;

    async fn setup_test_db() -> DynamoDb {
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "local");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "local");
            std::env::set_var("AWS_REGION", "us-east-1");
        }

        let table_name = format!("test_cedrus_table_{}", Uuid::now_v7().simple());
        let conf = crate::core::DynamoDBConfig {
            endpoint_url: Some("http://localhost:8000".to_string()),
            region: Some("us-east-1".to_string()),
            table_name,
            initialize: true,
        };

        let db = DynamoDb::new(&conf)
            .await
            .expect("Failed to create DynamoDb client");
        db.init().await.expect("Failed to initialize test table");
        db
    }

    async fn teardown_test_db(db: &DynamoDb) {
        let _ = db
            .client
            .delete_table()
            .table_name(&db.table_name)
            .send()
            .await;
    }

    #[tokio::test]
    async fn test_project_crud() {
        let db = setup_test_db().await;

        let project_id = Uuid::now_v7();
        let owner = EntityUid::new("User".to_string(), "owner-id".to_string());
        let project = Project::new(project_id, "test-project".to_string(), owner.clone());

        // Test save
        db.project_save(&project)
            .await
            .expect("Failed to save project");

        // Test load
        let loaded = db
            .project_load(&project_id)
            .await
            .expect("Failed to load project");
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, project.id);
        assert_eq!(loaded.name, project.name);
        assert_eq!(loaded.owner, project.owner);

        // Test projects_load
        let query = Query::default();
        let projects_page = db
            .projects_load(&query)
            .await
            .expect("Failed to load projects list");
        assert!(projects_page.items.iter().any(|p| p.id == project_id));

        // Test remove
        db.project_remove(&project_id)
            .await
            .expect("Failed to remove project");
        let loaded_after_remove = db
            .project_load(&project_id)
            .await
            .expect("Failed to load project after remove");
        assert!(loaded_after_remove.is_none());

        teardown_test_db(&db).await;
    }

    #[tokio::test]
    async fn test_apikey_crud() {
        let db = setup_test_db().await;

        let project_id = Uuid::now_v7();
        let owner = EntityUid::new("User".to_string(), "owner-id".to_string());
        let api_key_id = Uuid::now_v7();
        let api_key = ApiKey::new(
            api_key_id,
            "test-key-value-string".to_string(),
            "test-api-key".to_string(),
            project_id,
            owner.clone(),
        );

        let apikeys = vec![api_key.clone()];

        // Test save
        db.project_apikeys_save(&project_id, &apikeys)
            .await
            .expect("Failed to save api keys");

        // Test load
        let query = Query::default();
        let loaded_page = db
            .project_apikeys_load(&project_id, &query)
            .await
            .expect("Failed to load api keys");
        assert_eq!(loaded_page.items.len(), 1);
        let loaded_key = &loaded_page.items[0];
        assert_eq!(loaded_key.id, api_key.id);
        assert_eq!(loaded_key.key, api_key.key);
        assert_eq!(loaded_key.name, api_key.name);

        // Test remove
        db.project_apikeys_remove(&project_id, &vec![api_key_id])
            .await
            .expect("Failed to remove api keys");
        let loaded_after_remove = db
            .project_apikeys_load(&project_id, &query)
            .await
            .expect("Failed to load after remove");
        assert_eq!(loaded_after_remove.items.len(), 0);

        teardown_test_db(&db).await;
    }

    #[tokio::test]
    async fn test_identity_source_crud() {
        let db = setup_test_db().await;

        let project_id = Uuid::now_v7();
        let identity_source = IdentitySource {
            principal_entity_type: "User".to_string(),
            configuration: crate::core::is::Configuration::CognitoUserPoolConfiguration(
                crate::core::is::CognitoUserPoolConfiguration {
                    user_pool_arn:
                        "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abcdefghi"
                            .to_string(),
                    client_ids: vec!["client_id_1".to_string()],
                    group_configuration: None,
                },
            ),
        };

        // Test save
        db.project_identity_source_save(&project_id, &identity_source)
            .await
            .expect("Failed to save identity source");

        // Test load
        let loaded = db
            .project_identity_source_load(&project_id)
            .await
            .expect("Failed to load identity source");
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(
            loaded.principal_entity_type,
            identity_source.principal_entity_type
        );

        // Test remove
        db.project_identity_source_remove(&project_id)
            .await
            .expect("Failed to remove identity source");
        let loaded_after_remove = db
            .project_identity_source_load(&project_id)
            .await
            .expect("Failed to load after remove");
        assert!(loaded_after_remove.is_none());

        teardown_test_db(&db).await;
    }

    #[tokio::test]
    async fn test_schema_crud() {
        let db = setup_test_db().await;

        let project_id = Uuid::now_v7();
        let mut namespaces = HashMap::new();
        namespaces.insert("".to_string(), cedrus_cedar::schema::Namespace::default());
        let schema = Schema(namespaces);

        // Test save
        db.project_schema_save(&project_id, &schema)
            .await
            .expect("Failed to save schema");

        // Test load
        let loaded = db
            .project_schema_load(&project_id)
            .await
            .expect("Failed to load schema");
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert!(loaded.0.contains_key(""));

        // Test remove
        db.project_schema_remove(&project_id)
            .await
            .expect("Failed to remove schema");
        let loaded_after_remove = db
            .project_schema_load(&project_id)
            .await
            .expect("Failed to load after remove");
        assert!(loaded_after_remove.is_none());

        teardown_test_db(&db).await;
    }

    #[tokio::test]
    async fn test_entity_crud() {
        let db = setup_test_db().await;

        let project_id = Uuid::now_v7();
        let uid = EntityUid::new("User".to_string(), "alice".to_string());
        let entity = Entity::new_no_attrs(uid.clone(), HashSet::new());
        let entities = vec![entity.clone()];

        // Test save
        db.project_entities_save(&project_id, &entities)
            .await
            .expect("Failed to save entities");

        // Test load
        let query = Query::default();
        let loaded_page = db
            .project_entities_load(&project_id, &query)
            .await
            .expect("Failed to load entities");
        assert_eq!(loaded_page.items.len(), 1);
        assert_eq!(loaded_page.items[0].uid(), &uid);

        // Test remove
        db.project_entities_remove(&project_id, &vec![uid.clone()])
            .await
            .expect("Failed to remove entities");
        let loaded_after_remove = db
            .project_entities_load(&project_id, &query)
            .await
            .expect("Failed to load after remove");
        assert_eq!(loaded_after_remove.items.len(), 0);

        teardown_test_db(&db).await;
    }

    #[tokio::test]
    async fn test_policy_crud() {
        let db = setup_test_db().await;

        let project_id = Uuid::now_v7();
        let policy_id = PolicyId::from("policy-1".to_string());
        let policy = Policy::default();

        let mut policies = HashMap::new();
        policies.insert(policy_id.clone(), policy.clone());

        // Test save
        db.project_policies_save(&project_id, &policies)
            .await
            .expect("Failed to save policies");

        // Test load
        let query = Query::default();
        let loaded_page = db
            .project_policies_load(&project_id, &query)
            .await
            .expect("Failed to load policies");
        assert_eq!(loaded_page.items.len(), 1);
        assert!(loaded_page.items.contains_key(&policy_id));

        // Test remove
        db.project_policies_remove(&project_id, &vec![policy_id.clone()])
            .await
            .expect("Failed to remove policies");
        let loaded_after_remove = db
            .project_policies_load(&project_id, &query)
            .await
            .expect("Failed to load after remove");
        assert_eq!(loaded_after_remove.items.len(), 0);

        teardown_test_db(&db).await;
    }

    #[tokio::test]
    async fn test_template_crud() {
        let db = setup_test_db().await;

        let project_id = Uuid::now_v7();
        let template_id = PolicyId::from("template-1".to_string());
        let template = Template::default();

        let mut templates = HashMap::new();
        templates.insert(template_id.clone(), template.clone());

        // Test save
        db.project_templates_save(&project_id, &templates)
            .await
            .expect("Failed to save templates");

        // Test load
        let query = Query::default();
        let loaded_page = db
            .project_templates_load(&project_id, &query)
            .await
            .expect("Failed to load templates");
        assert_eq!(loaded_page.items.len(), 1);
        assert!(loaded_page.items.contains_key(&template_id));

        // Test remove
        db.project_templates_remove(&project_id, &vec![template_id.clone()])
            .await
            .expect("Failed to remove templates");
        let loaded_after_remove = db
            .project_templates_load(&project_id, &query)
            .await
            .expect("Failed to load after remove");
        assert_eq!(loaded_after_remove.items.len(), 0);

        teardown_test_db(&db).await;
    }

    #[tokio::test]
    async fn test_template_link_crud() {
        let db = setup_test_db().await;

        let project_id = Uuid::now_v7();
        let template_id = PolicyId::from("template-1".to_string());
        let link_id = PolicyId::from("link-1".to_string());
        let link = TemplateLink::new(template_id.clone(), link_id.clone(), HashMap::new());

        let links = vec![link.clone()];

        // Test save
        db.project_template_links_save(&project_id, &links)
            .await
            .expect("Failed to save template links");

        // Test load
        let query = Query::default();
        let loaded_page = db
            .project_template_links_load(&project_id, &query)
            .await
            .expect("Failed to load template links");
        assert_eq!(loaded_page.items.len(), 1);
        assert_eq!(loaded_page.items[0].new_id, link_id);
        assert_eq!(loaded_page.items[0].template_id, template_id);

        // Test remove
        db.project_template_links_remove(&project_id, &vec![link_id.clone()])
            .await
            .expect("Failed to remove template links");
        let loaded_after_remove = db
            .project_template_links_load(&project_id, &query)
            .await
            .expect("Failed to load after remove");
        assert_eq!(loaded_after_remove.items.len(), 0);

        teardown_test_db(&db).await;
    }

    #[tokio::test]
    async fn test_query_limit_and_pagination() {
        let db = setup_test_db().await;

        let mut items = Vec::with_capacity(3000);
        let owner = EntityUid::new("User".to_string(), "owner-id".to_string());
        for i in 0..3000 {
            let project_id = Uuid::now_v7();
            let project = Project::new(project_id, format!("mock-project-{}", i), owner.clone());
            let item = db.project_to_item(&project).expect("Failed to serialize project");
            items.push(item);
        }

        db.put_items(items).await.expect("Failed to batch save mock projects");

        // First page query with limit 2000
        let mut query = Query::default();
        query.limit = Some(2000);

        let first_page = db.projects_load(&query).await.expect("Failed to load first page of projects");
        assert_eq!(first_page.items.len(), 2000);
        assert!(first_page.last_key.is_some());

        // Second page query starting from last_key
        let last_key = first_page.last_key.unwrap();
        let mut query_two = Query::default();
        query_two.limit = Some(2000);
        query_two.start_key = Some(last_key);

        let second_page = db.projects_load(&query_two).await.expect("Failed to load second page of projects");
        assert_eq!(second_page.items.len(), 1000);

        teardown_test_db(&db).await;
    }
}
