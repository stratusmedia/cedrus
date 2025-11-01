use std::{collections::HashMap, error::Error};

use cedrus_cedar::{Entity, EntityUid, Policy, PolicyId, Schema, Template, TemplateLink};
use couch_rs::error::CouchError;
use uuid::Uuid;

use crate::{core::{project::Project, DbConfig, IdentitySource}, PageHash, PageList, Query};

pub mod couchdb;
pub mod dynamodb;

#[derive(Debug)]
pub enum DatabaseError {
    NotFound,
    Unknown,
    MissingAttribute(String),
    InvalidAttribute(String),
    JsonErro(serde_json::Error),
    CouchError(CouchError),
    SerdeDynamoError(serde_dynamo::Error),
    AwsSdkError(String),
    SerializationError(String),
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::NotFound => write!(f, "not found"),
            DatabaseError::Unknown => write!(f, "unknown"),
            DatabaseError::MissingAttribute(a) => write!(f, "missing attribute: {}", a),
            DatabaseError::InvalidAttribute(a) => write!(f, "invalid attribute: {}", a),
            DatabaseError::JsonErro(e) => write!(f, "json error: {}", e.to_string()),
            DatabaseError::CouchError(e) => write!(f, "couch error: {}", e),
            DatabaseError::SerdeDynamoError(e) => write!(f, "dynamodb error: {}", e.to_string()),
            DatabaseError::AwsSdkError(e) => write!(f, "aws sdk error: {}", e),
            DatabaseError::SerializationError(e) => write!(f, "serialization error: {}", e),
        }
    }
}

impl Error for DatabaseError {}

impl From<serde_dynamo::Error> for DatabaseError {
    fn from(e: serde_dynamo::Error) -> Self {
        DatabaseError::SerdeDynamoError(e)
    }
}

impl From<serde_json::Error> for DatabaseError {
    fn from(e: serde_json::Error) -> Self {
        DatabaseError::JsonErro(e)
    }
}

impl From<CouchError> for DatabaseError {
    fn from(e: CouchError) -> Self {
        DatabaseError::CouchError(e)
    }
}

#[async_trait::async_trait]
pub trait Database: Send + Sync {
    async fn projects_load(&self, query: &Query) -> Result<PageList<Project>, DatabaseError>;
    async fn project_load(&self, id: &Uuid) -> Result<Option<Project>, DatabaseError>;
    async fn project_save(&self, project: &Project) -> Result<(), DatabaseError>;
    async fn project_remove(&self, id: &Uuid) -> Result<(), DatabaseError>;

    async fn project_identity_source_load(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<IdentitySource>, DatabaseError>;
    async fn project_identity_source_save(
        &self,
        project_id: &Uuid,
        identity_source: &IdentitySource,
    ) -> Result<(), DatabaseError>;
    async fn project_identity_source_remove(
        &self,
        project_id: &Uuid
    ) -> Result<(), DatabaseError>;

    async fn project_schema_load(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<Schema>, DatabaseError>;
    async fn project_schema_save(
        &self,
        project_id: &Uuid,
        schema: &Schema,
    ) -> Result<(), DatabaseError>;
    async fn project_schema_remove(
        &self,
        project_id: &Uuid
    ) -> Result<(), DatabaseError>;

    async fn project_entities_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<Entity>, DatabaseError>;
    async fn project_entities_save(
        &self,
        project_id: &Uuid,
        entities: &Vec<Entity>,
    ) -> Result<(), DatabaseError>;
    async fn project_entities_remove(
        &self,
        project_id: &Uuid,
        entity_uids: &Vec<EntityUid>,
    ) -> Result<(), DatabaseError>;

    async fn project_policies_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageHash<PolicyId, Policy>, DatabaseError>;
    async fn project_policies_save(
        &self,
        project_id: &Uuid,
        policies: &HashMap<PolicyId, Policy>,
    ) -> Result<(), DatabaseError>;
    async fn project_policies_remove(
        &self,
        project_id: &Uuid,
        policy_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError>;

    async fn project_templates_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageHash<PolicyId, Template>, DatabaseError>;
    async fn project_templates_save(
        &self,
        project_id: &Uuid,
        templates: &HashMap<PolicyId, Template>,
    ) -> Result<(), DatabaseError>;
    async fn project_templates_remove(
        &self,
        project_id: &Uuid,
        template_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError>;

    async fn project_template_links_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<TemplateLink>, DatabaseError>;
    async fn project_template_links_save(
        &self,
        project_id: &Uuid,
        template_links: &Vec<TemplateLink>,
    ) -> Result<(), DatabaseError>;
    async fn project_template_links_remove(
        &self,
        project_id: &Uuid,
        link_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError>;
}

pub async fn database_factory(conf: &DbConfig) -> Box<dyn Database + Send + Sync> {
    match conf {
        DbConfig::DynamoDbConfig(conf) => {
            Box::new(dynamodb::DynamoDb::new(&conf).await)
        },
        DbConfig::CouchDbConfig(conf) => {
            let db = couchdb::CouchDb::new(&conf);
            db.init().await.unwrap();
            Box::new(db)
        },
    }
}