use std::{collections::HashMap, error::Error};

use cedrus_cedar::{
    Entity, EntityUid, Policy, PolicyId, PolicySet, Schema, Template, TemplateLink,
};
use redis::RedisError;
use uuid::Uuid;

use crate::core::{project::Project, IdentitySource};

pub mod valkey;
pub mod dashmap;

#[derive(Debug)]
pub enum CacheError {
    Connection,
    NotFound,
    RedisError(RedisError)
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::Connection => write!(f, "Connection error"),
            CacheError::NotFound => write!(f, "Not found"),
            CacheError::RedisError(err) => write!(f, "Redis error: {}", err),
        }
    }
}

impl Error for CacheError {}

impl From<RedisError> for CacheError {
    fn from(err: RedisError) -> Self {
        CacheError::RedisError(err)
    }
}

#[async_trait::async_trait]
pub trait Cache: Send + Sync {
    async fn project_clear(&self, project_id: &Uuid) -> Result<(), CacheError>;

    async fn projects_get(&self) -> Result<Vec<Project>, CacheError>;
    async fn project_get(&self, project_id: &Uuid) -> Result<Option<Project>, CacheError>;
    async fn project_set(&self, project: &Project) -> Result<(), CacheError>;
    async fn project_del(&self, project_id: &Uuid) -> Result<(), CacheError>;

    async fn project_get_identity_source(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<IdentitySource>, CacheError>;
    async fn project_set_identity_source(
        &self,
        project_id: &Uuid,
        identity_source: &IdentitySource,
    ) -> Result<(), CacheError>;
    async fn project_del_identity_source(&self, project_id: &Uuid) -> Result<(), CacheError>;

    async fn project_get_schema(&self, project_id: &Uuid) -> Result<Option<Schema>, CacheError>;
    async fn project_set_schema(
        &self,
        project_id: &Uuid,
        schema: &Schema,
    ) -> Result<(), CacheError>;
    async fn project_del_schema(&self, project_id: &Uuid) -> Result<(), CacheError>;

    async fn project_get_entities(
        &self,
        project_id: &Uuid,
        entity_uids: &[EntityUid],
    ) -> Result<Vec<Entity>, CacheError>;
    async fn project_set_entities(
        &self,
        project_id: &Uuid,
        entities: &[Entity],
    ) -> Result<(), CacheError>;
    async fn project_del_entities(
        &self,
        project_id: &Uuid,
        entity_uids: &[EntityUid],
    ) -> Result<(), CacheError>;

    async fn project_get_policies(
        &self,
        project_id: &Uuid,
    ) -> Result<HashMap<PolicyId, Policy>, CacheError>;
    async fn project_set_policies(
        &self,
        project_id: &Uuid,
        policies: &HashMap<PolicyId, Policy>,
    ) -> Result<(), CacheError>;
    async fn project_del_policies(
        &self,
        project_id: &Uuid,
        policy_ids: &[PolicyId],
    ) -> Result<(), CacheError>;

    async fn project_get_templates(
        &self,
        project_id: &Uuid,
    ) -> Result<HashMap<PolicyId, Template>, CacheError>;
    async fn project_set_templates(
        &self,
        project_id: &Uuid,
        templates: &HashMap<PolicyId, Template>,
    ) -> Result<(), CacheError>;
    async fn project_del_templates(
        &self,
        project_id: &Uuid,
        policy_ids: &[PolicyId],
    ) -> Result<(), CacheError>;

    async fn project_get_template_links(
        &self,
        project_id: &Uuid,
    ) -> Result<Vec<TemplateLink>, CacheError>;
    async fn project_set_template_links(
        &self,
        project_id: &Uuid,
        template_links: &[TemplateLink],
    ) -> Result<(), CacheError>;
    async fn project_del_template_links(
        &self,
        project_id: &Uuid,
        policy_ids: &[PolicyId],
    ) -> Result<(), CacheError>;

    async fn project_get_policy_set(&self, project_id: &Uuid) -> Result<PolicySet, CacheError>;
    async fn project_set_policy_set(
        &self,
        project_id: &Uuid,
        policy_set: &PolicySet,
    ) -> Result<(), CacheError>;
}

pub async fn cache_factory(conf: &crate::core::CacheConfig) -> Box<dyn Cache + Send + Sync> {
    match conf {
        crate::core::CacheConfig::ValKeyConfig(conf) => Box::new(valkey::ValKeyCache::new(&conf).await),
        crate::core::CacheConfig::DashMapConfig(_) => Box::new(dashmap::DashMapCache::new()),
    }
}
