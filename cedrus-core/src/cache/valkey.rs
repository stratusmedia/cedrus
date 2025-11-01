use std::collections::HashMap;

use base64::{Engine, prelude::BASE64_STANDARD};
use cedrus_cedar::{
    Entity, EntityUid, Policy, PolicyId, PolicySet, Schema, Template, TemplateLink, proto,
};
use prost::Message;
use redis::{
    AsyncCommands, RedisError, aio::MultiplexedConnection, cluster_async::ClusterConnection,
};
use uuid::Uuid;

use crate::core::{
    self, IdentitySource,
    project::{PROJECT_ENTITY_TYPE, Project},
};

use super::{Cache, CacheError};

enum ConnectionType {
    Multiplexed(MultiplexedConnection),
    Cluster(ClusterConnection),
}

impl ConnectionType {
    async fn get(&self, key: &str) -> Result<Option<String>, RedisError> {
        match self {
            ConnectionType::Multiplexed(conn) => {
                let mut conn = conn.clone();
                Ok(conn.get::<_, Option<String>>(key).await?)
            }
            ConnectionType::Cluster(conn) => {
                let mut conn = conn.clone();
                Ok(conn.get::<_, Option<String>>(key).await?)
            }
        }
    }

    async fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
        match self {
            ConnectionType::Multiplexed(conn) => {
                let mut conn = conn.clone();
                Ok(conn.set(key, value).await?)
            }
            ConnectionType::Cluster(conn) => {
                let mut conn = conn.clone();
                Ok(conn.set(key, value).await?)
            }
        }
    }

    async fn del(&self, keys: &Vec<String>) -> Result<(), RedisError> {
        match self {
            ConnectionType::Multiplexed(conn) => {
                let mut conn = conn.clone();
                Ok(conn.del(keys).await?)
            }
            ConnectionType::Cluster(conn) => {
                let mut conn = conn.clone();
                Ok(conn.del(keys).await?)
            }
        }
    }

    async fn scan_match(&self, pattern: &str) -> Result<Vec<String>, RedisError> {
        match self {
            ConnectionType::Multiplexed(conn) => {
                let mut keys = Vec::new();
                let mut conn = conn.clone();
                let mut iter = conn.scan_match::<_, Option<String>>(pattern).await.unwrap();
                while let Some(element) = iter.next_item().await {
                    if let Some(key) = element {
                        keys.push(key);
                    }
                }
                Ok(keys)
            }
            ConnectionType::Cluster(conn) => {
                let mut keys = Vec::new();
                let mut conn = conn.clone();
                let mut iter = conn.scan_match::<_, Option<String>>(pattern).await.unwrap();
                while let Some(element) = iter.next_item().await {
                    if let Some(key) = element {
                        keys.push(key);
                    }
                }
                Ok(keys)
            }
        }
    }

    async fn mget(&self, keys: &Vec<String>) -> Result<Vec<Option<String>>, RedisError> {
        match self {
            ConnectionType::Multiplexed(conn) => {
                let mut conn = conn.clone();
                Ok(conn.mget::<_, Vec<Option<String>>>(keys).await?)
            }
            ConnectionType::Cluster(conn) => {
                let mut conn = conn.clone();
                Ok(conn.mget::<_, Vec<Option<String>>>(keys).await?)
            }
        }
    }

    async fn mset(&self, sets: &Vec<(String, String)>) -> Result<(), RedisError> {
        match self {
            ConnectionType::Multiplexed(conn) => {
                let mut conn = conn.clone();
                Ok(conn.mset(sets).await?)
            }
            ConnectionType::Cluster(conn) => {
                let mut conn = conn.clone();
                Ok(conn.mset(sets).await?)
            }
        }
    }
}

pub struct ValKeyCache {
    conn: ConnectionType,
}

impl ValKeyCache {
    pub async fn new(conf: &core::ValKeyCacheConfig) -> Self {
        let conn = if conf.cluster {
            let client = redis::cluster::ClusterClient::new(conf.urls.clone()).unwrap();
            let conn = client.get_async_connection().await.unwrap();
            ConnectionType::Cluster(conn)
        } else {
            let url = conf.urls.get(0).unwrap();
            let client = redis::Client::open(url.clone()).unwrap();
            let conn = client.get_multiplexed_tokio_connection().await.unwrap();
            ConnectionType::Multiplexed(conn)
        };

        Self { conn }
    }

    fn project_identity_source_key(&self, project_id: &Uuid) -> String {
        format!("cedrus:p:{}:is", project_id)
    }

    fn project_schema_key(&self, project_id: &Uuid) -> String {
        format!("cedrus:p:{}:s", project_id)
    }

    fn entities_pattern(&self, project_id: &Uuid) -> String {
        format!("cedrus:p:{}:e:*", project_id)
    }
    fn entities_key(&self, project_id: &Uuid, entity_uid: &EntityUid) -> String {
        format!("cedrus:p:{}:e:{}", project_id, entity_uid.to_string())
    }

    fn policies_pattern(&self, project_id: &Uuid) -> String {
        format!("cedrus:p:{}:p:*", project_id)
    }
    fn policies_key(&self, project_id: &Uuid, policy_id: &PolicyId) -> String {
        format!("cedrus:p:{}:p:{}", project_id, policy_id.to_string())
    }

    fn templates_pattern(&self, project_id: &Uuid) -> String {
        format!("cedrus:p:{}:t:*", project_id)
    }
    fn templates_key(&self, project_id: &Uuid, policy_id: &PolicyId) -> String {
        format!("cedrus:p:{}:t:{}", project_id, policy_id.to_string())
    }

    fn template_links_pattern(&self, project_id: &Uuid) -> String {
        format!("cedrus:p:{}:tl:*", project_id)
    }
    fn template_links_key(&self, project_id: &Uuid, policy_id: &PolicyId) -> String {
        format!("cedrus:p:{}:tl:{}", project_id, policy_id.to_string())
    }

    fn project_pattern(&self) -> String {
        format!("cedrus:prj:*")
    }
    fn project_key(&self, project_id: &Uuid) -> String {
        format!("cedrus:prj:{}", project_id)
    }

    fn entity_to_val(&self, entity: &Entity) -> String {
        let proto: proto::Entity = entity.clone().into();
        BASE64_STANDARD.encode(proto.encode_to_vec())
    }

    fn entity_from_val(&self, val: String) -> Entity {
        let buf = BASE64_STANDARD.decode(val).unwrap();
        let proto = proto::Entity::decode(&*buf).unwrap();
        let entity: Entity = proto.into();
        entity
    }

    async fn keys_from_pattern(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        let keys = self.conn.scan_match(pattern).await?;
        Ok(keys)
    }
}

#[async_trait::async_trait]
impl Cache for ValKeyCache {
    async fn project_clear(&self, project_id: &Uuid) -> Result<(), CacheError> {
        let pattern = format!("cedrus:p:{}:*", project_id.to_string());
        let mut keys = self.keys_from_pattern(&pattern).await?;

        keys.push(self.project_key(project_id));

        let _: () = self.conn.del(&keys).await?;
        Ok(())
    }

    async fn projects_get(&self) -> Result<Vec<Project>, CacheError> {
        let pattern = self.project_pattern();
        let keys = self.keys_from_pattern(&pattern).await?;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut projects = Vec::new();
        let vals = self.conn.mget(&keys).await?;
        for val in vals {
            if let Some(val) = val {
                let project = serde_json::from_str(&val).unwrap();
                projects.push(project);
            }
        }

        Ok(projects)
    }

    async fn project_get(&self, project_id: &Uuid) -> Result<Option<Project>, CacheError> {
        let key = self.project_key(project_id);
        let val = self.conn.get(&key).await?;
        let project: Option<Project> = match val {
            Some(val) => Some(serde_json::from_str(&val).unwrap()),
            None => None,
        };

        Ok(project)
    }

    async fn project_set(&self, project: &Project) -> Result<(), CacheError> {
        let key = self.project_key(&project.id);
        let val = serde_json::to_string(project).unwrap();
        let _: () = self.conn.set(&key, &val).await?;

        Ok(())
    }

    async fn project_del(&self, project_id: &Uuid) -> Result<(), CacheError> {
        let pattern = format!("cedrus:p:{}:*", project_id.to_string());
        let mut keys = self.keys_from_pattern(&pattern).await?;

        keys.push(self.project_key(project_id));

        let uid = EntityUid::new(PROJECT_ENTITY_TYPE.to_string(), project_id.to_string());
        let key = format!("cedrus:p:{}:e:{}", Uuid::nil(), uid.to_string());
        keys.push(key);

        let pattern = format!("cedrus:p:{}:tl:{}_*", Uuid::nil(), project_id.to_string());
        let mut tls = self.keys_from_pattern(&pattern).await?;
        keys.append(&mut tls);

        let _: () = self.conn.del(&keys).await?;

        Ok(())
    }

    async fn project_get_identity_source(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<IdentitySource>, CacheError> {
        let key = self.project_identity_source_key(project_id);
        let val = self.conn.get(&key).await?;
        let identity_source: Option<IdentitySource> = match val {
            Some(val) => Some(serde_json::from_str(&val).unwrap()),
            None => None,
        };

        Ok(identity_source)
    }

    async fn project_set_identity_source(
        &self,
        project_id: &Uuid,
        identity_source: &IdentitySource,
    ) -> Result<(), CacheError> {
        let key = self.project_identity_source_key(project_id);
        let val = serde_json::to_string(identity_source).unwrap();
        let _: () = self.conn.set(&key, &val).await?;

        Ok(())
    }

    async fn project_del_identity_source(&self, project_id: &Uuid) -> Result<(), CacheError> {
        let key = self.project_identity_source_key(project_id);
        let keys = Vec::from([key]);

        let _: () = self.conn.del(&keys).await?;

        Ok(())
    }

    async fn project_get_schema(&self, project_id: &Uuid) -> Result<Option<Schema>, CacheError> {
        let key = self.project_schema_key(project_id);
        let val = self.conn.get(&key).await?;
        let schema: Option<Schema> = match val {
            Some(val) => Some(serde_json::from_str(&val).unwrap()),
            None => None,
        };

        Ok(schema)
    }

    async fn project_set_schema(
        &self,
        project_id: &Uuid,
        schema: &Schema,
    ) -> Result<(), CacheError> {
        let key = self.project_schema_key(project_id);
        let val = serde_json::to_string(schema).unwrap();
        let _: () = self.conn.set(&key, &val).await?;

        Ok(())
    }

    async fn project_del_schema(&self, project_id: &Uuid) -> Result<(), CacheError> {
        let key = self.project_schema_key(project_id);
        let keys = Vec::from([key]);

        let _: () = self.conn.del(&keys).await?;

        Ok(())
    }

    async fn project_get_entities(
        &self,
        project_id: &Uuid,
        entity_uids: &[EntityUid],
    ) -> Result<Vec<Entity>, CacheError> {
        let mut keys = Vec::new();
        if entity_uids.is_empty() {
            let pattern = self.entities_pattern(project_id);
            let data = self.keys_from_pattern(&pattern).await?;
            keys.extend(data);
        } else {
            for entity_uid in entity_uids {
                let key = self.entities_key(project_id, entity_uid);
                keys.push(key);
            }
        }

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut entities = Vec::new();
        let vals = self.conn.mget(&keys).await?;
        for val in vals {
            if let Some(val) = val {
                let entity = self.entity_from_val(val);
                entities.push(entity);
            }
        }

        Ok(entities)
    }

    async fn project_set_entities(
        &self,
        project_id: &Uuid,
        entities: &[Entity],
    ) -> Result<(), CacheError> {
        let mut map = HashMap::new();
        for entity in entities {
            let key = self.entities_key(project_id, entity.uid());
            let val = self.entity_to_val(entity);
            map.insert(key, val);
        }

        if map.is_empty() {
            return Ok(());
        }

        let vec_tuples = map.into_iter().collect::<Vec<(String, String)>>();

        let _: () = self.conn.mset(&vec_tuples).await?;

        Ok(())
    }

    async fn project_del_entities(
        &self,
        project_id: &Uuid,
        entity_uids: &[EntityUid],
    ) -> Result<(), CacheError> {
        let mut keys = Vec::new();
        for entity_uid in entity_uids {
            let key = self.entities_key(project_id, entity_uid);
            keys.push(key);
        }

        if keys.is_empty() {
            return Ok(());
        }

        let _: () = self.conn.del(&keys).await?;

        Ok(())
    }

    async fn project_get_policies(
        &self,
        project_id: &Uuid,
    ) -> Result<HashMap<PolicyId, Policy>, CacheError> {
        let pattern = self.policies_pattern(project_id);
        let keys = self.keys_from_pattern(&pattern).await?;

        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let mut policies = HashMap::new();
        let vals = self.conn.mget(&keys).await?;
        for (i, val) in vals.iter().enumerate() {
            if let Some(val) = val {
                let policy_id = PolicyId::from(keys[i].split(':').last().unwrap().to_string());
                let policy: Policy = serde_json::from_str(&val).unwrap();
                policies.insert(policy_id, policy);
            }
        }

        Ok(policies)
    }
    async fn project_set_policies(
        &self,
        project_id: &Uuid,
        policies: &HashMap<PolicyId, Policy>,
    ) -> Result<(), CacheError> {
        let mut map = HashMap::new();
        for (policy_id, policy) in policies {
            let key = self.policies_key(project_id, policy_id);
            let val = serde_json::to_string(policy).unwrap();
            map.insert(key, val);
        }

        if map.is_empty() {
            return Ok(());
        }

        let vec_tuples = map.into_iter().collect::<Vec<(String, String)>>();
        let _: () = self.conn.mset(&vec_tuples).await?;

        Ok(())
    }
    async fn project_del_policies(
        &self,
        project_id: &Uuid,
        policy_ids: &[PolicyId],
    ) -> Result<(), CacheError> {
        let mut keys = Vec::new();
        for policy_id in policy_ids {
            let key = self.policies_key(project_id, policy_id);
            keys.push(key);
        }

        if keys.is_empty() {
            return Ok(());
        }

        let _: () = self.conn.del(&keys).await?;

        Ok(())
    }

    async fn project_get_templates(
        &self,
        project_id: &Uuid,
    ) -> Result<HashMap<PolicyId, Template>, CacheError> {
        let pattern = self.templates_pattern(project_id);
        let keys = self.keys_from_pattern(&pattern).await?;

        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let mut templates = HashMap::new();
        let vals = self.conn.mget(&keys).await?;
        for (i, val) in vals.iter().enumerate() {
            if let Some(val) = val {
                let policy_id = PolicyId::from(keys[i].split(':').last().unwrap().to_string());
                let template: Template = serde_json::from_str(&val).unwrap();
                templates.insert(policy_id, template);
            }
        }

        Ok(templates)
    }
    async fn project_set_templates(
        &self,
        project_id: &Uuid,
        templates: &HashMap<PolicyId, Template>,
    ) -> Result<(), CacheError> {
        let mut map = HashMap::new();
        for (policy_id, template) in templates {
            let key = self.templates_key(project_id, policy_id);
            let val = serde_json::to_string(template).unwrap();
            map.insert(key, val);
        }

        if map.is_empty() {
            return Ok(());
        }

        let vec_tuples = map.into_iter().collect::<Vec<(String, String)>>();
        let _: () = self.conn.mset(&vec_tuples).await?;

        Ok(())
    }

    async fn project_del_templates(
        &self,
        project_id: &Uuid,
        policy_ids: &[PolicyId],
    ) -> Result<(), CacheError> {
        let mut keys = Vec::new();
        for policy_id in policy_ids {
            let key = self.templates_key(project_id, policy_id);
            keys.push(key);
        }

        if keys.is_empty() {
            return Ok(());
        }

        let _: () = self.conn.del(&keys).await?;

        Ok(())
    }

    async fn project_get_template_links(
        &self,
        project_id: &Uuid,
    ) -> Result<Vec<TemplateLink>, CacheError> {
        let pattern = self.template_links_pattern(project_id);
        let keys = self.keys_from_pattern(&pattern).await?;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut template_links = Vec::new();
        let vals = self.conn.mget(&keys).await?;
        for val in vals {
            if let Some(val) = val {
                let template_link: TemplateLink = serde_json::from_str(&val).unwrap();
                template_links.push(template_link);
            }
        }

        Ok(template_links)
    }
    async fn project_set_template_links(
        &self,
        project_id: &Uuid,
        template_links: &[TemplateLink],
    ) -> Result<(), CacheError> {
        let mut map = HashMap::new();
        for template_link in template_links {
            let key = self.template_links_key(project_id, &template_link.new_id);
            let val = serde_json::to_string(template_link).unwrap();
            map.insert(key, val);
        }

        if map.is_empty() {
            return Ok(());
        }

        let vec_tuples = map.into_iter().collect::<Vec<(String, String)>>();
        let _: () = self.conn.mset(&vec_tuples).await?;

        Ok(())
    }
    async fn project_del_template_links(
        &self,
        project_id: &Uuid,
        policy_ids: &[PolicyId],
    ) -> Result<(), CacheError> {
        let mut keys = Vec::new();
        for policy_id in policy_ids {
            let key = self.template_links_key(project_id, policy_id);
            keys.push(key);
        }

        if keys.is_empty() {
            return Ok(());
        }

        let _: () = self.conn.del(&keys).await?;

        Ok(())
    }

    async fn project_get_policy_set(&self, project_id: &Uuid) -> Result<PolicySet, CacheError> {
        let mut policy_set = PolicySet::default();

        let (static_policies, templates, template_links) = tokio::join!(
            self.project_get_policies(project_id),
            self.project_get_templates(project_id),
            self.project_get_template_links(project_id),
        );

        policy_set.static_policies = static_policies?;
        policy_set.templates = templates?;
        policy_set.template_links = template_links?;

        Ok(policy_set)
    }

    async fn project_set_policy_set(
        &self,
        project_id: &Uuid,
        policy_set: &PolicySet,
    ) -> Result<(), CacheError> {
        self.project_set_policies(project_id, &policy_set.static_policies)
            .await?;
        self.project_set_templates(project_id, &policy_set.templates)
            .await?;
        self.project_set_template_links(project_id, &policy_set.template_links)
            .await?;

        Ok(())
    }
}
