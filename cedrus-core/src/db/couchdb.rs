use std::collections::HashMap;

use couch_rs::types::{
    find::{FindQuery, IndexSpec, SortSpec},
    index::IndexFields,
};
use serde_json::{Value, json};
use uuid::Uuid;

use cedrus_cedar::{Entity, EntityUid, Policy, PolicyId, Schema, Template, TemplateLink};

use crate::{
    PageHash, PageList, Query,
    core::{self, IdentitySource, project::Project},
};

use super::{Database, DatabaseError};

const ENTITY_TYPE_DDOC: &str = "cedrus-entity-type-ddoc";
const ENTITY_TYPE_INDEX: &str = "cedrus-entity-type-index";

const ID_KEY: &str = "_id";
const ENTITY_TYPE_KEY: &str = "entityType";
const PROJECT_ID_KEY: &str = "projectId";
const POLICY_ID_KEY: &str = "policyId";
const SCHEMA_KEY: &str = "schema";

const PROJECT_TYPE: &str = "P";
const PROJECT_IDENTITY_SOURCE_TYPE: &str = "PIS";
const PROJECT_SCHEMA_TYPE: &str = "PS";
const PROJECT_ENTITY_TYPE: &str = "PE";
const PROJECT_POLICY_TYPE: &str = "PP";
const PROJECT_TEMPLATE_TYPE: &str = "PT";
const PROJECT_TEMPLATE_LINK_TYPE: &str = "PTL";

pub struct CouchDb {
    client: couch_rs::Client,
    db_name: String,
}

impl CouchDb {
    pub fn new(conf: &core::CouchDbConfig) -> CouchDb {
        let client = couch_rs::Client::new(&conf.uri, &conf.username, &conf.password).unwrap();
        Self {
            client,
            db_name: conf.db_name.clone(),
        }
    }

    pub async fn init(&self) -> Result<(), Box<dyn std::error::Error>> {
        let db = self.client.db(&self.db_name).await?;
        match db
            .insert_index(
                ENTITY_TYPE_INDEX,
                IndexFields {
                    fields: vec![
                        SortSpec::Simple(ENTITY_TYPE_KEY.to_string()),
                        SortSpec::Simple(PROJECT_ID_KEY.to_string()),
                    ],
                },
                None,
                Some(ENTITY_TYPE_DDOC.to_string()),
            )
            .await
        {
            Ok(doc_created) => match doc_created.result {
                Some(r) => println!("Index {} {}", ENTITY_TYPE_INDEX, r),
                None => println!("Index {} validated", ENTITY_TYPE_INDEX),
            },
            Err(e) => {
                println!("Unable to validate index {}: {}", ENTITY_TYPE_INDEX, e);
            }
        };
        Ok(())
    }

    fn project_id(project_id: &Uuid) -> String {
        format!("{}#{}", PROJECT_TYPE, project_id.to_string())
    }

    fn project_to_value(project: &Project) -> Result<Value, DatabaseError> {
        let id = Self::project_id(&project.id);
        let mut value = serde_json::to_value(project)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert(ID_KEY.to_string(), Value::String(id));
            obj.insert(
                ENTITY_TYPE_KEY.to_string(),
                Value::String(PROJECT_TYPE.to_string()),
            );
            obj.insert(
                PROJECT_ID_KEY.to_string(),
                Value::String(Uuid::nil().to_string()),
            );
        }
        Ok(value)
    }

    fn project_from_value(value: Value) -> Result<Project, DatabaseError> {
        Ok(serde_json::from_value(value)?)
    }

    fn project_identity_source_id(project_id: &Uuid) -> String {
        format!(
            "{}#{}",
            PROJECT_IDENTITY_SOURCE_TYPE,
            project_id.to_string()
        )
    }

    fn project_identity_source_to_value(
        project_id: &Uuid,
        identity_source: &IdentitySource,
    ) -> Result<Value, DatabaseError> {
        let id = Self::project_identity_source_id(project_id);
        let mut value = serde_json::to_value(identity_source)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert(ID_KEY.to_string(), Value::String(id));
            obj.insert(
                ENTITY_TYPE_KEY.to_string(),
                Value::String(PROJECT_IDENTITY_SOURCE_TYPE.to_string()),
            );
            obj.insert(
                PROJECT_ID_KEY.to_string(),
                Value::String(project_id.to_string()),
            );
        }
        Ok(value)
    }

    fn project_identity_source_from_value(value: Value) -> Result<IdentitySource, DatabaseError> {
        Ok(serde_json::from_value(value)?)
    }

    fn project_schema_id(project_id: &Uuid) -> String {
        format!("{}#{}", PROJECT_SCHEMA_TYPE, project_id.to_string())
    }

    fn project_schema_to_value(project_id: &Uuid, schema: &Schema) -> Result<Value, DatabaseError> {
        let id = Self::project_schema_id(project_id);
        let value = json!({
            ID_KEY: id,
            ENTITY_TYPE_KEY: PROJECT_SCHEMA_TYPE,
            PROJECT_ID_KEY: project_id,
            SCHEMA_KEY: schema,
        });

        Ok(value)
    }

    fn project_schema_from_value(value: Value) -> Result<Schema, DatabaseError> {
        let Some(schema) = value.get(SCHEMA_KEY) else {
            return Err(DatabaseError::MissingAttribute(SCHEMA_KEY.to_string()));
        };
        Ok(serde_json::from_value(schema.clone())?)
    }

    fn project_entity_id(project_id: &Uuid, entity_uid: &EntityUid) -> String {
        format!(
            "{}#{}#{}",
            PROJECT_ENTITY_TYPE,
            project_id,
            entity_uid.to_string()
        )
    }

    fn project_entity_to_value(project_id: &Uuid, entity: &Entity) -> Result<Value, DatabaseError> {
        let id = Self::project_entity_id(project_id, &entity.uid());
        let mut value = serde_json::to_value(entity)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert(ID_KEY.to_string(), Value::String(id.to_string()));
            obj.insert(
                ENTITY_TYPE_KEY.to_string(),
                Value::String(PROJECT_ENTITY_TYPE.to_string()),
            );
            obj.insert(
                PROJECT_ID_KEY.to_string(),
                Value::String(project_id.to_string()),
            );
        }
        Ok(value)
    }

    fn project_entity_from_value(value: Value) -> Result<Entity, DatabaseError> {
        Ok(serde_json::from_value(value)?)
    }

    fn project_policy_id(project_id: &Uuid, policy_id: &PolicyId) -> String {
        format!(
            "{}#{}#{}",
            PROJECT_POLICY_TYPE,
            project_id,
            policy_id.to_string()
        )
    }

    fn project_policy_to_value(
        project_id: &Uuid,
        policy_id: &PolicyId,
        policy: &Policy,
    ) -> Result<Value, DatabaseError> {
        let id = Self::project_policy_id(project_id, policy_id);
        let mut value = serde_json::to_value(policy)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert(ID_KEY.to_string(), Value::String(id.to_string()));
            obj.insert(
                ENTITY_TYPE_KEY.to_string(),
                Value::String(PROJECT_POLICY_TYPE.to_string()),
            );
            obj.insert(
                PROJECT_ID_KEY.to_string(),
                Value::String(project_id.to_string()),
            );
            obj.insert(
                POLICY_ID_KEY.to_string(),
                Value::String(policy_id.to_string()),
            );
        }
        Ok(value)
    }

    fn project_policy_from_value(value: Value) -> Result<Policy, DatabaseError> {
        Ok(serde_json::from_value(value)?)
    }

    fn project_template_id(project_id: &Uuid, policy_id: &PolicyId) -> String {
        format!(
            "{}#{}#{}",
            PROJECT_TEMPLATE_TYPE,
            project_id,
            policy_id.to_string()
        )
    }

    fn project_template_to_value(
        project_id: &Uuid,
        policy_id: &PolicyId,
        template: &Template,
    ) -> Result<Value, DatabaseError> {
        let id = Self::project_template_id(project_id, policy_id);
        let mut value = serde_json::to_value(template)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert(ID_KEY.to_string(), Value::String(id.to_string()));
            obj.insert(
                ENTITY_TYPE_KEY.to_string(),
                Value::String(PROJECT_TEMPLATE_TYPE.to_string()),
            );
            obj.insert(
                PROJECT_ID_KEY.to_string(),
                Value::String(project_id.to_string()),
            );
            obj.insert(
                POLICY_ID_KEY.to_string(),
                Value::String(policy_id.to_string()),
            );
        }
        Ok(value)
    }

    fn project_template_from_value(value: Value) -> Result<Template, DatabaseError> {
        Ok(serde_json::from_value(value)?)
    }

    fn project_template_link_id(project_id: &Uuid, new_id: &PolicyId) -> String {
        format!(
            "{}#{}#{}",
            PROJECT_TEMPLATE_LINK_TYPE,
            project_id,
            new_id.to_string()
        )
    }

    fn project_template_link_to_value(
        project_id: &Uuid,
        template_link: &TemplateLink,
    ) -> Result<Value, DatabaseError> {
        let id = Self::project_template_link_id(project_id, &template_link.new_id);
        let mut value = serde_json::to_value(template_link)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert(ID_KEY.to_string(), Value::String(id.to_string()));
            obj.insert(
                ENTITY_TYPE_KEY.to_string(),
                Value::String(PROJECT_TEMPLATE_LINK_TYPE.to_string()),
            );
            obj.insert(
                PROJECT_ID_KEY.to_string(),
                Value::String(project_id.to_string()),
            );
        }
        Ok(value)
    }

    fn project_template_link_from_value(value: Value) -> Result<TemplateLink, DatabaseError> {
        Ok(serde_json::from_value(value)?)
    }

    fn query_to_find_query(
        query: &Query,
        entity_type: &str,
        project_id: &Uuid,
    ) -> Result<FindQuery, DatabaseError> {
        let selector = match query.selector.as_ref() {
            Some(selector) => {
                let mut value = serde_json::to_value(selector)?;
                if let Some(obj) = value.as_object_mut() {
                    obj.insert(
                        ENTITY_TYPE_KEY.to_string(),
                        Value::String(entity_type.to_string()),
                    );
                    obj.insert(
                        PROJECT_ID_KEY.to_string(),
                        Value::String(project_id.to_string()),
                    );
                }
                value
            }
            None => serde_json::json!({
                ENTITY_TYPE_KEY: entity_type,
                PROJECT_ID_KEY: project_id.to_string()
            }),
        };
        let find = FindQuery::new(selector).use_index(IndexSpec::IndexName((
            ENTITY_TYPE_DDOC.to_string(),
            ENTITY_TYPE_INDEX.to_string(),
        )));

        Ok(find)
    }
}

#[async_trait::async_trait]
impl Database for CouchDb {
    async fn projects_load(&self, query: &Query) -> Result<PageList<Project>, DatabaseError> {
        let db = self.client.db(&self.db_name).await?;

        let find = Self::query_to_find_query(query, PROJECT_TYPE, &Uuid::nil())?;
        let docs = db.find_raw(&find).await?;

        let mut datas = Vec::new();
        for doc in docs.rows {
            datas.push(Self::project_from_value(doc)?);
        }

        Ok(PageList::new(datas, docs.bookmark))
    }

    async fn project_load(&self, id: &Uuid) -> Result<Option<Project>, DatabaseError> {
        let id = Self::project_id(id);
        let db = self.client.db(&self.db_name).await?;
        if let Some(doc) = db.get::<Value>(&id).await.ok() {
            return Ok(Some(Self::project_from_value(doc)?));
        }

        Ok(None)
    }

    async fn project_save(&self, project: &Project) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        let mut value = Self::project_to_value(project)?;
        db.upsert(&mut value).await?;

        Ok(())
    }

    async fn project_remove(&self, id: &Uuid) -> Result<(), DatabaseError> {
        let id = Self::project_id(id);
        let db = self.client.db(&self.db_name).await?;
        if let Some(doc) = db.get::<Value>(&id).await.ok() {
            let _ = db.remove(&doc).await;
        }

        Ok(())
    }

    async fn project_identity_source_load(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<IdentitySource>, DatabaseError> {
        let id = Self::project_identity_source_id(project_id);
        let db = self.client.db(&self.db_name).await?;
        if let Some(doc) = db.get::<Value>(&id).await.ok() {
            return Ok(Some(Self::project_identity_source_from_value(doc)?));
        }
        Ok(None)
    }

    async fn project_identity_source_save(
        &self,
        project_id: &Uuid,
        identity_source: &IdentitySource,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        let mut value = Self::project_identity_source_to_value(project_id, identity_source)?;
        db.upsert(&mut value).await?;

        Ok(())
    }

    async fn project_identity_source_remove(&self, project_id: &Uuid) -> Result<(), DatabaseError> {
        let id = Self::project_identity_source_id(project_id);
        let db = self.client.db(&self.db_name).await?;
        if let Some(doc) = db.get::<Value>(&id).await.ok() {
            let _ = db.remove(&doc).await;
        }

        Ok(())
    }

    async fn project_schema_load(
        &self,
        project_id: &Uuid,
    ) -> Result<Option<Schema>, DatabaseError> {
        let id = Self::project_schema_id(project_id);
        let db = self.client.db(&self.db_name).await?;
        if let Some(doc) = db.get::<Value>(&id).await.ok() {
            return Ok(Some(Self::project_schema_from_value(doc)?));
        }
        Ok(None)
    }

    async fn project_schema_save(
        &self,
        project_id: &Uuid,
        schema: &Schema,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        let mut value = Self::project_schema_to_value(project_id, schema)?;
        db.upsert(&mut value).await?;

        Ok(())
    }

    async fn project_schema_remove(&self, project_id: &Uuid) -> Result<(), DatabaseError> {
        let id = Self::project_schema_id(project_id);
        let db = self.client.db(&self.db_name).await?;
        if let Some(doc) = db.get::<Value>(&id).await.ok() {
            let _ = db.remove(&doc).await;
        }

        Ok(())
    }

    async fn project_entities_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<Entity>, DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        let find = Self::query_to_find_query(query, PROJECT_ENTITY_TYPE, project_id)?;
        let docs = db.find_raw(&find).await?;

        let mut datas = Vec::new();
        for doc in docs.rows {
            datas.push(Self::project_entity_from_value(doc)?);
        }

        Ok(PageList::new(datas, docs.bookmark))
    }

    async fn project_entities_save(
        &self,
        project_id: &Uuid,
        entities: &Vec<Entity>,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        for entity in entities {
            let mut value = Self::project_entity_to_value(&project_id, entity)?;
            db.upsert(&mut value).await?;
        }

        Ok(())
    }

    async fn project_entities_remove(
        &self,
        project_id: &Uuid,
        entity_uids: &Vec<EntityUid>,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        for entity_uid in entity_uids {
            let id = Self::project_entity_id(&project_id, entity_uid);
            if let Some(doc) = db.get::<Value>(&id).await.ok() {
                let _ = db.remove(&doc).await;
            }
        }

        Ok(())
    }

    async fn project_policies_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageHash<PolicyId, Policy>, DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        let find = Self::query_to_find_query(query, PROJECT_POLICY_TYPE, project_id)?;
        let docs = db.find_raw(&find).await?;

        let mut datas = HashMap::new();
        for doc in docs.rows {
            if let Some(policy_id) = doc.get(POLICY_ID_KEY) {
                if let Some(policy_id) = policy_id.as_str() {
                    datas.insert(
                        policy_id.to_string().into(),
                        Self::project_policy_from_value(doc)?,
                    );
                }
            }
        }

        Ok(PageHash::new(datas, docs.bookmark))
    }

    async fn project_policies_save(
        &self,
        project_id: &Uuid,
        policies: &HashMap<PolicyId, Policy>,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        for (policy_id, policy) in policies {
            let mut value = Self::project_policy_to_value(&project_id, policy_id, policy)?;
            db.upsert(&mut value).await?;
        }

        Ok(())
    }

    async fn project_policies_remove(
        &self,
        project_id: &Uuid,
        policy_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        for policy_id in policy_ids {
            let id = Self::project_policy_id(&project_id, policy_id);
            if let Some(doc) = db.get::<Value>(&id).await.ok() {
                let _ = db.remove(&doc).await;
            }
        }

        Ok(())
    }

    async fn project_templates_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageHash<PolicyId, Template>, DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        let find = Self::query_to_find_query(query, PROJECT_TEMPLATE_TYPE, project_id)?;
        let docs = db.find_raw(&find).await?;

        let mut datas = HashMap::new();
        for doc in docs.rows {
            if let Some(policy_id) = doc.get(POLICY_ID_KEY) {
                if let Some(policy_id) = policy_id.as_str() {
                    datas.insert(
                        policy_id.to_string().into(),
                        Self::project_template_from_value(doc)?,
                    );
                }
            }
        }

        Ok(PageHash::new(datas, docs.bookmark))
    }

    async fn project_templates_save(
        &self,
        project_id: &Uuid,
        templates: &HashMap<PolicyId, Template>,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        for (template_id, template) in templates {
            let mut value = Self::project_template_to_value(&project_id, template_id, template)?;
            db.upsert(&mut value).await?;
        }

        Ok(())
    }

    async fn project_templates_remove(
        &self,
        project_id: &Uuid,
        template_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        for template_id in template_ids {
            let id = Self::project_template_id(&project_id, template_id);
            if let Some(doc) = db.get::<Value>(&id).await.ok() {
                let _ = db.remove(&doc).await;
            }
        }

        Ok(())
    }

    async fn project_template_links_load(
        &self,
        project_id: &Uuid,
        query: &Query,
    ) -> Result<PageList<TemplateLink>, DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        let find = Self::query_to_find_query(query, PROJECT_TEMPLATE_LINK_TYPE, project_id)?;
        let docs = db.find_raw(&find).await?;

        let mut datas = Vec::new();
        for doc in docs.rows {
            datas.push(Self::project_template_link_from_value(doc)?);
        }

        Ok(PageList::new(datas, docs.bookmark))
    }

    async fn project_template_links_save(
        &self,
        project_id: &Uuid,
        template_links: &Vec<TemplateLink>,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        for template_link in template_links {
            let mut value = Self::project_template_link_to_value(&project_id, template_link)?;
            db.upsert(&mut value).await?;
        }

        Ok(())
    }

    async fn project_template_links_remove(
        &self,
        project_id: &Uuid,
        link_ids: &Vec<PolicyId>,
    ) -> Result<(), DatabaseError> {
        let db = self.client.db(&self.db_name).await?;
        for new_id in link_ids {
            let id = Self::project_template_link_id(&project_id, new_id);
            if let Some(doc) = db.get::<Value>(&id).await.ok() {
                let _ = db.remove(&doc).await;
            }
        }

        Ok(())
    }
}
