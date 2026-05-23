use std::collections::{HashMap, HashSet};

use base64::{Engine, prelude::BASE64_STANDARD};
use dashmap::DashMap;
use jwt_authorizer::{JwtAuthorizer, Validation};
use serde_json::Value;
use uuid::Uuid;

use cedrus_cedar::{
    Context, Entity, EntityUid, Policy, PolicyId, PolicySet, Request, Response, Schema, Template,
    TemplateLink,
};

use crate::{
    Authorizer, CedrusError, Event, EventType, PageHash, PageList, Query, cache::Cache,
    db::Database, pubsub::PubSub,
};

use super::{
    CedrusConfig, IdentitySource,
    is::Configuration,
    project::{ApiKey, Project},
};

pub async fn authorizer_factory(
    conf: &Configuration,
) -> Result<jwt_authorizer::Authorizer<Value>, CedrusError> {
    match conf {
        Configuration::CognitoUserPoolConfiguration(conf) => {
            let url = conf.url_keys();
            let iss = vec![conf.iss()];
            let validation = Validation::new().iss(&iss).aud(&conf.client_ids);
            JwtAuthorizer::from_jwks_url(&url)
                .validation(validation)
                .build()
                .await
                .map_err(|e| CedrusError::AuthorizerError(e.to_string()))
        }
        Configuration::OpenIdConnectConfiguration(conf) => {
            let validation = match &conf.token_selection {
                super::is::OpenIdConnectTokenSelection::AccessTokenOnly(configuration) => {
                    Validation::new().aud(&configuration.audiences)
                }
                super::is::OpenIdConnectTokenSelection::IdentityTokenOnly(configuration) => {
                    Validation::new().aud(&configuration.client_ids)
                }
            };
            JwtAuthorizer::from_oidc(&conf.issuer)
                .validation(validation)
                .build()
                .await
                .map_err(|e| CedrusError::AuthorizerError(e.to_string()))
        }
    }
}

pub struct Cedrus {
    pub id: Uuid, // Container Identity, used for cluster comunictaion

    pub db: Box<dyn Database + Send + Sync>,
    pub cache: Box<dyn Cache + Send + Sync>,
    pub pubsub: Box<dyn PubSub + Send + Sync>,

    pub exclude_policy_annotation: Option<String>,

    pub api_keys: DashMap<String, EntityUid>,

    pub project_authorizers: DashMap<Uuid, Option<Authorizer>>,
    pub project_cedar_schemas: DashMap<Uuid, Option<cedar_policy::Schema>>,
    pub project_cedar_entities: DashMap<Uuid, cedar_policy::Entities>,
    pub project_cedar_policies: DashMap<Uuid, cedar_policy::PolicySet>,
}

impl Cedrus {
    pub async fn new(
        db: Box<dyn Database + Send + Sync>,
        cache: Box<dyn Cache + Send + Sync>,
        pubsub: Box<dyn PubSub + Send + Sync>,
        exclude_policy_annotation: Option<String>,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),

            db,
            cache,
            pubsub,

            exclude_policy_annotation,

            api_keys: DashMap::new(),

            project_authorizers: DashMap::new(),
            project_cedar_schemas: DashMap::new(),
            project_cedar_entities: DashMap::new(),
            project_cedar_policies: DashMap::new(),
        }
    }

    pub async fn init_admin_project(
        &mut self,
        config: &CedrusConfig,
        admin_api_key: String,
    ) -> Result<(), CedrusError> {
        // Find project with id nil
        if let Some(project) = self.db.project_load(&Uuid::nil()).await? {
            let api_keys = self
                .db
                .project_apikeys_load(&project.id, &Query::new())
                .await?;
            let found = api_keys.items.iter().find(|ak| ak.id == Uuid::nil());

            if let Some(api_key) = found {
                if api_key.key != admin_api_key {
                    let mut api_key = api_key.clone();
                    api_key.key = admin_api_key;
                    api_key.updated_at = chrono::Utc::now();
                    self.db
                        .project_apikeys_save(&project.id, &vec![api_key])
                        .await?;
                }
            } else {
                let api_key = ApiKey::new(
                    Uuid::nil(),
                    admin_api_key,
                    "Admin API Key".to_string(),
                    Uuid::nil(),
                    project.owner.clone(),
                );
                self.db
                    .project_apikeys_save(&project.id, &vec![api_key])
                    .await?;
            }
        } else {
            let schema_str = include_str!("../../config/cedrus.cedarschema.json");
            let entities_str = include_str!("../../config/cedrus.cedarentities.json");
            let policy_set_str = include_str!("../../config/cedrus.cedar.json");
            let schema: Schema = serde_json::from_str(schema_str)?;
            let entities: Vec<Entity> = serde_json::from_str(entities_str)?;
            let policy_set: PolicySet = serde_json::from_str(policy_set_str)?;

            let now = chrono::Utc::now();
            let owner = EntityUid::new("User".to_string(), Uuid::nil().to_string());
            let project = Project {
                id: Uuid::nil(),
                name: "Cedrus Admin Project".to_string(),
                owner: owner.clone(),
                created_at: now,
                updated_at: now,
                ..Default::default()
            };
            self.db.project_save(&project).await?;

            let api_key = ApiKey::new(
                Uuid::nil(),
                admin_api_key,
                "Admin API Key".to_string(),
                Uuid::nil(),
                owner.clone(),
            );
            self.db
                .project_apikeys_save(&project.id, &vec![api_key])
                .await?;

            self.db.project_schema_save(&project.id, &schema).await?;

            self.db
                .project_entities_save(&project.id, &entities)
                .await?;

            self.db
                .project_policies_save(&project.id, &policy_set.static_policies)
                .await?;
            self.db
                .project_templates_save(&project.id, &policy_set.templates)
                .await?;
            self.db
                .project_template_links_save(&project.id, &policy_set.template_links)
                .await?;
        }

        if let Some(identity_source) = &config.identity_source {
            self.db
                .project_identity_source_save(&Uuid::nil(), identity_source)
                .await?;
        }

        Ok(())
    }

    pub async fn init_cache(&mut self) -> Result<(), CedrusError> {
        let query = Query::new();
        let projects = self.db.projects_load(&query).await?;

        let query = Query::new();
        for project in projects.items {
            self.cache.project_clear(&project.id).await?;

            let apikeys = self.db.project_apikeys_load(&project.id, &query).await?;
            let entities = self.db.project_entities_load(&project.id, &query).await?;
            let static_policies = self.db.project_policies_load(&project.id, &query).await?;
            let templates = self.db.project_templates_load(&project.id, &query).await?;
            let template_links = self
                .db
                .project_template_links_load(&project.id, &query)
                .await?;

            self.cache.project_set(&project).await?;

            if let Some(identity_source) = self.db.project_identity_source_load(&project.id).await?
            {
                self.cache
                    .project_set_identity_source(&project.id, &identity_source)
                    .await?;
            }

            if let Some(schema) = self.db.project_schema_load(&project.id).await? {
                self.cache.project_set_schema(&project.id, &schema).await?;
            }

            self.cache
                .project_set_apikeys(&project.id, &apikeys.items)
                .await?;
            self.cache
                .project_set_entities(&project.id, &entities.items)
                .await?;
            self.cache
                .project_set_policies(&project.id, &static_policies.items)
                .await?;
            self.cache
                .project_set_templates(&project.id, &templates.items)
                .await?;
            self.cache
                .project_set_template_links(&project.id, &template_links.items)
                .await?;
        }

        Ok(())
    }

    pub async fn load_cache(&self) -> Result<(), CedrusError> {
        let projects = self.cache.projects_get().await?;
        for project in projects {
            self.on_project_set(&project)?;

            let apikeys = self.cache.project_get_apikeys(&project.id).await?;
            self.on_project_apikeys_set(&apikeys)?;

            let cache_identity_source = self.cache.project_get_identity_source(&project.id).await?;
            if let Some(identity_source) = cache_identity_source {
                self.on_project_identity_source_set(&project.id, &identity_source)
                    .await?;
            }

            let cache_schema = self.cache.project_get_schema(&project.id).await?;
            if let Some(schema) = &cache_schema {
                self.on_project_schema_set(&project.id, &schema)?;
            }

            self.on_project_entities(&project.id).await?;
            self.on_project_policy_set(&project.id).await?;
        }

        Ok(())
    }

    fn on_project_set(&self, project: &Project) -> Result<(), CedrusError> {
        self.project_cedar_schemas.insert(project.id, None);
        self.project_cedar_entities
            .insert(project.id, cedar_policy::Entities::empty());
        self.project_cedar_policies
            .insert(project.id, cedar_policy::PolicySet::new());

        Ok(())
    }

    fn on_project_del(&self, project_id: &Uuid, api_keys: &[String]) -> Result<(), CedrusError> {
        self.project_cedar_schemas.remove(project_id);
        self.project_cedar_entities.remove(project_id);
        self.project_cedar_policies.remove(project_id);

        for api_key in api_keys {
            self.api_keys.remove(api_key);
        }

        Ok(())
    }

    fn on_project_apikeys_set(&self, api_keys: &[ApiKey]) -> Result<(), CedrusError> {
        for api_key in api_keys {
            self.api_keys
                .insert(api_key.key.clone(), api_key.owner.clone());
        }

        Ok(())
    }

    fn on_project_apikeys_del(&self, api_keys: &[String]) -> Result<(), CedrusError> {
        for api_key in api_keys {
            self.api_keys.remove(api_key);
        }

        Ok(())
    }

    async fn on_project_identity_source_set(
        &self,
        project_id: &Uuid,
        identity_source: &IdentitySource,
    ) -> Result<(), CedrusError> {
        let authorizer = authorizer_factory(&identity_source.configuration).await?;
        let authorizer = Authorizer::new(identity_source.clone(), authorizer);
        self.project_authorizers
            .insert(*project_id, Some(authorizer));

        Ok(())
    }

    fn on_project_identity_source_del(&self, project_id: &Uuid) -> Result<(), CedrusError> {
        self.project_authorizers.remove(project_id);

        Ok(())
    }

    fn on_project_schema_set(&self, project_id: &Uuid, schema: &Schema) -> Result<(), CedrusError> {
        let cedar_schema: Option<cedar_policy::Schema> = Some(schema.clone().try_into()?);
        self.project_cedar_schemas.insert(*project_id, cedar_schema);

        Ok(())
    }

    fn on_project_schema_del(&self, project_id: &Uuid) -> Result<(), CedrusError> {
        self.project_cedar_schemas.insert(*project_id, None);

        Ok(())
    }

    async fn on_project_entities(&self, project_id: &Uuid) -> Result<(), CedrusError> {
        let mut cache_entities = self.cache.project_get_entities(project_id, &[]).await?;

        let cache_schema: Option<Schema> = self.cache.project_get_schema(project_id).await?;
        if let Some(schema) = cache_schema.clone() {
            for ns_name in schema.0.keys() {
                let ns = schema.0.get(ns_name).unwrap();
                for entity_type_name in ns.entity_types.keys() {
                    let entity_type = ns.entity_types.get(entity_type_name).unwrap();
                    if let Some(r#enum) = &entity_type.r#enum {
                        let type_name = if ns_name.is_empty() {
                            entity_type_name.to_string()
                        } else {
                            format!("{}::{}", ns_name, entity_type_name)
                        };
                        for enum_value in r#enum {
                            let uid = EntityUid::from(format!("{}::{}", type_name, enum_value));
                            let entity = Entity::new(uid, HashMap::new(), HashSet::new());
                            cache_entities.push(entity);
                        }
                    }
                }
            }
        }

        let cedar_schema: Option<cedar_policy::Schema> =
            cache_schema.map(|s| s.try_into()).transpose()?;

        let mut cedar_entities_list: Vec<cedar_policy::Entity> = Vec::new();
        for entity in cache_entities.iter() {
            match entity.to_cedar_entity(cedar_schema.as_ref()) {
                Ok(cedar_entity) => cedar_entities_list.push(cedar_entity),
                Err(e) => {
                    tracing::error!("cedrus: on_project_entities: Entity: {:#?}", entity);
                    tracing::error!("cedrus: on_project_entities: error: {:?}", e);
                }
            }
        }

        let cedar_entities: cedar_policy::Entities =
            cedar_policy::Entities::from_entities(cedar_entities_list, cedar_schema.as_ref())?;

        {
            self.project_cedar_entities
                .insert(*project_id, cedar_entities);
        }

        Ok(())
    }

    async fn on_project_policy_set(&self, project_id: &Uuid) -> Result<(), CedrusError> {
        let cache_policy_set = self.cache.project_get_policy_set(project_id).await?;

        let exclude = self.exclude_policy_annotation.clone().unwrap_or_default();

        let static_policies: HashMap<PolicyId, Policy> = cache_policy_set
            .static_policies
            .into_iter()
            .filter(|(_key, policy)| !policy.annotations.contains_key(&exclude))
            .collect();

        let templates: HashMap<PolicyId, Template> = cache_policy_set
            .templates
            .into_iter()
            .filter(|(_key, policy)| !policy.annotations.contains_key(&exclude))
            .collect();

        let template_links: Vec<TemplateLink> = cache_policy_set
            .template_links
            .into_iter()
            .filter(|link| templates.contains_key(&link.template_id))
            .collect();

        let policy_set = PolicySet {
            static_policies,
            templates,
            template_links,
        };

        let cedar_policy_set: cedar_policy::PolicySet = policy_set.try_into()?;
        self.project_cedar_policies
            .insert(*project_id, cedar_policy_set);

        Ok(())
    }

    async fn publish(&self, message: Event) {
        self.update(&message, true).await;
        let _ = self.pubsub.publish(message).await;
    }

    pub fn is_allow(&self, principal: EntityUid, action: EntityUid, resource: EntityUid) -> bool {
        let cedar_principal: cedar_policy::EntityUid = principal.into();
        let cedar_action: cedar_policy::EntityUid = action.into();
        let cedar_resource: cedar_policy::EntityUid = resource.into();

        let cedar_request_result = cedar_policy::Request::new(
            cedar_principal,
            cedar_action,
            cedar_resource,
            cedar_policy::Context::empty(),
            None,
        );

        let Ok(cedar_request) = cedar_request_result else {
            return false;
        };

        let authorizer = cedar_policy::Authorizer::new();
        let decision = {
            let Some(cedar_entities) = self.project_cedar_entities.get(&Uuid::nil()) else {
                return false;
            };
            let Some(cedar_policies) = self.project_cedar_policies.get(&Uuid::nil()) else {
                return false;
            };
            authorizer.is_authorized(&cedar_request, &cedar_policies, &cedar_entities)
        };

        match decision.decision() {
            cedar_policy::Decision::Allow => true,
            cedar_policy::Decision::Deny => false,
        }
    }

    pub fn is_authorized(
        &self,
        project_id: &Uuid,
        principal: EntityUid,
        action: EntityUid,
        resource: EntityUid,
        context: Option<Context>,
    ) -> Result<Response, CedrusError> {
        let cedar_request = {
            let cedar_principal = principal.into();
            let cedar_action = action.into();
            let cedar_resource = resource.into();

            let cedar_schema = self
                .project_cedar_schemas
                .get(project_id)
                .ok_or(CedrusError::NotFound)?;

            let cedar_context = match context {
                Some(value) => {
                    let context_schema =
                        cedar_schema.as_ref().map(|schema| (schema, &cedar_action));
                    value.to_cedar_context(context_schema)?
                }
                _ => cedar_policy::Context::empty(),
            };

            cedar_policy::Request::new(
                cedar_principal,
                cedar_action,
                cedar_resource,
                cedar_context,
                cedar_schema.as_ref(),
            )?
        };

        let authorizer = cedar_policy::Authorizer::new();

        let answer = {
            let cedar_entities = self
                .project_cedar_entities
                .get(project_id)
                .ok_or(CedrusError::NotFound)?;

            let cedar_policies = self
                .project_cedar_policies
                .get(project_id)
                .ok_or(CedrusError::NotFound)?;

            authorizer.is_authorized(&cedar_request, &cedar_policies, &cedar_entities)
        };

        Ok(answer.into())
    }

    pub fn is_authorized_batch(
        &self,
        project_id: &Uuid,
        requests: Vec<Request>,
    ) -> Result<Vec<Response>, CedrusError> {
        let cedar_schema = self
            .project_cedar_schemas
            .get(project_id)
            .ok_or(CedrusError::NotFound)?;

        let mut answers = Vec::new();

        let mut entity_uids = HashSet::new();
        for request in &requests {
            entity_uids.insert(request.principal.clone());
            entity_uids.insert(request.resource.clone());
        }

        let cedar_entities = {
            self.project_cedar_entities
                .get(project_id)
                .ok_or(CedrusError::NotFound)?
        };

        let cedar_policies = {
            self.project_cedar_policies
                .get(project_id)
                .ok_or(CedrusError::NotFound)?
        };

        for request in requests {
            let cedar_request = {
                let cedar_principal = request.principal.into();
                let cedar_action = request.action.into();
                let cedar_resource = request.resource.into();

                let cedar_context = match request.context {
                    Some(value) => {
                        let context_schema =
                            cedar_schema.as_ref().map(|schema| (schema, &cedar_action));
                        value.to_cedar_context(context_schema)?
                    }
                    _ => cedar_policy::Context::empty(),
                };

                cedar_policy::Request::new(
                    cedar_principal,
                    cedar_action,
                    cedar_resource,
                    cedar_context,
                    cedar_schema.as_ref(),
                )?
            };

            let authorizer = cedar_policy::Authorizer::new();
            let answer = authorizer.is_authorized(&cedar_request, &cedar_policies, &cedar_entities);
            answers.push(answer.into());
        }

        Ok(answers)
    }

    pub async fn projects_find(&self, query: Query) -> Result<PageList<Project>, CedrusError> {
        Ok(self.db.projects_load(&query).await?)
    }

    pub async fn project_find(&self, project_id: Uuid) -> Result<Option<Project>, CedrusError> {
        Ok(self.db.project_load(&project_id).await?)
    }

    pub async fn project_create(
        &self,
        mut project: Project,
        owner: EntityUid,
    ) -> Result<Project, CedrusError> {
        project.owner = owner.clone();

        let now = chrono::Utc::now();
        project.created_at = now;
        project.updated_at = now;

        self.db.project_save(&project).await?;
        self.cache.project_set(&project).await?;

        self.on_project_set(&project)?;

        let nil = Uuid::nil();
        let entity = project.entity();

        self.db
            .project_entities_save(&nil, &Vec::from([entity.clone()]))
            .await?;
        self.cache
            .project_set_entities(&nil, &[entity.clone()])
            .await?;

        self.on_project_entities(&nil).await?;

        let mut bytes = [0u8; 64];
        rand::fill(&mut bytes);
        let api_key = ApiKey::new(
            Uuid::now_v7(),
            BASE64_STANDARD.encode(bytes),
            "Default API Key".to_string(),
            project.id,
            owner.clone(),
        );

        self.db
            .project_apikeys_save(&project.id, &vec![api_key.clone()])
            .await?;
        self.cache
            .project_set_apikeys(&project.id, &vec![api_key.clone()])
            .await?;

        self.on_project_apikeys_set(&[api_key])?;

        self.publish(Event::project_create(self.id, project.id))
            .await;

        Ok(project)
    }

    pub async fn project_update(
        &self,
        project_id: Uuid,
        project: Project,
    ) -> Result<Project, CedrusError> {
        let Some(mut original) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        if original.updated_at != project.updated_at {
            return Err(CedrusError::BadRequest);
        }

        let mut pristine = true;
        if original.name != project.name {
            original.name = project.name;
            pristine = false;
        }

        if original.enabled != project.enabled {
            original.enabled = project.enabled;
            pristine = false;
        }

        let now = chrono::Utc::now();
        if original.created_at.timestamp_millis() == 0 {
            original.created_at = now;
            pristine = false;
        }
        if original.updated_at.timestamp_millis() == 0 {
            original.updated_at = now;
            pristine = false;
        }

        if !pristine {
            original.updated_at = now;

            self.db.project_save(&original).await?;
            self.cache.project_set(&original).await?;

            self.on_project_set(&original)?;

            let nil = Uuid::nil();
            let entity = original.entity();
            self.db
                .project_entities_save(&nil, &Vec::from([entity.clone()]))
                .await?;
            self.cache
                .project_set_entities(&nil, &[entity.clone()])
                .await?;

            self.on_project_entities(&nil).await?;

            self.publish(Event::project_update(self.id, project_id))
                .await;
        }

        Ok(original)
    }

    pub async fn project_remove(&self, project_id: Uuid) -> Result<Project, CedrusError> {
        let Some(project) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        let query = Query::new();
        let api_keys = self.db.project_apikeys_load(&project_id, &query).await?;

        self.db.project_remove(&project_id).await?;
        self.cache.project_del(&project_id).await?;

        let api_key_ids = api_keys
            .items
            .iter()
            .map(|x| x.key.clone())
            .collect::<Vec<String>>();
        self.on_project_del(&project_id, &api_key_ids)?;

        self.on_project_entities(&Uuid::nil()).await?;

        self.publish(Event::project_remove(
            self.id,
            project_id,
            api_keys.items.iter().map(|x| x.key.clone()).collect(),
        ))
        .await;

        Ok(project)
    }

    pub async fn project_apikeys_find(
        &self,
        project_id: Uuid,
        query: Query,
    ) -> Result<PageList<ApiKey>, CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        Ok(self.db.project_apikeys_load(&project_id, &query).await?)
    }

    pub async fn project_apikeys_add(
        &self,
        project_id: Uuid,
        mut apikey: ApiKey,
    ) -> Result<ApiKey, CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        let mut bytes = [0u8; 64];
        rand::fill(&mut bytes);
        apikey.key = BASE64_STANDARD.encode(bytes);
        apikey.project_id = project_id;
        apikey.created_at = chrono::Utc::now();

        self.db
            .project_apikeys_save(&project_id, &vec![apikey.clone()])
            .await?;
        self.cache
            .project_set_apikeys(&project_id, &vec![apikey.clone()])
            .await?;

        self.on_project_apikeys_set(&[apikey.clone()])?;

        self.publish(Event::project_add_apikeys(
            self.id,
            project_id,
            HashSet::from([apikey.id]),
        ))
        .await;

        Ok(apikey)
    }

    pub async fn project_apikeys_update(
        &self,
        project_id: Uuid,
        apikey: ApiKey,
    ) -> Result<ApiKey, CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        let page = self
            .db
            .project_apikeys_load(&project_id, &Query::new())
            .await?;
        let Some(original) = page.items.iter().find(|ak| ak.id == apikey.id) else {
            return Err(CedrusError::NotFound);
        };

        let mut original = original.clone();
        original.name = apikey.name;
        original.updated_at = chrono::Utc::now();

        self.db
            .project_apikeys_save(&project_id, &vec![original.clone()])
            .await?;
        self.cache
            .project_set_apikeys(&project_id, &vec![original.clone()])
            .await?;

        self.on_project_apikeys_set(&[original.clone()])?;

        self.publish(Event::project_add_apikeys(
            self.id,
            project_id,
            HashSet::from([original.id]),
        ))
        .await;

        Ok(original)
    }

    pub async fn project_apikeys_remove(
        &self,
        project_id: Uuid,
        id: Uuid,
    ) -> Result<(), CedrusError> {
        if id.is_nil() {
            return Err(CedrusError::BadRequest);
        }

        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        let page = self
            .db
            .project_apikeys_load(&project_id, &Query::new())
            .await?;
        let Some(apikey) = page.items.iter().find(|ak| ak.id == id) else {
            return Err(CedrusError::NotFound);
        };

        self.db
            .project_apikeys_remove(&project_id, &vec![id.clone()])
            .await?;
        self.cache
            .project_del_apikeys(&project_id, &vec![id.clone()])
            .await?;

        self.on_project_apikeys_del(&[apikey.key.clone()])?;

        self.publish(Event::project_remove_apikeys(
            self.id,
            project_id,
            HashSet::from([apikey.key.clone()]),
        ))
        .await;

        Ok(())
    }

    pub async fn project_identity_source_find(
        &self,
        project_id: Uuid,
    ) -> Result<Option<IdentitySource>, CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        Ok(self.db.project_identity_source_load(&project_id).await?)
    }

    pub async fn project_identity_source_update(
        &self,
        project_id: Uuid,
        identity_source: IdentitySource,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        let _authorizer = authorizer_factory(&identity_source.configuration).await;

        self.db
            .project_identity_source_save(&project_id, &identity_source)
            .await?;
        self.cache
            .project_set_identity_source(&project_id, &identity_source)
            .await?;

        self.on_project_identity_source_set(&project_id, &identity_source)
            .await?;

        self.publish(Event::project_put_identity_source(self.id, project_id))
            .await;

        Ok(())
    }

    pub async fn project_identity_source_remove(
        &self,
        project_id: Uuid,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        self.db.project_identity_source_remove(&project_id).await?;
        self.cache.project_del_identity_source(&project_id).await?;

        self.on_project_identity_source_del(&project_id)?;

        self.publish(Event::project_remove_identity_source(self.id, project_id))
            .await;

        Ok(())
    }

    pub async fn project_schema_find(
        &self,
        project_id: Uuid,
    ) -> Result<Option<Schema>, CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        Ok(self.db.project_schema_load(&project_id).await?)
    }

    pub async fn project_schema_update(
        &self,
        project_id: Uuid,
        schema: Schema,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        let cedar_schema: cedar_policy::Schema = schema.clone().try_into()?;

        let entities = self
            .db
            .project_entities_load(&project_id, &Query::new())
            .await?
            .items;
        if !entities.is_empty() {
            let cedar_schema = Some(cedar_schema);
            for entry in &entities {
                entry.to_cedar_entity(cedar_schema.as_ref())?;
            }
        }

        self.db.project_schema_save(&project_id, &schema).await?;
        self.cache.project_set_schema(&project_id, &schema).await?;

        self.on_project_schema_set(&project_id, &schema)?;

        self.publish(Event::project_put_schema(self.id, project_id))
            .await;

        Ok(())
    }

    pub async fn project_schema_remove(&self, project_id: Uuid) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        self.db.project_schema_remove(&project_id).await?;
        self.cache.project_del_schema(&project_id).await?;

        self.on_project_schema_del(&project_id)?;

        self.publish(Event::project_remove_schema(self.id, project_id))
            .await;

        Ok(())
    }

    pub async fn project_entities_find(
        &self,
        project_id: Uuid,
        query: Query,
    ) -> Result<PageList<Entity>, CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        Ok(self.db.project_entities_load(&project_id, &query).await?)
    }

    pub async fn project_entities_add(
        &self,
        project_id: Uuid,
        entities: Vec<Entity>,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        let schema = self.db.project_schema_load(&project_id).await?;

        let cedar_schema = schema.map(|s| s.try_into()).transpose()?;
        for entry in &entities {
            entry.to_cedar_entity(cedar_schema.as_ref())?;
        }

        self.db
            .project_entities_save(&project_id, &entities)
            .await?;
        self.cache
            .project_set_entities(&project_id, &entities)
            .await?;

        self.on_project_entities(&project_id).await?;

        let entity_uids = entities
            .iter()
            .map(|e| e.uid().clone())
            .collect::<HashSet<_>>();
        self.publish(Event::project_add_entities(
            self.id,
            project_id,
            entity_uids,
        ))
        .await;

        Ok(())
    }

    pub async fn project_entities_remove(
        &self,
        project_id: Uuid,
        entity_uids: Vec<EntityUid>,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        self.db
            .project_entities_remove(&project_id, &entity_uids)
            .await?;
        self.cache
            .project_del_entities(&project_id, &entity_uids)
            .await?;

        self.on_project_entities(&project_id).await?;

        let entity_uids = entity_uids.into_iter().collect::<HashSet<_>>();
        self.publish(Event::project_remove_entities(
            self.id,
            project_id,
            entity_uids,
        ))
        .await;

        Ok(())
    }

    pub async fn project_policies_find(
        &self,
        project_id: Uuid,
        query: Query,
    ) -> Result<PageHash<PolicyId, Policy>, CedrusError> {
        let page = self.db.project_policies_load(&project_id, &query).await?;
        Ok(page)
    }

    pub async fn project_policies_add(
        &self,
        project_id: Uuid,
        mut policies: HashMap<PolicyId, Policy>,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        let _ = policies.iter_mut().map(|(id, policy)| {
            policy
                .annotations
                .insert("id".to_string(), Some(id.to_string()))
        });

        self.db
            .project_policies_save(&project_id, &policies)
            .await?;
        self.cache
            .project_set_policies(&project_id, &policies)
            .await?;

        self.on_project_policy_set(&project_id).await?;

        let policy_ids = policies.into_keys().collect();
        self.publish(Event::project_add_policies(self.id, project_id, policy_ids))
            .await;

        Ok(())
    }

    pub async fn project_policies_remove(
        &self,
        project_id: Uuid,
        policy_ids: Vec<PolicyId>,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        self.db
            .project_policies_remove(&project_id, &policy_ids)
            .await?;
        self.cache
            .project_del_policies(&project_id, &policy_ids)
            .await?;

        self.on_project_policy_set(&project_id).await?;

        let policy_ids = policy_ids.into_iter().collect();
        self.publish(Event::project_remove_policies(
            self.id, project_id, policy_ids,
        ))
        .await;

        Ok(())
    }

    pub async fn project_templates_find(
        &self,
        project_id: Uuid,
        query: Query,
    ) -> Result<PageHash<PolicyId, Template>, CedrusError> {
        Ok(self.db.project_templates_load(&project_id, &query).await?)
    }

    pub async fn project_templates_add(
        &self,
        project_id: Uuid,
        mut templates: HashMap<PolicyId, Template>,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        let _ = templates.iter_mut().map(|(policy_id, template)| {
            template
                .annotations
                .insert("id".to_string(), Some(policy_id.to_string()))
        });

        self.db
            .project_templates_save(&project_id, &templates)
            .await?;
        self.cache
            .project_set_templates(&project_id, &templates)
            .await?;

        self.on_project_policy_set(&project_id).await?;

        let policy_ids = templates.into_keys().collect();
        self.publish(Event::project_add_templates(
            self.id, project_id, policy_ids,
        ))
        .await;

        Ok(())
    }

    pub async fn project_templates_remove(
        &self,
        project_id: Uuid,
        template_ids: Vec<PolicyId>,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        self.db
            .project_templates_remove(&project_id, &template_ids)
            .await?;
        self.cache
            .project_del_templates(&project_id, &template_ids)
            .await?;

        self.on_project_policy_set(&project_id).await?;

        let policy_ids = template_ids.into_iter().collect();
        self.publish(Event::project_remove_templates(
            self.id, project_id, policy_ids,
        ))
        .await;

        Ok(())
    }

    pub async fn project_template_links_find(
        &self,
        project_id: Uuid,
        query: Query,
    ) -> Result<PageList<TemplateLink>, CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        Ok(self
            .db
            .project_template_links_load(&project_id, &query)
            .await?)
    }

    pub async fn project_template_links_add(
        &self,
        project_id: Uuid,
        template_links: Vec<TemplateLink>,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        self.db
            .project_template_links_save(&project_id, &template_links)
            .await?;
        self.cache
            .project_set_template_links(&project_id, &template_links)
            .await?;

        self.on_project_policy_set(&project_id).await?;

        let policy_ids = template_links.into_iter().map(|tl| tl.new_id).collect();
        self.publish(Event::project_add_template_links(
            self.id, project_id, policy_ids,
        ))
        .await;

        Ok(())
    }

    pub async fn project_template_links_remove(
        &self,
        project_id: Uuid,
        policy_ids: Vec<PolicyId>,
    ) -> Result<(), CedrusError> {
        let Some(_) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        self.db
            .project_template_links_remove(&project_id, &policy_ids)
            .await?;
        self.cache
            .project_del_template_links(&project_id, &policy_ids)
            .await?;

        self.on_project_policy_set(&project_id).await?;

        let policy_ids = policy_ids.into_iter().collect();
        self.publish(Event::project_remove_template_links(
            self.id, project_id, policy_ids,
        ))
        .await;

        Ok(())
    }

    pub async fn update(&self, event: &Event, intern: bool) {
        if !intern && event.sender == self.id {
            return;
        }

        match event.msg() {
            EventType::ReloadAll => {
                let _ = self.load_cache().await;
            }
            EventType::ProjectCreate(id) => {
                let Ok(project_cache) = self.cache.project_get(id).await else {
                    return;
                };

                if let Some(project) = project_cache {
                    let _ = self.on_project_set(&project);
                    let _ = self.on_project_entities(&Uuid::nil());

                    let Ok(cache_api_keys) = self.cache.project_get_apikeys(id).await else {
                        return;
                    };
                    let _ = self.on_project_apikeys_set(&cache_api_keys);
                }
            }
            EventType::ProjectUpdate(id) => {
                let Ok(project_cache) = self.cache.project_get(id).await else {
                    return;
                };

                if let Some(project) = project_cache {
                    let _ = self.on_project_set(&project);
                    let _ = self.on_project_entities(&Uuid::nil());
                }
            }
            EventType::ProjectRemove(id, api_keys) => {
                let _ = self.on_project_del(id, &Vec::from_iter(api_keys.clone()));
                let _ = self.on_project_entities(&Uuid::nil());
            }
            EventType::ProjectAddApikeys(project_id, api_key_ids) => {
                let Ok(cache_api_keys) = self.cache.project_get_apikeys(&project_id).await else {
                    return;
                };

                let api_keys_vec = cache_api_keys
                    .into_iter()
                    .filter(|ak| api_key_ids.contains(&ak.id))
                    .collect::<Vec<ApiKey>>();

                let _ = self.on_project_apikeys_set(&api_keys_vec);
            }
            EventType::ProjectRemoveApikeys(_project_id, api_keys) => {
                let _ = self.on_project_apikeys_del(&Vec::from_iter(api_keys.clone()));
            }
            EventType::ProjectPutIdentitySource(id) => {
                let Ok(cache_identity_source) = self.cache.project_get_identity_source(id).await
                else {
                    return;
                };

                if let Some(identity_source) = cache_identity_source {
                    let _ = self
                        .on_project_identity_source_set(id, &identity_source)
                        .await;
                }
            }
            EventType::ProjectRemoveIdentitySource(id) => {
                let _ = self.on_project_identity_source_del(id);
            }
            EventType::ProjectPutSchema(id) => {
                let Ok(schema_cache) = self.cache.project_get_schema(id).await else {
                    return;
                };
                if let Some(schema) = schema_cache {
                    let _ = self.on_project_schema_set(id, &schema);
                }
            }
            EventType::ProjectRemoveSchema(id) => {
                let _ = self.on_project_schema_del(&id);
            }
            EventType::ProjectAddEntities(id, _entity_uids) => {
                let _ = self.on_project_entities(id);
            }
            EventType::ProjectRemoveEntities(id, _entity_uids) => {
                let _ = self.on_project_entities(id);
            }
            EventType::ProjectAddPolicies(id, _policy_ids) => {
                let _ = self.on_project_policy_set(id).await;
            }
            EventType::ProjectRemovePolicies(id, _policy_ids) => {
                let _ = self.on_project_policy_set(id).await;
            }
            EventType::ProjectAddTemplates(id, _template_ids) => {
                let _ = self.on_project_policy_set(id).await;
            }
            EventType::ProjectRemoveTemplates(id, _template_ids) => {
                let _ = self.on_project_policy_set(id).await;
            }
            EventType::ProjectAddTemplateLinks(id, _template_link_ids) => {
                let _ = self.on_project_policy_set(id).await;
            }
            EventType::ProjectRemoveTemplateLinks(id, _template_link_ids) => {
                let _ = self.on_project_policy_set(id).await;
            }
        }
    }
}
