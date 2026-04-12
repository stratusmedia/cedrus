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
    Authorizer, CedrusError, Event, EventType, PageHash, PageList, Query,
    TEMPLATE_PROJECT_ADMIN_ROLE, cache::Cache, db::Database, pubsub::PubSub,
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

    pub api_keys: DashMap<String, EntityUid>,

    pub project_authorizers: DashMap<Uuid, Option<Authorizer>>,
    pub project_cedar_schemas: DashMap<Uuid, Option<cedar_policy::Schema>>,
    pub project_cedar_entities: DashMap<Uuid, DashMap<EntityUid, (Entity, cedar_policy::Entity)>>,
    pub project_cedar_policies: DashMap<Uuid, cedar_policy::PolicySet>,
}

impl Cedrus {
    pub async fn new(
        db: Box<dyn Database + Send + Sync>,
        cache: Box<dyn Cache + Send + Sync>,
        pubsub: Box<dyn PubSub + Send + Sync>,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),

            db,
            cache,
            pubsub,

            api_keys: DashMap::new(),

            project_authorizers: DashMap::new(),
            project_cedar_schemas: DashMap::new(),
            project_cedar_entities: DashMap::new(),
            project_cedar_policies: DashMap::new(),
        }
    }

    pub async fn init_project(state: &Cedrus, config: &CedrusConfig) -> Result<(), CedrusError> {
        // Find project with id nil
        if let Some(mut project) = state.db.project_load(&Uuid::nil()).await? {
            let api_keys = state
                .db
                .project_apikeys_load(&project.id, &Query::new())
                .await?;

            let found = api_keys
                .items
                .iter()
                .find(|ak| ak.key == config.server.api_key);

            if found.is_none() {
                project.api_keys.push(ApiKey::new(
                    config.server.api_key.clone(),
                    "Admin API Key".to_string(),
                    Uuid::nil(),
                    project.owner.clone(),
                ));
                state
                    .db
                    .project_apikeys_save(&project.id, &vec![project.api_keys[0].clone()])
                    .await?;
            }
            state.db.project_save(&project).await?;
        } else {
            let schema_str = include_str!("../../config/cedrus.cedarschema.json");
            let entities_str = include_str!("../../config/cedrus.cedarentities.json");
            let policy_set_str = include_str!("../../config/cedrus.cedar.json");
            let schema: Schema = serde_json::from_str(schema_str)?;
            let entities: Vec<Entity> = serde_json::from_str(entities_str)?;
            let policy_set: PolicySet = serde_json::from_str(policy_set_str)?;

            let now = chrono::Utc::now();
            let owner = EntityUid::new("Cedrus::User".to_string(), Uuid::nil().to_string());
            let mut project = Project {
                id: Uuid::nil(),
                name: "Cedrus Admin Project".to_string(),
                owner: owner.clone(),
                api_keys: vec![ApiKey::new(
                    config.server.api_key.clone(),
                    "Admin API Key".to_string(),
                    Uuid::nil(),
                    owner.clone(),
                )],
                created_at: now,
                updated_at: now,
                ..Default::default()
            };
            project.roles.insert(
                owner.to_string(),
                HashSet::from([TEMPLATE_PROJECT_ADMIN_ROLE.to_string()]),
            );
            state.db.project_save(&project).await?;

            state.db.project_schema_save(&project.id, &schema).await?;

            state
                .db
                .project_entities_save(&project.id, &entities)
                .await?;

            state
                .db
                .project_policies_save(&project.id, &policy_set.static_policies)
                .await?;
            state
                .db
                .project_templates_save(&project.id, &policy_set.templates)
                .await?;
            state
                .db
                .project_template_links_save(&project.id, &policy_set.template_links)
                .await?;
        }

        state
            .db
            .project_identity_source_save(&Uuid::nil(), &config.identity_source)
            .await?;

        Ok(())
    }

    pub async fn init_cache(state: &Cedrus) -> Result<(), CedrusError> {
        let query = Query::new();
        let projects = state.db.projects_load(&query).await?;

        let query = Query::new();
        for project in projects.items {
            state.cache.project_clear(&project.id).await?;

            let entities = state.db.project_entities_load(&project.id, &query).await?;
            let static_policies = state.db.project_policies_load(&project.id, &query).await?;
            let templates = state.db.project_templates_load(&project.id, &query).await?;
            let template_links = state
                .db
                .project_template_links_load(&project.id, &query)
                .await?;

            state.cache.project_set(&project).await?;

            if let Some(identity_source) =
                state.db.project_identity_source_load(&project.id).await?
            {
                state
                    .cache
                    .project_set_identity_source(&project.id, &identity_source)
                    .await?;
            }

            if let Some(schema) = state.db.project_schema_load(&project.id).await? {
                state.cache.project_set_schema(&project.id, &schema).await?;
            }

            state
                .cache
                .project_set_entities(&project.id, &entities.items)
                .await?;
            state
                .cache
                .project_set_policies(&project.id, &static_policies.items)
                .await?;
            state
                .cache
                .project_set_templates(&project.id, &templates.items)
                .await?;
            state
                .cache
                .project_set_template_links(&project.id, &template_links.items)
                .await?;
        }

        Ok(())
    }

    pub async fn reload_all(&self) -> Result<(), CedrusError> {
        let projects = self.cache.projects_get().await?;
        for project in projects {
            for api_key in &project.api_keys {
                self.api_keys
                    .insert(api_key.key.clone(), project.owner.clone());
            }

            let cache_identity_source = self.cache.project_get_identity_source(&project.id).await?;
            if let Some(identity_source) = cache_identity_source {
                let jwt = authorizer_factory(&identity_source.configuration).await?;
                let authorizer = Authorizer::new(identity_source, jwt);
                self.project_authorizers
                    .insert(project.id, Some(authorizer));
            } else {
                self.project_authorizers.insert(project.id, None);
            }

            let cache_schema = self.cache.project_get_schema(&project.id).await?;
            let cedar_schema: Option<cedar_policy::Schema> =
                cache_schema.map(|s| s.try_into()).transpose()?;

            let cache_entities = self.cache.project_get_entities(&project.id, &[]).await?;
            let cedar_entities = DashMap::new();
            for entity in cache_entities.into_iter() {
                let entity_uid = entity.uid().clone();
                let cedar_entity: cedar_policy::Entity = entity.clone().try_into()?;

                cedar_entities.insert(entity_uid, (entity, cedar_entity));
            }

            let cache_policy_set: PolicySet =
                self.cache.project_get_policy_set(&project.id).await?;
            let cedar_policy_set: cedar_policy::PolicySet = cache_policy_set.try_into()?;

            {
                self.project_cedar_schemas.insert(project.id, cedar_schema);
                self.project_cedar_entities
                    .insert(project.id, cedar_entities);

                self.project_cedar_policies
                    .insert(project.id, cedar_policy_set);
            }
        }

        Ok(())
    }

    pub async fn load_cache(state: &Cedrus) -> Result<(), CedrusError> {
        state.reload_all().await?;
        Ok(())
    }

    fn project_add_entities(
        &self,
        project_id: &Uuid,
        entities: &[Entity],
    ) -> Result<(), CedrusError> {
        let project_cedar_entities = self
            .project_cedar_entities
            .get_mut(project_id)
            .ok_or(CedrusError::NotFound)?;

        for entity in entities {
            let entity_uid = entity.uid().clone();
            let cedar_entity: cedar_policy::Entity = entity.clone().try_into()?;

            project_cedar_entities.insert(entity_uid, (entity.clone(), cedar_entity));
        }

        Ok(())
    }

    fn project_remove_entities(
        &self,
        project_id: &Uuid,
        entity_uids: &[EntityUid],
    ) -> Result<(), CedrusError> {
        if let Some(cedar_entities) = self.project_cedar_entities.get_mut(project_id) {
            for entity_uid in entity_uids {
                cedar_entities.remove(entity_uid);
            }
        }
        Ok(())
    }

    async fn project_set_policy_set(&self, project_id: &Uuid) -> Result<(), CedrusError> {
        let cache_policy_set = self.cache.project_get_policy_set(project_id).await?;
        let cedar_policy_set: cedar_policy::PolicySet = cache_policy_set.try_into()?;
        self.project_cedar_policies
            .insert(*project_id, cedar_policy_set);
        Ok(())
    }

    async fn project_add_policy_set(
        &self,
        project_id: &Uuid,
        policies_to_add: &HashMap<PolicyId, Policy>,
        templates_to_add: &HashMap<PolicyId, Template>,
        links_to_add: &Vec<TemplateLink>,
    ) -> Result<(), CedrusError> {
        let mut policies = self
            .db
            .project_policies_load(project_id, &Query::new())
            .await?
            .items;
        let mut templates = self
            .db
            .project_templates_load(project_id, &Query::new())
            .await?
            .items;
        let mut template_links = self
            .db
            .project_template_links_load(project_id, &Query::new())
            .await?
            .items
            .into_iter()
            .map(|tl| (tl.new_id.clone(), tl))
            .collect::<HashMap<PolicyId, TemplateLink>>();

        for (policy_id, policy) in policies_to_add {
            policies.insert(policy_id.clone(), policy.clone());
        }
        for (policy_id, template) in templates_to_add {
            templates.insert(policy_id.clone(), template.clone());
        }
        for link in links_to_add {
            template_links.insert(link.new_id.clone(), link.clone());
        }

        let policy_set = PolicySet {
            static_policies: policies,
            templates,
            template_links: template_links.into_values().collect(),
        };

        let _cedar_policy_set: cedar_policy::PolicySet = policy_set.try_into()?;

        Ok(())
    }

    async fn project_remove_policy_set(
        &self,
        project_id: &Uuid,
        policies_to_remove: &Vec<PolicyId>,
        templates_to_remove: &Vec<PolicyId>,
        links_to_remove: &Vec<PolicyId>,
    ) -> Result<(), CedrusError> {
        let mut policies = self
            .db
            .project_policies_load(project_id, &Query::new())
            .await?
            .items;
        let mut templates = self
            .db
            .project_templates_load(project_id, &Query::new())
            .await?
            .items;
        let mut template_links = self
            .db
            .project_template_links_load(project_id, &Query::new())
            .await?
            .items
            .into_iter()
            .map(|tl| (tl.new_id.clone(), tl))
            .collect::<HashMap<PolicyId, TemplateLink>>();

        for policy_id in policies_to_remove {
            policies.remove(policy_id);
        }
        for policy_id in templates_to_remove {
            templates.remove(policy_id);
        }
        for policy_id in links_to_remove {
            template_links.remove(policy_id);
        }

        let policy_set = PolicySet {
            static_policies: policies,
            templates,
            template_links: template_links.into_values().collect(),
        };

        let _cedar_policy_set: cedar_policy::PolicySet = policy_set.try_into()?;

        Ok(())
    }

    async fn publish(&self, message: Event) {
        self.update(&message, true).await;
        let _ = self.pubsub.publish(message).await;
    }

    fn get_entity_parents(
        &self,
        project_id: &Uuid,
        entity_uid: &EntityUid,
        entities: &mut HashMap<EntityUid, cedar_policy::Entity>,
    ) {
        let Some(list) = self.project_cedar_entities.get(project_id) else {
            return;
        };
        let Some(value) = list.get(entity_uid) else {
            return;
        };
        entities.insert(entity_uid.clone(), value.1.clone());
        let parents = value.0.parents().clone();

        for parent_uid in parents {
            if entities.contains_key(&parent_uid) {
                continue;
            }
            self.get_entity_parents(project_id, &parent_uid, entities);
        }
    }

    fn get_cedar_entities(
        &self,
        project_id: &Uuid,
        entity_uids: &HashSet<EntityUid>,
    ) -> Result<cedar_policy::Entities, CedrusError> {
        let mut entities = HashMap::new();
        for entity_id in entity_uids {
            self.get_entity_parents(project_id, entity_id, &mut entities);
        }

        let entities = entities.into_values().collect::<Vec<_>>();
        Ok(cedar_policy::Entities::from_entities(entities, None)?)
    }

    pub fn is_allow(&self, principal: EntityUid, action: EntityUid, resource: EntityUid) -> bool {
        let entity_uids = HashSet::from([principal.clone(), resource.clone()]);

        let Ok(cedar_entities) = self.get_cedar_entities(&Uuid::nil(), &entity_uids) else {
            return false;
        };

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
        let entity_uids = HashSet::from([principal.clone(), resource.clone()]);
        let cedar_entities = self.get_cedar_entities(project_id, &entity_uids)?;

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

        let cedar_entities = self.get_cedar_entities(project_id, &entity_uids)?;

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
            let answer = {
                let cedar_policies = self
                    .project_cedar_policies
                    .get(project_id)
                    .ok_or(CedrusError::NotFound)?;
                authorizer.is_authorized(&cedar_request, &cedar_policies, &cedar_entities)
            };
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
        project.roles.insert(
            owner.to_string(),
            HashSet::from([TEMPLATE_PROJECT_ADMIN_ROLE.to_string()]),
        );

        if project.api_keys.is_empty() {
            let mut buf = [0u8; 128];
            rand::fill(&mut buf);
            project.api_keys.push(ApiKey {
                key: BASE64_STANDARD.encode(buf),
                name: "Default API Key".to_string(),
                project_id: project.id,
                owner: owner.clone(),
                created_at: chrono::Utc::now(),
            });
        }

        let now = chrono::Utc::now();
        project.created_at = now;
        project.updated_at = now;

        self.db.project_save(&project).await?;
        self.cache.project_set(&project).await?;

        let nil = Uuid::nil();

        let entity = project.entity();
        self.db
            .project_entities_save(&nil, &Vec::from([entity.clone()]))
            .await?;
        self.cache.project_set_entities(&nil, &[entity]).await?;

        let template_links = project.template_links();
        self.db
            .project_template_links_save(&nil, &template_links)
            .await?;
        self.cache
            .project_set_template_links(&nil, &template_links)
            .await?;

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
        if original.api_keys != project.api_keys {
            original.api_keys = project.api_keys.clone();
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

            self.publish(Event::project_update(self.id, project_id))
                .await;
        }

        Ok(original)
    }

    pub async fn project_remove(&self, project_id: Uuid) -> Result<Project, CedrusError> {
        let Some(project) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        self.db.project_remove(&project_id).await?;

        self.cache.project_del(&project_id).await?;

        self.publish(Event::project_remove(
            self.id,
            project_id,
            project
                .api_keys
                .iter()
                .map(|k| k.key.clone())
                .collect::<Vec<String>>(),
        ))
        .await;

        Ok(project)
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

        self.publish(Event::project_remove_identity_source(self.id, project_id))
            .await;

        Ok(())
    }

    pub async fn project_apikeys_find(&self, project_id: Uuid) -> Result<Vec<ApiKey>, CedrusError> {
        let Some(project) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        Ok(project.api_keys)
    }

    pub async fn project_apikeys_add(
        &self,
        project_id: Uuid,
        mut apikey: ApiKey,
    ) -> Result<ApiKey, CedrusError> {
        let Some(mut project) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        apikey.project_id = project_id;
        self.db
            .project_apikeys_save(&project_id, &vec![apikey.clone()])
            .await?;

        project.api_keys.push(apikey.clone());
        self.db.project_save(&project).await?;
        self.cache.project_set(&project).await?;

        self.publish(Event::project_update(self.id, project_id))
            .await;

        Ok(apikey)
    }

    pub async fn project_apikeys_update(
        &self,
        project_id: Uuid,
        mut apikey: ApiKey,
    ) -> Result<ApiKey, CedrusError> {
        let Some(mut project) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        apikey.project_id = project_id;
        self.db
            .project_apikeys_save(&project_id, &vec![apikey.clone()])
            .await?;

        if let Some(ak) = project.api_keys.iter_mut().find(|ak| ak.key == apikey.key) {
            *ak = apikey.clone();
        }
        self.db.project_save(&project).await?;
        self.cache.project_set(&project).await?;

        self.publish(Event::project_update(self.id, project_id))
            .await;

        Ok(apikey)
    }

    pub async fn project_apikeys_remove(
        &self,
        project_id: Uuid,
        key: String,
    ) -> Result<(), CedrusError> {
        let Some(mut project) = self.db.project_load(&project_id).await? else {
            return Err(CedrusError::NotFound);
        };

        self.db
            .project_apikeys_remove(&project_id, &vec![key.clone()])
            .await?;

        project.api_keys.retain(|ak| ak.key != key);
        self.db.project_save(&project).await?;
        self.cache.project_set(&project).await?;

        self.publish(Event::project_update(self.id, project_id))
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

        self.project_remove_entities(&project_id, &entity_uids)?;

        self.cache
            .project_del_entities(&project_id, &entity_uids)
            .await?;

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

        self.project_add_policy_set(&project_id, &policies, &HashMap::new(), &Vec::new())
            .await?;

        self.db
            .project_policies_save(&project_id, &policies)
            .await?;

        self.cache
            .project_set_policies(&project_id, &policies)
            .await?;

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

        self.project_remove_policy_set(&project_id, &policy_ids, &Vec::new(), &Vec::new())
            .await?;

        self.db
            .project_policies_remove(&project_id, &policy_ids)
            .await?;

        self.cache
            .project_del_policies(&project_id, &policy_ids)
            .await?;

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

        self.project_add_policy_set(&project_id, &HashMap::new(), &templates, &Vec::new())
            .await?;

        self.db
            .project_templates_save(&project_id, &templates)
            .await?;

        self.cache
            .project_set_templates(&project_id, &templates)
            .await?;

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

        self.project_remove_policy_set(&project_id, &Vec::new(), &template_ids, &Vec::new())
            .await?;

        self.db
            .project_templates_remove(&project_id, &template_ids)
            .await?;

        self.cache
            .project_del_templates(&project_id, &template_ids)
            .await?;

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

        self.project_add_policy_set(
            &project_id,
            &HashMap::new(),
            &HashMap::new(),
            &template_links,
        )
        .await?;

        self.db
            .project_template_links_save(&project_id, &template_links)
            .await?;

        self.cache
            .project_set_template_links(&project_id, &template_links)
            .await?;

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

        self.project_remove_policy_set(&project_id, &Vec::new(), &Vec::new(), &policy_ids)
            .await?;

        self.db
            .project_template_links_remove(&project_id, &policy_ids)
            .await?;

        self.cache
            .project_del_template_links(&project_id, &policy_ids)
            .await?;

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
                let _ = self.reload_all().await;
            }
            EventType::ProjectCreate(id) => {
                let Ok(project_cache) = self.cache.project_get(id).await else {
                    return;
                };

                if let Some(project) = project_cache {
                    self.project_cedar_schemas.insert(*id, None);
                    self.project_cedar_policies
                        .insert(*id, cedar_policy::PolicySet::new());
                    self.project_cedar_entities.insert(*id, DashMap::new());

                    for api_key in &project.api_keys {
                        self.api_keys
                            .insert(api_key.key.clone(), project.owner.clone());
                    }
                }
            }
            EventType::ProjectUpdate(id) => {
                let Ok(project_cache) = self.cache.project_get(id).await else {
                    return;
                };

                if let Some(project) = project_cache {
                    for api_key in &project.api_keys {
                        self.api_keys
                            .insert(api_key.key.clone(), project.owner.clone());
                    }
                }
            }
            EventType::ProjectRemove(id, extracted_api_keys) => {
                for key in extracted_api_keys {
                    self.api_keys.remove(key);
                }
                self.project_authorizers.remove(id);
                self.project_cedar_schemas.remove(id);
                self.project_cedar_entities.remove(id);
                self.project_cedar_policies.remove(id);
            }
            EventType::ProjectPutIdentitySource(id) => {
                let Ok(is_cache) = self.cache.project_get_identity_source(id).await else {
                    return;
                };

                if let Some(identity_source) = is_cache {
                    let Ok(jwt) = authorizer_factory(&identity_source.configuration).await else {
                        return;
                    };
                    let authorizer = Authorizer::new(identity_source, jwt);
                    self.project_authorizers.insert(*id, Some(authorizer));
                } else {
                    self.project_authorizers.insert(*id, None);
                }
            }
            EventType::ProjectRemoveIdentitySource(id) => {
                self.project_authorizers.insert(*id, None);
            }
            EventType::ProjectPutSchema(id) => {
                let Ok(schema_cache) = self.cache.project_get_schema(id).await else {
                    return;
                };
                match schema_cache {
                    Some(schema) => {
                        let Ok(cedar_schema) = schema.try_into() else {
                            return;
                        };
                        self.project_cedar_schemas.insert(*id, Some(cedar_schema));
                    }
                    None => {
                        self.project_cedar_schemas.insert(*id, None);
                    }
                }
            }
            EventType::ProjectRemoveSchema(id) => {
                self.project_cedar_schemas.insert(*id, None);
            }
            EventType::ProjectAddEntities(id, entity_uids) => {
                let cache_entities = self
                    .cache
                    .project_get_entities(id, &Vec::from_iter(entity_uids.clone()))
                    .await
                    .unwrap_or_default();
                let _ = self.project_add_entities(id, &cache_entities);
            }
            EventType::ProjectRemoveEntities(id, entity_uids) => {
                let _ = self.project_remove_entities(id, &Vec::from_iter(entity_uids.clone()));
            }
            EventType::ProjectAddPolicies(id, _policy_ids) => {
                let _ = self.project_set_policy_set(id).await;
            }
            EventType::ProjectRemovePolicies(id, _policy_ids) => {
                let _ = self.project_set_policy_set(id).await;
            }
            EventType::ProjectAddTemplates(id, _template_ids) => {
                let _ = self.project_set_policy_set(id).await;
            }
            EventType::ProjectRemoveTemplates(id, _template_ids) => {
                let _ = self.project_set_policy_set(id).await;
            }
            EventType::ProjectAddTemplateLinks(id, _template_link_ids) => {
                let _ = self.project_set_policy_set(id).await;
            }
            EventType::ProjectRemoveTemplateLinks(id, _template_link_ids) => {
                let _ = self.project_set_policy_set(id).await;
            }
        }
    }
}
