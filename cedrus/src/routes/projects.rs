use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    routing::{delete, get, post, put},
    Extension, Json, Router,
};
use cedrus_cedar::{Context, Entity, EntityUid, Policy, PolicyId, PolicySet, Request, Response, Schema, Template, TemplateLink};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use cedrus_core::{
    core::{cedrus::Cedrus, project::Project, IdentitySource}, CedrusActions, PageHash, PageList, Selector
};

use crate::{AppError, AppJson, QueryParams};

#[derive(Default, Clone, Serialize, Deserialize, ToSchema)]
pub struct IsAuthorizedRequest {
    pub principal: EntityUid,
    pub action: EntityUid,
    pub resource: EntityUid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,
}

#[derive(Default, Clone, Serialize, Deserialize, ToSchema)]
pub struct IsAuthorizedRequests {
    pub requests: Vec<Request>,
}

#[derive(Default, Clone, Serialize, Deserialize, ToSchema)]
pub struct CedarSyntax {
    pub cedar: Option<String>,
}

#[utoipa::path(
    get,
    path = "/v1/projects",
    params(
        QueryParams,
    ),
    responses(
        (status = 200, description = "Projects Page", body = PageList<Project>)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Query(query_params): Query<QueryParams>,
) -> Result<AppJson<PageList<Project>>, AppError> {
    tracing::info!("principal: {:?}", principal);
    let page = if state.is_admin(&principal) {
        state.projects_find(query_params.into()).await?
    } else {
        let mut query: cedrus_core::Query = query_params.into();
        let rol = HashMap::from([(principal.to_string(), Selector::Exists(true))]);
        let roles = HashMap::from([("roles".to_string(), Selector::Record(rol))]);
        query.selector = Some(Selector::Record(roles));
        state.projects_find(query).await?
    };

    Ok(AppJson(page))
}

#[utoipa::path(
    post,
    path = "/v1/projects",
    request_body = Project,
    responses(
        (status = 200, description = "Project", body = Project)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_post(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Json(mut project): Json<Project>,
) -> Result<AppJson<Project>, AppError> 
{
    project.id = Uuid::now_v7();
    let project = state.project_create(project, principal).await?;

    Ok(AppJson(project))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}",
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    responses(
        (status = 200, description = "Project", body = Project)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
) -> Result<AppJson<Project>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProject.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let Some(project) = state.project_find(id).await? else {
        return Err(AppError::NotFound);
    };

    Ok(AppJson(project))
}

#[utoipa::path(
    put,
    path = "/v1/projects/{id}",
    request_body = Project,
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    responses(
        (status = 200, description = "Project", body = Project)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_put(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(project): Json<Project>,
) -> Result<AppJson<Project>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::PutProject.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let project = state.project_update(id, project).await?;

    Ok(AppJson(project))
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{id}",
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    responses(
        (status = 200, description = "Project", body = Project)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_delete(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
) -> Result<AppJson<Project>, AppError> 
{
    if id.is_nil() {
        return Err(AppError::Forbidden);
    }
    if !state.is_allow(principal, CedrusActions::DeleteProject.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let project = state.project_remove(id).await?;

    Ok(AppJson(project))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/identity-source",
    params(
        ("id" = Uuid, Path, description = "Project id"),
    ),
    responses(
        (status = 200, description = "Entities page", body = Option<IdentitySource>)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_identity_source_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
) -> Result<AppJson<Option<IdentitySource>>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectIdentitySource.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let identity_source = state.project_identity_source_find(id).await?;

    Ok(AppJson(identity_source))
}

#[utoipa::path(
    put,
    path = "/v1/projects/{id}/identity-source",
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    request_body = IdentitySource,
    responses(
        (status = 200, description = "Entities added")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_identity_source_put(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(identity_source): Json<IdentitySource>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::PutProjectIdentitySource.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_identity_source_update(id, identity_source).await?;

    Ok(())
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{id}/identity-source",
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    responses(
        (status = 200, description = "Entities deleted")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_identity_source_delete(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::DeleteProjectIdentitySource.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_identity_source_remove(id).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/schema",
    params(
        ("id" = Uuid, Path, description = "Project id"),
    ),
    responses(
        (status = 200, description = "Schema", body = Option<Schema>)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_schema_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
) -> Result<AppJson<Option<Schema>>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectSchema.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let schema = state.project_schema_find(id).await?;

    Ok(AppJson(schema))
}

#[utoipa::path(
    put,
    path = "/v1/projects/{id}/schema",
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    request_body = Schema,
    responses(
        (status = 200, description = "Schema created", body = Schema)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_schema_put(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(schema): Json<Schema>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::PutProjectSchema.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_schema_update(id, schema).await?;

    Ok(())
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{id}/schema",
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    responses(
        (status = 200, description = "Schema deleted")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_schema_delete(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::DeleteProjectSchema.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_schema_remove(id).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/schema/cedar",
    params(
        ("id" = Uuid, Path, description = "Project id"),
    ),
    responses(
        (status = 200, description = "Entities page", body = CedarSyntax)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_schema_cedar_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
) -> Result<AppJson<CedarSyntax>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectSchema.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let schema = state.project_schema_find(id).await?;
    let schema = match schema {
        Some(schema) => {
            let value = serde_json::to_value(&schema).unwrap();
            let cedar_schema = cedar_policy::SchemaFragment::from_json_value(value).unwrap();
            let schema = cedar_schema.to_cedarschema().unwrap();
            CedarSyntax { cedar: Some(schema) }
        },
        None => return Ok(AppJson(CedarSyntax { cedar: None})),
    };

    Ok(AppJson(schema))
}

#[utoipa::path(
    put,
    path = "/v1/projects/{id}/schema/cedar",
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    request_body = CedarSyntax,
    responses(
        (status = 200, description = "Entities added")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_schema_cedar_put(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(syntax): Json<CedarSyntax>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::PutProjectSchema.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let schema = match syntax.cedar {
        Some(str) => {
            let (cedar_schema, _warnings) = cedar_policy::SchemaFragment::from_cedarschema_str(&str).unwrap();
            let json = cedar_schema.to_json_value().unwrap();
            let schema: Schema = serde_json::from_value(json).unwrap();
            schema
        },
        None => return Ok(()),
    };

    state.project_schema_update(id, schema).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/entities",
    params(
        ("id" = Uuid, Path, description = "Project id"),
        QueryParams
    ),
    responses(
        (status = 200, description = "Entities page", body = PageList<Entity>)
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_entities_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Query(query_params): Query<QueryParams>,
) -> Result<AppJson<PageList<Entity>>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectEntities.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let page = state.project_entities_find(id, query_params.into()).await?;

    Ok(AppJson(page))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{id}/entities",
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    request_body = Vec<Entity>,
    responses(
        (status = 200, description = "Entities added")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_entities_post(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(entities): Json<Vec<Entity>>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::PostProjectEntities.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_entities_add(id, entities).await?;

    Ok(())
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{id}/entities",
    params(
        ("id" = Uuid, Path, description = "Project id")
    ),
    request_body = Vec<EntityUid>,
    responses(
        (status = 200, description = "Entities deleted")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_entities_delete(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(project_ids): Json<Vec<EntityUid>>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::DeleteProjectEntities.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_entities_remove(id, project_ids).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/policies",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
        QueryParams
    ),
    responses(
        (status = 200, description = "Get Policies", body = PageHash<PolicyId, Policy>),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_policies_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Query(query_params): Query<QueryParams>,
) -> Result<AppJson<PageHash<PolicyId, Policy>>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectPolicies.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let page = state.project_policies_find(id, query_params.into()).await?;    

    Ok(AppJson(page))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/policies/{policyId}/cedar",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
        ("policyId" = String, Path, description = "Policy Id"),
    ),
    responses(
        (status = 200, description = "Get Policy Cedar", body = CedarSyntax),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_policies_policy_id_cedar_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path((id, policy_id)): Path<(Uuid, String)>,
) -> Result<AppJson<CedarSyntax>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectPolicies.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let selector = Selector::Eq(Box::new(Selector::String(policy_id.clone())));
    let map = HashMap::from([("policyId".to_string(), selector)]);
    let query = cedrus_core::Query {
        selector: Some(Selector::Record(map)),
        ..Default::default()
    };
    let items = state.project_policies_find(id, query).await?.items;    
    if items.is_empty() {
        return Err(AppError::NotFound);
    }

    let (_, mut policy) = items.into_iter().next().unwrap();
    policy.annotations.insert("id".to_string(), Some(policy_id));
    let json = serde_json::to_value(policy).unwrap();
    let cedar_policy = cedar_policy::Policy::from_json(None, json).unwrap();

    let cedar = cedar_policy.to_cedar().unwrap();

    Ok(AppJson(CedarSyntax { cedar: Some(cedar) }))
}

#[utoipa::path(
    put,
    path = "/v1/projects/{id}/policies/{policyId}/cedar",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
        ("policyId" = String, Path, description = "Policy Id"),
    ),
    request_body = CedarSyntax,
    responses(
        (status = 200, description = "Get Policy Cedar"),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_policies_policy_id_cedar_put(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path((id, policy_id)): Path<(Uuid, String)>,
    Json(syntax): Json<CedarSyntax>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectPolicies.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let cedar_policy_id = cedar_policy::PolicyId::new(policy_id.clone());
    let cedar_policy = cedar_policy::Policy::parse(Some(cedar_policy_id), syntax.cedar.unwrap()).unwrap();

    let policy: Policy = cedar_policy.try_into().unwrap();

    state.project_policies_add(id, HashMap::from([(policy_id.into(), policy)])).await?;

    Ok(())
}

#[utoipa::path(
    post,
    path = "/v1/projects/{id}/policies",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
    ),
    request_body = HashMap<PolicyId, Policy>,
    responses(
        (status = 200, description = "add policies"),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_policies_post(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(policies): Json<HashMap<PolicyId, Policy>>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::PostProjectPolicies.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_policies_add(id, policies).await?;

    Ok(())
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{id}/policies",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
    ),
    request_body = Vec<PolicyId>,
    responses(
        (status = 200, description = "add policies"),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_policies_delete(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(policy_ids): Json<Vec<PolicyId>>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::DeleteProjectPolicies.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_policies_remove(id, policy_ids).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/templates",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
        QueryParams
    ),
    responses(
        (status = 200, description = "get templates", body = PageHash<PolicyId, Template>),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_templates_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Query(query_params): Query<QueryParams>,
) -> Result<AppJson<PageHash<PolicyId, Template>>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectTemplates.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let page = state.project_templates_find(id, query_params.into()).await?;

    Ok(AppJson(page))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{id}/templates",
    params(
        ("id" = Uuid, Path, description = "Project Id")
    ),
    request_body = HashMap<PolicyId, Template>,
    responses(
        (status = 200, description = "add templates"),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_templates_post(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(templates): Json<HashMap<PolicyId, Template>>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::PostProjectTemplates.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_templates_add(id, templates).await?;

    Ok(())
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{id}/templates",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
    ),
    request_body = Vec<PolicyId>,    
    responses(
        (status = 200, description = "add templates"),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_templates_delete(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(template_ids): Json<Vec<PolicyId>>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::DeleteProjectTemplates.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_templates_remove(id, template_ids).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/templates/{templateId}/cedar",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
        ("templateId" = String, Path, description = "Template Id"),
    ),
    responses(
        (status = 200, description = "Get Template Cedar", body = CedarSyntax),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_templates_template_id_cedar_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path((id, template_id)): Path<(Uuid, String)>,
) -> Result<AppJson<CedarSyntax>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectPolicies.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let selector = Selector::Eq(Box::new(Selector::String(template_id)));
    let map = HashMap::from([("policyId".to_string(), selector)]);
    let query = cedrus_core::Query {
        selector: Some(Selector::Record(map)),
        ..Default::default()
    };
    let items = state.project_templates_find(id, query).await?.items;    
    if items.is_empty() {
        return Err(AppError::NotFound);
    }

    let (_, template) = items.into_iter().next().unwrap();
    let json = serde_json::to_value(template).unwrap();
    let cedar_template = cedar_policy::Template::from_json(None, json).unwrap();
    let cedar = cedar_template.to_cedar();

    Ok(AppJson(CedarSyntax { cedar: Some(cedar) }))
}

#[utoipa::path(
    put,
    path = "/v1/projects/{id}/templates/{templateId}/cedar",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
        ("templateId" = String, Path, description = "Template Id"),
    ),
    request_body = CedarSyntax,
    responses(
        (status = 200, description = "Get Template Cedar"),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_templates_template_id_cedar_put(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path((id, template_id)): Path<(Uuid, String)>,
    Json(syntax): Json<CedarSyntax>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectPolicies.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let cedar_template_id = cedar_policy::PolicyId::new(template_id.clone());
    let cedar_template = cedar_policy::Template::parse(Some(cedar_template_id), syntax.cedar.unwrap()).unwrap();

    let template: Template = cedar_template.try_into().unwrap();

    state.project_templates_add(id, HashMap::from([(template_id.into(), template)])).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/template-links",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
        QueryParams
    ),
    responses(
        (status = 200, description = "get template links", body = PageList<TemplateLink>),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_template_links_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Query(query_params): Query<QueryParams>,
) -> Result<AppJson<PageList<TemplateLink>>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectTemplateLinks.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let page = state.project_template_links_find(id, query_params.into()).await?;

    Ok(AppJson(page))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{id}/template-links",
    params(
        ("id" = Uuid, Path, description = "Project Id")
    ),
    request_body = Vec<TemplateLink>,
    responses(
        (status = 200, description = "add policies"),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_template_links_post(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(template_links): Json<Vec<TemplateLink>>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::PostProjectTemplateLinks.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_template_links_add(id, template_links).await?;

    Ok(())
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{id}/template-links",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
    ),
    request_body = Vec<(PolicyId, PolicyId)>,
    responses(
        (status = 200, description = "add policies"),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_template_links_delete(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(template_link_ids): Json<Vec<PolicyId>>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::DeleteProjectTemplateLinks.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    state.project_template_links_remove(id, template_link_ids).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/template-links/{policyId}/cedar",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
        ("templateId" = String, Path, description = "Template Id"),
    ),
    responses(
        (status = 200, description = "Get Template Cedar", body = CedarSyntax),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_template_links_policy_id_cedar_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path((id, template_id)): Path<(Uuid, String)>,
) -> Result<AppJson<CedarSyntax>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectPolicies.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let selector = Selector::Eq(Box::new(Selector::String(template_id)));
    let map = HashMap::from([("newId".to_string(), selector)]);
    let query = cedrus_core::Query {
        selector: Some(Selector::Record(map)),
        ..Default::default()
    };
    let items = state.project_templates_find(id, query).await?.items;    
    if items.is_empty() {
        return Err(AppError::NotFound);
    }

    let (_, template) = items.into_iter().next().unwrap();
    let json = serde_json::to_value(template).unwrap();
    let cedar_template = cedar_policy::Template::from_json(None, json).unwrap();
    let cedar = cedar_template.to_cedar();

    Ok(AppJson(CedarSyntax { cedar: Some(cedar) }))
}

#[utoipa::path(
    put,
    path = "/v1/projects/{id}/template-links/{policyId}/cedar",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
        ("templateId" = String, Path, description = "Template Id"),
    ),
    request_body = CedarSyntax,
    responses(
        (status = 200, description = "Get Template Cedar"),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_template_links_policy_id_cedar_put(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path((id, template_id)): Path<(Uuid, String)>,
    Json(syntax): Json<CedarSyntax>,
) -> Result<(), AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectPolicies.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let cedar_template_id = cedar_policy::PolicyId::new(template_id.clone());
    let cedar_template = cedar_policy::Template::parse(Some(cedar_template_id), syntax.cedar.unwrap()).unwrap();

    let template: Template = cedar_template.try_into().unwrap();

    state.project_templates_add(id, HashMap::from([(template_id.into(), template)])).await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/policy-set",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
    ),
    responses(
        (status = 200, description = "Get Policy Set Cedar Syntax", body = PolicySet),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_policy_set_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
) -> Result<AppJson<PolicySet>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectTemplateLinks.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let query = cedrus_core::Query::new();
    let static_policies = state.project_policies_find(id, query.clone()).await?.items;
    let templates = state.project_templates_find(id, query.clone()).await?.items;
    let template_links = state.project_template_links_find(id, query).await?.items;

    let policy_set = PolicySet {
        static_policies,
        templates,
        template_links,
    };

    Ok(AppJson(policy_set))
}

#[utoipa::path(
    get,
    path = "/v1/projects/{id}/policy-set/cedar",
    params(
        ("id" = Uuid, Path, description = "Project Id"),
    ),
    responses(
        (status = 200, description = "Get Policy Set Cedar Syntax", body = CedarSyntax),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_policy_set_cedar_get(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
) -> Result<AppJson<CedarSyntax>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::GetProjectTemplateLinks.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let query = cedrus_core::Query::new();
    let static_policies = state.project_policies_find(id, query.clone()).await?.items;
    let templates = state.project_templates_find(id, query.clone()).await?.items;
    // let template_links = state.project_template_links_find(id, query).await?.items;

    let policy_set = PolicySet {
        static_policies,
        templates,
        template_links: Vec::new(),
    };

    let cedar_policy_set: cedar_policy::PolicySet = policy_set.try_into()?;
    let cedar = cedar_policy_set.to_cedar();

    Ok(AppJson(CedarSyntax { cedar }))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{id}/is-authorized",
    params(
        ("id" = Uuid, Path, description = "Project Id")
    ),
    request_body = IsAuthorizedRequest,
    responses(
        (status = 200, description = "is authorized", body = Response),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_is_authorized_post(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(request): Json<IsAuthorizedRequest>,
) -> Result<AppJson<Response>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::PostProjectIsAuthorized.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let answer = state.is_authorized(&id, request.principal, request.action, request.resource, request.context)?;

    Ok(AppJson(answer))
}

#[utoipa::path(
    post,
    path = "/v1/projects/{id}/is-authorized-batch",
    params(
        ("id" = Uuid, Path, description = "Project Id")
    ),
    request_body = IsAuthorizedRequests,
    responses(
        (status = 200, description = "is authorized", body = Vec<Response>),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Store not found")
    ),
    security(
        ("bearerAuth" = []),
        ("apiKey" = []),
    )
)]
async fn projects_id_is_authorized_batch_post(
    Extension(principal): Extension<EntityUid>,
    State(state): State<Arc<Cedrus>>,
    Path(id): Path<Uuid>,
    Json(request): Json<IsAuthorizedRequests>,
) -> Result<AppJson<Vec<Response>>, AppError> 
{
    if !state.is_allow(principal, CedrusActions::PostProjectIsAuthorized.value(), Project::entity_uid(id)) {
        return Err(AppError::Forbidden);
    }

    let answers = state.is_authorized_batch(&id, request.requests)?;

    Ok(AppJson(answers))
}

pub fn routes() -> Router<Arc<Cedrus>> 
{
    Router::new()
        .route("/", get(projects_get))
        .route("/", post(projects_post))
        .route("/{id}", get(projects_id_get))
        .route("/{id}", put(projects_id_put))
        .route("/{id}", delete(projects_id_delete))
        .route("/{id}/identity-source", get(projects_id_identity_source_get))
        .route("/{id}/identity-source", put(projects_id_identity_source_put))
        .route("/{id}/identity-source", delete(projects_id_identity_source_delete))
        .route("/{id}/schema", get(projects_id_schema_get))
        .route("/{id}/schema", put(projects_id_schema_put))
        .route("/{id}/schema", delete(projects_id_schema_delete))
        .route("/{id}/schema/cedar", get(projects_id_schema_cedar_get))
        .route("/{id}/schema/cedar", put(projects_id_schema_cedar_put))
        .route("/{id}/entities", get(projects_id_entities_get))
        .route("/{id}/entities", post(projects_id_entities_post))
        .route(
            "/{id}/entities",
            delete(projects_id_entities_delete),
        )
        .route(
            "/{id}/policies",
            get(projects_id_policies_get),
        )
        .route(
            "/{id}/policies",
            post(projects_id_policies_post),
        )
        .route(
            "/{id}/policies",
            delete(projects_id_policies_delete),
        )
        .route(
            "/{id}/policies/{policyId}/cedar",
            get(projects_id_policies_policy_id_cedar_get),
        )
        .route(
            "/{id}/policies/{policyId}/cedar",
            put(projects_id_policies_policy_id_cedar_put),
        )
        .route(
            "/{id}/templates",
            get(projects_id_templates_get),
        )
        .route(
            "/{id}/templates",
            post(projects_id_templates_post),
        )
        .route(
            "/{id}/templates",
            delete(projects_id_templates_delete),
        )
        .route(
            "/{id}/templates/{templateId}/cedar",
            get(projects_id_templates_template_id_cedar_get),
        )
        .route(
            "/{id}/templates/{templateId}/cedar",
            put(projects_id_templates_template_id_cedar_put),
        )
        .route(
            "/{id}/template-links",
            get(projects_id_template_links_get),
        )
        .route(
            "/{id}/template-links",
            post(projects_id_template_links_post),
        )
        .route(
            "/{id}/template-links",
            delete(projects_id_template_links_delete),
        )
        .route(
            "/{id}/template-links/{policyId}/cedar",
            get(projects_id_template_links_policy_id_cedar_get),
        )
        .route(
            "/{id}/template-links/{policyId}/cedar",
            put(projects_id_template_links_policy_id_cedar_put),
        )
        .route("/{id}/policy-set", get(projects_id_policy_set_get))
        .route("/{id}/policy-set/cedar", get(projects_id_policy_set_cedar_get))
        .route(
            "/{id}/is-authorized",
            post(projects_id_is_authorized_post),
        )
        .route(
            "/{id}/is-authorized-batch",
            post(projects_id_is_authorized_batch_post),
        )
}
