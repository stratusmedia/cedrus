#![doc = include_str!("../README.md")]

use std::{collections::{HashMap, HashSet}, error::Error, fmt, hash::Hash};

use cedrus_cedar::{EntityUid, PolicyId};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{cache::CacheError, db::DatabaseError, pubsub::PubSubError};

pub const DEFAULT_LIMIT: usize = 1000;
const TEMPLATE_PROJECT_ADMIN_ROLE: &'static str = "ProjectAdminRole";

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PageList<T> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_key: Option<String>,
}

impl<T> PageList<T> {
    pub fn new(items: Vec<T>, last_key: Option<String>) -> Self {
        Self { items, last_key }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PageHash<K, V>
where
    K: Eq + Hash,
{
    pub items: HashMap<K, V>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_key: Option<String>,
}

impl<K, V> PageHash<K, V>
where
    K: Eq + Hash,
{
    pub fn new(items: HashMap<K, V>, last_key: Option<String>) -> Self {
        Self { items, last_key }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Sort {
    field: String,
    order: SortOrder,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]

pub enum Selector {
    #[serde(rename = "$and")]
    #[schema(no_recursion)]
    And(Vec<Selector>),
    #[serde(rename = "$or")]
    #[schema(no_recursion)]
    Or(Vec<Selector>),

    #[serde(rename = "$eq")]
    Eq(Box<Selector>),
    #[serde(rename = "$neq")]
    Neq(Box<Selector>),
    #[serde(rename = "$gt")]
    Gt(Box<Selector>),
    #[serde(rename = "$gte")]
    Gte(Box<Selector>),
    #[serde(rename = "$lt")]
    Lt(Box<Selector>),
    #[serde(rename = "$lte")]
    Lte(Box<Selector>),
    #[serde(rename = "$exists")]
    #[schema(no_recursion)]
    Exists(bool),

    #[serde(rename = "$in")]
    #[schema(no_recursion)]
    In(Vec<Selector>),
    #[serde(rename = "$nin")]
    #[schema(no_recursion)]
    Nin(Vec<Selector>),

    #[serde(untagged)]
    #[schema(no_recursion)]
    Record(HashMap<String, Selector>),

    #[serde(untagged)]
    String(String),
    #[serde(untagged)]
    Number(i64),
    #[serde(untagged)]
    Boolean(bool),
}

impl Default for Selector {
    fn default() -> Self {
        Selector::Record(HashMap::new())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<Selector>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sort: Vec<Sort>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_key: Option<String>,
    pub limit: u32,
    pub skip: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
}

impl Query {
    pub fn new() -> Self {
        Query {
            ..Default::default()
        }
    }
}

pub enum CedrusActions {
    GetProjects,
    PostProject,
    GetProject,
    PutProject,
    DeleteProject,
    GetProjectIdentitySource,
    PutProjectIdentitySource,
    DeleteProjectIdentitySource,
    GetProjectSchema,
    PutProjectSchema,
    DeleteProjectSchema,
    PostProjectEntities,
    GetProjectEntities,
    DeleteProjectEntities,
    PostProjectPolicies,
    GetProjectPolicies,
    DeleteProjectPolicies,
    PostProjectTemplates,
    GetProjectTemplates,
    DeleteProjectTemplates,
    PostProjectTemplateLinks,
    GetProjectTemplateLinks,
    DeleteProjectTemplateLinks,
    PostProjectIsAuthorized,
}

impl CedrusActions {
    pub fn value(&self) -> EntityUid {
        match *self {
            CedrusActions::GetProjects => {
                EntityUid::new("Cedrus::Action".to_string(), "getProjects".to_string())
            }
            CedrusActions::PostProject => {
                EntityUid::new("Cedrus::Action".to_string(), "postProject".to_string())
            }
            CedrusActions::GetProject => {
                EntityUid::new("Cedrus::Action".to_string(), "getProject".to_string())
            }
            CedrusActions::PutProject => {
                EntityUid::new("Cedrus::Action".to_string(), "putProject".to_string())
            }
            CedrusActions::DeleteProject => {
                EntityUid::new("Cedrus::Action".to_string(), "deleteProject".to_string())
            }
            CedrusActions::GetProjectIdentitySource => {
                EntityUid::new("Cedrus::Action".to_string(), "getProjectSchema".to_string())
            }
            CedrusActions::PutProjectIdentitySource => {
                EntityUid::new("Cedrus::Action".to_string(), "putProjectSchema".to_string())
            }
            CedrusActions::DeleteProjectIdentitySource => EntityUid::new(
                "Cedrus::Action".to_string(),
                "deleteProjectSchema".to_string(),
            ),
            CedrusActions::GetProjectSchema => {
                EntityUid::new("Cedrus::Action".to_string(), "getProjectSchema".to_string())
            }
            CedrusActions::PutProjectSchema => {
                EntityUid::new("Cedrus::Action".to_string(), "putProjectSchema".to_string())
            }
            CedrusActions::DeleteProjectSchema => EntityUid::new(
                "Cedrus::Action".to_string(),
                "deleteProjectSchema".to_string(),
            ),
            CedrusActions::PostProjectEntities => EntityUid::new(
                "Cedrus::Action".to_string(),
                "postProjectEntities".to_string(),
            ),
            CedrusActions::GetProjectEntities => EntityUid::new(
                "Cedrus::Action".to_string(),
                "getProjectEntities".to_string(),
            ),
            CedrusActions::DeleteProjectEntities => EntityUid::new(
                "Cedrus::Action".to_string(),
                "deleteProjectEntities".to_string(),
            ),
            CedrusActions::PostProjectPolicies => EntityUid::new(
                "Cedrus::Action".to_string(),
                "postProjectPolicies".to_string(),
            ),
            CedrusActions::GetProjectPolicies => EntityUid::new(
                "Cedrus::Action".to_string(),
                "getProjectPolicies".to_string(),
            ),
            CedrusActions::DeleteProjectPolicies => EntityUid::new(
                "Cedrus::Action".to_string(),
                "deleteProjectPolicies".to_string(),
            ),
            CedrusActions::PostProjectTemplates => EntityUid::new(
                "Cedrus::Action".to_string(),
                "postProjectTemplates".to_string(),
            ),
            CedrusActions::GetProjectTemplates => EntityUid::new(
                "Cedrus::Action".to_string(),
                "getProjectTemplates".to_string(),
            ),
            CedrusActions::DeleteProjectTemplates => EntityUid::new(
                "Cedrus::Action".to_string(),
                "deleteProjectTemplates".to_string(),
            ),
            CedrusActions::PostProjectTemplateLinks => EntityUid::new(
                "Cedrus::Action".to_string(),
                "postProjectTemplateLinks".to_string(),
            ),
            CedrusActions::GetProjectTemplateLinks => EntityUid::new(
                "Cedrus::Action".to_string(),
                "getProjectTemplateLinks".to_string(),
            ),
            CedrusActions::DeleteProjectTemplateLinks => EntityUid::new(
                "Cedrus::Action".to_string(),
                "deleteProjectTemplateLinks".to_string(),
            ),
            CedrusActions::PostProjectIsAuthorized => EntityUid::new(
                "Cedrus::Action".to_string(),
                "postProjectIsAuthorized".to_string(),
            ),
        }
    }
}

// The kinds of errors we can hit in our application.
#[derive(Debug)]
pub enum CedrusError {
    BadRequest,   // 400
    Unauthorized, // 401
    Forbidden,    // 403
    NotFound,     // 404

    DatabaseError(DatabaseError),
    CacheError(CacheError),
    PubSubError(PubSubError),

    SchemaError(cedar_policy::SchemaError),
    EntitiesError(cedar_policy::entities_errors::EntitiesError),
    PolicyFromJsonError(cedar_policy::PolicyFromJsonError),
    PolicyToJsonError(cedar_policy::PolicyToJsonError),
    PolicySetError(cedar_policy::PolicySetError),
    ContextJsonError(cedar_policy::ContextJsonError),
}

impl Error for CedrusError {
}

impl fmt::Display for CedrusError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CedrusError::BadRequest => write!(f, "Bad request"),
            CedrusError::Unauthorized => write!(f, "Unauthorized"),
            CedrusError::Forbidden => write!(f, "Forbidden"),
            CedrusError::NotFound => write!(f, "Not found"),
            CedrusError::DatabaseError(ref err) => err.fmt(f),
            CedrusError::CacheError(ref err) => err.fmt(f),
            CedrusError::PubSubError(ref err) => err.fmt(f),
            CedrusError::SchemaError(ref err) => err.fmt(f),
            CedrusError::EntitiesError(ref err) => err.fmt(f),
            CedrusError::PolicyFromJsonError(ref err) => err.fmt(f),
            CedrusError::PolicyToJsonError(ref err) => err.fmt(f),
            CedrusError::PolicySetError(ref err) => err.fmt(f),
            CedrusError::ContextJsonError(ref err) => err.fmt(f),
        }
    }
}

impl From<DatabaseError> for CedrusError {
    fn from(error: DatabaseError) -> Self {
        Self::DatabaseError(error)
    }
}

impl From<CacheError> for CedrusError {
    fn from(error: CacheError) -> Self {
        Self::CacheError(error)
    }
}

impl From<PubSubError> for CedrusError {
    fn from(error: PubSubError) -> Self {
        Self::PubSubError(error)
    }
}

impl From<cedar_policy::SchemaError> for CedrusError {
    fn from(error: cedar_policy::SchemaError) -> Self {
        Self::SchemaError(error)
    }
}

impl From<cedar_policy::entities_errors::EntitiesError> for CedrusError {
    fn from(error: cedar_policy::entities_errors::EntitiesError) -> Self {
        Self::EntitiesError(error)
    }
}

impl From<cedar_policy::PolicyFromJsonError> for CedrusError {
    fn from(error: cedar_policy::PolicyFromJsonError) -> Self {
        Self::PolicyFromJsonError(error)
    }
}

impl From<cedar_policy::PolicyToJsonError> for CedrusError {
    fn from(error: cedar_policy::PolicyToJsonError) -> Self {
        Self::PolicyToJsonError(error)
    }
}

impl From<cedar_policy::PolicySetError> for CedrusError {
    fn from(error: cedar_policy::PolicySetError) -> Self {
        Self::PolicySetError(error)
    }
}

impl From<cedar_policy::ContextJsonError> for CedrusError {
    fn from(error: cedar_policy::ContextJsonError) -> Self {
        Self::ContextJsonError(error)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventType {
    ReloadAll,
    ProjectCreate(Uuid),
    ProjectUpdate(Uuid),
    ProjectRemove(Uuid, String),
    ProjectPutIdentitySource(Uuid),
    ProjectRemoveIdentitySource(Uuid),
    ProjectPutSchema(Uuid),
    ProjectRemoveSchema(Uuid),
    ProjectAddEntities(Uuid, HashSet<EntityUid>),
    ProjectRemoveEntities(Uuid, HashSet<EntityUid>),
    ProjectAddPolicies(Uuid, HashSet<PolicyId>),
    ProjectRemovePolicies(Uuid, HashSet<PolicyId>),
    ProjectAddTemplates(Uuid, HashSet<PolicyId>),
    ProjectRemoveTemplates(Uuid, HashSet<PolicyId>),
    ProjectAddTemplateLinks(Uuid, HashSet<PolicyId>),
    ProjectRemoveTemplateLinks(Uuid, HashSet<PolicyId>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    sender: Uuid,
    msg: EventType,
}

impl Event {
    pub fn new(sender: Uuid, msg: EventType) -> Self {
        Self { sender, msg }
    }

    pub fn project_create(sender: Uuid, project_id: Uuid) -> Self {
        Self {
            sender,
            msg: EventType::ProjectCreate(project_id),
        }
    }

    pub fn project_update(sender: Uuid, project_id: Uuid) -> Self {
        Self {
            sender,
            msg: EventType::ProjectUpdate(project_id),
        }
    }

    pub fn project_remove(sender: Uuid, project_id: Uuid, api_key: String) -> Self {
        Self {
            sender,
            msg: EventType::ProjectRemove(project_id, api_key),
        }
    }

    pub fn project_put_identity_source(sender: Uuid, project_id: Uuid) -> Self {
        Self {
            sender,
            msg: EventType::ProjectPutIdentitySource(project_id),
        }
    }

    pub fn project_remove_identity_source(sender: Uuid, project_id: Uuid) -> Self {
        Self {
            sender,
            msg: EventType::ProjectRemoveIdentitySource(project_id),
        }
    }

    pub fn project_put_schema(sender: Uuid, project_id: Uuid) -> Self {
        Self {
            sender,
            msg: EventType::ProjectPutSchema(project_id),
        }
    }

    pub fn project_remove_schema(sender: Uuid, project_id: Uuid) -> Self {
        Self {
            sender,
            msg: EventType::ProjectRemoveSchema(project_id),
        }
    }

    pub fn project_add_entities(
        sender: Uuid,
        project_id: Uuid,
        entities_uids: HashSet<EntityUid>,
    ) -> Self {
        Self {
            sender,
            msg: EventType::ProjectAddEntities(project_id, entities_uids),
        }
    }

    pub fn project_remove_entities(
        sender: Uuid,
        project_id: Uuid,
        entities_uids: HashSet<EntityUid>,
    ) -> Self {
        Self {
            sender,
            msg: EventType::ProjectRemoveEntities(project_id, entities_uids),
        }
    }

    pub fn project_add_policies(
        sender: Uuid,
        project_id: Uuid,
        policy_ids: HashSet<PolicyId>,
    ) -> Self {
        Self {
            sender,
            msg: EventType::ProjectAddPolicies(project_id, policy_ids),
        }
    }

    pub fn project_remove_policies(
        sender: Uuid,
        project_id: Uuid,
        policy_ids: HashSet<PolicyId>,
    ) -> Self {
        Self {
            sender,
            msg: EventType::ProjectRemovePolicies(project_id, policy_ids),
        }
    }

    pub fn project_add_templates(
        sender: Uuid,
        project_id: Uuid,
        policy_ids: HashSet<PolicyId>,
    ) -> Self {
        Self {
            sender,
            msg: EventType::ProjectAddTemplates(project_id, policy_ids),
        }
    }

    pub fn project_remove_templates(
        sender: Uuid,
        project_id: Uuid,
        policy_ids: HashSet<PolicyId>,
    ) -> Self {
        Self {
            sender,
            msg: EventType::ProjectRemoveTemplates(project_id, policy_ids),
        }
    }

    pub fn project_add_template_links(
        sender: Uuid,
        project_id: Uuid,
        policy_ids: HashSet<PolicyId>,
    ) -> Self {
        Self {
            sender,
            msg: EventType::ProjectAddTemplateLinks(project_id, policy_ids),
        }
    }

    pub fn project_remove_template_links(
        sender: Uuid,
        project_id: Uuid,
        policy_ids: HashSet<PolicyId>,
    ) -> Self {
        Self {
            sender,
            msg: EventType::ProjectRemoveTemplateLinks(project_id, policy_ids),
        }
    }

    pub fn sender(&self) -> Uuid {
        self.sender
    }

    pub fn msg(&self) -> &EventType {
        &self.msg
    }
}

#[async_trait::async_trait]
pub trait Observer: Send + Sync {
    async fn update(&self, event: &Event);
}

#[async_trait::async_trait]
pub trait Observable<'a, T: Observer> {
    fn add_observer(&mut self, observer: &'a T);
    fn remove_observer(&mut self, observer: &'a T);
    async fn notify_observers(&self, event: &Event);
}

pub fn option_uuid_eq(a: Option<Uuid>, b: Option<Uuid>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.eq(&b),
        (None, None) => true,
        _ => false,
    }
}

pub fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

pub mod cache;
pub mod db;
pub mod pubsub;

pub mod core;
