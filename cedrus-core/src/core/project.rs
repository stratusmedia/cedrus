use std::collections::{HashMap, HashSet};

use cedrus_cedar::{
    Entity, EntityUid, EntityValue, PolicyId, SlotId, TemplateLink, entity::EntityAttr,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

pub const PROJECT_ENTITY_TYPE: &str = "Cedrus::Project";

const ATTR_OWNER: &str = "owner";
const TAG_NAME: &str = "name";

const TEMPLATE_PROJECT_ADMIN_ROLE: &str = "ProjectAdminRole";

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct ApiKey {
    pub key: String,
    pub name: String,
    pub project_id: Uuid,
    pub owner: EntityUid,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl ApiKey {
    pub fn new(key: String, name: String, project_id: Uuid, owner: EntityUid) -> Self {
        let now = chrono::Utc::now();

        Self {
            key,
            name,
            project_id,
            owner,
            created_at: now,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct Project {
    pub id: Uuid,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub api_keys: Vec<ApiKey>,

    pub owner: EntityUid,

    pub roles: HashMap<String, HashSet<String>>,

    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl Project {
    pub fn new(id: Uuid, name: String, owner: EntityUid) -> Self {
        let now = chrono::Utc::now();

        Self {
            id,
            name,
            owner,
            roles: HashMap::new(),
            api_keys: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn entity_uid(id: Uuid) -> EntityUid {
        EntityUid::new(PROJECT_ENTITY_TYPE.to_string(), id.to_string())
    }

    pub fn entity(&self) -> Entity {
        let uid = EntityUid::new(PROJECT_ENTITY_TYPE.to_string(), self.id.to_string());
        let attrs = HashMap::from([(
            ATTR_OWNER.to_string(),
            EntityAttr::EntityUid(self.owner.clone()),
        )]);
        let parents = HashSet::new();
        let tags = HashMap::from([(TAG_NAME.to_string(), EntityAttr::String(self.name.clone()))]);

        Entity::new_with_tags(uid, attrs, parents, tags)
    }

    pub fn template_links(&self) -> Vec<TemplateLink> {
        let mut links = Vec::new();

        let template_id: PolicyId = TEMPLATE_PROJECT_ADMIN_ROLE.to_string().into();
        let new_id: PolicyId = format!("{}_{}", self.id, TEMPLATE_PROJECT_ADMIN_ROLE).into();
        let values = HashMap::from([
            (
                SlotId::Principal,
                EntityValue::EntityUid(self.owner.clone()),
            ),
            (
                SlotId::Resource,
                EntityValue::EntityUid(Project::entity_uid(self.id)),
            ),
        ]);

        let admin_project_role = TemplateLink::new(template_id, new_id, values);

        links.push(admin_project_role);

        links
    }
}
