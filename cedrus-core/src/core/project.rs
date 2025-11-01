use std::collections::{HashMap, HashSet};

use cedrus_cedar::{
    entity::EntityAttr, Entity, EntityUid, EntityValue, PolicyId, SlotId, TemplateLink,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

pub const PROJECT_ENTITY_TYPE: &'static str = "Cedrus::Project";

const ATTR_OWNER: &'static str = "owner";
const TAG_NAME: &'static str = "name";

const TEMPLATE_PROJECT_ADMIN_ROLE: &'static str = "ProjectAdminRole";

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct Project {
    pub id: Uuid,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub api_key: String,

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
            api_key: "".to_string(),
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
        let entity = Entity::new_with_tags(uid, attrs, parents, tags);
        return entity;
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

        return links;
    }
}
