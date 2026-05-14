use std::collections::{HashMap, HashSet};

use cedrus_cedar::{Entity, EntityUid, entity::EntityAttr};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

pub const PROJECT_ENTITY_TYPE: &str = "Project";
pub const PARENT_UID: &str = "Application::Cedrus";

const ATTR_ENABLED: &str = "enabled";
const ATTR_OWNER: &str = "owner";
const TAG_NAME: &str = "name";

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct ApiKey {
    pub id: Uuid,
    pub key: String,
    pub name: String,
    pub project_id: Uuid,
    pub owner: EntityUid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl ApiKey {
    pub fn new(id: Uuid, key: String, name: String, project_id: Uuid, owner: EntityUid) -> Self {
        let now = chrono::Utc::now();

        Self {
            id,
            key,
            name,
            project_id,
            owner,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct Project {
    pub id: Uuid,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,

    pub enabled: bool,

    pub owner: EntityUid,

    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl Project {
    pub fn new(id: Uuid, name: String, owner: EntityUid) -> Self {
        let now = chrono::Utc::now();

        Self {
            id,
            name,
            enabled: true,
            owner,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn entity_uid(id: Uuid) -> EntityUid {
        EntityUid::new(PROJECT_ENTITY_TYPE.to_string(), id.to_string())
    }

    pub fn entity(&self) -> Entity {
        let uid = EntityUid::new(PROJECT_ENTITY_TYPE.to_string(), self.id.to_string());
        let attrs = HashMap::from([
            (ATTR_ENABLED.to_string(), EntityAttr::Boolean(true)),
            (
                ATTR_OWNER.to_string(),
                EntityAttr::EntityUid(self.owner.clone()),
            ),
        ]);
        let parents = HashSet::from([EntityUid::from(PARENT_UID)]);
        let tags = HashMap::from([(TAG_NAME.to_string(), EntityAttr::String(self.name.clone()))]);

        Entity::new_with_tags(uid, attrs, parents, tags)
    }
}
