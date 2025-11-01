#![doc = include_str!("../README.md")]

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/cedar.rs"));
}

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    hash::Hash,
    str::{self, FromStr},
};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(
    Debug, Default, Clone, Eq, PartialOrd, Ord, Hash, PartialEq, Serialize, Deserialize, ToSchema,
)]
pub struct EntityUid {
    #[serde(rename = "type")]
    r#type: String,
    id: String,
}

impl EntityUid {
    pub fn new(r#type: String, id: String) -> Self {
        Self { r#type, id }
    }

    pub fn type_name(&self) -> &str {
        &self.r#type
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl From<cedar_policy::EntityUid> for EntityUid {
    fn from(value: cedar_policy::EntityUid) -> Self {
        Self {
            r#type: value.type_name().to_string(),
            id: value.id().unescaped().to_string(),
        }
    }
}

impl Into<cedar_policy::EntityUid> for EntityUid {
    fn into(self) -> cedar_policy::EntityUid {
        cedar_policy::EntityUid::from_type_name_and_id(
            cedar_policy::EntityTypeName::from_str(&self.r#type).unwrap(),
            cedar_policy::EntityId::from_str(&self.id).unwrap(),
        )
    }
}

impl From<proto::EntityUid> for EntityUid {
    fn from(value: proto::EntityUid) -> Self {
        Self {
            r#type: value.r#type,
            id: value.name,
        }
    }
}

impl Into<proto::EntityUid> for EntityUid {
    fn into(self) -> proto::EntityUid {
        proto::EntityUid {
            r#type: self.r#type,
            name: self.id,
        }
    }
}

impl ToString for EntityUid {
    fn to_string(&self) -> String {
        format!("{}::{}", self.r#type, self.id)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionFn {
    #[serde(rename = "fn")]
    r#fn: String,
    arg: String,
}

impl From<proto::ExtensionFn> for ExtensionFn {
    fn from(value: proto::ExtensionFn) -> Self {
        Self {
            r#fn: value.r#fn,
            arg: value.arg,
        }
    }
}

impl Into<proto::ExtensionFn> for ExtensionFn {
    fn into(self) -> proto::ExtensionFn {
        proto::ExtensionFn {
            r#fn: self.r#fn,
            arg: self.arg,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EntityUidEscape {
    #[serde(rename = "__entity")]
    entity: EntityUid,
}

impl From<cedar_policy::EntityUid> for EntityUidEscape {
    fn from(value: cedar_policy::EntityUid) -> Self {
        let entity = EntityUid {
            r#type: value.type_name().to_string(),
            id: value.id().unescaped().to_string(),
        };
        Self { entity }
    }
}

impl Into<cedar_policy::EntityUid> for EntityUidEscape {
    fn into(self) -> cedar_policy::EntityUid {
        cedar_policy::EntityUid::from_type_name_and_id(
            cedar_policy::EntityTypeName::from_str(&self.entity.r#type).unwrap(),
            cedar_policy::EntityId::from_str(&self.entity.id).unwrap(),
        )
    }
}

impl From<proto::EntityUidEscape> for EntityUidEscape {
    fn from(value: proto::EntityUidEscape) -> Self {
        let entity = EntityUid {
            r#type: value.r#type,
            id: value.name,
        };
        Self { entity }
    }
}

impl Into<proto::EntityUidEscape> for EntityUidEscape {
    fn into(self) -> proto::EntityUidEscape {
        proto::EntityUidEscape {
            r#type: self.entity.r#type,
            name: self.entity.id,
        }
    }
}

impl From<EntityUid> for EntityUidEscape {
    fn from(value: EntityUid) -> Self {
        Self { entity: value }
    }
}

impl Into<EntityUid> for EntityUidEscape {
    fn into(self) -> EntityUid {
        EntityUid {
            r#type: self.entity.r#type,
            id: self.entity.id,
        }
    }
}

impl ToString for EntityUidEscape {
    fn to_string(&self) -> String {
        format!("{}::{}", self.entity.r#type, self.entity.id)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionFnEscape {
    #[serde(rename = "__extn")]
    extn: ExtensionFn,
}

impl From<ExtensionFn> for ExtensionFnEscape {
    fn from(value: ExtensionFn) -> Self {
        Self { extn: value }
    }
}

impl Into<ExtensionFn> for ExtensionFnEscape {
    fn into(self) -> ExtensionFn {
        ExtensionFn {
            r#fn: self.extn.r#fn,
            arg: self.extn.arg,
        }
    }
}

impl From<proto::ExtensionFnEscape> for ExtensionFnEscape {
    fn from(value: proto::ExtensionFnEscape) -> Self {
        let extn = ExtensionFn {
            r#fn: value.r#fn,
            arg: value.arg,
        };
        Self { extn }
    }
}

impl Into<proto::ExtensionFnEscape> for ExtensionFnEscape {
    fn into(self) -> proto::ExtensionFnEscape {
        proto::ExtensionFnEscape {
            r#fn: self.extn.r#fn,
            arg: self.extn.arg,
        }
    }
}

pub mod entity {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
    #[serde(untagged)]
    pub enum EntityAttr {
        String(String),
        Number(i64),
        Boolean(bool),
        #[schema(no_recursion)]
        Set(Vec<EntityAttr>),
        #[schema(no_recursion)]
        Record(HashMap<String, EntityAttr>),
        EntityUid(EntityUid),
        Function(ExtensionFn),
        EntityUidEscape(EntityUidEscape),
        FunctionEscape(ExtensionFnEscape),
    }

    impl Default for EntityAttr {
        fn default() -> Self {
            EntityAttr::String(String::default())
        }
    }

    impl From<proto::entity::EntityAttr> for EntityAttr {
        fn from(value: proto::entity::EntityAttr) -> Self {
            match value.value.unwrap() {
                proto::entity::entity_attr::Value::S(s) => Self::String(s),
                proto::entity::entity_attr::Value::I(n) => Self::Number(n as i64),
                proto::entity::entity_attr::Value::B(b) => Self::Boolean(b),
                proto::entity::entity_attr::Value::Euid(e) => Self::EntityUid(e.into()),
                proto::entity::entity_attr::Value::Efn(f) => Self::Function(f.into()),
                proto::entity::entity_attr::Value::Euide(e) => Self::EntityUidEscape(e.into()),
                proto::entity::entity_attr::Value::Efne(f) => Self::FunctionEscape(f.into()),
                proto::entity::entity_attr::Value::Set(set) => {
                    let attrs = set
                        .elements
                        .into_iter()
                        .map(|a| a.into())
                        .collect::<Vec<EntityAttr>>();
                    Self::Set(attrs)
                }
                proto::entity::entity_attr::Value::Record(record) => {
                    let attrs = record
                        .items
                        .into_iter()
                        .map(|(k, v)| (k, v.into()))
                        .collect::<HashMap<String, EntityAttr>>();
                    Self::Record(attrs)
                }
            }
        }
    }

    impl Into<proto::entity::EntityAttr> for EntityAttr {
        fn into(self) -> proto::entity::EntityAttr {
            let value = match self {
                EntityAttr::String(s) => proto::entity::entity_attr::Value::S(s),
                EntityAttr::Number(n) => proto::entity::entity_attr::Value::I(n),
                EntityAttr::Boolean(b) => proto::entity::entity_attr::Value::B(b),
                EntityAttr::EntityUid(e) => proto::entity::entity_attr::Value::Euid(e.into()),
                EntityAttr::Function(f) => proto::entity::entity_attr::Value::Efn(f.into()),
                EntityAttr::EntityUidEscape(e) => {
                    proto::entity::entity_attr::Value::Euide(e.into())
                }
                EntityAttr::FunctionEscape(f) => proto::entity::entity_attr::Value::Efne(f.into()),
                EntityAttr::Set(set) => {
                    let elements = set
                        .into_iter()
                        .map(|a| a.into())
                        .collect::<Vec<proto::entity::EntityAttr>>();
                    proto::entity::entity_attr::Value::Set(proto::entity::Set { elements })
                }
                EntityAttr::Record(record) => {
                    let items = record
                        .into_iter()
                        .map(|(k, v)| (k, v.into()))
                        .collect::<HashMap<String, proto::entity::EntityAttr>>();
                    proto::entity::entity_attr::Value::Record(proto::entity::Record { items })
                }
            };

            proto::entity::EntityAttr { value: Some(value) }
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct Entity {
    uid: EntityUid,
    attrs: HashMap<String, entity::EntityAttr>,
    parents: HashSet<EntityUid>,
    tags: HashMap<String, entity::EntityAttr>,
}

impl Entity {
    pub fn new(
        uid: EntityUid,
        attrs: HashMap<String, entity::EntityAttr>,
        parents: HashSet<EntityUid>,
    ) -> Self {
        Self {
            uid,
            attrs,
            parents,
            tags: HashMap::new(),
        }
    }

    pub fn new_no_attrs(uid: EntityUid, parents: HashSet<EntityUid>) -> Self {
        Self {
            uid,
            attrs: HashMap::new(),
            parents,
            tags: HashMap::new(),
        }
    }

    pub fn new_with_tags(
        uid: EntityUid,
        attrs: HashMap<String, entity::EntityAttr>,
        parents: HashSet<EntityUid>,
        tags: HashMap<String, entity::EntityAttr>,
    ) -> Self {
        Self {
            uid,
            attrs,
            parents,
            tags,
        }
    }

    pub fn uid(&self) -> &EntityUid {
        &self.uid
    }

    pub fn parents(&self) -> &HashSet<EntityUid> {
        &self.parents
    }

    pub fn attrs(&self) -> &HashMap<String, entity::EntityAttr> {
        &self.attrs
    }

    pub fn tags(&self) -> &HashMap<String, entity::EntityAttr> {
        &self.tags
    }

    pub fn to_cedar_entity(
        &self,
        cedar_schema: Option<&cedar_policy::Schema>,
    ) -> Result<cedar_policy::Entity, cedar_policy::entities_errors::EntitiesError> {
        let json = serde_json::to_value(self).unwrap();
        cedar_policy::Entity::from_json_value(json, cedar_schema)
    }
}

impl PartialEq for Entity {
    fn eq(&self, other: &Self) -> bool {
        self.uid == other.uid
    }
}

impl Eq for Entity {}

impl Hash for Entity {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.uid.hash(state);
    }
}

impl TryFrom<cedar_policy::Entity> for Entity {
    type Error = cedar_policy::entities_errors::EntitiesError;

    fn try_from(value: cedar_policy::Entity) -> Result<Self, Self::Error> {
        match value.to_json_value() {
            Ok(json) => Ok(serde_json::from_value(json).unwrap()),
            Err(e) => Err(e),
        }
    }
}

impl TryInto<cedar_policy::Entity> for Entity {
    type Error = cedar_policy::entities_errors::EntitiesError;

    fn try_into(self) -> Result<cedar_policy::Entity, Self::Error> {
        cedar_policy::Entity::from_json_value(serde_json::to_value(self).unwrap(), None)
    }
}

impl From<proto::Entity> for Entity {
    fn from(value: proto::Entity) -> Self {
        let uid = value.uid.unwrap().into();
        let attrs = value
            .attrs
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect::<HashMap<String, entity::EntityAttr>>();
        let parents = value
            .parents
            .into_iter()
            .map(|p| p.into())
            .collect::<HashSet<EntityUid>>();
        let tags = value
            .tags
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect::<HashMap<String, entity::EntityAttr>>();

        Self {
            uid,
            attrs,
            parents,
            tags,
        }
    }
}

impl Into<proto::Entity> for Entity {
    fn into(self) -> proto::Entity {
        let uid = Some(self.uid.into());
        let attrs = self
            .attrs
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect::<HashMap<String, proto::entity::EntityAttr>>();
        let parents = self
            .parents
            .into_iter()
            .map(|p| p.into())
            .collect::<Vec<proto::EntityUid>>();
        let tags = self
            .tags
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect::<HashMap<String, proto::entity::EntityAttr>>();

        proto::Entity {
            uid,
            attrs,
            parents,
            tags,
        }
    }
}

pub mod schema {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
    #[schema(as = schema::TypeJson)]
    #[serde(tag = "type")]
    pub enum TypeJson {
        Long {
            #[serde(skip_serializing_if = "Option::is_none")]
            required: Option<bool>,
        },
        String {
            #[serde(skip_serializing_if = "Option::is_none")]
            required: Option<bool>,
        },
        Boolean {
            #[serde(skip_serializing_if = "Option::is_none")]
            required: Option<bool>,
        },
        Set {
            #[schema(no_recursion)]
            element: Box<TypeJson>,
            #[serde(skip_serializing_if = "Option::is_none")]
            required: Option<bool>,
        },
        Entity {
            name: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            required: Option<bool>,
        },
        Record {
            #[schema(no_recursion)]
            attributes: HashMap<String, TypeJson>,
            #[serde(skip_serializing_if = "Option::is_none")]
            required: Option<bool>,
        },
        Extension {
            name: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            required: Option<bool>,
        },
        EntityOrCommon {
            name: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            required: Option<bool>,
        },
    }

    impl Default for TypeJson {
        fn default() -> Self {
            Self::String { required: None }
        }
    }

    impl From<proto::schema::TypeJson> for TypeJson {
        fn from(value: proto::schema::TypeJson) -> Self {
            match value.value.unwrap() {
                proto::schema::type_json::Value::L(long) => Self::Long {
                    required: match long.required {
                        true => None,
                        false => Some(false),
                    },
                },
                proto::schema::type_json::Value::S(string) => Self::String {
                    required: match string.required {
                        true => None,
                        false => Some(false),
                    },
                },
                proto::schema::type_json::Value::B(boolean) => Self::Boolean {
                    required: match boolean.required {
                        true => None,
                        false => Some(false),
                    },
                },
                proto::schema::type_json::Value::Set(set) => Self::Set {
                    element: Box::new(TypeJson::from(*set.element.unwrap())),
                    required: match set.required {
                        true => None,
                        false => Some(false),
                    },
                },
                proto::schema::type_json::Value::Entity(entity) => Self::Entity {
                    name: entity.name,
                    required: match entity.required {
                        true => None,
                        false => Some(false),
                    },
                },
                proto::schema::type_json::Value::Record(record) => Self::Record {
                    attributes: record
                        .attributes
                        .into_iter()
                        .map(|(k, v)| (k, TypeJson::from(v)))
                        .collect(),
                    required: match record.required {
                        true => None,
                        false => Some(false),
                    },
                },
                proto::schema::type_json::Value::Ext(extension) => Self::Extension {
                    name: extension.name,
                    required: match extension.required {
                        true => None,
                        false => Some(false),
                    },
                },
                proto::schema::type_json::Value::Eorc(entity_or_common) => Self::EntityOrCommon {
                    name: entity_or_common.name,
                    required: match entity_or_common.required {
                        true => None,
                        false => Some(false),
                    },
                },
            }
        }
    }

    impl Into<proto::schema::TypeJson> for TypeJson {
        fn into(self) -> proto::schema::TypeJson {
            let value = match self {
                TypeJson::Long { required } => {
                    proto::schema::type_json::Value::L(proto::schema::Long {
                        required: required.unwrap_or(true),
                    })
                }
                TypeJson::String { required } => {
                    proto::schema::type_json::Value::S(proto::schema::String {
                        required: required.unwrap_or(true),
                    })
                }
                TypeJson::Boolean { required } => {
                    proto::schema::type_json::Value::B(proto::schema::Boolean {
                        required: required.unwrap_or(true),
                    })
                }
                TypeJson::Set { element, required } => proto::schema::type_json::Value::Set(
                    ::prost::alloc::boxed::Box::new(proto::schema::Set {
                        element: Some(::prost::alloc::boxed::Box::new((*element).into())),
                        required: required.unwrap_or(true),
                    }),
                ),
                TypeJson::Entity { name, required } => {
                    proto::schema::type_json::Value::Entity(proto::schema::Entity {
                        name,
                        required: required.unwrap_or(true),
                    })
                }
                TypeJson::Record {
                    attributes,
                    required,
                } => proto::schema::type_json::Value::Record(proto::schema::Record {
                    attributes: attributes.into_iter().map(|(k, v)| (k, v.into())).collect(),
                    required: required.unwrap_or(true),
                }),
                TypeJson::Extension { name, required } => {
                    proto::schema::type_json::Value::Ext(proto::schema::Extension {
                        name,
                        required: required.unwrap_or(true),
                    })
                }
                TypeJson::EntityOrCommon { name, required } => {
                    proto::schema::type_json::Value::Eorc(proto::schema::EntityOrCommon {
                        name,
                        required: required.unwrap_or(true),
                    })
                }
            };

            proto::schema::TypeJson { value: Some(value) }
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
    #[schema(as = schema::EntityType)]
    #[serde(rename_all = "camelCase", default)]
    pub struct EntityType {
        #[serde(skip_serializing_if = "Option::is_none")]
        member_of_types: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        shape: Option<TypeJson>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tags: Option<TypeJson>,
        #[serde(skip_serializing_if = "HashMap::is_empty")]
        annotations: HashMap<String, String>,
    }

    impl From<proto::schema::EntityType> for EntityType {
        fn from(value: proto::schema::EntityType) -> Self {
            Self {
                member_of_types: match value.member_of_types.is_empty() {
                    true => None,
                    false => Some(value.member_of_types),
                },
                shape: value.shape.map(TypeJson::from),
                tags: value.tags.map(TypeJson::from),
                annotations: value.annotations,
            }
        }
    }

    impl Into<proto::schema::EntityType> for EntityType {
        fn into(self) -> proto::schema::EntityType {
            proto::schema::EntityType {
                member_of_types: self.member_of_types.unwrap_or_default(),
                shape: self.shape.map(|s| s.into()),
                tags: self.tags.map(|s| s.into()),
                annotations: self.annotations,
            }
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
    #[schema(as = schema::AppliesTo)]
    #[serde(rename_all = "camelCase", default)]
    pub struct AppliesTo {
        principal_types: Vec<String>,
        resource_types: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        context: Option<TypeJson>,
    }

    impl From<proto::schema::AppliesTo> for AppliesTo {
        fn from(value: proto::schema::AppliesTo) -> Self {
            Self {
                principal_types: value.principal_types,
                resource_types: value.resource_types,
                context: value.context.map(TypeJson::from),
            }
        }
    }

    impl Into<proto::schema::AppliesTo> for AppliesTo {
        fn into(self) -> proto::schema::AppliesTo {
            proto::schema::AppliesTo {
                principal_types: self.principal_types,
                resource_types: self.resource_types,
                context: self.context.map(|c| c.into()),
            }
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
    #[schema(as = schema::Action)]
    #[serde(rename_all = "camelCase", default)]
    pub struct Action {
        #[serde(skip_serializing_if = "Option::is_none")]
        member_of: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        applies_to: Option<AppliesTo>,
        #[serde(skip_serializing_if = "HashMap::is_empty")]
        annotations: HashMap<String, String>,
    }

    impl From<proto::schema::Action> for Action {
        fn from(value: proto::schema::Action) -> Self {
            Self {
                member_of: match value.member_of.is_empty() {
                    true => None,
                    false => Some(value.member_of),
                },
                applies_to: value.applies_to.map(AppliesTo::from),
                annotations: value.annotations,
            }
        }
    }

    impl Into<proto::schema::Action> for Action {
        fn into(self) -> proto::schema::Action {
            proto::schema::Action {
                member_of: self.member_of.unwrap_or_default(),
                applies_to: self.applies_to.map(|a| a.into()),
                annotations: self.annotations,
            }
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
    #[schema(as = schema::Namespace)]
    #[serde(rename_all = "camelCase", default)]
    pub struct Namespace {
        entity_types: HashMap<String, EntityType>,
        actions: HashMap<String, Action>,
        #[serde(skip_serializing_if = "Option::is_none")]
        common_types: Option<HashMap<String, TypeJson>>,
    }

    impl From<proto::schema::Namespace> for Namespace {
        fn from(value: proto::schema::Namespace) -> Self {
            let common_types = {
                if value.common_types.is_empty() {
                    None
                } else {
                    Some(
                        value
                            .common_types
                            .into_iter()
                            .map(|(k, v)| (k, TypeJson::from(v)))
                            .collect(),
                    )
                }
            };

            Self {
                entity_types: value
                    .entity_types
                    .into_iter()
                    .map(|(k, v)| (k, EntityType::from(v)))
                    .collect(),
                actions: value
                    .actions
                    .into_iter()
                    .map(|(k, v)| (k, Action::from(v)))
                    .collect(),
                common_types,
            }
        }
    }

    impl Into<proto::schema::Namespace> for Namespace {
        fn into(self) -> proto::schema::Namespace {
            let common_types = {
                if let Some(common_types) = self.common_types {
                    common_types
                        .into_iter()
                        .map(|(k, v)| (k, v.into()))
                        .collect()
                } else {
                    HashMap::new()
                }
            };

            proto::schema::Namespace {
                entity_types: self
                    .entity_types
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
                actions: self
                    .actions
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
                common_types,
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct Schema(pub HashMap<String, schema::Namespace>);

impl TryInto<cedar_policy::Schema> for Schema {
    type Error = cedar_policy::SchemaError;

    fn try_into(self) -> Result<cedar_policy::Schema, Self::Error> {
        let value = serde_json::to_value(&self).unwrap();
        let schema = cedar_policy::Schema::from_json_value(value)?;
        Ok(schema)
    }
}

impl From<proto::Schema> for Schema {
    fn from(value: proto::Schema) -> Self {
        Self(value.ns.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl Into<proto::Schema> for Schema {
    fn into(self) -> proto::Schema {
        proto::Schema {
            ns: self.0.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum SlotId {
    #[default]
    #[serde(rename = "?principal")]
    Principal,
    #[serde(rename = "?resource")]
    Resource,
}

impl From<String> for SlotId {
    fn from(value: String) -> Self {
        if "?principal".eq(&value) {
            Self::Principal
        } else {
            Self::Resource
        }
    }
}

impl From<cedar_policy::SlotId> for SlotId {
    fn from(value: cedar_policy::SlotId) -> Self {
        if "?principal".eq(&value.to_string()) {
            Self::Principal
        } else {
            Self::Resource
        }
    }
}

impl Into<cedar_policy::SlotId> for SlotId {
    fn into(self) -> cedar_policy::SlotId {
        match self {
            Self::Principal => cedar_policy::SlotId::principal(),
            Self::Resource => cedar_policy::SlotId::resource(),
        }
    }
}

impl From<proto::SlotId> for SlotId {
    fn from(value: proto::SlotId) -> Self {
        match value {
            proto::SlotId::Principal => Self::Principal,
            proto::SlotId::Resource => Self::Resource,
        }
    }
}

impl Into<proto::SlotId> for SlotId {
    fn into(self) -> proto::SlotId {
        match self {
            Self::Principal => proto::SlotId::Principal,
            Self::Resource => proto::SlotId::Resource,
        }
    }
}

impl ToString for SlotId {
    fn to_string(&self) -> String {
        match self {
            Self::Principal => "?principal".to_string(),
            Self::Resource => "?resource".to_string(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EntityOrSlot {
    #[serde(skip_serializing_if = "Option::is_none")]
    entity: Option<EntityUid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slot: Option<SlotId>,
}

impl From<proto::EntityOrSlot> for EntityOrSlot {
    fn from(value: proto::EntityOrSlot) -> Self {
        if let Some(entity) = value.entity {
            Self {
                entity: Some(entity.into()),
                slot: None,
            }
        } else {
            Self {
                entity: None,
                slot: Some(value.slot().into()),
            }
        }
    }
}

impl Into<proto::EntityOrSlot> for EntityOrSlot {
    fn into(self) -> proto::EntityOrSlot {
        if let Some(entity) = self.entity {
            proto::EntityOrSlot {
                entity: Some(entity.into()),
                slot: 0,
            }
        } else {
            let slot: proto::SlotId = self.slot.unwrap().into();
            proto::EntityOrSlot {
                entity: None,
                slot: slot.into(),
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum PrincipalOperator {
    #[default]
    All,
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "in")]
    In,
    #[serde(rename = "is")]
    Is,
}

impl From<proto::principal_op::Operator> for PrincipalOperator {
    fn from(value: proto::principal_op::Operator) -> Self {
        match value {
            proto::principal_op::Operator::All => Self::All,
            proto::principal_op::Operator::Eq => Self::Eq,
            proto::principal_op::Operator::In => Self::In,
            proto::principal_op::Operator::Is => Self::Is,
        }
    }
}

impl Into<proto::principal_op::Operator> for PrincipalOperator {
    fn into(self) -> proto::principal_op::Operator {
        match self {
            Self::All => proto::principal_op::Operator::All,
            Self::Eq => proto::principal_op::Operator::Eq,
            Self::In => proto::principal_op::Operator::In,
            Self::Is => proto::principal_op::Operator::Is,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum ResourceOperator {
    #[default]
    All,
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "in")]
    In,
    #[serde(rename = "is")]
    Is,
}

impl From<proto::resource_op::Operator> for ResourceOperator {
    fn from(value: proto::resource_op::Operator) -> Self {
        match value {
            proto::resource_op::Operator::All => Self::All,
            proto::resource_op::Operator::Eq => Self::Eq,
            proto::resource_op::Operator::In => Self::In,
            proto::resource_op::Operator::Is => Self::Is,
        }
    }
}

impl Into<proto::resource_op::Operator> for ResourceOperator {
    fn into(self) -> proto::resource_op::Operator {
        match self {
            Self::All => proto::resource_op::Operator::All,
            Self::Eq => proto::resource_op::Operator::Eq,
            Self::In => proto::resource_op::Operator::In,
            Self::Is => proto::resource_op::Operator::Is,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum ActionOperator {
    #[default]
    All,
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "in")]
    In,
}

impl From<proto::action_op::Operator> for ActionOperator {
    fn from(value: proto::action_op::Operator) -> Self {
        match value {
            proto::action_op::Operator::All => Self::All,
            proto::action_op::Operator::Eq => Self::Eq,
            proto::action_op::Operator::In => Self::In,
        }
    }
}

impl Into<proto::action_op::Operator> for ActionOperator {
    fn into(self) -> proto::action_op::Operator {
        match self {
            Self::All => proto::action_op::Operator::All,
            Self::Eq => proto::action_op::Operator::Eq,
            Self::In => proto::action_op::Operator::In,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct PrincipalOp {
    op: PrincipalOperator,
    #[serde(skip_serializing_if = "Option::is_none")]
    entity: Option<EntityUid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slot: Option<SlotId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entity_type: Option<String>,
    #[serde(rename = "in")]
    #[serde(skip_serializing_if = "Option::is_none")]
    r#in: Option<EntityOrSlot>,
}

impl From<proto::PrincipalOp> for PrincipalOp {
    fn from(value: proto::PrincipalOp) -> Self {
        let op = proto::principal_op::Operator::try_from(value.op)
            .unwrap()
            .into();

        match op {
            PrincipalOperator::All => Self {
                op,
                ..Default::default()
            },
            PrincipalOperator::Is => Self {
                op,
                entity_type: Some(value.entity_type),
                r#in: value.eors.map(|v| v.into()),
                ..Default::default()
            },
            _ => {
                if let Some(entity) = value.entity {
                    Self {
                        op,
                        entity: Some(entity.into()),
                        ..Default::default()
                    }
                } else {
                    let slot_id = proto::SlotId::try_from(value.slot).unwrap();
                    Self {
                        op,
                        slot: Some(slot_id.into()),
                        ..Default::default()
                    }
                }
            }
        }
    }
}

impl Into<proto::PrincipalOp> for PrincipalOp {
    fn into(self) -> proto::PrincipalOp {
        let op: proto::principal_op::Operator = self.op.into();

        match op {
            proto::principal_op::Operator::All => proto::PrincipalOp {
                op: op.into(),
                ..Default::default()
            },
            proto::principal_op::Operator::Is => proto::PrincipalOp {
                op: op.into(),
                entity_type: self.entity_type.unwrap_or_default(),
                eors: self.r#in.map(|v| v.into()),
                ..Default::default()
            },
            _ => {
                if let Some(entity) = self.entity {
                    proto::PrincipalOp {
                        op: op.into(),
                        entity: Some(entity.into()),
                        ..Default::default()
                    }
                } else {
                    let slot: proto::SlotId = self.slot.unwrap().into();
                    proto::PrincipalOp {
                        op: op.into(),
                        slot: slot.into(),
                        ..Default::default()
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct ResourceOp {
    op: ResourceOperator,
    #[serde(skip_serializing_if = "Option::is_none")]
    entity: Option<EntityUid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slot: Option<SlotId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entity_type: Option<String>,
    #[serde(rename = "in")]
    #[serde(skip_serializing_if = "Option::is_none")]
    r#in: Option<EntityOrSlot>,
}

impl From<proto::ResourceOp> for ResourceOp {
    fn from(value: proto::ResourceOp) -> Self {
        let op = proto::resource_op::Operator::try_from(value.op)
            .unwrap()
            .into();

        match op {
            ResourceOperator::All => Self {
                op,
                ..Default::default()
            },
            ResourceOperator::Is => Self {
                op,
                entity_type: Some(value.entity_type),
                r#in: value.eors.map(|v| v.into()),
                ..Default::default()
            },
            _ => {
                if let Some(entity) = value.entity {
                    Self {
                        op,
                        entity: Some(entity.into()),
                        ..Default::default()
                    }
                } else {
                    let slot_id = proto::SlotId::try_from(value.slot).unwrap();
                    Self {
                        op,
                        slot: Some(slot_id.into()),
                        ..Default::default()
                    }
                }
            }
        }
    }
}

impl Into<proto::ResourceOp> for ResourceOp {
    fn into(self) -> proto::ResourceOp {
        let op: proto::resource_op::Operator = self.op.into();

        match op {
            proto::resource_op::Operator::All => proto::ResourceOp {
                op: op.into(),
                ..Default::default()
            },
            proto::resource_op::Operator::Is => proto::ResourceOp {
                op: op.into(),
                entity_type: self.entity_type.unwrap_or_default(),
                eors: self.r#in.map(|v| v.into()),
                ..Default::default()
            },
            _ => {
                if let Some(entity) = self.entity {
                    proto::ResourceOp {
                        op: op.into(),
                        entity: Some(entity.into()),
                        ..Default::default()
                    }
                } else {
                    let slot: proto::SlotId = self.slot.unwrap().into();
                    proto::ResourceOp {
                        op: op.into(),
                        slot: slot.into(),
                        ..Default::default()
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct ActionOp {
    op: ActionOperator,
    #[serde(skip_serializing_if = "Option::is_none")]
    entity: Option<EntityUid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entities: Option<Vec<EntityUid>>,
}

impl From<proto::ActionOp> for ActionOp {
    fn from(value: proto::ActionOp) -> Self {
        let op = proto::action_op::Operator::try_from(value.op)
            .unwrap()
            .into();

        match op {
            ActionOperator::All => Self {
                op,
                ..Default::default()
            },
            _ => {
                if let Some(entity) = value.entity {
                    Self {
                        op,
                        entity: Some(entity.into()),
                        ..Default::default()
                    }
                } else {
                    Self {
                        op,
                        entities: Some(value.entities.into_iter().map(|e| e.into()).collect()),
                        ..Default::default()
                    }
                }
            }
        }
    }
}

impl Into<proto::ActionOp> for ActionOp {
    fn into(self) -> proto::ActionOp {
        let op: proto::action_op::Operator = self.op.into();

        match op {
            proto::action_op::Operator::All => proto::ActionOp {
                op: op.into(),
                ..Default::default()
            },
            _ => {
                if let Some(entity) = self.entity {
                    proto::ActionOp {
                        op: op.into(),
                        entity: Some(entity.into()),
                        ..Default::default()
                    }
                } else {
                    proto::ActionOp {
                        op: op.into(),
                        entities: self
                            .entities
                            .unwrap()
                            .into_iter()
                            .map(|e| e.into())
                            .collect(),
                        ..Default::default()
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SetExpr {
    #[serde(rename = "Set")]
    #[schema(no_recursion)]
    set: Vec<JsonExpr>,
}

impl From<proto::json_expr::value_expr::Set> for SetExpr {
    fn from(value: proto::json_expr::value_expr::Set) -> Self {
        Self {
            set: value.set.into_iter().map(JsonExpr::from).collect(),
        }
    }
}

impl Into<proto::json_expr::value_expr::Set> for SetExpr {
    fn into(self) -> proto::json_expr::value_expr::Set {
        proto::json_expr::value_expr::Set {
            set: self.set.into_iter().map(|e| e.into()).collect(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RecordExpr {
    #[serde(rename = "Record")]
    #[schema(no_recursion)]
    record: HashMap<String, JsonExpr>,
}

impl From<proto::json_expr::value_expr::Record> for RecordExpr {
    fn from(value: proto::json_expr::value_expr::Record) -> Self {
        Self {
            record: value
                .record
                .into_iter()
                .map(|(k, v)| (k, JsonExpr::from(v)))
                .collect(),
        }
    }
}

impl Into<proto::json_expr::value_expr::Record> for RecordExpr {
    fn into(self) -> proto::json_expr::value_expr::Record {
        proto::json_expr::value_expr::Record {
            record: self
                .record
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ValueExpr {
    String(String),
    Number(i64),
    Boolean(bool),
    Set(SetExpr),
    Record(RecordExpr),
    EntityUid(EntityUid),
    Function(ExtensionFn),
}

impl Default for ValueExpr {
    fn default() -> Self {
        ValueExpr::String(String::default())
    }
}

impl From<proto::json_expr::ValueExpr> for ValueExpr {
    fn from(value: proto::json_expr::ValueExpr) -> Self {
        match value.value.unwrap() {
            proto::json_expr::value_expr::Value::S(s) => ValueExpr::String(s),
            proto::json_expr::value_expr::Value::I(n) => ValueExpr::Number(n),
            proto::json_expr::value_expr::Value::B(b) => ValueExpr::Boolean(b),
            proto::json_expr::value_expr::Value::Set(s) => ValueExpr::Set(SetExpr {
                set: s.set.into_iter().map(JsonExpr::from).collect(),
            }),
            proto::json_expr::value_expr::Value::Record(r) => ValueExpr::Record(RecordExpr {
                record: r
                    .record
                    .into_iter()
                    .map(|(k, v)| (k, JsonExpr::from(v)))
                    .collect(),
            }),
            proto::json_expr::value_expr::Value::Euid(e) => {
                ValueExpr::EntityUid(EntityUid::from(e))
            }
            proto::json_expr::value_expr::Value::Efn(f) => {
                ValueExpr::Function(ExtensionFn::from(f))
            }
        }
    }
}

impl Into<proto::json_expr::ValueExpr> for ValueExpr {
    fn into(self) -> proto::json_expr::ValueExpr {
        proto::json_expr::ValueExpr {
            value: Some(match self {
                ValueExpr::String(s) => proto::json_expr::value_expr::Value::S(s),
                ValueExpr::Number(n) => proto::json_expr::value_expr::Value::I(n),
                ValueExpr::Boolean(b) => proto::json_expr::value_expr::Value::B(b),
                ValueExpr::Set(s) => {
                    proto::json_expr::value_expr::Value::Set(proto::json_expr::value_expr::Set {
                        set: s.set.into_iter().map(|e| e.into()).collect(),
                    })
                }
                ValueExpr::Record(r) => proto::json_expr::value_expr::Value::Record(
                    proto::json_expr::value_expr::Record {
                        record: r.record.into_iter().map(|(k, v)| (k, v.into())).collect(),
                    },
                ),
                ValueExpr::EntityUid(e) => proto::json_expr::value_expr::Value::Euid(e.into()),
                ValueExpr::Function(f) => proto::json_expr::value_expr::Value::Efn(f.into()),
            }),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum VarValue {
    #[default]
    #[serde(rename = "principal")]
    Principal,
    #[serde(rename = "action")]
    Action,
    #[serde(rename = "resource")]
    Resource,
    #[serde(rename = "context")]
    Context,
}

impl From<proto::json_expr::VarValue> for VarValue {
    fn from(value: proto::json_expr::VarValue) -> Self {
        match value {
            proto::json_expr::VarValue::Principal => VarValue::Principal,
            proto::json_expr::VarValue::Action => VarValue::Action,
            proto::json_expr::VarValue::Resource => VarValue::Resource,
            proto::json_expr::VarValue::Context => VarValue::Context,
        }
    }
}

impl Into<proto::json_expr::VarValue> for VarValue {
    fn into(self) -> proto::json_expr::VarValue {
        match self {
            VarValue::Principal => proto::json_expr::VarValue::Principal,
            VarValue::Action => proto::json_expr::VarValue::Action,
            VarValue::Resource => proto::json_expr::VarValue::Resource,
            VarValue::Context => proto::json_expr::VarValue::Context,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HasExpr {
    #[schema(no_recursion)]
    left: JsonExpr,
    attr: String,
}

impl From<proto::json_expr::HasExpr> for HasExpr {
    fn from(value: proto::json_expr::HasExpr) -> Self {
        Self {
            left: JsonExpr::from(*value.left.unwrap()),
            attr: value.attr,
        }
    }
}

impl Into<proto::json_expr::HasExpr> for HasExpr {
    fn into(self) -> proto::json_expr::HasExpr {
        proto::json_expr::HasExpr {
            left: Some(::prost::alloc::boxed::Box::new(self.left.into())),
            attr: self.attr,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BinaryExpr {
    #[schema(no_recursion)]
    left: JsonExpr,
    #[schema(no_recursion)]
    right: JsonExpr,
}

impl From<proto::json_expr::BinaryExpr> for BinaryExpr {
    fn from(value: proto::json_expr::BinaryExpr) -> Self {
        Self {
            left: JsonExpr::from(*value.left.unwrap()),
            right: JsonExpr::from(*value.right.unwrap()),
        }
    }
}

impl Into<proto::json_expr::BinaryExpr> for BinaryExpr {
    fn into(self) -> proto::json_expr::BinaryExpr {
        proto::json_expr::BinaryExpr {
            left: Some(::prost::alloc::boxed::Box::new(self.left.into())),
            right: Some(::prost::alloc::boxed::Box::new(self.right.into())),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NegExpr {
    #[schema(no_recursion)]
    arg: JsonExpr,
}

impl From<proto::json_expr::NegExpr> for NegExpr {
    fn from(value: proto::json_expr::NegExpr) -> Self {
        Self {
            arg: JsonExpr::from(*value.arg.unwrap()),
        }
    }
}

impl Into<proto::json_expr::NegExpr> for NegExpr {
    fn into(self) -> proto::json_expr::NegExpr {
        proto::json_expr::NegExpr {
            arg: Some(::prost::alloc::boxed::Box::new(self.arg.into())),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct IsExpr {
    #[schema(no_recursion)]
    left: JsonExpr,
    entity_type: String,
    #[serde(rename = "in")]
    #[serde(skip_serializing_if = "Option::is_none")]
    r#in: Option<EntityUid>,
}

impl From<proto::json_expr::IsExpr> for IsExpr {
    fn from(value: proto::json_expr::IsExpr) -> Self {
        Self {
            left: JsonExpr::from(*value.left.unwrap()),
            entity_type: value.entity_type,
            r#in: value.r#in.map(|e| EntityUid::from(e)),
        }
    }
}

impl Into<proto::json_expr::IsExpr> for IsExpr {
    fn into(self) -> proto::json_expr::IsExpr {
        proto::json_expr::IsExpr {
            left: Some(::prost::alloc::boxed::Box::new(self.left.into())),
            entity_type: self.entity_type,
            r#in: self.r#in.map(|e| e.into()),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct LikeExpr {
    #[schema(no_recursion)]
    left: JsonExpr,
    pattern: String,
}

impl From<proto::json_expr::LikeExpr> for LikeExpr {
    fn from(value: proto::json_expr::LikeExpr) -> Self {
        Self {
            left: JsonExpr::from(*value.left.unwrap()),
            pattern: value.pattern,
        }
    }
}

impl Into<proto::json_expr::LikeExpr> for LikeExpr {
    fn into(self) -> proto::json_expr::LikeExpr {
        proto::json_expr::LikeExpr {
            left: Some(::prost::alloc::boxed::Box::new(self.left.into())),
            pattern: self.pattern,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct IfThenElseExpr {
    #[serde(rename = "if")]
    #[schema(no_recursion)]
    pub r#if: JsonExpr,
    #[serde(rename = "then")]
    #[schema(no_recursion)]
    pub then: JsonExpr,
    #[serde(rename = "else")]
    #[schema(no_recursion)]
    pub r#else: JsonExpr,
}

impl From<proto::json_expr::IfThenElseExpr> for IfThenElseExpr {
    fn from(value: proto::json_expr::IfThenElseExpr) -> Self {
        Self {
            r#if: JsonExpr::from(*value.r#if.unwrap()),
            then: JsonExpr::from(*value.then.unwrap()),
            r#else: JsonExpr::from(*value.r#else.unwrap()),
        }
    }
}

impl Into<proto::json_expr::IfThenElseExpr> for IfThenElseExpr {
    fn into(self) -> proto::json_expr::IfThenElseExpr {
        proto::json_expr::IfThenElseExpr {
            r#if: Some(Box::new(self.r#if.into())),
            then: Some(Box::new(self.then.into())),
            r#else: Some(Box::new(self.r#else.into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum JsonExpr {
    Value(ValueExpr),
    Var(VarValue),
    Slot(SlotId),

    #[serde(rename = "!")]
    Bang(Box<NegExpr>),
    #[serde(rename = "neg")]
    Neg(Box<NegExpr>),

    #[serde(rename = "==")]
    Eq(Box<BinaryExpr>),
    #[serde(rename = "!=")]
    Neq(Box<BinaryExpr>),
    #[serde(rename = "in")]
    In(Box<BinaryExpr>),
    #[serde(rename = "<")]
    Lt(Box<BinaryExpr>),
    #[serde(rename = "<=")]
    Lte(Box<BinaryExpr>),
    #[serde(rename = ">")]
    Gt(Box<BinaryExpr>),
    #[serde(rename = ">=")]
    Gte(Box<BinaryExpr>),
    #[serde(rename = "&&")]
    And(Box<BinaryExpr>),
    #[serde(rename = "||")]
    Or(Box<BinaryExpr>),
    #[serde(rename = "+")]
    Plus(Box<BinaryExpr>),
    #[serde(rename = "-")]
    Minus(Box<BinaryExpr>),
    #[serde(rename = "*")]
    Mul(Box<BinaryExpr>),
    #[serde(rename = "contains")]
    Contains(Box<BinaryExpr>),
    #[serde(rename = "containsAll")]
    ContainsAll(Box<BinaryExpr>),
    #[serde(rename = "containsAny")]
    ContainsAny(Box<BinaryExpr>),
    #[serde(rename = "hasTag")]
    HasTag(Box<BinaryExpr>),
    #[serde(rename = "getTag")]
    GetTag(Box<BinaryExpr>),

    #[serde(rename = ".")]
    Dot(Box<HasExpr>),
    #[serde(rename = "has")]
    Has(Box<HasExpr>),

    #[serde(rename = "is")]
    Is(Box<IsExpr>),

    #[serde(rename = "like")]
    Like(Box<LikeExpr>),

    #[serde(rename = "if-then-else")]
    IfThenElse(Box<IfThenElseExpr>),

    #[schema(no_recursion)]
    Set(Vec<JsonExpr>),
    #[schema(no_recursion)]
    Record(HashMap<String, JsonExpr>),

    #[serde(rename = "decimal")]
    #[schema(no_recursion)]
    Decimal(Vec<JsonExpr>),
    #[serde(rename = "ip")]
    #[schema(no_recursion)]
    Ip(Vec<JsonExpr>),
    #[serde(rename = "isInRange")]
    #[schema(no_recursion)]
    IsInRange(Vec<JsonExpr>),
}

impl Default for JsonExpr {
    fn default() -> Self {
        JsonExpr::Value(ValueExpr::default())
    }
}

impl From<proto::JsonExpr> for JsonExpr {
    fn from(value: proto::JsonExpr) -> Self {
        match value.expr.unwrap() {
            proto::json_expr::Expr::Value(expr) => JsonExpr::Value(expr.into()),
            proto::json_expr::Expr::Var(var) => JsonExpr::Var(VarValue::from(
                proto::json_expr::VarValue::try_from(var).unwrap(),
            )),
            proto::json_expr::Expr::Slot(slot_id) => {
                JsonExpr::Slot(SlotId::from(proto::SlotId::try_from(slot_id).unwrap()))
            }
            proto::json_expr::Expr::Neg(expr) => JsonExpr::Neg(Box::new((*expr).into())),
            proto::json_expr::Expr::Bang(expr) => JsonExpr::Neg(Box::new((*expr).into())),
            proto::json_expr::Expr::Eq(expr) => JsonExpr::Eq(Box::new((*expr).into())),
            proto::json_expr::Expr::Neq(expr) => JsonExpr::Neq(Box::new((*expr).into())),
            proto::json_expr::Expr::In(expr) => JsonExpr::In(Box::new((*expr).into())),
            proto::json_expr::Expr::Lt(expr) => JsonExpr::Lt(Box::new((*expr).into())),
            proto::json_expr::Expr::Lte(expr) => JsonExpr::Lte(Box::new((*expr).into())),
            proto::json_expr::Expr::Gt(expr) => JsonExpr::Gt(Box::new((*expr).into())),
            proto::json_expr::Expr::Gte(expr) => JsonExpr::Gte(Box::new((*expr).into())),
            proto::json_expr::Expr::And(expr) => JsonExpr::And(Box::new((*expr).into())),
            proto::json_expr::Expr::Or(expr) => JsonExpr::Or(Box::new((*expr).into())),
            proto::json_expr::Expr::Plus(expr) => JsonExpr::Plus(Box::new((*expr).into())),
            proto::json_expr::Expr::Minus(expr) => JsonExpr::Minus(Box::new((*expr).into())),
            proto::json_expr::Expr::Mul(expr) => JsonExpr::Mul(Box::new((*expr).into())),
            proto::json_expr::Expr::Contains(expr) => JsonExpr::Contains(Box::new((*expr).into())),
            proto::json_expr::Expr::ContainsAll(expr) => {
                JsonExpr::ContainsAll(Box::new((*expr).into()))
            }
            proto::json_expr::Expr::ContainsAny(expr) => {
                JsonExpr::ContainsAny(Box::new((*expr).into()))
            }
            proto::json_expr::Expr::HasTag(expr) => JsonExpr::HasTag(Box::new((*expr).into())),
            proto::json_expr::Expr::GetTag(expr) => JsonExpr::GetTag(Box::new((*expr).into())),
            proto::json_expr::Expr::Has(expr) => JsonExpr::Has(Box::new((*expr).into())),
            proto::json_expr::Expr::Dot(expr) => JsonExpr::Dot(Box::new((*expr).into())),
            proto::json_expr::Expr::Is(expr) => JsonExpr::Is(Box::new((*expr).into())),
            proto::json_expr::Expr::Like(expr) => JsonExpr::Like(Box::new((*expr).into())),
            proto::json_expr::Expr::IfThenElse(expr) => {
                JsonExpr::IfThenElse(Box::new((*expr).into()))
            }
            proto::json_expr::Expr::Set(set) => {
                JsonExpr::Set(set.set.into_iter().map(|e| JsonExpr::from(e)).collect())
            }
            proto::json_expr::Expr::Record(record) => JsonExpr::Record(
                record
                    .record
                    .into_iter()
                    .map(|(k, v)| (k, JsonExpr::from(v)))
                    .collect(),
            ),
            proto::json_expr::Expr::Decimal(set) => {
                JsonExpr::Decimal(set.set.into_iter().map(|e| JsonExpr::from(e)).collect())
            }
            proto::json_expr::Expr::Ip(set) => {
                JsonExpr::Decimal(set.set.into_iter().map(|e| JsonExpr::from(e)).collect())
            }
            proto::json_expr::Expr::IsInRange(set) => {
                JsonExpr::Decimal(set.set.into_iter().map(|e| JsonExpr::from(e)).collect())
            }
        }
    }
}

impl Into<proto::JsonExpr> for JsonExpr {
    fn into(self) -> proto::JsonExpr {
        match self {
            JsonExpr::Value(value_expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Value(value_expr.into())),
            },
            JsonExpr::Var(var) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Var(
                    Into::<proto::json_expr::VarValue>::into(var) as i32,
                )),
            },
            JsonExpr::Slot(slot_id) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Var(
                    Into::<proto::SlotId>::into(slot_id).into(),
                )),
            },
            JsonExpr::Neg(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Neg(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Bang(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Bang(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Eq(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Eq(::prost::alloc::boxed::Box::new(
                    (*expr).into(),
                ))),
            },
            JsonExpr::Neq(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Neq(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::In(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::In(::prost::alloc::boxed::Box::new(
                    (*expr).into(),
                ))),
            },
            JsonExpr::Lt(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Lt(::prost::alloc::boxed::Box::new(
                    (*expr).into(),
                ))),
            },
            JsonExpr::Lte(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Lte(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Gt(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Gt(::prost::alloc::boxed::Box::new(
                    (*expr).into(),
                ))),
            },
            JsonExpr::Gte(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Gte(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::And(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::And(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Or(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Or(::prost::alloc::boxed::Box::new(
                    (*expr).into(),
                ))),
            },
            JsonExpr::Plus(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Plus(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Minus(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Minus(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Mul(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Mul(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Contains(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Contains(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::ContainsAll(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::ContainsAll(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::ContainsAny(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::ContainsAny(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::HasTag(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::HasTag(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::GetTag(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::GetTag(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Has(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Has(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Dot(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Dot(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Is(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Is(::prost::alloc::boxed::Box::new(
                    (*expr).into(),
                ))),
            },
            JsonExpr::Like(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Like(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::IfThenElse(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::IfThenElse(
                    ::prost::alloc::boxed::Box::new((*expr).into()),
                )),
            },
            JsonExpr::Set(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Set(proto::json_expr::Set {
                    set: expr.into_iter().map(|v| v.into()).collect(),
                })),
            },
            JsonExpr::Record(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Record(proto::json_expr::Record {
                    record: expr.into_iter().map(|(k, v)| (k, v.into())).collect(),
                })),
            },
            JsonExpr::Decimal(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Decimal(proto::json_expr::Set {
                    set: expr.into_iter().map(|v| v.into()).collect(),
                })),
            },
            JsonExpr::Ip(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::Ip(proto::json_expr::Set {
                    set: expr.into_iter().map(|v| v.into()).collect(),
                })),
            },
            JsonExpr::IsInRange(expr) => proto::JsonExpr {
                expr: Some(proto::json_expr::Expr::IsInRange(proto::json_expr::Set {
                    set: expr.into_iter().map(|v| v.into()).collect(),
                })),
            },
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum ConditionKind {
    #[default]
    #[serde(rename = "when")]
    When,
    #[serde(rename = "unless")]
    Unless,
}

impl From<proto::ConditionKind> for ConditionKind {
    fn from(value: proto::ConditionKind) -> Self {
        match value {
            proto::ConditionKind::When => ConditionKind::When,
            proto::ConditionKind::Unless => ConditionKind::Unless,
        }
    }
}

impl Into<proto::ConditionKind> for ConditionKind {
    fn into(self) -> proto::ConditionKind {
        match self {
            ConditionKind::When => proto::ConditionKind::When,
            ConditionKind::Unless => proto::ConditionKind::Unless,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Condition {
    kind: ConditionKind,
    body: JsonExpr,
}

impl From<proto::Condition> for Condition {
    fn from(value: proto::Condition) -> Self {
        Self {
            kind: value.kind().into(),
            body: value.body.unwrap().into(),
        }
    }
}

impl Into<proto::Condition> for Condition {
    fn into(self) -> proto::Condition {
        proto::Condition {
            kind: Into::<proto::ConditionKind>::into(self.kind) as i32,
            body: Some(self.body.into()),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum PolicyEffect {
    #[default]
    #[serde(rename = "permit")]
    Permit,
    #[serde(rename = "forbid")]
    Forbid,
}

impl From<proto::Effect> for PolicyEffect {
    fn from(value: proto::Effect) -> Self {
        match value {
            proto::Effect::Permit => PolicyEffect::Permit,
            proto::Effect::Forbid => PolicyEffect::Forbid,
        }
    }
}

impl Into<proto::Effect> for PolicyEffect {
    fn into(self) -> proto::Effect {
        match self {
            PolicyEffect::Permit => proto::Effect::Permit,
            PolicyEffect::Forbid => proto::Effect::Forbid,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct Policy {
    pub effect: PolicyEffect,
    pub principal: PrincipalOp,
    pub action: ActionOp,
    pub resource: ResourceOp,
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub annotations: HashMap<String, Option<String>>,
}

impl Policy {
    pub fn to_cedar(
        &self,
        policy_id: PolicyId,
    ) -> Result<cedar_policy::Policy, cedar_policy::PolicyFromJsonError> {
        let json = serde_json::to_value(self).unwrap();
        cedar_policy::Policy::from_json(Some(policy_id.into()), json)
    }
}

impl From<proto::Policy> for Policy {
    fn from(value: proto::Policy) -> Self {
        Self {
            effect: value.effect().into(),
            principal: value.principal.unwrap().into(),
            action: value.action.unwrap().into(),
            resource: value.resource.unwrap().into(),
            conditions: value
                .conditions
                .into_iter()
                .map(|c| c.into())
                .collect::<Vec<Condition>>(),
            annotations: value
                .annotations
                .into_iter()
                .map(|(k, v)| (k, Some(v)))
                .collect(),
        }
    }
}

impl Into<proto::Policy> for Policy {
    fn into(self) -> proto::Policy {
        proto::Policy {
            effect: Into::<proto::Effect>::into(self.effect) as i32,
            principal: Some(self.principal.into()),
            action: Some(self.action.into()),
            resource: Some(self.resource.into()),
            conditions: self.conditions.into_iter().map(|c| c.into()).collect(),
            annotations: self
                .annotations
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or_default()))
                .collect(),
        }
    }
}

impl TryFrom<cedar_policy::Policy> for Policy {
    type Error = cedar_policy::PolicyToJsonError;

    fn try_from(value: cedar_policy::Policy) -> Result<Self, Self::Error> {
        match value.to_json() {
            Ok(json) => Ok(serde_json::from_value(json).unwrap()),
            Err(e) => Err(e),
        }
    }
}

impl TryInto<cedar_policy::Policy> for Policy {
    type Error = cedar_policy::PolicyFromJsonError;

    fn try_into(self) -> Result<cedar_policy::Policy, Self::Error> {
        let json = serde_json::to_value(self).unwrap();
        cedar_policy::Policy::from_json(None, json)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct Template {
    pub effect: PolicyEffect,
    pub principal: PrincipalOp,
    pub action: ActionOp,
    pub resource: ResourceOp,
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub annotations: HashMap<String, Option<String>>,
}

impl Template {
    pub fn to_cedar(
        &self,
        policy_id: PolicyId,
    ) -> Result<cedar_policy::Template, cedar_policy::PolicyFromJsonError> {
        let json = serde_json::to_value(self).unwrap();
        cedar_policy::Template::from_json(Some(policy_id.into()), json)
    }
}

impl From<proto::Template> for Template {
    fn from(value: proto::Template) -> Self {
        Self {
            effect: value.effect().into(),
            principal: value.principal.unwrap().into(),
            action: value.action.unwrap().into(),
            resource: value.resource.unwrap().into(),
            conditions: value
                .conditions
                .into_iter()
                .map(|c| c.into())
                .collect::<Vec<Condition>>(),
            annotations: value
                .annotations
                .into_iter()
                .map(|(k, v)| (k, Some(v)))
                .collect(),
        }
    }
}

impl Into<proto::Template> for Template {
    fn into(self) -> proto::Template {
        proto::Template {
            effect: Into::<proto::Effect>::into(self.effect) as i32,
            principal: Some(self.principal.into()),
            action: Some(self.action.into()),
            resource: Some(self.resource.into()),
            conditions: self.conditions.into_iter().map(|c| c.into()).collect(),
            annotations: self
                .annotations
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or_default()))
                .collect(),
        }
    }
}

impl TryFrom<cedar_policy::Template> for Template {
    type Error = cedar_policy::PolicyToJsonError;

    fn try_from(value: cedar_policy::Template) -> Result<Self, Self::Error> {
        match value.to_json() {
            Ok(json) => Ok(serde_json::from_value(json).unwrap()),
            Err(e) => Err(e),
        }
    }
}

impl TryInto<cedar_policy::Template> for Template {
    type Error = cedar_policy::PolicyFromJsonError;

    fn try_into(self) -> Result<cedar_policy::Template, Self::Error> {
        let json = serde_json::to_value(self).unwrap();
        cedar_policy::Template::from_json(None, json)
    }
}

#[derive(
    Debug, Default, Clone, Eq, PartialOrd, Ord, Hash, PartialEq, Serialize, Deserialize, ToSchema,
)]
pub struct PolicyId(String);

impl From<String> for PolicyId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<cedar_policy::PolicyId> for PolicyId {
    fn from(value: cedar_policy::PolicyId) -> Self {
        Self(value.to_string())
    }
}

impl Into<cedar_policy::PolicyId> for PolicyId {
    fn into(self) -> cedar_policy::PolicyId {
        cedar_policy::PolicyId::new(&self.0)
    }
}

impl ToString for PolicyId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl Borrow<str> for PolicyId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum EntityValue {
    EntityUid(EntityUid),
    EntityEscape(EntityUidEscape),
}

impl Default for EntityValue {
    fn default() -> Self {
        Self::EntityEscape(EntityUidEscape::default())
    }
}

impl From<cedar_policy::EntityUid> for EntityValue {
    fn from(value: cedar_policy::EntityUid) -> Self {
        Self::EntityEscape(EntityUidEscape::from(value))
    }
}

impl Into<cedar_policy::EntityUid> for EntityValue {
    fn into(self) -> cedar_policy::EntityUid {
        match self {
            EntityValue::EntityUid(e) => e.into(),
            EntityValue::EntityEscape(e) => e.into(),
        }
    }
}

impl From<proto::EntityValue> for EntityValue {
    fn from(value: proto::EntityValue) -> Self {
        match value.value.unwrap() {
            proto::entity_value::Value::Ee(e) => EntityValue::EntityEscape(e.into()),
            proto::entity_value::Value::Euid(e) => EntityValue::EntityUid(e.into()),
        }
    }
}

impl Into<proto::EntityValue> for EntityValue {
    fn into(self) -> proto::EntityValue {
        match self {
            EntityValue::EntityUid(e) => proto::EntityValue {
                value: Some(proto::entity_value::Value::Euid(e.into())),
            },
            EntityValue::EntityEscape(e) => proto::EntityValue {
                value: Some(proto::entity_value::Value::Ee(e.into())),
            },
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct TemplateLink {
    pub template_id: PolicyId,
    pub new_id: PolicyId,
    pub values: HashMap<SlotId, EntityValue>,
}

impl TemplateLink {
    pub fn new(
        template_id: PolicyId,
        new_id: PolicyId,
        values: HashMap<SlotId, EntityValue>,
    ) -> Self {
        Self {
            template_id,
            new_id,
            values,
        }
    }

    pub fn to_cedar_vals(&self) -> HashMap<cedar_policy::SlotId, cedar_policy::EntityUid> {
        self.values
            .iter()
            .map(|(k, v)| (k.clone().into(), v.clone().into()))
            .collect()
    }
}

impl From<proto::TemplateLink> for TemplateLink {
    fn from(value: proto::TemplateLink) -> Self {
        Self {
            template_id: value.template_id.into(),
            new_id: value.new_id.into(),
            values: value
                .values
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

impl Into<proto::TemplateLink> for TemplateLink {
    fn into(self) -> proto::TemplateLink {
        proto::TemplateLink {
            template_id: self.template_id.to_string(),
            new_id: self.new_id.to_string(),
            values: self
                .values
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.into()))
                .collect(),
        }
    }
}

impl From<cedar_policy::Policy> for TemplateLink {
    fn from(value: cedar_policy::Policy) -> Self {
        let template_id = value.template_id().unwrap().clone().into();
        let new_id = value.id().clone().into();
        let template_links = value.template_links().unwrap();

        let values = template_links
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<HashMap<SlotId, EntityValue>>();

        Self {
            template_id,
            new_id,
            values,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct PolicySet {
    pub static_policies: HashMap<PolicyId, Policy>,
    pub templates: HashMap<PolicyId, Template>,
    pub template_links: Vec<TemplateLink>,
}

impl From<proto::PolicySet> for PolicySet {
    fn from(value: proto::PolicySet) -> Self {
        Self {
            static_policies: value
                .static_policies
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            templates: value
                .templates
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            template_links: value.template_links.into_iter().map(|v| v.into()).collect(),
        }
    }
}

impl Into<proto::PolicySet> for PolicySet {
    fn into(self) -> proto::PolicySet {
        proto::PolicySet {
            static_policies: self
                .static_policies
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.into()))
                .collect(),
            templates: self
                .templates
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.into()))
                .collect(),
            template_links: self.template_links.into_iter().map(|v| v.into()).collect(),
        }
    }
}

impl TryFrom<cedar_policy::PolicySet> for PolicySet {
    type Error = cedar_policy::PolicySetError;
    fn try_from(value: cedar_policy::PolicySet) -> Result<Self, Self::Error> {
        Ok(serde_json::from_value(value.to_json()?).unwrap())
    }
}

impl TryInto<cedar_policy::PolicySet> for PolicySet {
    type Error = cedar_policy::PolicySetError;
    fn try_into(self) -> Result<cedar_policy::PolicySet, Self::Error> {
        cedar_policy::PolicySet::from_json_value(serde_json::to_value(self).unwrap())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Context(HashMap<String, entity::EntityAttr>);

impl Context {
    pub fn to_cedar_context(
        &self,
        schema: Option<(&cedar_policy::Schema, &cedar_policy::EntityUid)>,
    ) -> Result<cedar_policy::Context, cedar_policy::ContextJsonError> {
        let json = serde_json::to_value(self).unwrap();
        cedar_policy::Context::from_json_value(json, schema)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum Decision {
    Allow,
    #[default]
    Deny,
}

impl From<cedar_policy::Decision> for Decision {
    fn from(value: cedar_policy::Decision) -> Self {
        match value {
            cedar_policy::Decision::Allow => Self::Allow,
            cedar_policy::Decision::Deny => Self::Deny,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct Response {
    pub decision: Decision,
    pub reason: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Request {
    pub principal: EntityUid,
    pub action: EntityUid,
    pub resource: EntityUid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,
}

impl From<cedar_policy::Response> for Response {
    fn from(value: cedar_policy::Response) -> Self {
        let decision = match value.decision() {
            cedar_policy::Decision::Allow => Decision::Allow,
            cedar_policy::Decision::Deny => Decision::Deny,
        };
        let reason = value
            .diagnostics()
            .reason()
            .into_iter()
            .map(|r| r.to_string())
            .collect::<Vec<String>>();
        let errors = value
            .diagnostics()
            .errors()
            .into_iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>();

        Self {
            decision,
            reason,
            errors,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cedar_schema() {
        let json = serde_json::to_string_pretty(&PolicySet::default()).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_policy() {
        let policy0 = Policy {
            effect: PolicyEffect::Permit,
            principal: PrincipalOp {
                op: PrincipalOperator::Eq,
                entity: Some(EntityUid {
                    r#type: "User".to_string(),
                    id: "12UA45".to_string(),
                }),
                slot: None,
                entity_type: None,
                r#in: None,
            },
            action: ActionOp {
                op: ActionOperator::Eq,
                entity: Some(EntityUid {
                    r#type: "Action".to_string(),
                    id: "view".to_string(),
                }),
                entities: None,
            },
            resource: ResourceOp {
                op: ResourceOperator::In,
                entity: Some(EntityUid {
                    r#type: "Folder".to_string(),
                    id: "abc".to_string(),
                }),
                slot: None,
                entity_type: None,
                r#in: None,
            },
            conditions: vec![Condition {
                kind: ConditionKind::When,
                body: JsonExpr::Eq(Box::new(BinaryExpr {
                    left: JsonExpr::Dot(Box::new(HasExpr {
                        left: JsonExpr::Var(VarValue::Context),
                        attr: "tls_version".to_string(),
                    })),
                    right: JsonExpr::Value(ValueExpr::String("1.3".to_string())),
                })),
            }],
            annotations: HashMap::from([
                ("id".to_string(), Some("id".to_string())),
                ("annotation".to_string(), None),
            ]),
        };
        let json = serde_json::to_string_pretty(&policy0).unwrap();
        println!("{}", json);

        let json = r#"
{
    "effect": "permit",
    "principal": {
        "op": "==",
        "entity": { "type": "User", "id": "12UA45" }
    },
    "action": {
        "op": "==",
        "entity": { "type": "Action", "id": "view" }
    },
    "resource": {
        "op": "in",
        "entity": { "type": "Folder", "id": "abc" }
    },
    "conditions": [
        {
            "kind": "when",
            "body": {
                "==": {
                    "left": {
                        ".": {
                            "left": {
                                "Var": "context"
                            },
                            "attr": "tls_version"
                        }
                    },
                    "right": {
                        "Value": "1.3"
                    }
                }
            }
        }
    ]
}
        "#;

        let policy1: Policy = serde_json::from_str(json).unwrap();
        println!("{}", serde_json::to_string_pretty(&policy1).unwrap());

        assert_eq!(policy0, policy1);
    }

    #[test]
    fn test_policy_set() {
        let json = r#"
{
    "staticPolicies": {
        "policy0": {
            "effect": "permit",
            "principal": {
                "op": "==",
                "entity": { "type": "User", "id": "12UA45" }
            },
            "action": {
                "op": "==",
                "entity": { "type": "Action", "id": "view" }
            },
            "resource": {
                "op": "in",
                "entity": { "type": "Folder", "id": "abc" }
            },
            "conditions": []
        }
    },
    "templates": {
        "template0": {
            "effect": "permit",
            "principal": {
                "op": "==",
                "entity": { "type": "User", "id": "12UA45" }
            },
            "action": {
                "op": "==",
                "entity": { "type": "Action", "id": "view" }
            },
            "resource": {
                "op": "in",
                "slot": "?resource"
            },
            "conditions": []
        }
    },
    "templateLinks": [
        {
            "templateId": "template0",
            "newId": "link_policy0",
            "values": {
                "?resource": {
                    "type": "Folder",
                    "id": "def"
                }
            }
        }
    ]
}
"#;
        let _slot_id: cedar_policy::SlotId = serde_json::from_str(r#""?resource""#).unwrap();

        let policy_set: PolicySet = serde_json::from_str(json).unwrap();
        println!("{}", serde_json::to_string_pretty(&policy_set).unwrap());
        let ps = cedar_policy::PolicySet::from_json_str(json).unwrap();
        let value = ps.to_json().unwrap();
        println!("{}", serde_json::to_string_pretty(&value).unwrap());
    }

    #[test]
    pub fn test_slot() {
        let template = cedar_policy::Template::parse(
            None,
            r#"
permit (
  principal in ?principal,
  action in [Action::"view", Action::"comment"], 
  resource in ?resource
);
        "#,
        )
        .unwrap();

        let json = serde_json::to_string_pretty(&template.to_json().unwrap()).unwrap();

        println!("{}", json);

        let template: Template = serde_json::from_str(&json).unwrap();
        println!("{}", serde_json::to_string_pretty(&template).unwrap());
    }
}
