# Cedrus Cedar

Core library for Cedar Policy serialization and type bindings.

## Overview

`cedrus-cedar` provides Rust types and serialization/deserialization for Cedar Policy language constructs. It bridges between:

- **JSON format**: Human-readable Cedar policy representation
- **Protobuf format**: Efficient binary serialization for storage/transmission
- **Cedar Policy types**: Native Cedar policy engine types

## Features

- **Type-safe Cedar representations**: Rust structs for entities, policies, schemas, templates
- **JSON serialization**: Convert between Cedar JSON format and Rust types
- **Protobuf serialization**: Binary format for efficient storage
- **Cedar Policy integration**: Convert to/from `cedar-policy` crate types
- **OpenAPI schema support**: Types annotated with `utoipa` for API documentation

## Core Types

### Entity Types

- `EntityUid`: Unique identifier for entities (type + id)
- `Entity`: Complete entity with attributes, parents, and tags
- `EntityAttr`: Entity attribute values (strings, numbers, booleans, sets, records, entity references)

### Schema Types

- `Schema`: Complete Cedar schema definition
- `TypeJson`: Type definitions for entity attributes
- `EntityType`: Entity type definitions with shapes and member relationships
- `Action`: Action definitions with principal/resource constraints

### Policy Types

- `Policy`: Static Cedar policy with effect, principal, action, resource, and conditions
- `Template`: Policy template with slots for principals/resources
- `TemplateLink`: Instantiation of a template with specific values
- `PolicySet`: Collection of policies, templates, and template links

### Expression Types

- `JsonExpr`: Cedar expressions in JSON format
- `Condition`: Policy conditions (when/unless clauses)
- `PrincipalOp`, `ActionOp`, `ResourceOp`: Policy scope operators

## Usage

### Working with Entities

```rust
use cedrus_cedar::{Entity, EntityUid, entity::EntityAttr};
use std::collections::{HashMap, HashSet};

// Create an entity UID
let uid = EntityUid::new("MyApp::User".to_string(), "alice".to_string());

// Create entity attributes
let mut attrs = HashMap::new();
attrs.insert("email".to_string(), EntityAttr::String("alice@example.com".to_string()));
attrs.insert("age".to_string(), EntityAttr::Number(30));

// Create entity
let entity = Entity::new(uid, attrs, HashSet::new());

// Convert to Cedar policy entity
let cedar_entity = entity.to_cedar_entity(None)?;
```

### Working with Schemas

```rust
use cedrus_cedar::Schema;

// Deserialize from JSON
let schema_json = r#"{"MyApp": {"entityTypes": {...}, "actions": {...}}}"#;
let schema: Schema = serde_json::from_str(schema_json)?;

// Convert to Cedar schema
let cedar_schema: cedar_policy::Schema = schema.try_into()?;
```

### Working with Policies

```rust
use cedrus_cedar::{Policy, PolicySet};
use std::collections::HashMap;

// Deserialize policy from JSON
let policy_json = r#"{"effect": "permit", "principal": {...}, ...}"#;
let policy: Policy = serde_json::from_str(policy_json)?;

// Create policy set
let mut policies = HashMap::new();
policies.insert("my-policy".to_string().into(), policy);

let policy_set = PolicySet {
    static_policies: policies,
    templates: HashMap::new(),
    template_links: Vec::new(),
};

// Convert to Cedar policy set
let cedar_policy_set: cedar_policy::PolicySet = policy_set.try_into()?;
```

### Protobuf Serialization

```rust
use cedrus_cedar::{Entity, proto};
use prost::Message;

// Convert to protobuf
let entity: Entity = /* ... */;
let proto_entity: proto::Entity = entity.into();
let bytes = proto_entity.encode_to_vec();

// Convert from protobuf
let decoded = proto::Entity::decode(&bytes[..])?;
let entity: Entity = decoded.into();
```

## Type Conversions

The library provides conversions between three representations:

```text
JSON (serde) ←→ Rust Types ←→ Protobuf (prost)
                     ↕
            Cedar Policy Types
```

### Conversion Traits

- `From<cedar_policy::T>` / `Into<cedar_policy::T>`: Convert to/from Cedar types
- `From<proto::T>` / `Into<proto::T>`: Convert to/from Protobuf types
- `Serialize` / `Deserialize`: JSON serialization via serde

## Build

The library uses a build script to generate Protobuf types from `src/cedar.proto`:

```bash
cargo build
```

Generated code is placed in `$OUT_DIR/cedar.rs` and included via `include!` macro.

## Dependencies

- `cedar-policy`: Official Cedar policy engine
- `prost`: Protobuf serialization
- `serde`: JSON serialization
- `utoipa`: OpenAPI schema generation

## Integration

This library is used by:

- **cedrus-core**: Business logic and authorization engine
- **cedrus-http**: REST API server with OpenAPI documentation

It provides the foundational types for storing, transmitting, and processing Cedar policies across the Cedrus system.
