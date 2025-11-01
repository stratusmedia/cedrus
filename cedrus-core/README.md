# Cedrus Core

Business logic and authorization engine for Cedrus.

## Overview

`cedrus-core` is the core library that implements the Cedar Policy authorization engine with multi-tenant support. It provides:

- **Multi-project isolation**: Each project has its own schemas, entities, and policies
- **Authorization engine**: Real-time policy evaluation using Cedar
- **Caching layer**: Fast in-memory access with distributed cache support
- **Database abstraction**: Pluggable storage backends (DynamoDB, CouchDB)
- **Cluster synchronization**: Pub/sub for multi-instance deployments
- **Thread-safe operations**: Concurrent access using DashMap

## Architecture

### Core Components

```text
┌─────────────────────────────────────────────────────────┐
│                    Cedrus Core                          │
├─────────────────────────────────────────────────────────┤
│  Authorization Engine (Cedar Policy Evaluation)         │
├─────────────────────────────────────────────────────────┤
│  Cache Layer (DashMap / Valkey)                         │
├─────────────────────────────────────────────────────────┤
│  Database Layer (DynamoDB / CouchDB)                    │
├─────────────────────────────────────────────────────────┤
│  PubSub Layer (Valkey / Dummy)                          │
└─────────────────────────────────────────────────────────┘
```

### Storage Traits

The library defines three main traits for pluggable backends:

#### Database Trait

Persists Cedar policies, entities, and schemas:

- **Implementations**: DynamoDB, CouchDB
- **Operations**: CRUD for projects, schemas, entities, policies, templates, template links
- **Query support**: Filtering and pagination

#### Cache Trait

Provides fast in-memory access to authorization data:

- **Implementations**: Valkey (Redis), DashMap (in-memory)
- **Purpose**: Reduce database load and improve authorization performance
- **Scope**: Per-project caching of schemas, entities, and policy sets

#### PubSub Trait

Enables cluster synchronization across multiple Cedrus instances:

- **Implementations**: Valkey (Redis), Dummy (single instance)
- **Purpose**: Notify other instances of data changes
- **Events**: Project updates, entity changes, policy modifications

## Multi-Tenant Model

### Admin Project

Cedrus uses Cedar policies to control access to itself:

- **Project ID**: `00000000-0000-0000-0000-000000000000` (nil UUID)
- **Purpose**: Manages users and their access to projects
- **Entities**: 
  - `Cedrus::User`: Represents users
  - `Cedrus::Project`: Represents tenant projects
  - `Cedrus::Group`: User groups (e.g., "Admins")

### User Projects

Each project is an isolated tenant with:

- **Unique ID**: UUID v7
- **API Key**: Base64-encoded key for service authentication
- **Owner**: User who created the project
- **Roles**: Template-based role assignments
- **Isolated data**: Own schemas, entities, and policies

### Authorization Flow

1. **Authentication**: User authenticates via OIDC (JWT token) or API key
2. **Principal resolution**: Map token/key to `Cedrus::User` entity
3. **Admin check**: Verify if user is in "Admins" group
4. **Project authorization**: Evaluate Cedar policies in Admin Project
5. **Resource authorization**: Evaluate Cedar policies in target project

## Key Features

### Real-Time Authorization

```rust
// Check if user can perform action on resource
let response = cedrus.is_authorized(
    &project_id,
    principal,  // EntityUid
    action,     // EntityUid
    resource,   // EntityUid
    context,    // Optional<Context>
)?;

match response.decision {
    Decision::Allow => { /* authorized */ },
    Decision::Deny => { /* forbidden */ },
}
```

### Batch Authorization

```rust
// Check multiple requests at once
let requests = vec![
    Request { principal, action, resource, context },
    // ... more requests
];

let responses = cedrus.is_authorized_batch(&project_id, requests)?;
```

### Entity Hierarchy

Entities support parent relationships for group membership:

```rust
let entity = Entity::new(
    EntityUid::new("MyApp::User".to_string(), "alice".to_string()),
    attrs,
    parents,  // HashSet<EntityUid> - e.g., groups
);
```

### Policy Templates

Reusable policy patterns with slots:

```rust
// Template with ?principal and ?resource slots
let template = Template { /* ... */ };

// Link template to specific entities
let link = TemplateLink {
    template_id: "AdminRole".into(),
    new_id: "alice-admin".into(),
    values: HashMap::from([
        (SlotId::Principal, EntityValue::EntityUid(alice)),
        (SlotId::Resource, EntityValue::EntityUid(project)),
    ]),
};
```

## Initialization

### Single Instance Setup

```rust
use cedrus_core::{
    core::cedrus::Cedrus,
    db::database_factory,
    cache::cache_factory,
    pubsub::pubsub_factory,
};

// Create backends
let db = database_factory(&config.db).await;
let cache = cache_factory(&config.cache).await;
let pubsub = pubsub_factory(&config.pubsub).await;

// Initialize Cedrus
let cedrus = Arc::new(Cedrus::new(db, cache, pubsub).await);

// Initialize admin project (first run only)
Cedrus::init_project(&cedrus, &config).await?;

// Load data from database to cache
Cedrus::init_cache(&cedrus).await?;

// Load cache data to memory
Cedrus::load_cache(&cedrus).await?;
```

### Cluster Setup

For multi-instance deployments:

1. **One instance** calls `init_project()` and `init_cache()`
2. **All instances** call `load_cache()` to load from shared cache
3. **All instances** subscribe to pub/sub for synchronization

```rust
// Subscribe to cluster events
let ops = [subscribe_closure(&cedrus)];
tokio::spawn(async move {
    cedrus.pubsub.subscribe(&ops).await;
});
```

## Configuration

### Database Options

**CouchDB**:
```rust
DbConfig::CouchDbConfig(CouchDbConfig {
    uri: "http://localhost:5984".to_string(),
    username: "admin".to_string(),
    password: "password".to_string(),
    db_name: "cedrus".to_string(),
})
```

**DynamoDB**:
```rust
DbConfig::DynamoDbConfig(DynamoDBConfig {
    table_name: "cedrus-table".to_string(),
    region: Some("us-east-1".to_string()),
    endpoint_url: None,
})
```

### Cache Options

**In-Memory (Development)**:
```rust
CacheConfig::DashMapConfig(DashMapCacheConfig {})
```

**Distributed (Production)**:
```rust
CacheConfig::ValKeyConfig(ValKeyCacheConfig {
    urls: vec!["redis://localhost:6379".to_string()],
    cluster: false,
    ..Default::default()
})
```

### PubSub Options

**Single Instance**:
```rust
PubSubConfig::DummyConfig(DummyPubSubConfig {})
```

**Cluster**:
```rust
PubSubConfig::ValKeyConfig(ValKeyPubSubConfig {
    urls: vec!["redis://localhost:6379/?protocol=resp3".to_string()],
    channel_name: "cedrus".to_string(),
    cluster: false,
    ..Default::default()
})
```

## Event System

Cedrus uses an event-driven architecture for cache synchronization:

### Event Types

- `ProjectCreate`: New project created
- `ProjectUpdate`: Project metadata updated
- `ProjectRemove`: Project deleted
- `ProjectPutSchema`: Schema updated
- `ProjectAddEntities`: Entities added
- `ProjectRemoveEntities`: Entities deleted
- `ProjectAddPolicies`: Policies added
- `ProjectRemovePolicies`: Policies deleted
- `ProjectAddTemplates`: Templates added
- `ProjectAddTemplateLinks`: Template links added

### Event Flow

1. **Local change**: Instance modifies data in database and cache
2. **Publish event**: Instance publishes event to pub/sub channel
3. **Receive event**: Other instances receive event
4. **Update cache**: Other instances update their local cache

## Performance Considerations

### Caching Strategy

- **Schemas**: Cached per project (rarely change)
- **Entities**: Cached per project (moderate changes)
- **Policies**: Cached per project (moderate changes)
- **Authorization**: Uses in-memory Cedar engine with cached data

### Entity Resolution

The authorization engine automatically resolves entity hierarchies:

```rust
// Fetches entity and all parent entities recursively
let entities = cedrus.get_cedar_entities(&project_id, &entity_uids)?;
```

### Batch Operations

Use batch operations for better performance:

- `project_entities_add()`: Add multiple entities at once
- `project_policies_add()`: Add multiple policies at once
- `is_authorized_batch()`: Evaluate multiple requests at once

## Integration

This library is used by:

- **cedrus-http**: REST API server that exposes Cedrus functionality
- **Custom applications**: Can embed Cedrus Core directly

## Dependencies

- `cedrus-cedar`: Type definitions and serialization
- `cedar-policy`: Official Cedar policy engine
- `dashmap`: Concurrent hash map
- `aws-sdk-dynamodb`: DynamoDB client
- `couch_rs`: CouchDB client
- `redis`: Valkey/Redis client
- `jwt-authorizer`: JWT validation
