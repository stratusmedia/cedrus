# Cedrus

![Cedrus Logo](/cedrus.png "Cedrus Logo")

Cedrus is a REST API server for [Cedar Policy](https://www.cedarpolicy.com/) authorization, designed for internal infrastructure. It provides a multi-tenant authorization service inspired by [Amazon Verified Permissions](https://aws.amazon.com/verified-permissions/).

## What is Cedrus?

Cedrus allows you to:
- **Manage authorization policies** using Cedar Policy language
- **Organize policies by project** with isolated storage per project
- **Entity Storage** entities can be stored and modified at any time
- **Evaluate authorization requests** in real-time
- **Integrate with OIDC providers** (Keycloak, AWS Cognito, etc.) for user authentication
- **Scale horizontally** with distributed cache and pub/sub synchronization

## Overview

`cedrus` provides a production-ready HTTP server that exposes Cedrus Core functionality through a RESTful API. It includes:

- **Axum web framework**: High-performance async HTTP server
- **OpenAPI/Swagger UI**: Interactive API documentation
- **Authentication middleware**: JWT bearer tokens and API key support
- **CORS support**: Cross-origin resource sharing
- **Compression**: Response compression for better performance
- **Request tracing**: Built-in logging and observability

## Features

### API Endpoints

The server exposes the following endpoint groups:

- **Projects**: Create, read, update, delete projects
- **Identity Sources**: Configure OIDC/Cognito authentication per project
- **Schemas**: Manage Cedar schemas (JSON and Cedar syntax)
- **Entities**: CRUD operations for entities
- **Policies**: Manage static policies (JSON and Cedar syntax)
- **Templates**: Manage policy templates (JSON and Cedar syntax)
- **Template Links**: Link templates to specific entities
- **Authorization**: Real-time authorization checks (single and batch)

## Architecture

### Modules

- **cedrus-cedar**: Core library for Cedar JSON/Protobuf serialization and Cedar policy bindings
- **cedrus-core**: Business logic including database operations, caching, and authorization engine
- **cedrus-http**: Axum-based HTTP server with OpenAPI documentation

### Key Concepts

- **Project**: Isolated namespace containing schemas, entities, policies, and templates
- **Entity**: Represents principals (users), resources, and groups in your authorization model
- **Policy**: Cedar policy rules that define who can do what
- **Template**: Reusable policy patterns with slots for principals/resources
- **Template Link**: Instantiation of a template with specific values

## Prerequisites

### 1. Database (Required)

Cedrus requires a database to persist policies and entities. Supported options:

**CouchDB** (recommended for local development):
```bash
docker run --name cedrus-couchdb \
  -e COUCHDB_USER=admin \
  -e COUCHDB_PASSWORD=admin \
  -p 5984:5984 \
  -d couchdb
```

**DynamoDB** (recommended for AWS deployments):
- Use AWS DynamoDB service or DynamoDB Local for testing

### 2. Cache (Optional but Recommended)

For production deployments with multiple instances:

**Valkey/Redis**:
```bash
docker run --name cedrus-cache \
  -p 6379:6379 \
  -d valkey/valkey:latest
```

### 3. Authentication Provider (Required)

Cedrus is an **authorization** server and requires an **authentication** provider (OIDC).

**Keycloak** (example):
```bash
docker run --name cedrus-keycloak \
  -p 8080:8080 \
  -e KC_BOOTSTRAP_ADMIN_USERNAME=admin \
  -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
  quay.io/keycloak/keycloak:26.4.2 start-dev
```

After starting Keycloak:
1. Access http://localhost:8080
2. Create a realm (e.g., "myrealm")
3. Create a client (e.g., "myclient")
4. Configure client for OIDC authentication

## Installation

### Build from Source

```bash
# Clone the repository
git clone <repository-url>
cd cedrus

# Build release binary
cargo build --release

# Install binary (optional)
sudo cp target/release/cedrus /usr/local/bin/
```

## Configuration

Create a configuration file (e.g., `cedrus.config.json`):

### Minimal Configuration (CouchDB + OIDC)

```json
{
  "server": {
    "port": 3000,
    "host": "0.0.0.0",
    "apiKey": "YOUR_BASE64_ADMIN_API_KEY"
  },
  "db": {
    "couchDbConfig": {
      "dbName": "cedrus",
      "uri": "http://localhost:5984",
      "username": "admin",
      "password": "admin"
    }
  },
  "identitySource": {
    "principalEntityType": "Cedrus::User",
    "configuration": {
      "openIdConnectConfiguration": {
        "issuer": "http://localhost:8080/realms/myrealm",
        "tokenSelection": {
          "identityTokenOnly": {
            "clientIds": ["myclient"],
            "principalIdClaim": "sub"
          }
        },
        "groupConfiguration": {
          "groupClaim": "groups",
          "groupEntityType": "Cedrus::Group"
        }
      }
    }
  }
}
```

### Full Configuration (DynamoDB + Valkey + AWS Cognito)

```json
{
  "server": {
    "port": 3000,
    "host": "0.0.0.0",
    "apiKey": "YOUR_BASE64_ADMIN_API_KEY"
  },
  "db": {
    "dynamoDbConfig": {
      "tableName": "cedrus-table",
      "region": "us-east-1"
    }
  },
  "cache": {
    "valKeyConfig": {
      "urls": ["redis://localhost:6379"],
      "cluster": false
    }
  },
  "pubsub": {
    "valKeyConfig": {
      "urls": ["redis://localhost:6379/?protocol=resp3"],
      "channelName": "cedrus",
      "cluster": false
    }
  },
  "identitySource": {
    "principalEntityType": "Cedrus::User",
    "configuration": {
      "cognitoUserPoolConfiguration": {
        "userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789:userpool/us-east-1_ABC123",
        "clientIds": ["your-client-id"],
        "groupConfiguration": {
          "groupEntityType": "Cedrus::Group"
        }
      }
    }
  }
}
```

### Configuration Options

#### Server
- `port`: HTTP server port (default: 3000)
- `host`: Bind address (use "0.0.0.0" for all interfaces)
- `apiKey`: Admin API key for Cedrus management (base64 encoded)

Generate a secure API key:
```bash
head -c128 /dev/urandom | base64 --wrap=0
```

#### Database

**CouchDB**:
- `dbName`: Database name
- `uri`: CouchDB server URL
- `username`: Admin username
- `password`: Admin password

**DynamoDB**:
- `tableName`: DynamoDB table name
- `region`: AWS region (optional, uses default AWS config)
- `endpointUrl`: Custom endpoint for DynamoDB Local (optional)

#### Cache (Optional)
- `urls`: List of Valkey/Redis server URLs
- `cluster`: Enable cluster mode (true/false)

#### PubSub (Optional)
- `urls`: List of Valkey/Redis server URLs for pub/sub
- `channelName`: Channel name for cluster synchronization
- `cluster`: Enable cluster mode (true/false)

#### Identity Source

**OpenID Connect**:
```json
{
  "openIdConnectConfiguration": {
    "issuer": "https://your-oidc-provider.com",
    "tokenSelection": {
      "identityTokenOnly": {
        "clientIds": ["client-id"],
        "principalIdClaim": "sub"
      }
    },
    "groupConfiguration": {
      "groupClaim": "groups",
      "groupEntityType": "Cedrus::Group"
    }
  }
}
```

**AWS Cognito**:
```json
{
  "cognitoUserPoolConfiguration": {
    "userPoolArn": "arn:aws:cognito-idp:region:account:userpool/pool-id",
    "clientIds": ["client-id"],
    "groupConfiguration": {
      "groupEntityType": "Cedrus::Group"
    }
  }
}
```

### Example Configuration File

See `config/cedrus-local.config.json` for a complete example.

## Running Cedrus

```bash
# Using the binary
cedrus /path/to/cedrus.config.json

# Or with cargo
cargo run --release -- /path/to/cedrus.config.json
```

The server will start on the configured port (default: http://localhost:3000).

## API Documentation

Once running, access the interactive API documentation:

**Swagger UI**: http://localhost:3000/swagger-ui/

The Swagger UI provides:
- Complete API endpoint documentation
- Request/response schemas
- Interactive testing interface
- Authentication configuration

### Using Swagger UI

1. Open http://localhost:3000/swagger-ui/
2. Click "Authorize" button
3. Enter your API key or JWT token
4. Test endpoints interactively

### Authentication

Cedrus supports two authentication methods:

1. **Bearer Token** (for end users): Use JWT tokens from your OIDC provider
   ```
   Authorization: Bearer <jwt-token>
   ```

2. **API Key** (for service accounts): Use project-specific API keys
   ```
   X-API-KEY: <project-api-key>
   ```

## Quick Start Example

### 1. Create a Project

```bash
curl -X POST http://localhost:3000/v1/projects \
  -H "X-API-KEY: YOUR_ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My Application",
    "owner": {
      "type": "Cedrus::User",
      "id": "user-123"
    }
  }'
```

### 2. Define a Schema

```bash
curl -X PUT http://localhost:3000/v1/projects/{project-id}/schema \
  -H "X-API-KEY: YOUR_PROJECT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "MyApp": {
      "entityTypes": {
        "User": {},
        "Document": {
          "shape": {
            "type": "Record",
            "attributes": {
              "owner": {
                "type": "Entity",
                "name": "User"
              }
            }
          }
        }
      },
      "actions": {
        "viewDocument": {
          "appliesTo": {
            "principalTypes": ["User"],
            "resourceTypes": ["Document"]
          }
        }
      }
    }
  }'
```

### 3. Add Entities

```bash
curl -X POST http://localhost:3000/v1/projects/{project-id}/entities \
  -H "X-API-KEY: YOUR_PROJECT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "uid": {"type": "MyApp::User", "id": "alice"},
      "attrs": {},
      "parents": []
    },
    {
      "uid": {"type": "MyApp::Document", "id": "doc1"},
      "attrs": {
        "owner": {"type": "MyApp::User", "id": "alice"}
      },
      "parents": []
    }
  ]'
```

### 4. Create a Policy

```bash
curl -X POST http://localhost:3000/v1/projects/{project-id}/policies \
  -H "X-API-KEY: YOUR_PROJECT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "owner-can-view": {
      "effect": "permit",
      "principal": {"op": "All"},
      "action": {
        "op": "==",
        "entity": {"type": "MyApp::Action", "id": "viewDocument"}
      },
      "resource": {"op": "All"},
      "conditions": [{
        "kind": "when",
        "body": {
          "==": {
            "left": {".": {"left": {"Var": "resource"}, "attr": "owner"}},
            "right": {"Var": "principal"}
          }
        }
      }]
    }
  }'
```

### 5. Check Authorization

```bash
curl -X POST http://localhost:3000/v1/projects/{project-id}/is-authorized \
  -H "X-API-KEY: YOUR_PROJECT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "principal": {"type": "MyApp::User", "id": "alice"},
    "action": {"type": "MyApp::Action", "id": "viewDocument"},
    "resource": {"type": "MyApp::Document", "id": "doc1"}
  }'
```

Response:
```json
{
  "decision": "Allow",
  "diagnostics": {
    "reason": ["owner-can-view"],
    "errors": []
  }
}
```

## Server Architecture

```text
┌─────────────────────────────────────────────────────────┐
│                   Cedrus HTTP Server                    │
├─────────────────────────────────────────────────────────┤
│  Axum Router                                            │
│  ├─ Authentication Middleware                           │
│  ├─ CORS Layer                                          │
│  ├─ Compression Layer                                   │
│  └─ Tracing Layer                                       │
├─────────────────────────────────────────────────────────┤
│  REST API Handlers                                      │
│  ├─ Projects                                            │
│  ├─ Schemas                                             │
│  ├─ Entities                                            │
│  ├─ Policies                                            │
│  └─ Authorization                                       │
├─────────────────────────────────────────────────────────┤
│  Cedrus Core (Business Logic)                           │
└─────────────────────────────────────────────────────────┘
```

## Middleware

### Authentication Middleware

Applied to all `/v1/projects/*` routes:

1. Checks for `X-API-KEY` header
   - If present, validates against project API keys
   - Maps to project owner entity

2. If no API key, checks for `Authorization: Bearer` header
   - Validates JWT token with configured OIDC provider
   - Extracts user identity from token claims
   - Maps to `Cedrus::User` entity

3. Injects principal `EntityUid` into request extensions

### CORS Middleware

Allows cross-origin requests:
- All origins accepted
- All methods allowed
- All headers allowed

### Compression Middleware

Automatically compresses responses for better performance.

### Tracing Middleware

Logs all HTTP requests with:
- Request method and path
- Response status code
- Request duration

## Development

### Running Tests

```bash
cargo test
```

### Building

```bash
cargo build --release
```

The binary will be available at `target/release/cedrus`.

## Multi-Tenant Architecture

Cedrus implements a multi-tenant model:

- **Admin Project** (UUID: 00000000-0000-0000-0000-000000000000): Controls access to Cedrus itself
- **User Projects**: Each project is isolated with its own schemas, entities, and policies
- **Project API Keys**: Each project has a unique API key for service-to-service authentication
- **Role-Based Access**: Users can have different roles across projects

## Deployment

### Single Instance (Development)

Use DashMap cache (in-memory, no external dependencies):
```json
{
  "cache": {"dashMapConfig": {}},
  "pubsub": {"dummyConfig": {}}
}
```

### Multi-Instance (Production)

Use Valkey/Redis for distributed cache and pub/sub:
```json
{
  "cache": {
    "valKeyConfig": {
      "urls": ["redis://cache-server:6379"],
      "cluster": true
    }
  },
  "pubsub": {
    "valKeyConfig": {
      "urls": ["redis://cache-server:6379/?protocol=resp3"],
      "channelName": "cedrus",
      "cluster": true
    }
  }
}
```

## Troubleshooting

### Connection Issues

- Verify database is running and accessible
- Check firewall rules for database and cache ports
- Ensure OIDC issuer URL is reachable

### Authentication Failures

- Verify JWT token is valid and not expired
- Check OIDC client configuration matches Cedrus config
- Ensure `principalIdClaim` matches the claim in your JWT

### Authorization Denials

- Review policies using the Swagger UI
- Check entity relationships and attributes
- Use the Cedar policy playground to test policy logic

### Server Won't Start

- Check configuration file syntax (valid JSON)
- Verify database is accessible
- Ensure port is not already in use
- Check file permissions on config file

### Authentication Failures

- Verify OIDC issuer URL is correct and reachable
- Check client IDs match your OIDC configuration
- Ensure JWT tokens are not expired
- Verify API keys are correct

### Authorization Denials

- Check policies are correctly defined
- Verify entities exist and have correct attributes
- Review entity parent relationships
- Use Swagger UI to inspect current policies

## Dependencies

- `axum`: Web framework
- `cedrus-core`: Business logic
- `cedrus-cedar`: Type definitions
- `tower-http`: HTTP middleware
- `utoipa`: OpenAPI documentation
- `utoipa-swagger-ui`: Swagger UI integration
- `jwt-authorizer`: JWT validation
- `tokio`: Async runtime

## Integration

This server can be:

- **Deployed standalone**: As a microservice
- **Embedded in applications**: Using cedrus-core directly
- **Used as a sidecar**: For authorization in Kubernetes

## License

Apache-2.0

## Resources

- [Cedar Policy Language](https://www.cedarpolicy.com/)
- [Cedar Policy Documentation](https://docs.cedarpolicy.com/)
- [Amazon Verified Permissions](https://aws.amazon.com/verified-permissions/)

## Copyright

Stratus Media Solutions SL. All Rights Reserved.