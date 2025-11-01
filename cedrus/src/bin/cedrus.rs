use std::{future::Future, pin::Pin, sync::Arc};

use axum::{Router, middleware, routing::get};
use cedrus_core::{
    Event, Selector,
    cache::cache_factory,
    core::{CedrusConfig, cedrus::Cedrus},
    db::database_factory,
    pubsub::pubsub_factory,
};
use cedrus::{
    QueryParams,
    routes::{auth, projects},
};
use clap::Parser;
use tracing_subscriber::prelude::*;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use utoipa::{
    Modify, OpenApi,
    openapi::security::{ApiKey, ApiKeyValue, Http, HttpAuthScheme, SecurityScheme},
    schema,
};
use utoipa_swagger_ui::SwaggerUi;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components: &mut utoipa::openapi::Components = openapi.components.as_mut().unwrap(); // we can unwrap safely since there already is components registered.

        let query_params = schema!(
            #[inline]
            QueryParams
        );
        components
            .schemas
            .insert("QueryParams".to_string(), query_params);
        let selector = schema!(
            #[inline]
            Selector
        );
        components.schemas.insert("Selector".to_string(), selector);
        //let sort = schema!(#[inline] Sort);
        //components.schemas.insert("Sort".to_string(), sort);
        //let sort_order = schema!(#[inline] SortOrder);
        //components.schemas.insert("SortOrder".to_string(), sort_order);

        let mut http = Http::new(HttpAuthScheme::Bearer);
        http.bearer_format = Some("JWT".to_owned());
        components.add_security_scheme("bearerAuth", SecurityScheme::Http(http));

        let api_key = ApiKey::Header(ApiKeyValue::with_description(
            "X-API-KEY",
            "project api key",
        ));
        components.add_security_scheme("apiKey", SecurityScheme::ApiKey(api_key));
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Cedrus Server API",
        description = "Cedrus Server is a Cedar Policy based authorization system for data access control.",
        version = "0.1.0",
        license(
            name = "Apache License",
            identifier = "Apache-2.0",
        ),
    ),
    modifiers(&SecurityAddon),
    paths(
        projects::projects_get,
        projects::projects_post,
        projects::projects_id_get,
        projects::projects_id_put,
        projects::projects_id_delete,
        projects::projects_id_identity_source_get,
        projects::projects_id_identity_source_put,
        projects::projects_id_identity_source_delete,
        projects::projects_id_schema_get,
        projects::projects_id_schema_put,
        projects::projects_id_schema_delete,
        projects::projects_id_schema_cedar_get,
        projects::projects_id_entities_get,
        projects::projects_id_entities_post,
        projects::projects_id_entities_delete,
        projects::projects_id_policies_get,
        projects::projects_id_policies_post,
        projects::projects_id_policies_delete,
        projects::projects_id_policies_policy_id_cedar_get,
        projects::projects_id_policies_policy_id_cedar_put,
        projects::projects_id_templates_get,
        projects::projects_id_templates_post,
        projects::projects_id_templates_delete,
        projects::projects_id_templates_template_id_cedar_get,
        projects::projects_id_templates_template_id_cedar_put,
        projects::projects_id_template_links_get,
        projects::projects_id_template_links_post,
        projects::projects_id_template_links_delete,
        projects::projects_id_template_links_policy_id_cedar_get,
        projects::projects_id_template_links_policy_id_cedar_put,
        projects::projects_id_policy_set_get,
        projects::projects_id_policy_set_cedar_get,
        projects::projects_id_is_authorized_post,
        projects::projects_id_is_authorized_batch_post,
    ),
    tags(
        (name = "Cedrus", description = "Cedar Policy Server")
    )
)]
struct ApiDoc;
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    config: Option<String>,
}

fn subscribe_closure<'a>(
    state: &'a Arc<Cedrus>,
) -> Box<dyn 'a + Send + Sync + Fn(Event) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>> {
    let closure = move |msg: Event| -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            state.update(&msg, false).await;
        })
    };
    Box::new(closure)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let config_file_name = args.config.expect("config file is required");
    let config_file = std::fs::File::open(config_file_name).unwrap();
    let config: CedrusConfig = serde_json::from_reader(config_file).unwrap();

    let db = database_factory(&config.db).await;
    let cache = cache_factory(&config.cache).await;
    let pubsub = pubsub_factory(&config.pubsub).await;

    let state = Cedrus::new(db, cache, pubsub).await;
    let shared_state = Arc::new(state);
    let _ = Cedrus::init_project(&shared_state, &config).await.unwrap();
    let _ = Cedrus::init_cache(&shared_state).await.unwrap();
    let _ = Cedrus::load_cache(&shared_state).await.unwrap();

    let shared = shared_state.clone();
    tokio::spawn(async move {
        let ops = [subscribe_closure(&shared)];
        shared.pubsub.subscribe(&ops).await;
    });

    let cors = CorsLayer::new()
        .allow_headers(Any)
        .allow_methods(Any)
        .allow_origin(Any);

    let app = Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/", get(|| async { "Hello, World!" }))
        .layer(cors.clone())
        .layer(CompressionLayer::new())
        .nest(
            "/v1/projects",
            projects::routes().layer(middleware::from_fn_with_state(
                shared_state.clone(),
                auth::authorize,
            )),
        )
        .layer(cors.clone())
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(shared_state);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

#[cfg(test)]
mod tests {
    use cedar_policy::{Authorizer, Context, EntityUid, Schema};
    use cedrus_core::core::CedrusConfig;
    use std::collections::HashMap;
    //use utoipa::openapi::schema; // Body collect
    use std::error::Error;
    use std::fs::{self, File};

    #[tokio::test]
    async fn test_cedrus_cedar_json() {
        let schema_file_name = format!("./config/cedrus.cedarschema.json");
        let policy_set_file_name = format!("./config/cedrus.cedar.json");
        let entities_file_name = format!("./config/cedrus.cedarentities.json");

        let schema_file = File::open(schema_file_name).expect("Unable to open file");
        let policy_set_file = File::open(policy_set_file_name).expect("Unable to open file");
        let entities_file = File::open(entities_file_name).expect("Unable to open file");

        let cedar_schema: cedar_policy::Schema =
            cedar_policy::Schema::from_json_file(schema_file).expect("Unable to parse file schema");
        let _policy_set: cedar_policy::PolicySet =
            cedar_policy::PolicySet::from_json_file(policy_set_file)
                .expect("Unable to parse file policies");
        let cedar_entities: cedar_policy::Entities =
            cedar_policy::Entities::from_json_file(entities_file, Some(&cedar_schema))
                .expect("Unable to parse file entities");

        for entity in cedar_entities {
            println!("{:?}", entity);
        }
    }

    #[tokio::test]
    async fn test_cedrus_config_file() {
        let config_file_name = format!("./config/cedrus.config.json");
        let config_file = File::open(config_file_name).expect("Unable to open file");
        let _cedrus_config: CedrusConfig =
            serde_json::from_reader(config_file).expect("Unable to parse file config");
    }

    #[tokio::test]
    async fn test_cedar_schema() {
        let cedar_schema_json =
            fs::read_to_string("./test/tinytodo/cedarschema.json").expect("Unable to read file");

        let schema: cedrus_cedar::Schema = serde_json::from_str(&cedar_schema_json).unwrap();
        println!("SCHEMA: {:?}", schema);
        let str = serde_json::to_string(&schema).unwrap();
        println!("SCHEMA: {}", str);

        match cedar_policy::Schema::from_json_str(&cedar_schema_json) {
            Ok(schema) => schema,
            Err(e) => panic!("Error: {}", e),
        };
    }

    #[tokio::test]
    async fn test_policies() {
        /*
        let policy_src = r#"
        permit (
            principal,
            action in
                [Action::"UpdateList",
                 Action::"CreateTask",
                 Action::"UpdateTask",
                 Action::"DeleteTask"],
            resource
        )
        when { principal in resource.editors };"#;
                let p = cedar_policy::Policy::parse(None, policy_src).unwrap();
                println!("POLICY: {}", serde_json::to_string(&p.to_json().unwrap()).unwrap());
        */

        let policies_json =
            fs::read_to_string("./test/tinytodo/policies.cedar.json").expect("Unable to read file");
        let value: serde_json::Value = serde_json::from_str(&policies_json).unwrap();

        /*
        let policies = value
            .as_array()
            .unwrap()
            .iter()
            .map(|v| cedar_policy::Policy::from_json(None, v.clone()).unwrap())
            .collect::<Vec<cedar_policy::Policy>>();
        */

        let _json_policies = value
            .as_array()
            .unwrap()
            .iter()
            .map(
                |v| match serde_json::from_str(&serde_json::to_string_pretty(v).unwrap()) {
                    Ok(policy) => policy,
                    Err(e) => {
                        println!("Error1: {}", serde_json::to_string_pretty(v).unwrap());
                        panic!("Error: {:?}", e)
                    }
                },
            )
            .collect::<Vec<cedrus_cedar::Policy>>();

        let policies = value
            .as_array()
            .unwrap()
            .iter()
            .map(|v| match cedar_policy::Policy::from_json(None, v.clone()) {
                Ok(policy) => policy,
                Err(e) => {
                    println!("Error1: {}", serde_json::to_string(v).unwrap());
                    panic!("Error: {:?}", e.source())
                }
            })
            .collect::<Vec<cedar_policy::Policy>>();

        let mut policy_set = cedar_policy::PolicySet::new();
        for policy in policies {
            println!("POLICY: {}", policy.to_string());
            let _ = policy_set.add(policy);
        }

        /*
        let policy_set = match cedar_policy::PolicySet::from_policies(policies) {
            Ok(policy_set) => policy_set,
            Err(e) => panic!("Error: {}", e),
        };
        println!("POLICY SET: {}", serde_json::to_string(&policy_set.to_json().unwrap()).unwrap());
        */
    }

    #[tokio::test]
    async fn test_entities() {
        let schema_json =
            fs::read_to_string("./test/tinytodo/schema.json").expect("Unable to read file");
        let schema = match Schema::from_json_str(&schema_json) {
            Ok(schema) => schema,
            Err(e) => panic!("Error: {}", e),
        };

        let file_json =
            fs::read_to_string("./test/tinytodo/entities.json").expect("Unable to read file");
        let value: serde_json::Value = serde_json::from_str(&file_json).unwrap();

        let entities_list = value
            .as_array()
            .unwrap()
            .iter()
            .map(|value| {
                match cedar_policy::Entity::from_json_value(value.to_owned(), Some(&schema)) {
                    Ok(entity) => entity,
                    Err(e) => {
                        println!("Error1: {}", serde_json::to_string(value).unwrap());
                        println!("Error2: {:?}", e);
                        panic!("Error3: {:?}", e.source().unwrap())
                    }
                }
            })
            .collect::<Vec<cedar_policy::Entity>>();

        let _entities = match cedar_policy::Entities::from_entities(entities_list, Some(&schema)) {
            Ok(entities) => entities,
            Err(e) => panic!("Error: {}", e),
        };

        /*         let write = NamedTempFile::new().unwrap();
               let mut read = write.reopen().unwrap();

               let _ = entities.write_to_json(write).unwrap();

               let mut buf = String::new();
               read.read_to_string(&mut buf).unwrap();
        */ // println!("ENTITIES: {}", buf);
    }

    #[tokio::test]
    async fn test_authorized() {
        let schema_json =
            fs::read_to_string("./test/tinytodo/schema.json").expect("Unable to read file");
        let schema = match Schema::from_json_str(&schema_json) {
            Ok(schema) => schema,
            Err(e) => panic!("Error: {}", e),
        };

        let file_json =
            fs::read_to_string("./test/tinytodo/entities.json").expect("Unable to read file");
        let value: serde_json::Value = serde_json::from_str(&file_json).unwrap();

        let entities_list = value
            .as_array()
            .unwrap()
            .iter()
            .map(|value| {
                match cedar_policy::Entity::from_json_value(value.to_owned(), Some(&schema)) {
                    Ok(entity) => entity,
                    Err(e) => {
                        println!("Error1: {}", serde_json::to_string(value).unwrap());
                        println!("Error2: {:?}", e);
                        panic!("Error3: {:?}", e.source().unwrap())
                    }
                }
            })
            .collect::<Vec<cedar_policy::Entity>>();

        let entities = match cedar_policy::Entities::from_entities(entities_list, Some(&schema)) {
            Ok(entities) => entities,
            Err(e) => panic!("Error: {}", e),
        };

        let policies_json =
            fs::read_to_string("./test/tinytodo/policies.json").expect("Unable to read file");
        let value: serde_json::Value = serde_json::from_str(&policies_json).unwrap();

        let policies = value
            .as_array()
            .unwrap()
            .iter()
            .map(|v| cedar_policy::Policy::from_json(None, v.clone()).unwrap())
            .collect::<Vec<cedar_policy::Policy>>();

        // let policy_set = cedar_policy::PolicySet::from_policies(policies).unwrap();

        let mut policy_set = cedar_policy::PolicySet::new();
        for policy in policies {
            println!("POLICY: {}", policy.to_string());
            let _ = policy_set.add(policy);
        }

        let json_data = serde_json::json!({ "type": "TinyTodo::User", "id": "emina" });
        let principal = EntityUid::from_json(json_data).unwrap();

        let json_data = serde_json::json!({ "type": "TinyTodo::Action", "id": "GetLists" });
        let action = EntityUid::from_json(json_data).unwrap();

        let json_data = serde_json::json!({ "type": "TinyTodo::Application", "id": "TinyTodo" });
        let resource = EntityUid::from_json(json_data).unwrap();

        let authorizer = Authorizer::new();
        let request = cedar_policy::Request::new(
            principal,
            action,
            resource,
            Context::empty(),
            Some(&schema),
        )
        .unwrap();

        let answer = authorizer.is_authorized(&request, &policy_set, &entities);

        println!("{:?}", answer);
    }

    #[tokio::test]
    async fn test_cedrus() {
        let file = File::open("./cedar/cedrus.cedarschema").expect("Unable to open file");
        let (schema, _) = cedar_policy::Schema::from_cedarschema_file(file).unwrap();

        let file = File::open("./cedar/cedrus.cedarentities.json").expect("Unable to open file");
        let entities = cedar_policy::Entities::from_json_file(file, Some(&schema)).unwrap();

        let file = File::open("./cedar/cedrus.cedar.json").expect("Unable to open file");
        let mut policy_set = cedar_policy::PolicySet::from_json_file(file).unwrap();

        let template_id = cedar_policy::PolicyId::new("AdminStoresRole");
        let policy_id = cedar_policy::PolicyId::new("AdminStoresRole_00001_00001");
        let vals: HashMap<cedar_policy::SlotId, cedar_policy::EntityUid> = HashMap::from([
            (
                cedar_policy::SlotId::principal(),
                EntityUid::from_json(serde_json::json!({ "type": "Cedrus::User", "id": "00001" }))
                    .unwrap(),
            ),
            (
                cedar_policy::SlotId::resource(),
                EntityUid::from_json(serde_json::json!({ "type": "Cedrus::Store", "id": "00001" }))
                    .unwrap(),
            ),
        ]);
        policy_set.link(template_id, policy_id, vals).unwrap();

        let template_id = cedar_policy::PolicyId::new("AdminSchemaStoreRole");
        let policy_id = cedar_policy::PolicyId::new("AdminSchemaStoreRole_00001_00002");
        let vals: HashMap<cedar_policy::SlotId, cedar_policy::EntityUid> = HashMap::from([
            (
                cedar_policy::SlotId::principal(),
                EntityUid::from_json(serde_json::json!({ "type": "Cedrus::User", "id": "00001" }))
                    .unwrap(),
            ),
            (
                cedar_policy::SlotId::resource(),
                EntityUid::from_json(serde_json::json!({ "type": "Cedrus::Store", "id": "00002" }))
                    .unwrap(),
            ),
        ]);
        policy_set.link(template_id, policy_id, vals).unwrap();

        let json_data = serde_json::json!({ "type": "Cedrus::User", "id": "00001" });
        let principal = EntityUid::from_json(json_data).unwrap();

        let json_data = serde_json::json!({ "type": "Cedrus::Action", "id": "getEntityStore" });
        let action = EntityUid::from_json(json_data).unwrap();

        let json_data = serde_json::json!({ "type": "Cedrus::EntityStore", "id": "00001" });
        let resource = EntityUid::from_json(json_data).unwrap();

        let authorizer = Authorizer::new();
        let request = cedar_policy::Request::new(
            principal,
            action,
            resource,
            Context::empty(),
            Some(&schema),
        )
        .unwrap();

        let answer = authorizer.is_authorized(&request, &policy_set, &entities);

        println!("{:?}", answer);
    }
}
