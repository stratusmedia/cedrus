use std::{future::Future, pin::Pin, sync::Arc};

use axum::{Router, middleware, routing::get};
use cedrus::{
    AppState, QueryParams,
    routes::{auth, projects},
};
use cedrus_core::{
    Event, Selector,
    cache::cache_factory,
    core::{CedrusConfig, cedrus::Cedrus},
    db::database_factory,
    pubsub::pubsub_factory,
};
use clap::Parser;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::{
    Modify, OpenApi,
    openapi::security::{ApiKey, ApiKeyValue, Http, HttpAuthScheme, SecurityScheme},
    schema,
};
use utoipa_swagger_ui::SwaggerUi;

/// Initializes the OpenTelemetry tracer provider with OTLP gRPC export.
/// The OTLP endpoint defaults to `http://localhost:4317` and can be overridden
/// via the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable.
#[cfg(all(feature = "otlp", feature = "trace"))]
fn init_tracer() -> opentelemetry_sdk::trace::SdkTracerProvider {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create OTLP span exporter");

    opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("cedrus")
                .build(),
        )
        .build()
}

#[cfg(all(feature = "otlp", feature = "logs"))]
fn init_logs() -> opentelemetry_sdk::logs::SdkLoggerProvider {
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create OTLP log exporter");
    opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_simple_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("cedrus")
                .build(),
        )
        .build()
}

#[cfg(all(feature = "otlp", feature = "metrics"))]
fn init_metrics() -> opentelemetry_sdk::metrics::SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create OTLP metric exporter");
    opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("cedrus")
                .build(),
        )
        .build()
}

#[cfg(all(feature = "stdout", feature = "trace"))]
fn init_tracer() -> opentelemetry_sdk::trace::SdkTracerProvider {
    let exporter = opentelemetry_stdout::SpanExporter::default();

    opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("cedrus")
                .build(),
        )
        .build()
}

#[cfg(all(feature = "stdout", feature = "logs"))]
fn init_logs() -> opentelemetry_sdk::logs::SdkLoggerProvider {
    let exporter = opentelemetry_stdout::LogExporter::default();
    opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_simple_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("cedrus")
                .build(),
        )
        .build()
}

#[cfg(all(feature = "stdout", feature = "metrics"))]
fn init_metrics() -> opentelemetry_sdk::metrics::SdkMeterProvider {
    let exporter = opentelemetry_stdout::MetricExporter::default();
    opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("cedrus")
                .build(),
        )
        .build()
}

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
        projects::projects_id_apikeys_get,
        projects::projects_id_apikeys_post,
        projects::projects_id_apikeys_key_put,
        projects::projects_id_apikeys_key_delete,
        projects::projects_id_schema_get,
        projects::projects_id_schema_put,
        projects::projects_id_schema_delete,
        projects::projects_id_schema_cedar_get,
        projects::projects_id_schema_cedar_put,
        projects::projects_id_schema_validate_cedar_post,
        projects::projects_id_schema_validate_json_post,
        projects::projects_id_entities_get,
        projects::projects_id_entities_post,
        projects::projects_id_entities_delete,
        projects::projects_id_policies_get,
        projects::projects_id_policies_post,
        projects::projects_id_policies_delete,
        projects::projects_id_policies_validate_cedar_post,
        projects::projects_id_policies_validate_json_post,
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
    #[arg(short = 'c', long)]
    config: Option<String>,
    #[arg(short = 'u', long)]
    url_config: Option<String>,
}

type SubscribeFn<'a> =
    Box<dyn 'a + Send + Sync + Fn(Event) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>>;

fn subscribe_closure<'a>(state: &'a Cedrus) -> SubscribeFn<'a> {
    let closure = move |msg: Event| -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            state.update(&msg, false).await;
        })
    };
    Box::new(closure)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up W3C Trace Context propagation
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // Initialize the OpenTelemetry tracer provider
    #[cfg(feature = "trace")]
    let tracer_provider = {
        let tracer_provider = init_tracer();
        opentelemetry::global::set_tracer_provider(tracer_provider.clone());
        tracer_provider
    };

    // Initialize the OpenTelemetry logs provider
    #[cfg(feature = "logs")]
    let logs_provider = {
        let logs_provider = init_logs();
        logs_provider
    };

    // Initialize the OpenTelemetry metrics provider
    #[cfg(feature = "metrics")]
    let metrics_provider = {
        let metrics_provider = init_metrics();
        opentelemetry::global::set_meter_provider(metrics_provider.clone());
        metrics_provider
    };

    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))?;

    // Build the tracing subscriber with both fmt (console) and conditionally OpenTelemetry layers
    let registry = tracing_subscriber::registry()
        .with(filter_layer)
        .with(tracing_subscriber::fmt::layer());

    #[cfg(all(feature = "trace", feature = "logs"))]
    {
        use opentelemetry::trace::TracerProvider;
        registry
            .with(tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("cedrus")))
            .with(
                opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
                    &logs_provider,
                ),
            )
            .init();
    }

    #[cfg(all(feature = "trace", not(feature = "logs")))]
    registry
        .with(tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("cedrus")))
        .init();

    #[cfg(all(not(feature = "trace"), feature = "logs"))]
    registry
        .with(layer::OpenTelemetryTracingBridge::new(&logs_provider))
        .init();

    #[cfg(all(not(feature = "trace"), not(feature = "logs")))]
    registry.init();

    let args = Args::parse();

    let config: CedrusConfig = if let Some(config_file_name) = args.config {
        let config_file = std::fs::File::open(&config_file_name)
            .unwrap_or_else(|_| panic!("Failed to open config file: {}", config_file_name));
        serde_json::from_reader(config_file).expect("Failed to parse config file")
    } else if let Some(url_config) = args.url_config {
        let response = reqwest::get(&url_config)
            .await
            .unwrap_or_else(|_| panic!("Failed connect to url: {}", url_config));
        let config_file = response
            .text()
            .await
            .unwrap_or_else(|_| panic!("Failed to get config from url: {}", url_config));
        // Get config json from config_url url
        serde_json::from_str(&config_file).expect("Failed to parse config file")
    } else {
        panic!("Either the config file or the config url argument must be provided");
    };

    let db = database_factory(&config.db).await?;
    let cache = cache_factory(&config.cache).await?;
    let pubsub = pubsub_factory(&config.pubsub).await?;

    let cedrus = Cedrus::new(db, cache, pubsub).await;
    let state = AppState::new(cedrus);
    let shared_state = Arc::new(state);
    let _ = Cedrus::init_project(&shared_state.cedrus, &config).await;
    let _ = Cedrus::init_cache(&shared_state.cedrus).await;
    let _ = Cedrus::load_cache(&shared_state.cedrus).await;

    let shared = shared_state.clone();
    tokio::spawn(async move {
        let ops = [subscribe_closure(&shared.cedrus)];
        let _ = shared.cedrus.pubsub.subscribe(&ops).await;
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

    let addr = if std::env::var("CEDRUS_IPV6").is_ok() {
        format!("[{}]:{}", config.server.host, config.server.port)
    } else {
        format!("{}:{}", config.server.host, config.server.port)
    };

    tracing::info!("Server starting on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    tracing::info!("Server stopped");

    // Gracefully shut down the tracer provider, flushing remaining spans
    #[cfg(feature = "trace")]
    tracer_provider.shutdown()?;
    #[cfg(feature = "logs")]
    logs_provider.shutdown()?;
    #[cfg(feature = "metrics")]
    metrics_provider.shutdown()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    // TODO: Create tests
}
