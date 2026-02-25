use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Json, Request, State},
    http::{self, Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};
use headers::{Authorization, HeaderMapExt, authorization::Bearer};
use serde_json::json;
use uuid::Uuid;

use crate::AppState;

const X_API_KEY: &str = "x-api-key";

#[tracing::instrument(skip(h))]
fn stract_token(h: &http::HeaderMap) -> Option<String> {
    let bearer_o: Option<Authorization<Bearer>> = h.typed_get();
    bearer_o.map(|b| String::from(b.0.token()))
}

pub struct AuthError {
    message: String,
    status_code: StatusCode,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response<Body> {
        let body = Json(json!({
            "error": self.message,
        }));

        (self.status_code, body).into_response()
    }
}

#[tracing::instrument(name = "authorize", skip(state, req, next))]
pub async fn authorize(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response<Body>, AuthError> {
    if let Some(api_key) = req.headers().get(X_API_KEY) {
        let principal = state.cedrus.api_keys.get(api_key.to_str().unwrap());
        let Some(principal) = principal else {
            return Err(AuthError {
                message: "Unauthorized".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        };

        req.extensions_mut().insert(principal.value().clone());
    } else {
        let Some(token) = stract_token(req.headers()) else {
            return Err(AuthError {
                message: "Unauthorized".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        };

        match state.tokens.get_value_or_guard_async(&token).await {
            Ok(entity_uid) => {
                tracing::info!("auth cache hit");
                req.extensions_mut().insert(entity_uid);
            }
            Err(guard) => {
                tracing::info!("auth cache miss");
                let authorizer = state.cedrus.project_authorizers.get(&Uuid::nil());
                let Some(authorizer) = authorizer else {
                    tracing::warn!("no authorizer found for nil project");
                    return Err(AuthError {
                        message: "Unauthorized".to_string(),
                        status_code: StatusCode::UNAUTHORIZED,
                    });
                };

                let authorizer = authorizer.as_ref().unwrap();

                let token_data = match authorizer.jwt.check_auth(&token).await {
                    Ok(token_data) => token_data,
                    Err(err) => {
                        tracing::warn!(error = ?err, "JWT validation failed");
                        return Err(AuthError {
                            message: "Unauthorized".to_string(),
                            status_code: StatusCode::UNAUTHORIZED,
                        });
                    }
                };

                let entity = authorizer.get_entity(token_data.claims).unwrap();
                let cedar_entity: cedar_policy::Entity = entity.to_cedar_entity(None).unwrap();

                let entities = state
                    .cedrus
                    .project_cedar_entities
                    .get(&Uuid::nil())
                    .unwrap();
                entities.insert(entity.uid().clone(), (entity.clone(), cedar_entity));

                req.extensions_mut().insert(entity.uid().clone());

                let _ = guard.insert(entity.uid().clone());
            }
        }
    }

    Ok(next.run(req).await)
}
