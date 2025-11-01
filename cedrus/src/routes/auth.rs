use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Json, Request, State},
    http::{Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};
use cedrus_cedar::EntityUid;
use serde_json::json;
use uuid::Uuid;

use cedrus_core::core::cedrus::Cedrus;

const X_API_KEY: &str = "x-api-key";

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

pub async fn authorize(
    State(state): State<Arc<Cedrus>>,
    mut req: Request,
    next: Next,
) -> Result<Response<Body>, AuthError> {
    let header = req.headers().get(X_API_KEY);
    if let Some(api_key) = header {
        let principal = state.api_keys.get(api_key.to_str().unwrap());
        let Some(principal) = principal else {
            return Err(AuthError {
                message: "Unauthorized".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        };

        req.extensions_mut().insert(principal.value().clone());
    } else {
        let authorizer = state.project_authorizers.get(&Uuid::nil());
        let Some(authorizer) = authorizer else {
            return Err(AuthError {
                message: "Unauthorized".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        };

        let authorizer = authorizer.as_ref().unwrap();

        let Some(token) = authorizer.jwt.extract_token(req.headers()) else {
            return Err(AuthError {
                message: "Unauthorized".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        };

        authorizer.jwt.check_auth(&token).await.unwrap();

        let Ok(token_data) = authorizer.jwt.check_auth(&token).await else {
            return Err(AuthError {
                message: "Unauthorized".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        };

        let sub = match token_data.claims.as_object() {
            Some(obj) => match obj.get(&authorizer.id_claim) {
                Some(sub) => match sub.as_str() {
                    Some(sub) => Some(sub),
                    None => None,
                },
                None => None,
            },
            None => None,
        };

        let Some(sub) = sub else {
            return Err(AuthError {
                message: "Unauthorized".to_string(),
                status_code: StatusCode::UNAUTHORIZED,
            });
        };

        let id = format!("{}|{sub}", authorizer.prefix);
        let principal = EntityUid::new(
            authorizer.identity_source.principal_entity_type.to_string(),
            id,
        );

        req.extensions_mut().insert(principal);
    }

    Ok(next.run(req).await)
}
