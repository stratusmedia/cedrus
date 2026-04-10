use std::{error::Error, fmt, sync::Arc};

use axum::{
    body::Body,
    extract::{Request, State},
    http::{self, Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};
use headers::{Authorization, HeaderMapExt, authorization::Bearer};
use uuid::Uuid;

use crate::{AppState, AuthData};

const X_API_KEY: &str = "x-api-key";

#[tracing::instrument(skip(h))]
fn stract_token(h: &http::HeaderMap) -> Option<String> {
    let bearer_o: Option<Authorization<Bearer>> = h.typed_get();
    bearer_o.map(|b| String::from(b.0.token()))
}

#[derive(Debug)]
pub enum AuthError {
    Unauthorized,
}

impl Error for AuthError {}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AuthError::Unauthorized => write!(f, "Unauthorized"),
        }
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response<Body> {
        match self {
            AuthError::Unauthorized => (StatusCode::UNAUTHORIZED, Body::empty()).into_response(),
        }
    }
}

#[tracing::instrument(name = "authorize", skip(state, req, next))]
pub async fn authorize(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response<Body>, AuthError> {
    if let Some(header_api_key) = req.headers().get(X_API_KEY) {
        let api_key = header_api_key
            .to_str()
            .map_err(|_| AuthError::Unauthorized)?;
        let principal = state
            .cedrus
            .api_keys
            .get(api_key)
            .ok_or(AuthError::Unauthorized)?;

        req.extensions_mut().insert(principal.value().clone());
    } else {
        let Some(token) = stract_token(req.headers()) else {
            return Err(AuthError::Unauthorized);
        };

        match state.tokens.get_value_or_guard_async(&token).await {
            Ok(auth_data) => {
                let now = chrono::Utc::now().timestamp() as u64;
                if auth_data.expires_at < now {
                    return Err(AuthError::Unauthorized);
                }

                req.extensions_mut().insert(auth_data.entity_uid.clone());
            }
            Err(guard) => {
                let authorizer = state.cedrus.project_authorizers.get(&Uuid::nil());
                let Some(authorizer) = authorizer else {
                    return Err(AuthError::Unauthorized);
                };

                let authorizer = authorizer.as_ref().ok_or(AuthError::Unauthorized)?;

                let token_data = match authorizer.jwt.check_auth(&token).await {
                    Ok(token_data) => token_data,
                    Err(_err) => {
                        return Err(AuthError::Unauthorized);
                    }
                };

                let expires_at = token_data
                    .claims
                    .get("exp")
                    .ok_or(AuthError::Unauthorized)?
                    .as_u64()
                    .ok_or(AuthError::Unauthorized)?;

                let entity_uid = authorizer
                    .get_entity_uid(&token_data.claims)
                    .map_err(|_e| AuthError::Unauthorized)?;

                req.extensions_mut().insert(entity_uid.clone());

                let auth_data = AuthData {
                    token: token_data,
                    entity_uid,
                    expires_at,
                };

                let _ = guard.insert(auth_data);
            }
        }
    }

    Ok(next.run(req).await)
}
