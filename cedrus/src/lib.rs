#![doc = include_str!("../README.md")]

use std::error::Error;

use axum::{
    extract::{rejection::JsonRejection, FromRequest},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cedrus_core::{Query, Selector};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub const DEFAULT_LIMIT: usize = 1000;

#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(AppError))]
pub struct AppJson<T>(pub T);

impl<T> IntoResponse for AppJson<T>
where
    axum::Json<T>: IntoResponse,
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

// The kinds of errors we can hit in our application.
#[derive(Debug)]
pub enum AppError {
    BadRequest,   // 400
    Unauthorized, // 401
    Forbidden,    // 403
    NotFound,     // 404

    JsonRejection(JsonRejection), // 422
    CedrusError(cedrus_core::CedrusError),

    SchemaError(cedar_policy::SchemaError),
    EntitiesError(cedar_policy::entities_errors::EntitiesError),
    PolicyFromJsonError(cedar_policy::PolicyFromJsonError),
    PolicyToJsonError(cedar_policy::PolicyToJsonError),
    PolicySetError(cedar_policy::PolicySetError),
    ContextJsonError(cedar_policy::ContextJsonError),
}

// Tell axum how `AppError` should be converted into a response.
//
// This is also a convenient place to log errors.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // How we want errors responses to be serialized
        #[derive(Default, Serialize)]
        struct ErrorResponse {
            error: String,   // error code
            message: String, // human readable error message
            detail: String,  // additional details about the error
        }

        let (status, error_response) = match self {
            AppError::BadRequest => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    message: "Not Found".to_owned(),
                    ..Default::default()
                },
            ),
            AppError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                ErrorResponse {
                    message: "Unauthorized".to_owned(),
                    ..Default::default()
                },
            ),
            AppError::Forbidden => (
                StatusCode::FORBIDDEN,
                ErrorResponse {
                    message: "Forbidden".to_owned(),
                    ..Default::default()
                },
            ),
            AppError::NotFound => (
                StatusCode::NOT_FOUND,
                ErrorResponse {
                    message: "Not Found".to_owned(),
                    ..Default::default()
                },
            ),
            AppError::JsonRejection(rejection) => {
                // This error is caused by bad user input so don't log it
                (
                    rejection.status(),
                    ErrorResponse {
                        message: rejection.body_text(),
                        ..Default::default()
                    },
                )
            }

            AppError::CedrusError(cedrus_error) => {
                let status = match cedrus_error {
                    cedrus_core::CedrusError::NotFound => StatusCode::NOT_FOUND,
                    cedrus_core::CedrusError::Unauthorized => StatusCode::UNAUTHORIZED,
                    cedrus_core::CedrusError::Forbidden => StatusCode::FORBIDDEN,
                    cedrus_core::CedrusError::BadRequest => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };

                let error_response = ErrorResponse {
                    error: "CedrusError".to_string(),
                    message: cedrus_error.to_string(),
                    detail: cedrus_error.to_string(),
                };

                (status, error_response)
            }

            AppError::EntitiesError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse {
                    message: "Entities Error".to_owned(),
                    detail: format!("{:?}", e.source().unwrap()),
                    ..Default::default()
                },
            ),
            AppError::SchemaError(e) => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    message: "Schema Error".to_owned(),
                    detail: format!("{:?}", e.source().unwrap()),
                    ..Default::default()
                },
            ),
            AppError::PolicyFromJsonError(e) => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    message: "PolicyFromJson Error".to_owned(),
                    detail: format!("{:?}", e.source().unwrap()),
                    ..Default::default()
                },
            ),
            AppError::PolicyToJsonError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse {
                    message: "PolicyToJson Error".to_owned(),
                    detail: format!("{:?}", e.source().unwrap()),
                    ..Default::default()
                },
            ),
            AppError::PolicySetError(e) => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    message: "PolicySet Error".to_owned(),
                    detail: format!("{:?}", e.source().unwrap()),
                    ..Default::default()
                },
            ),
            AppError::ContextJsonError(e) => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    message: "ContextJson Error".to_owned(),
                    detail: format!("{:?}", e.source().unwrap()),
                    ..Default::default()
                },
            ),
        };

        (status, AppJson(error_response)).into_response()
    }
}

impl From<JsonRejection> for AppError {
    fn from(rejection: JsonRejection) -> Self {
        Self::JsonRejection(rejection)
    }
}

impl From<cedrus_core::CedrusError> for AppError {
    fn from(error: cedrus_core::CedrusError) -> Self {
        Self::CedrusError(error)
    }
}

impl From<cedar_policy::SchemaError> for AppError {
    fn from(error: cedar_policy::SchemaError) -> Self {
        Self::SchemaError(error)
    }
}

impl From<cedar_policy::entities_errors::EntitiesError> for AppError {
    fn from(error: cedar_policy::entities_errors::EntitiesError) -> Self {
        Self::EntitiesError(error)
    }
}

impl From<cedar_policy::PolicyFromJsonError> for AppError {
    fn from(error: cedar_policy::PolicyFromJsonError) -> Self {
        Self::PolicyFromJsonError(error)
    }
}

impl From<cedar_policy::PolicyToJsonError> for AppError {
    fn from(error: cedar_policy::PolicyToJsonError) -> Self {
        Self::PolicyToJsonError(error)
    }
}

impl From<cedar_policy::PolicySetError> for AppError {
    fn from(error: cedar_policy::PolicySetError) -> Self {
        Self::PolicySetError(error)
    }
}

impl From<cedar_policy::ContextJsonError> for AppError {
    fn from(error: cedar_policy::ContextJsonError) -> Self {
        Self::ContextJsonError(error)
    }
}

pub fn option_uuid_eq(a: Option<Uuid>, b: Option<Uuid>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.eq(&b),
        (None, None) => true,
        _ => false,
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct QueryParams {
    #[param(style = DeepObject, explode, inline, nullable)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<Selector>,
    /*
    #[param(style = DeepObject, explode, inline, nullable)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<Sort>>,
    #[param(nullable)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
     */
    #[param(nullable)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_key: Option<String>,
    #[param(nullable)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /*
    #[param(nullable)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip: Option<u32>,
    #[param(nullable)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    */
}

impl From<Query> for QueryParams {
    fn from(query: Query) -> Self {
        Self {
            selector: query.selector,
            /*
            sort: query.sort.len().ne(&0).then(|| query.sort),
            fields: query.fields.len().ne(&0).then(|| query.fields),
            */
            start_key: query.start_key,
            limit: query.limit.ge(&0).then(|| query.limit),
            /*
            skip: query.skip.ge(&0).then(|| query.skip),
            index: query.index,
            */
        }
    }
}

impl Into<Query> for QueryParams {
    fn into(self) -> Query {
        Query {
            selector: self.selector,
            sort: Vec::new(), // self.sort.unwrap_or_default(),
            fields: Vec::new(), // self.fields.unwrap_or_default(),
            start_key: self.start_key,
            limit: self.limit.unwrap_or(0),
            skip: 0, // self.skip.unwrap_or(0),
            index: None, //self.index,
        }
    }
}

pub mod routes;
