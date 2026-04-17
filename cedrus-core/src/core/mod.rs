use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::core::is::OpenIdConnectTokenSelection;

pub mod cedrus;
pub mod project;

pub mod is {
    use super::*;

    #[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
    #[schema(as = identitysource::CognitoGroupConfiguration)]
    #[serde(rename_all = "camelCase")]
    pub struct CognitoGroupConfiguration {
        pub group_entity_type: String,
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
    #[schema(as = identitysource::CognitoUserPoolConfiguration)]
    #[serde(rename_all = "camelCase")]
    pub struct CognitoUserPoolConfiguration {
        pub user_pool_arn: String,
        pub client_ids: Vec<String>,
        pub group_configuration: Option<CognitoGroupConfiguration>,
    }

    impl CognitoUserPoolConfiguration {
        pub fn iss(&self) -> String {
            let parts: Vec<&str> = self.user_pool_arn.split(':').collect();
            let region = parts.get(3).expect("Invalid user pool ARN");
            let user_pool_id = self
                .user_pool_arn
                .split('/')
                .next_back()
                .expect("Invalid user pool ARN")
                .to_string();

            format!("https://cognito-idp.{region}.amazonaws.com/{user_pool_id}")
        }

        pub fn url_keys(&self) -> String {
            let parts: Vec<&str> = self.user_pool_arn.split(':').collect();
            let region = parts.get(3).expect("Invalid user pool ARN");
            let user_pool_id = self
                .user_pool_arn
                .split('/')
                .next_back()
                .expect("Invalid user pool ARN")
                .to_string();

            format!(
                "https://cognito-idp.{region}.amazonaws.com/{user_pool_id}/.well-known/jwks.json"
            )
        }

        pub fn prefix(&self) -> String {
            self.user_pool_arn
                .split('/')
                .next_back()
                .expect("Invalid user pool ARN")
                .to_string()
        }
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
    #[schema(as = identitysource::OpenIdConnectAccessTokenConfiguration)]
    #[serde(rename_all = "camelCase", default)]
    pub struct OpenIdConnectAccessTokenConfiguration {
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub audiences: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub principal_id_claim: Option<String>,
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
    #[schema(as = identitysource::OpenIdConnectIdentityTokenConfiguration)]
    #[serde(rename_all = "camelCase", default)]
    pub struct OpenIdConnectIdentityTokenConfiguration {
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub client_ids: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub principal_id_claim: Option<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
    #[schema(as = identitysource::OpenIdConnectTokenSelection)]
    #[serde(rename_all = "camelCase")]
    pub enum OpenIdConnectTokenSelection {
        AccessTokenOnly(OpenIdConnectAccessTokenConfiguration),
        IdentityTokenOnly(OpenIdConnectIdentityTokenConfiguration),
    }

    impl Default for OpenIdConnectTokenSelection {
        fn default() -> Self {
            OpenIdConnectTokenSelection::AccessTokenOnly(
                OpenIdConnectAccessTokenConfiguration::default(),
            )
        }
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
    #[schema(as = identitysource::OpenIdConnectGroupConfiguration)]
    #[serde(rename_all = "camelCase")]
    pub struct OpenIdConnectGroupConfiguration {
        pub group_claim: String,
        pub group_entity_type: String,
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
    #[schema(as = identitysource::OpenIdConnectConfiguration)]
    #[serde(rename_all = "camelCase")]
    pub struct OpenIdConnectConfiguration {
        pub issuer: String,
        pub token_selection: OpenIdConnectTokenSelection,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub entity_id_prefix: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub group_configuration: Option<OpenIdConnectGroupConfiguration>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
    #[schema(as = identitysource::Configuration)]
    #[serde(rename_all = "camelCase")]
    pub enum Configuration {
        CognitoUserPoolConfiguration(CognitoUserPoolConfiguration),
        OpenIdConnectConfiguration(OpenIdConnectConfiguration),
    }

    impl Default for Configuration {
        fn default() -> Self {
            Configuration::CognitoUserPoolConfiguration(CognitoUserPoolConfiguration::default())
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IdentitySource {
    pub principal_entity_type: String,
    pub configuration: is::Configuration,
}

impl IdentitySource {
    pub fn prefix(&self) -> String {
        match &self.configuration {
            is::Configuration::CognitoUserPoolConfiguration(config) => config
                .user_pool_arn
                .split('/')
                .next_back()
                .expect("Invalid user pool ARN")
                .to_string(),
            is::Configuration::OpenIdConnectConfiguration(config) => {
                match config.entity_id_prefix {
                    Some(ref val) => val.clone(),
                    None => config.issuer.replace("http://", "").replace("https://", ""),
                }
            }
        }
    }

    pub fn id_claim(&self) -> String {
        match &self.configuration {
            is::Configuration::CognitoUserPoolConfiguration(_) => "sub".to_string(),
            is::Configuration::OpenIdConnectConfiguration(conf) => match &conf.token_selection {
                OpenIdConnectTokenSelection::AccessTokenOnly(token) => {
                    match &token.principal_id_claim {
                        Some(val) => val.clone(),
                        None => "sub".to_string(),
                    }
                }
                OpenIdConnectTokenSelection::IdentityTokenOnly(token) => {
                    match &token.principal_id_claim {
                        Some(val) => val.clone(),
                        None => "sub".to_string(),
                    }
                }
            },
        }
    }

    pub fn group_claim(&self) -> Option<String> {
        match &self.configuration {
            is::Configuration::CognitoUserPoolConfiguration(_) => {
                Some("[cognito:groups]".to_string())
            }
            is::Configuration::OpenIdConnectConfiguration(conf) => conf
                .group_configuration
                .as_ref()
                .map(|group| group.group_claim.clone()),
        }
    }

    pub fn group_entity_type(&self) -> Option<String> {
        match &self.configuration {
            is::Configuration::CognitoUserPoolConfiguration(conf) => conf
                .group_configuration
                .as_ref()
                .map(|group| group.group_entity_type.clone()),
            is::Configuration::OpenIdConnectConfiguration(conf) => conf
                .group_configuration
                .as_ref()
                .map(|group| group.group_entity_type.clone()),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DynamoDBConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    pub table_name: String,
    #[serde(default)]
    pub initialize: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CouchDbConfig {
    pub uri: String,
    pub username: String,
    pub password: String,
    pub db_name: String,
    #[serde(default)]
    pub initialize: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum DbConfig {
    DynamoDbConfig(DynamoDBConfig),
    CouchDbConfig(CouchDbConfig),
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig::CouchDbConfig(CouchDbConfig::default())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
pub struct ValKeyCacheConfig {
    pub urls: Vec<String>,
    pub cluster: bool,
    pub root_key: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
pub struct DashMapCacheConfig {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum CacheConfig {
    ValKeyConfig(ValKeyCacheConfig),
    DashMapConfig(DashMapCacheConfig),
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig::DashMapConfig(DashMapCacheConfig::default())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValKeyPubSubConfig {
    pub urls: Vec<String>,
    pub channel_name: String,
    pub cluster: bool,
    pub root_key: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
pub struct DummyPubSubConfig {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum PubSubConfig {
    ValKeyConfig(ValKeyPubSubConfig),
    DummyConfig(DummyPubSubConfig),
}

impl Default for PubSubConfig {
    fn default() -> Self {
        PubSubConfig::DummyConfig(DummyPubSubConfig::default())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub api_key: String,
    pub public_key: Option<String>,
    pub private_key: Option<String>,
    pub chains_key: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CedrusConfig {
    pub server: ServerConfig,
    pub db: DbConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub pubsub: PubSubConfig,
    pub identity_source: Option<IdentitySource>,
}
