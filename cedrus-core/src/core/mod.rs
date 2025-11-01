use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

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
        pub group_configuration: CognitoGroupConfiguration,
    }

    impl CognitoUserPoolConfiguration {
        pub fn url_keys(&self) -> String {
            let parts: Vec<&str> = self.user_pool_arn.split(':').collect();
            let region = parts.get(3).expect("Invalid user pool ARN");
            let user_pool_id = self
                .user_pool_arn
                .split('/')
                .last()
                .expect("Invalid user pool ARN")
                .to_string();

            format!(
                "https://cognito-idp.{region}.amazonaws.com/{user_pool_id}/.well-known/jwks.json"
            )
        }

        pub fn prefix(&self) -> String {
            self.user_pool_arn
                .split('/')
                .last()
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

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DynamoDBConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    pub table_name: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CouchDbConfig {
    pub uri: String,
    pub username: String,
    pub password: String,
    pub db_name: String,
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
    pub identity_source: IdentitySource,
}

pub struct Authorizer {
    pub identity_source: IdentitySource,
    pub jwt: jwt_authorizer::Authorizer<Value>,
    pub prefix: String,
    pub id_claim: String,
}

impl Authorizer {
    pub fn new(identity_source: IdentitySource, jwt: jwt_authorizer::Authorizer<Value>) -> Self {
        let prefix = match &identity_source.configuration {
            is::Configuration::CognitoUserPoolConfiguration(conf) => conf.prefix(),
            is::Configuration::OpenIdConnectConfiguration(conf) => match &conf.entity_id_prefix {
                Some(val) => val.clone(),
                None => "OpenIdConnect".to_string(),
            },
        };

        let id_claim = match &identity_source.configuration {
            is::Configuration::CognitoUserPoolConfiguration(_) => "sub".to_string(),
            is::Configuration::OpenIdConnectConfiguration(conf) => match &conf.token_selection {
                is::OpenIdConnectTokenSelection::AccessTokenOnly(token) => {
                    match &token.principal_id_claim {
                        Some(val) => val.clone(),
                        None => "sub".to_string(),
                    }
                }
                is::OpenIdConnectTokenSelection::IdentityTokenOnly(token) => {
                    match &token.principal_id_claim {
                        Some(val) => val.clone(),
                        None => "sub".to_string(),
                    }
                }
            },
        };

        Self {
            identity_source,
            jwt,
            prefix,
            id_claim,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is::*;
    use super::*;

    #[tokio::test]
    async fn test_cedrus_config() {
        let db = DynamoDBConfig {
            endpoint_url: None,
            region: None,
            table_name: "Cedrus_Temp".to_string(),
        };

        let cache = ValKeyCacheConfig {
            urls: Vec::from(["redis://localhost".to_string()]),
            ..Default::default()
        };

        let pubsub = ValKeyPubSubConfig {
            urls: Vec::from(["redis://localhost/?protocol=resp3".to_string()]),
            channel_name: "cedrus".to_string(),
            ..Default::default()
        };

        let configuration = Configuration::CognitoUserPoolConfiguration(CognitoUserPoolConfiguration {
            user_pool_arn: "arn:aws:dynamodb:eu-west-1:414827610504:table/Dev-CedrusAwsStack-CedrusTable5F145212-FIBPKSAN5W0".to_string(),
            client_ids: vec!["3e1ots47s5fq86k4a6vup0rove".to_string()],
            group_configuration: CognitoGroupConfiguration {
                group_entity_type: "Cedrus::Group".to_string(),
            },
        });

        let identity_source = IdentitySource {
            principal_entity_type: "Cedrus::User".to_string(),
            configuration,
        };

        let server = ServerConfig {
            host: "localhost".to_string(),
            port: 3000,
            public_key: "public_key".to_string(),
            private_key: "private_key".to_string(),
            chains_key: "chains_key".to_string(),
            api_key: "api_key".to_string(),
        };

        let config = CedrusConfig {
            server: server,
            db: DbConfig::DynamoDbConfig(db),
            cache: CacheConfig::ValKeyConfig(cache),
            pubsub: PubSubConfig::ValKeyConfig(pubsub),
            identity_source: identity_source,
        };

        let json = serde_json::to_string(&config).unwrap();
        println!("JSON: {}", json);
    }
}
