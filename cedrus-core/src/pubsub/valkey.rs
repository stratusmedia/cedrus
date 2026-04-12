use redis::{
    AsyncCommands, aio::MultiplexedConnection, cluster::ClusterClientBuilder,
    cluster_async::ClusterConnection,
};

use crate::{Event, core::ValKeyPubSubConfig};

use super::{Op, PubSub, PubSubError};

enum ConnectionType {
    Multiplexed(MultiplexedConnection),
    Cluster(ClusterConnection),
}

pub struct ValKeyPubSub {
    conn: ConnectionType,
    urls: Vec<String>,
    topic: String,
}

impl ValKeyPubSub {
    pub async fn new(conf: &ValKeyPubSubConfig) -> Result<Self, PubSubError> {
        let conn = if conf.cluster {
            let client = redis::cluster::ClusterClient::new(conf.urls.clone())
                .map_err(|_| PubSubError::Connection)?;
            let conn = client
                .get_async_connection()
                .await
                .map_err(|_| PubSubError::Connection)?;
            ConnectionType::Cluster(conn)
        } else {
            let url = conf.urls.first().ok_or(PubSubError::Connection)?;
            let client = redis::Client::open(url.clone()).map_err(|_| PubSubError::Connection)?;
            let conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|_| PubSubError::Connection)?;
            ConnectionType::Multiplexed(conn)
        };

        let topic = conf.channel_name.clone();

        Ok(Self {
            conn,
            urls: conf.urls.clone(),
            topic,
        })
    }
}

#[async_trait::async_trait]
impl PubSub for ValKeyPubSub {
    async fn subscribe(&self, ops: &[Op<'_>]) -> Result<(), PubSubError> {
        match &self.conn {
            ConnectionType::Multiplexed(_) => {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

                let url = self.urls.first().ok_or(PubSubError::Connection)?;
                let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
                let client =
                    redis::Client::open(url.clone()).map_err(|_| PubSubError::Connection)?;
                let mut conn = client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await
                    .map_err(|_| PubSubError::Connection)?;
                let _ = conn
                    .subscribe(&[&self.topic])
                    .await
                    .map_err(|_| PubSubError::Connection)?;

                while let Some(msg) = rx.recv().await {
                    if msg.kind == redis::PushKind::Message {
                        let Some(message) = msg.data.get(1) else {
                            continue;
                        };
                        if let redis::Value::BulkString(data) = message {
                            let Ok(str) = String::from_utf8(data.clone()) else {
                                continue;
                            };
                            let Ok(message) = serde_json::from_str::<Event>(&str) else {
                                continue;
                            };
                            for op in ops {
                                op(message.clone()).await;
                            }
                        }
                    }
                }
            }
            ConnectionType::Cluster(_) => {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

                let client = ClusterClientBuilder::new(self.urls.clone())
                    .use_protocol(redis::ProtocolVersion::RESP3)
                    .push_sender(tx)
                    .build()
                    .map_err(|_| PubSubError::Connection)?;
                let mut conn = client
                    .get_async_connection()
                    .await
                    .map_err(|_| PubSubError::Connection)?;
                let _ = conn
                    .subscribe(&[&self.topic])
                    .await
                    .map_err(|_| PubSubError::Connection)?;

                while let Some(msg) = rx.recv().await {
                    if msg.kind == redis::PushKind::Message {
                        let Some(message) = msg.data.get(1) else {
                            continue;
                        };
                        if let redis::Value::BulkString(data) = message {
                            let Ok(str) = String::from_utf8(data.clone()) else {
                                continue;
                            };
                            let Ok(message) = serde_json::from_str::<Event>(&str) else {
                                continue;
                            };
                            for op in ops {
                                op(message.clone()).await;
                            }
                        }
                    }
                }
            }
        };

        Ok(())
    }

    async fn publish(&self, message: Event) -> Result<(), PubSubError> {
        let msg = serde_json::to_string(&message).map_err(|_| PubSubError::Connection)?;
        match &self.conn {
            ConnectionType::Multiplexed(conn) => {
                let mut conn = conn.clone();
                conn.publish::<String, String, String>(self.topic.clone(), msg)
                    .await
                    .map_err(|_| PubSubError::Publish)?;
            }
            ConnectionType::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.publish::<String, String, String>(self.topic.clone(), msg)
                    .await
                    .map_err(|_| PubSubError::Publish)?;
            }
        }

        Ok(())
    }
}
