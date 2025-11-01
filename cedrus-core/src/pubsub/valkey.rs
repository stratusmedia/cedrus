use redis::{
    aio::MultiplexedConnection, cluster::ClusterClientBuilder, cluster_async::ClusterConnection,
    AsyncCommands,
};

use crate::{core::ValKeyPubSubConfig, Event};

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
    pub async fn new(conf: &ValKeyPubSubConfig) -> Self {
        let conn = if conf.cluster {
            let client = redis::cluster::ClusterClient::new(conf.urls.clone()).unwrap();
            let conn = client.get_async_connection().await.unwrap();
            ConnectionType::Cluster(conn)
        } else {
            let url = conf.urls.get(0).unwrap();
            let client = redis::Client::open(url.clone()).unwrap();
            let conn = client.get_multiplexed_tokio_connection().await.unwrap();
            ConnectionType::Multiplexed(conn)
        };

        let topic = conf.channel_name.clone();

        Self {
            conn,
            urls: conf.urls.clone(),
            topic,
        }
    }
}

#[async_trait::async_trait]
impl PubSub for ValKeyPubSub {
    async fn subscribe(&self, ops: &[Op<'_>]) {
        match &self.conn {
            ConnectionType::Multiplexed(_) => {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

                let url = self.urls.get(0).unwrap();
                let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
                let client = redis::Client::open(url.clone()).unwrap();
                let mut conn = client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await
                    .unwrap();
                let _ = conn.subscribe(&[&self.topic]).await.unwrap();
                println!("Multiplexed subscribe");

                while let Some(msg) = rx.recv().await {
                    println!("subscribe1 recv {:?}", msg);
                    match msg.kind {
                        redis::PushKind::Message => {
                            let Some(message) = msg.data.get(1) else {
                                continue;
                            };
                            match message {
                                redis::Value::BulkString(data) => {
                                    let Ok(str) = String::from_utf8(data.clone()) else {
                                        continue;
                                    };
                                    println!("subscribe2 recv1: {}", str);
                                    let Ok(message) = serde_json::from_str::<Event>(&str) else {
                                        continue;
                                    };
                                    for op in ops {
                                        op(message.clone()).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }
            ConnectionType::Cluster(_) => {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

                let client = ClusterClientBuilder::new(self.urls.clone())
                    .use_protocol(redis::ProtocolVersion::RESP3)
                    .push_sender(tx)
                    .build()
                    .unwrap();
                let mut conn = client.get_async_connection().await.unwrap();
                let _ = conn.subscribe(&[&self.topic]).await.unwrap();
                println!("Cluster subscribe");

                while let Some(msg) = rx.recv().await {
                    println!("subscribe1 recv {:?}", msg);
                    match msg.kind {
                        redis::PushKind::Message => {
                            let Some(message) = msg.data.get(1) else {
                                continue;
                            };
                            match message {
                                redis::Value::BulkString(data) => {
                                    let Ok(str) = String::from_utf8(data.clone()) else {
                                        continue;
                                    };
                                    println!("subscribe2 recv1: {}", str);
                                    let Ok(message) = serde_json::from_str::<Event>(&str) else {
                                        continue;
                                    };
                                    for op in ops {
                                        op(message.clone()).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    async fn publish(&self, message: Event) -> Result<(), PubSubError> {
        let msg = serde_json::to_string(&message).unwrap();
        match &self.conn {
            ConnectionType::Multiplexed(conn) => {
                println!("publish Multiplexed: {} {}", self.topic, msg);
                let mut conn = conn.clone();
                conn.publish::<String, String, String>(self.topic.clone(), msg)
                    .await
                    .unwrap();
            }
            ConnectionType::Cluster(conn) => {
                println!("publish Cluster: {} {}", self.topic, msg);
                let mut conn = conn.clone();
                conn.publish::<String, String, String>(self.topic.clone(), msg)
                    .await
                    .unwrap();
            }
        }

        Ok(())
    }
}
