use std::{error::Error, future::Future, pin::Pin};

use crate::{core::PubSubConfig, Event};

pub mod valkey;
pub mod dummy;

#[derive(Debug)]
pub enum PubSubError {
    Connection,
    NotFound,
}

impl std::fmt::Display for PubSubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PubSubError::Connection => write!(f, "Connection error"),
            PubSubError::NotFound => write!(f, "Not found"),
        }
    }
}

impl Error for PubSubError {}

pub type Op<'a> =
    Box<dyn 'a + Send + Sync + Fn(Event) -> Pin<Box<dyn Future<Output = ()> + 'a + Send>>>;

#[async_trait::async_trait]
pub trait PubSub: Send + Sync {
    async fn subscribe(&self, ops: &[Op<'_>]);
    async fn publish(&self, msg: Event) -> Result<(), PubSubError>;
}

pub async fn pubsub_factory(conf: &PubSubConfig) -> Box<dyn PubSub + Send + Sync> {
    match conf {
        PubSubConfig::ValKeyConfig(conf) => Box::new(valkey::ValKeyPubSub::new(&conf).await),
        PubSubConfig::DummyConfig(_) => Box::new(dummy::DummyPubSub::new()),
    }
}
