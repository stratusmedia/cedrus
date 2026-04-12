use crate::Event;

use super::{Op, PubSub, PubSubError};

pub struct DummyPubSub;

impl Default for DummyPubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl DummyPubSub {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl PubSub for DummyPubSub {
    async fn subscribe(&self, _ops: &[Op<'_>]) -> Result<(), PubSubError> {
        Ok(())
    }

    async fn publish(&self, _msg: Event) -> Result<(), PubSubError> {
        Ok(())
    }
}
