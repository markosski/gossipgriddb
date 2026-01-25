use reqwest::Client;

use crate::{event_bus::EventBus, store::Store, wal::Wal};

pub struct Env {
    pub store: Box<dyn Store + Send + Sync>,
    pub wal: Box<dyn Wal + Send + Sync>,
    pub event_bus: EventBus,
    pub http_client: Client,
}

impl Env {
    pub fn new(store: Box<dyn Store>, wal: Box<dyn Wal>, event_bus: EventBus) -> Self {
        Env {
            store,
            wal,
            event_bus,
            http_client: Client::builder()
                .pool_max_idle_per_host(1000)
                .build()
                .unwrap(),
        }
    }

    pub fn get_event_bus(&self) -> &EventBus {
        &self.event_bus
    }

    pub fn get_wal(&self) -> &(dyn Wal + Send + Sync) {
        &*self.wal
    }

    pub fn get_store(&self) -> &(dyn Store + Send + Sync) {
        &*self.store
    }

    pub fn get_http_client(&self) -> &Client {
        &self.http_client
    }
}
