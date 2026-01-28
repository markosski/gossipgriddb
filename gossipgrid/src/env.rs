use reqwest::Client;

use crate::{event_bus::EventBus, function_registry::FunctionRegistry, store::Store, wal::Wal};

pub struct Env {
    pub store: Box<dyn Store + Send + Sync>,
    pub wal: Box<dyn Wal + Send + Sync>,
    pub event_bus: EventBus,
    pub http_client: Client,
    pub function_registry: FunctionRegistry,
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
            function_registry: FunctionRegistry::new(),
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

    pub fn get_function_registry(&self) -> &FunctionRegistry {
        &self.function_registry
    }
}
