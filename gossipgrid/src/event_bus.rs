use std::sync::Arc;

use log::{error, info};
use tokio::sync::{RwLock, mpsc};

use crate::{
    env::Env,
    node::{JoinedNode, NodeState},
};

use futures::future::BoxFuture;

pub type EventJob =
    Box<dyn FnOnce(Arc<RwLock<NodeState>>, Arc<Env>) -> BoxFuture<'static, ()> + Send>;

#[doc(hidden)]
pub enum Event {
    StartInMemoryHydration,
    Run(EventJob),
}

#[derive(Debug, Clone)]
pub struct EventBus {
    pub tx: mpsc::Sender<Event>,
    pub rx: std::sync::Arc<std::sync::Mutex<Option<mpsc::Receiver<Event>>>>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<Event>(1024);
        Self {
            tx,
            // TODO why we changes to Arc?
            rx: std::sync::Arc::new(std::sync::Mutex::new(Some(rx))),
        }
    }

    pub fn take_receiver(&self) -> Option<mpsc::Receiver<Event>> {
        self.rx.lock().unwrap().take()
    }

    pub fn emit(&self, ev: Event) {
        if let Err(e) = self.tx.try_send(ev) {
            error!("Failed to emit event: {e}");
        }
    }

    pub fn run<F, Fut>(&self, f: F)
    where
        F: FnOnce(Arc<RwLock<NodeState>>, Arc<Env>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let job = Box::new(move |node_state, env| {
            let fut = f(node_state, env);
            Box::pin(fut) as BoxFuture<'static, ()>
        });
        self.emit(Event::Run(job));
    }
}

// EventBus is ideal in cases where we want to notify node of some event happening.
// Ideal actions to perform in response to event are ones that do not require update state other than node state.
#[doc(hidden)]
pub async fn start_event_loop(
    mut rx: mpsc::Receiver<Event>,
    node_state: Arc<RwLock<NodeState>>,
    env: Arc<Env>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
) -> tokio::task::JoinHandle<()> {
    let mut shutdown_rx = shutdown_tx.subscribe();
    let node_address = {
        let node = node_state.read().await;
        node.get_address().clone()
    };

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("node={}; Shutting down event loop task", &node_address);
                    break;
                }
                maybe_ev = rx.recv() => {
                    match maybe_ev {
                        Some(ev) => {
                            match ev {
                                Event::StartInMemoryHydration => {
                                    let _ =
                                        JoinedNode::hydrate_store_from_wal_task(node_state.clone(), env.clone())
                                            .await;
                                }
                                Event::Run(job) => {
                                    job(node_state.clone(), env.clone()).await;
                                }
                            }
                        }
                        None => {
                            info!("node={}; Event channel closed, shutting down task", &node_address);
                            break;
                        }
                    }
                }
            }
        }
    })
}
