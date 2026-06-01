//! A minimal, runnable hocuspocus server.
//!
//! ```sh
//! cargo run -p hocuspocu-rs --example server
//! # custom port + in-memory persistence + lifecycle logging:
//! HP_PORT=8088 HP_PERSIST=1 HP_LOG=1 cargo run -p hocuspocu-rs --example server
//! # enable outbound message chunking at 60 KB per frame:
//! HP_CHUNK=61440 cargo run -p hocuspocu-rs --example server
//! ```
//!
//! Point any Yjs client at it, e.g. `@hocuspocus/provider`:
//!
//! ```js
//! import { HocuspocusProvider } from "@hocuspocus/provider";
//! new HocuspocusProvider({ url: "ws://127.0.0.1:8088", name: "my-doc", document });
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hocuspocu_rs::{
    Configuration, Extension, HookResult, LoadDocumentResult, LoadedDocument,
    OnLoadDocumentPayload, OnStoreDocumentPayload, Server, ServerConfiguration,
};
use tokio::sync::Mutex;

/// An in-memory persistence extension: stores each document's full Yjs state on
/// the debounced `on_store_document` hook and seeds it back via `on_load_document`.
/// Real deployments would swap the `HashMap` for SQLite/Postgres/S3/etc.
struct InMemoryPersistence {
    store: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl Extension for InMemoryPersistence {
    fn name(&self) -> &str {
        "InMemoryPersistence"
    }

    async fn on_load_document(&self, payload: &OnLoadDocumentPayload) -> LoadDocumentResult {
        let store = self.store.lock().await;
        if let Some(update) = store.get(&payload.document_name) {
            return Ok(Some(LoadedDocument::Update(update.clone())));
        }
        Ok(None)
    }

    async fn on_store_document(&self, payload: &OnStoreDocumentPayload) -> HookResult {
        let update = payload.document.encode_state_as_update();
        let mut store = self.store.lock().await;
        store.insert(payload.document_name.clone(), update);
        Ok(None)
    }
}

/// Logs lifecycle events so you can watch connections come and go.
struct Logger;

#[async_trait]
impl Extension for Logger {
    fn name(&self) -> &str {
        "Logger"
    }

    async fn connected(&self, payload: &hocuspocu_rs::ConnectedPayload) -> HookResult {
        println!("[connected] document={}", payload.document_name);
        Ok(None)
    }

    async fn on_disconnect(&self, payload: &hocuspocu_rs::OnDisconnectPayload) -> HookResult {
        println!(
            "[disconnect] document={} clients_left={}",
            payload.document_name, payload.clients_count
        );
        Ok(None)
    }
}

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("HP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8088);

    let mut extensions: Vec<Arc<dyn Extension>> = Vec::new();
    if std::env::var("HP_PERSIST").is_ok() {
        extensions.push(Arc::new(InMemoryPersistence {
            store: Arc::new(Mutex::new(HashMap::new())),
        }));
    }
    if std::env::var("HP_LOG").is_ok() {
        extensions.push(Arc::new(Logger));
    }

    // Short debounce so persistence/store fires quickly in demos and tests.
    let debounce: u64 = std::env::var("HP_DEBOUNCE")
        .ok()
        .and_then(|d| d.parse().ok())
        .unwrap_or(200);

    let message_chunk_size: usize = std::env::var("HP_CHUNK")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let server = Server::with_config(ServerConfiguration {
        port,
        address: "127.0.0.1".to_string(),
        stop_on_signals: true,
        config: Configuration {
            debounce,
            max_debounce: debounce.max(1) * 5,
            extensions,
            message_chunk_size,
            ..Configuration::default()
        },
    });

    println!("hocuspocu-rs listening on ws://127.0.0.1:{port}");
    if let Err(e) = server.listen(None).await {
        eprintln!("server error: {e}");
    }
}
