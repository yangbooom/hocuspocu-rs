use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use yrs::sync::Awareness;
use yrs::sync::AwarenessUpdate as YrsAwarenessUpdate;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, Transact, Update};

use crate::encoding;
use crate::types::*;

pub struct Document {
    pub awareness: Awareness,
    pub name: String,
    pub is_loading: RwLock<bool>,
    pub is_destroyed: RwLock<bool>,
    pub last_change_time: RwLock<u64>,
    pub direct_connections_count: RwLock<usize>,
    pub save_mutex: Mutex<()>,

    connections: RwLock<HashMap<String, ConnectionEntry>>,

    on_update_callback: RwLock<
        Option<Arc<dyn Fn(Arc<Document>, Option<TransactionOrigin>, Vec<u8>) + Send + Sync>>,
    >,
    before_broadcast_stateless_callback:
        RwLock<Option<Arc<dyn Fn(Arc<Document>, String) + Send + Sync>>>,
    before_handle_awareness_callback: RwLock<
        Option<
            Arc<
                dyn Fn(
                        Arc<Document>,
                        Vec<u8>,
                        Option<TransactionOrigin>,
                    ) -> std::pin::Pin<
                        Box<
                            dyn std::future::Future<
                                    Output = Result<
                                        Vec<u8>,
                                        Box<dyn std::error::Error + Send + Sync>,
                                    >,
                                > + Send,
                        >,
                    > + Send
                    + Sync,
            >,
        >,
    >,
}

struct ConnectionEntry {
    pub clients: HashSet<u64>,
    pub socket_id: String,
    pub read_only: bool,
    pub message_address: String,
    pub ws: Arc<dyn WebSocketSink>,
}

impl Document {
    pub fn new(name: &str) -> Self {
        let doc = Doc::new();
        let awareness = Awareness::new(doc);
        Self {
            awareness,
            name: name.to_string(),
            is_loading: RwLock::new(true),
            is_destroyed: RwLock::new(false),
            last_change_time: RwLock::new(0),
            direct_connections_count: RwLock::new(0),
            save_mutex: Mutex::new(()),
            connections: RwLock::new(HashMap::new()),
            on_update_callback: RwLock::new(None),
            before_broadcast_stateless_callback: RwLock::new(None),
            before_handle_awareness_callback: RwLock::new(None),
        }
    }

    pub fn doc(&self) -> &Doc {
        self.awareness.doc()
    }

    pub async fn set_on_update(
        &self,
        callback: Arc<dyn Fn(Arc<Document>, Option<TransactionOrigin>, Vec<u8>) + Send + Sync>,
    ) {
        let mut cb = self.on_update_callback.write().await;
        *cb = Some(callback);
    }

    pub async fn set_before_broadcast_stateless(
        &self,
        callback: Arc<dyn Fn(Arc<Document>, String) + Send + Sync>,
    ) {
        let mut cb = self.before_broadcast_stateless_callback.write().await;
        *cb = Some(callback);
    }

    pub async fn set_before_handle_awareness(
        &self,
        callback: Arc<
            dyn Fn(
                    Arc<Document>,
                    Vec<u8>,
                    Option<TransactionOrigin>,
                ) -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<
                                    Vec<u8>,
                                    Box<dyn std::error::Error + Send + Sync>,
                                >,
                            > + Send,
                    >,
                > + Send
                + Sync,
        >,
    ) {
        let mut cb = self.before_handle_awareness_callback.write().await;
        *cb = Some(callback);
    }

    pub async fn add_connection(
        &self,
        connection_id: &str,
        socket_id: &str,
        read_only: bool,
        message_address: &str,
        ws: Arc<dyn WebSocketSink>,
    ) {
        let mut conns = self.connections.write().await;
        conns.insert(
            connection_id.to_string(),
            ConnectionEntry {
                clients: HashSet::new(),
                socket_id: socket_id.to_string(),
                read_only,
                message_address: message_address.to_string(),
                ws,
            },
        );
    }

    pub async fn has_connection(&self, connection_id: &str) -> bool {
        let conns = self.connections.read().await;
        conns.contains_key(connection_id)
    }

    pub async fn remove_connection(&self, connection_id: &str) {
        let mut conns = self.connections.write().await;
        if let Some(entry) = conns.remove(connection_id) {
            for client_id in &entry.clients {
                self.awareness.remove_state(*client_id);
            }
        }
    }

    pub async fn add_direct_connection(&self) {
        let mut count = self.direct_connections_count.write().await;
        *count += 1;
    }

    pub async fn remove_direct_connection(&self) {
        let mut count = self.direct_connections_count.write().await;
        if *count > 0 {
            *count -= 1;
        }
    }

    pub async fn get_connections_count(&self) -> usize {
        let conns = self.connections.read().await;
        let dc = self.direct_connections_count.read().await;
        conns.len() + *dc
    }

    pub async fn get_connection_ids(&self) -> Vec<String> {
        let conns = self.connections.read().await;
        conns.keys().cloned().collect()
    }

    pub async fn get_connection_socket_ids(&self) -> Vec<String> {
        let conns = self.connections.read().await;
        conns.values().map(|e| e.socket_id.clone()).collect()
    }

    pub async fn get_connection_info(
        &self,
        connection_id: &str,
    ) -> Option<(String, bool, String, Arc<dyn WebSocketSink>)> {
        let conns = self.connections.read().await;
        conns.get(connection_id).map(|e| {
            (
                e.socket_id.clone(),
                e.read_only,
                e.message_address.clone(),
                e.ws.clone(),
            )
        })
    }

    pub async fn add_client_to_connection(&self, connection_id: &str, client_id: u64) {
        let mut conns = self.connections.write().await;
        if let Some(entry) = conns.get_mut(connection_id) {
            entry.clients.insert(client_id);
        }
    }

    pub async fn remove_client_from_connection(&self, connection_id: &str, client_id: u64) {
        let mut conns = self.connections.write().await;
        if let Some(entry) = conns.get_mut(connection_id) {
            entry.clients.remove(&client_id);
        }
    }

    pub fn has_awareness_states(&self) -> bool {
        self.awareness.iter().any(|_| true)
    }

    pub fn encode_awareness_update_all(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let update = self.awareness.update().map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        Ok(update.encode_v1())
    }

    pub fn encode_awareness_update_clients(
        &self,
        clients: &[u64],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let update = self
            .awareness
            .update_with_clients(clients.iter().copied())
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        Ok(update.encode_v1())
    }

    pub async fn apply_awareness_update(
        self: &Arc<Self>,
        update_data: &[u8],
        origin: Option<TransactionOrigin>,
        connection_id: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let before_cb = {
            let cb = self.before_handle_awareness_callback.read().await;
            cb.clone()
        };

        let processed_data = if let Some(ref callback) = before_cb {
            callback(self.clone(), update_data.to_vec(), origin.clone()).await?
        } else {
            update_data.to_vec()
        };

        let update = YrsAwarenessUpdate::decode_v1(&processed_data)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;

        let added: Vec<u64> = update
            .clients
            .keys()
            .filter(|id| self.awareness.meta(**id).is_none())
            .copied()
            .collect();

        self.awareness
            .apply_update(update)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

        if let Some(conn_id) = connection_id {
            let mut conns = self.connections.write().await;
            if let Some(entry) = conns.get_mut(conn_id) {
                for client_id in &added {
                    entry.clients.insert(*client_id);
                }
            }
        }

        self.broadcast_awareness_to_connections(&processed_data).await;

        Ok(())
    }

    async fn broadcast_awareness_to_connections(&self, update_data: &[u8]) {
        let conns = self.connections.read().await;

        for entry in conns.values() {
            let mut msg = Vec::new();
            encoding::write_var_string(&mut msg, &entry.message_address);
            encoding::write_var_uint(&mut msg, MessageType::Awareness as u64);
            encoding::write_var_uint8_array(&mut msg, update_data);
            let _ = entry.ws.send(msg);
        }
    }

    pub fn encode_state_as_update(&self) -> Vec<u8> {
        let txn = self.doc().transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    }

    pub async fn is_save_mutex_locked(&self) -> bool {
        self.save_mutex.try_lock().is_err()
    }

    pub fn encode_state_vector(&self) -> Vec<u8> {
        let txn = self.doc().transact();
        txn.state_vector().encode_v1()
    }

    pub fn apply_update(&self, update: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut txn = self.doc().transact_mut();
        let u = Update::decode_v1(update)?;
        txn.apply_update(u)?;
        Ok(())
    }

    pub async fn handle_update(
        self: &Arc<Self>,
        update: Vec<u8>,
        origin: Option<TransactionOrigin>,
    ) {
        {
            let cb = self.on_update_callback.read().await;
            if let Some(ref callback) = *cb {
                callback(self.clone(), origin, update.clone());
            }
        }

        let conns = self.connections.read().await;
        for entry in conns.values() {
            let mut msg = Vec::new();
            encoding::write_var_string(&mut msg, &entry.message_address);
            encoding::write_var_uint(&mut msg, MessageType::Sync as u64);
            encoding::write_var_uint(&mut msg, 2); // messageYjsUpdate
            encoding::write_var_uint8_array(&mut msg, &update);
            let _ = entry.ws.send(msg);
        }
    }

    pub async fn broadcast_stateless(
        &self,
        payload: &str,
        filter: Option<Box<dyn Fn(&str) -> bool + Send + Sync>>,
    ) {
        let conns = self.connections.read().await;
        for (conn_id, entry) in conns.iter() {
            if let Some(ref f) = filter {
                if !f(conn_id) {
                    continue;
                }
            }
            let mut msg = Vec::new();
            encoding::write_var_string(&mut msg, &entry.message_address);
            encoding::write_var_uint(&mut msg, MessageType::Stateless as u64);
            encoding::write_var_string(&mut msg, payload);
            let _ = entry.ws.send(msg);
        }
    }

    pub async fn send_to_connection(&self, connection_id: &str, data: &[u8]) {
        let conns = self.connections.read().await;
        if let Some(entry) = conns.get(connection_id) {
            let _ = entry.ws.send(data.to_vec());
        }
    }

    pub async fn send_to_all_connections(&self, data_fn: impl Fn(&str) -> Vec<u8>) {
        let conns = self.connections.read().await;
        for entry in conns.values() {
            let data = data_fn(&entry.message_address);
            let _ = entry.ws.send(data);
        }
    }

    pub async fn close_all_connections(&self, event: &hocuspocus_common::CloseEvent) {
        let conns = self.connections.read().await;
        for entry in conns.values() {
            let _ = entry.ws.close(event.code, &event.reason);
        }
    }

    pub async fn destroy(&self) {
        let mut destroyed = self.is_destroyed.write().await;
        *destroyed = true;
    }
}
