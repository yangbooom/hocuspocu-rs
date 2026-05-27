use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, Transact, Update};

use crate::encoding;
use crate::types::*;

pub struct Document {
    pub doc: Doc,
    pub name: String,
    pub is_loading: RwLock<bool>,
    pub is_destroyed: RwLock<bool>,
    pub last_change_time: RwLock<u64>,
    pub direct_connections_count: RwLock<usize>,
    pub save_mutex: Mutex<()>,

    connections: RwLock<HashMap<String, ConnectionEntry>>,

    awareness_states: RwLock<HashMap<u64, HashMap<String, serde_json::Value>>>,

    on_update_callback: RwLock<
        Option<
            Arc<dyn Fn(Arc<Document>, Option<TransactionOrigin>, Vec<u8>) + Send + Sync>,
        >,
    >,
    before_broadcast_stateless_callback:
        RwLock<Option<Arc<dyn Fn(Arc<Document>, String) + Send + Sync>>>,
    before_handle_awareness_callback: RwLock<
        Option<
            Arc<
                dyn Fn(
                        Arc<Document>,
                        HashMap<u64, HashMap<String, serde_json::Value>>,
                        Option<TransactionOrigin>,
                    )
                        -> std::pin::Pin<
                        Box<
                            dyn std::future::Future<
                                    Output = Result<
                                        HashMap<u64, HashMap<String, serde_json::Value>>,
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
        Self {
            doc,
            name: name.to_string(),
            is_loading: RwLock::new(true),
            is_destroyed: RwLock::new(false),
            last_change_time: RwLock::new(0),
            direct_connections_count: RwLock::new(0),
            save_mutex: Mutex::new(()),
            connections: RwLock::new(HashMap::new()),
            awareness_states: RwLock::new(HashMap::new()),
            on_update_callback: RwLock::new(None),
            before_broadcast_stateless_callback: RwLock::new(None),
            before_handle_awareness_callback: RwLock::new(None),
        }
    }

    pub fn on_update(
        &self,
        callback: Arc<dyn Fn(Arc<Document>, Option<TransactionOrigin>, Vec<u8>) + Send + Sync>,
    ) {
        let mut cb = self.on_update_callback.blocking_write();
        *cb = Some(callback);
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
                    HashMap<u64, HashMap<String, serde_json::Value>>,
                    Option<TransactionOrigin>,
                )
                    -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<
                                    HashMap<u64, HashMap<String, serde_json::Value>>,
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
            let mut states = self.awareness_states.write().await;
            for client_id in &entry.clients {
                states.remove(client_id);
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

    pub async fn has_awareness_states(&self) -> bool {
        let states = self.awareness_states.read().await;
        !states.is_empty()
    }

    pub async fn get_awareness_states(
        &self,
    ) -> HashMap<u64, HashMap<String, serde_json::Value>> {
        let states = self.awareness_states.read().await;
        states.clone()
    }

    pub async fn set_awareness_states(
        &self,
        new_states: HashMap<u64, HashMap<String, serde_json::Value>>,
    ) {
        let mut states = self.awareness_states.write().await;
        *states = new_states;
    }

    pub async fn apply_awareness_update(
        self: &Arc<Self>,
        update_data: &[u8],
        origin: Option<TransactionOrigin>,
        connection_id: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let incoming_states = decode_awareness_update(update_data)?;

        let before_cb = {
            let cb = self.before_handle_awareness_callback.read().await;
            cb.clone()
        };

        let processed_states = if let Some(ref callback) = before_cb {
            callback(self.clone(), incoming_states.clone(), origin.clone()).await?
        } else {
            incoming_states.clone()
        };

        let mut added = Vec::new();
        let mut updated = Vec::new();
        let removed = Vec::new();

        {
            let mut states = self.awareness_states.write().await;

            for (client_id, new_state) in &processed_states {
                if states.contains_key(client_id) {
                    updated.push(*client_id);
                } else {
                    added.push(*client_id);
                }
                states.insert(*client_id, new_state.clone());
            }

            if let Some(conn_id) = connection_id {
                let mut conns = self.connections.write().await;
                if let Some(entry) = conns.get_mut(conn_id) {
                    for client_id in &added {
                        entry.clients.insert(*client_id);
                    }
                    for client_id in &removed {
                        entry.clients.remove(client_id);
                    }
                }
            }
        }

        let changed_clients: Vec<u64> = added
            .iter()
            .chain(updated.iter())
            .chain(removed.iter())
            .copied()
            .collect();

        self.broadcast_awareness_update(&changed_clients).await;

        Ok(())
    }

    pub async fn broadcast_awareness_update(&self, changed_clients: &[u64]) {
        let states = self.awareness_states.read().await;
        let conns = self.connections.read().await;

        let update = encode_awareness_update(&states, changed_clients);

        for entry in conns.values() {
            let mut msg = Vec::new();
            encoding::write_var_string(&mut msg, &entry.message_address);
            encoding::write_var_uint(&mut msg, MessageType::Awareness as u64);
            encoding::write_var_uint8_array(&mut msg, &update);
            let _ = entry.ws.send(msg);
        }
    }

    pub fn encode_state_as_update(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    }

    pub fn encode_state_vector(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        txn.state_vector().encode_v1()
    }

    pub fn apply_update(&self, update: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut txn = self.doc.transact_mut();
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

    pub async fn broadcast_stateless(&self, payload: &str, filter: Option<Box<dyn Fn(&str) -> bool + Send + Sync>>) {
        {
            let cb = self.before_broadcast_stateless_callback.read().await;
            if cb.is_some() {
                // Callback invoked by caller with Arc<Self>
            }
        }

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

fn encode_awareness_update(
    states: &HashMap<u64, HashMap<String, serde_json::Value>>,
    clients: &[u64],
) -> Vec<u8> {
    let mut buf = Vec::new();
    let filtered: Vec<_> = clients
        .iter()
        .filter_map(|id| states.get(id).map(|s| (*id, s)))
        .collect();

    encoding::write_var_uint(&mut buf, filtered.len() as u64);
    for (client_id, state) in filtered {
        encoding::write_var_uint(&mut buf, client_id);
        let clock = state
            .get("__clock")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        encoding::write_var_uint(&mut buf, clock);
        let json = serde_json::to_string(state).unwrap_or_else(|_| "{}".to_string());
        encoding::write_var_string(&mut buf, &json);
    }
    buf
}

fn decode_awareness_update(
    data: &[u8],
) -> Result<
    HashMap<u64, HashMap<String, serde_json::Value>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mut decoder = encoding::Decoder::new(data);
    let len = decoder.read_var_uint()? as usize;
    let mut states = HashMap::new();
    for _ in 0..len {
        let client_id = decoder.read_var_uint()?;
        let _clock = decoder.read_var_uint()?;
        let json_str = decoder.read_var_string()?;
        let state: HashMap<String, serde_json::Value> = if json_str == "null" || json_str.is_empty()
        {
            HashMap::new()
        } else {
            serde_json::from_str(&json_str).unwrap_or_default()
        };
        states.insert(client_id, state);
    }
    Ok(states)
}
