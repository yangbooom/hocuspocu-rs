use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};

use hocuspocus_common::{self as common, WsReadyState};

use crate::connection::Connection;
use crate::document::Document;
use crate::encoding::Decoder;
use crate::fragment::ChunkingSink;
use crate::hocuspocus::Hocuspocus;
use crate::outgoing_message::OutgoingMessage;
use crate::types::*;

pub struct ClientConnection {
    websocket: Arc<dyn WebSocketSink>,
    request: RequestInfo,
    socket_id: String,
    default_context: Context,

    document_connections: RwLock<HashMap<String, Arc<Connection>>>,
    incoming_message_queue: RwLock<HashMap<String, Vec<Vec<u8>>>>,
    document_connections_established: RwLock<std::collections::HashSet<String>>,
    hook_payloads: RwLock<HashMap<String, HookPayloadEntry>>,
    on_close_callbacks: RwLock<Vec<Arc<dyn Fn(Arc<Document>, OnDisconnectPayload) + Send + Sync>>>,

    last_message_received_at: RwLock<Instant>,
    ping_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,

    hocuspocus: Arc<Hocuspocus>,
}

#[derive(Clone)]
struct HookPayloadEntry {
    request: RequestInfo,
    connection_config: ConnectionConfiguration,
    socket_id: String,
    context: Context,
    provider_version: Option<String>,
}

impl ClientConnection {
    pub fn new(
        websocket: Arc<dyn WebSocketSink>,
        request: RequestInfo,
        hocuspocus: Arc<Hocuspocus>,
        timeout: u64,
        default_context: Context,
    ) -> Arc<Self> {
        let socket_id = uuid::Uuid::new_v4().to_string();

        let cc = Arc::new(Self {
            websocket,
            request,
            socket_id,
            default_context,
            document_connections: RwLock::new(HashMap::new()),
            incoming_message_queue: RwLock::new(HashMap::new()),
            document_connections_established: RwLock::new(std::collections::HashSet::new()),
            hook_payloads: RwLock::new(HashMap::new()),
            on_close_callbacks: RwLock::new(Vec::new()),
            last_message_received_at: RwLock::new(Instant::now()),
            ping_handle: Mutex::new(None),
            hocuspocus,
        });

        let cc_clone = cc.clone();
        let timeout_ms = timeout;
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
                let last = *cc_clone.last_message_received_at.read().await;
                if last.elapsed() > Duration::from_millis(timeout_ms) {
                    cc_clone.close_all(Some(common::connection_timeout())).await;
                    break;
                }
            }
        });

        // Store handle directly - Mutex is uncontended at construction time
        {
            let mut ph = cc
                .ping_handle
                .try_lock()
                .expect("ping_handle uncontended at construction");
            *ph = Some(handle);
        }

        cc
    }

    pub async fn on_close(
        &self,
        callback: Arc<dyn Fn(Arc<Document>, OnDisconnectPayload) + Send + Sync>,
    ) {
        let mut cbs = self.on_close_callbacks.write().await;
        cbs.push(callback);
    }

    pub async fn handle_close(&self, event: Option<common::CloseEvent>) {
        self.close_all(event).await;
        let mut ph = self.ping_handle.lock().await;
        if let Some(handle) = ph.take() {
            handle.abort();
        }
    }

    async fn close_all(&self, event: Option<common::CloseEvent>) {
        let conns = self.document_connections.read().await;
        for conn in conns.values() {
            conn.close(event.clone()).await;
        }
    }

    pub async fn handle_message(self: &Arc<Self>, data: Vec<u8>) {
        {
            let mut last = self.last_message_received_at.write().await;
            *last = Instant::now();
        }

        let mut dec = Decoder::new(&data);
        let raw_key = match dec.read_var_string() {
            Ok(k) => k,
            Err(_) => {
                let _ = self
                    .websocket
                    .close(common::unauthorized().code, &common::unauthorized().reason);
                return;
            }
        };

        let sep_idx = raw_key.find('\0');
        let document_name = match sep_idx {
            Some(idx) => raw_key[..idx].to_string(),
            None => raw_key.clone(),
        };

        // Check if we already have a connection for this raw key
        {
            let conns = self.document_connections.read().await;
            if let Some(conn) = conns.get(&raw_key).or_else(|| conns.get(&document_name)) {
                conn.handle_message(data).await;
                return;
            }
        }

        // First message for this document - set up queuing
        let is_first = {
            let mut queue = self.incoming_message_queue.write().await;
            if queue.contains_key(&raw_key) {
                false
            } else {
                queue.insert(raw_key.clone(), Vec::new());
                true
            }
        };

        if is_first {
            let mut payloads = self.hook_payloads.write().await;
            payloads.insert(
                raw_key.clone(),
                HookPayloadEntry {
                    request: self.request.clone(),
                    connection_config: ConnectionConfiguration::default(),
                    socket_id: self.socket_id.clone(),
                    context: self.default_context.clone(),
                    provider_version: None,
                },
            );
        }

        self.handle_queueing_message(data, &raw_key, &document_name)
            .await;
    }

    async fn handle_queueing_message(
        self: &Arc<Self>,
        data: Vec<u8>,
        raw_key: &str,
        document_name: &str,
    ) {
        let mut dec = Decoder::new(&data);
        let _ = dec.read_var_string(); // skip address
        let msg_type = match dec.read_var_uint() {
            Ok(t) => t,
            Err(_) => {
                let _ = self.websocket.close(
                    common::reset_connection().code,
                    &common::reset_connection().reason,
                );
                return;
            }
        };

        let is_auth = msg_type == MessageType::Auth as u64;
        let already_established = {
            let established = self.document_connections_established.read().await;
            established.contains(raw_key)
        };

        if !is_auth || already_established {
            let mut queue = self.incoming_message_queue.write().await;
            if let Some(q) = queue.get_mut(raw_key) {
                q.push(data);
            }
            return;
        }

        // Mark this document as established
        {
            let mut established = self.document_connections_established.write().await;
            established.insert(raw_key.to_string());
        }

        // Read auth message
        let _auth_sub_type = match dec.read_var_uint() {
            Ok(t) => t,
            Err(_) => return,
        };
        let token = match dec.read_var_string() {
            Ok(t) => t,
            Err(_) => return,
        };

        let provider_version = if dec.has_content() {
            dec.read_var_string().ok()
        } else {
            None
        };

        let sep_idx = raw_key.find('\0');
        let session_id = sep_idx.map(|idx| raw_key[idx + 1..].to_string());

        let response_address = raw_key.to_string();

        // Get hook payload
        let mut hook_payload = {
            let payloads = self.hook_payloads.read().await;
            match payloads.get(raw_key) {
                Some(p) => p.clone(),
                None => return,
            }
        };

        hook_payload.provider_version = provider_version.clone();

        // Run onConnect hook
        let on_connect_payload = OnConnectPayload {
            context: hook_payload.context.clone(),
            document_name: document_name.to_string(),
            request: hook_payload.request.clone(),
            socket_id: hook_payload.socket_id.clone(),
            connection_config: hook_payload.connection_config.clone(),
            provider_version: provider_version.clone(),
        };

        match self.hocuspocus.hooks_on_connect(&on_connect_payload).await {
            Ok(Some(ctx)) => hook_payload.context = ctx,
            Err(e) => {
                let msg =
                    OutgoingMessage::new(&response_address).write_permission_denied(&e.to_string());
                let _ = self.websocket.send(msg.to_vec());
                self.cleanup_document_state(raw_key).await;
                return;
            }
            _ => {}
        }

        // Run onAuthenticate hook
        let on_auth_payload = OnAuthenticatePayload {
            context: hook_payload.context.clone(),
            document_name: document_name.to_string(),
            request: hook_payload.request.clone(),
            socket_id: hook_payload.socket_id.clone(),
            token: token.clone(),
            connection_config: hook_payload.connection_config.clone(),
            provider_version: provider_version.clone(),
        };

        match self
            .hocuspocus
            .hooks_on_authenticate(&on_auth_payload)
            .await
        {
            Ok(Some(ctx)) => hook_payload.context = ctx,
            Err(e) => {
                let reason = e.to_string();
                let msg = OutgoingMessage::new(&response_address).write_permission_denied(
                    if reason.is_empty() {
                        "permission-denied"
                    } else {
                        &reason
                    },
                );
                let _ = self.websocket.send(msg.to_vec());
                self.cleanup_document_state(raw_key).await;
                return;
            }
            _ => {}
        }

        hook_payload.connection_config.is_authenticated = true;

        // Update stored payload
        {
            let mut payloads = self.hook_payloads.write().await;
            payloads.insert(raw_key.to_string(), hook_payload.clone());
        }

        // Send authenticated message
        let auth_msg = OutgoingMessage::new(&response_address)
            .write_authenticated(hook_payload.connection_config.read_only);
        let _ = self.websocket.send(auth_msg.to_vec());

        // Set up connection
        self.setup_new_connection(raw_key, document_name, session_id)
            .await;
    }

    async fn setup_new_connection(
        self: &Arc<Self>,
        raw_key: &str,
        document_name: &str,
        session_id: Option<String>,
    ) {
        let hook_payload = {
            let payloads = self.hook_payloads.read().await;
            match payloads.get(raw_key) {
                Some(p) => p.clone(),
                None => return,
            }
        };

        let document = match self
            .hocuspocus
            .create_document(
                document_name,
                &hook_payload.request,
                &hook_payload.socket_id,
                &hook_payload.connection_config,
                Some(hook_payload.context.clone()),
            )
            .await
        {
            Ok(doc) => doc,
            Err(e) => {
                // matches TS: a createDocument failure surfaces as a PermissionDenied
                // frame to the client before the connection state is cleaned up.
                tracing::error!("Failed to create document: {:?}", e);
                let reason = e.to_string();
                let msg =
                    OutgoingMessage::new(raw_key).write_permission_denied(if reason.is_empty() {
                        "permission-denied"
                    } else {
                        &reason
                    });
                let _ = self.websocket.send(msg.to_vec());
                self.cleanup_document_state(raw_key).await;
                return;
            }
        };

        let conn_id = uuid::Uuid::new_v4().to_string();
        let message_address = match &session_id {
            Some(sid) => format!("{}\0{}", document_name, sid),
            None => document_name.to_string(),
        };

        let chunk_size = self.hocuspocus.configuration.read().await.message_chunk_size;
        let sink: Arc<dyn WebSocketSink> = if chunk_size > 0 {
            Arc::new(ChunkingSink::new(self.websocket.clone(), chunk_size))
        } else {
            self.websocket.clone()
        };

        document
            .add_connection(
                &conn_id,
                &hook_payload.socket_id,
                hook_payload.connection_config.read_only,
                &message_address,
                sink.clone(),
            )
            .await;

        let connection = Connection::new(
            conn_id.clone(),
            sink,
            hook_payload.request.clone(),
            document.clone(),
            hook_payload.socket_id.clone(),
            hook_payload.context.clone(),
            hook_payload.connection_config.read_only,
            session_id,
            hook_payload.provider_version.clone(),
        );

        // Set up close callback - use Weak to break the Arc cycle:
        // ClientConnection -> document_connections -> Connection -> on_close_callbacks -> closure
        let hocuspocus = self.hocuspocus.clone();
        let raw_key_owned = raw_key.to_string();
        let self_weak = Arc::downgrade(self);
        let conn_weak = Arc::downgrade(&connection);
        connection
            .on_close(Arc::new(move |doc, _event| {
                let hp = hocuspocus.clone();
                let rk = raw_key_owned.clone();
                let sw = self_weak.clone();
                let cw = conn_weak.clone();

                tokio::spawn(async move {
                    let cr = match cw.upgrade() {
                        Some(c) => c,
                        None => return,
                    };
                    let sr = match sw.upgrade() {
                        Some(s) => s,
                        None => return,
                    };
                    cr.wait_for_pending_messages().await;

                    let ctx = cr.context.read().await;
                    let disconnect_payload = OnDisconnectPayload {
                        clients_count: doc.get_connections_count().await,
                        context: ctx.clone(),
                        document: doc.clone(),
                        socket_id: cr.socket_id.clone(),
                        document_name: doc.name.clone(),
                        request: cr.request.clone(),
                    };

                    let _ = hp.hooks_on_disconnect(&disconnect_payload).await;

                    let cbs = sr.on_close_callbacks.read().await;
                    for cb in cbs.iter() {
                        cb(doc.clone(), disconnect_payload.clone());
                    }

                    // Cleanup
                    {
                        let mut payloads = sr.hook_payloads.write().await;
                        payloads.remove(&rk);
                    }
                    {
                        let mut conns = sr.document_connections.write().await;
                        conns.remove(&rk);
                    }
                    {
                        let mut queue = sr.incoming_message_queue.write().await;
                        queue.remove(&rk);
                    }
                    {
                        let mut established = sr.document_connections_established.write().await;
                        established.remove(&rk);
                    }
                });
            }))
            .await;

        // Set up token sync callback
        let hp_clone = self.hocuspocus.clone();
        let hook_payload_clone = hook_payload.clone();
        let doc_clone = document.clone();
        let conn_id_for_token = connection.id.clone();
        let doc_name = document_name.to_string();
        connection
            .set_token_sync_callback(Arc::new(move |token: String| {
                let hp = hp_clone.clone();
                let hp2 = hook_payload_clone.clone();
                let dc = doc_clone.clone();
                let cid = conn_id_for_token.clone();
                let dn = doc_name.clone();
                Box::pin(async move {
                    let payload = OnTokenSyncPayload {
                        context: hp2.context.clone(),
                        document_name: dn,
                        document: dc,
                        request: hp2.request.clone(),
                        socket_id: hp2.socket_id.clone(),
                        token,
                        connection_config: hp2.connection_config.clone(),
                        connection_id: cid,
                    };
                    hp.hooks_on_token_sync(&payload).await.map(|_| ())
                })
            }))
            .await;

        // Set up beforeHandleMessage callback
        let hp_bhm = self.hocuspocus.clone();
        let hp_bhm2 = hook_payload.clone();
        let doc_bhm = document.clone();
        connection
            .set_before_handle_message(Arc::new(move |conn_id: String, update: Vec<u8>| {
                let hp = hp_bhm.clone();
                let hp2 = hp_bhm2.clone();
                let dc = doc_bhm.clone();
                let cid = conn_id;
                Box::pin(async move {
                    let payload = BeforeHandleMessagePayload {
                        clients_count: dc.get_connections_count().await,
                        context: hp2.context.clone(),
                        document: dc.clone(),
                        document_name: dc.name.clone(),
                        request: hp2.request.clone(),
                        update,
                        socket_id: hp2.socket_id.clone(),
                        connection_id: cid,
                    };
                    hp.hooks_before_handle_message(&payload).await.map(|_| ())
                })
            }))
            .await;

        // Set up beforeSync callback
        let hp_bs = self.hocuspocus.clone();
        let hp_bs2 = hook_payload.clone();
        let doc_bs = document.clone();
        connection
            .set_before_sync(Arc::new(
                move |conn_id: String, sync_type: u64, payload: Vec<u8>| {
                    let hp = hp_bs.clone();
                    let hp2 = hp_bs2.clone();
                    let dc = doc_bs.clone();
                    let cid = conn_id;
                    Box::pin(async move {
                        let payload = BeforeSyncPayload {
                            clients_count: dc.get_connections_count().await,
                            context: hp2.context.clone(),
                            document: dc.clone(),
                            document_name: dc.name.clone(),
                            connection_id: cid,
                            sync_type,
                            payload,
                        };
                        hp.hooks_before_sync(&payload).await.map(|_| ())
                    })
                },
            ))
            .await;

        // Set up stateless callback
        let hp_sc = self.hocuspocus.clone();
        connection
            .set_stateless_callback(Arc::new(move |payload: OnStatelessPayload| {
                let hp = hp_sc.clone();
                Box::pin(async move { hp.hooks_on_stateless(&payload).await.map(|_| ()) })
            }))
            .await;

        // Store connection
        {
            let mut conns = self.document_connections.write().await;
            conns.insert(raw_key.to_string(), connection.clone());
        }

        // Check if websocket already disconnected
        let ws_state = self.websocket.ready_state();
        if ws_state == WsReadyState::Closing || ws_state == WsReadyState::Closed {
            self.close_all(None).await;
            return;
        }

        // Initialize awareness
        connection.init_awareness().await;

        // Drain queued messages
        let queued = {
            let mut queue = self.incoming_message_queue.write().await;
            queue.remove(raw_key).unwrap_or_default()
        };

        for msg in queued {
            connection.handle_message(msg).await;
        }

        // Run connected hook
        let connected_payload = ConnectedPayload {
            context: hook_payload.context.clone(),
            document_name: document_name.to_string(),
            request: hook_payload.request.clone(),
            socket_id: hook_payload.socket_id.clone(),
            connection_config: hook_payload.connection_config.clone(),
            connection_id: conn_id,
            provider_version: hook_payload.provider_version.clone(),
        };

        let _ = self.hocuspocus.hooks_connected(&connected_payload).await;
    }

    async fn cleanup_document_state(&self, raw_key: &str) {
        {
            let mut established = self.document_connections_established.write().await;
            established.remove(raw_key);
        }
        {
            let mut payloads = self.hook_payloads.write().await;
            payloads.remove(raw_key);
        }
        {
            let mut queue = self.incoming_message_queue.write().await;
            queue.remove(raw_key);
        }
    }
}
