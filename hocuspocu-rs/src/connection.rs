use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use hocuspocus_common::{self as common, WsReadyState};

use crate::document::Document;
use crate::encoding::Decoder;
use crate::fragment::FragmentBuffer;
use crate::message_receiver::MessageReceiver;
use crate::outgoing_message::OutgoingMessage;
use crate::types::*;

pub struct Connection {
    pub id: String,
    pub socket_id: String,
    pub context: RwLock<Context>,
    pub document: Arc<Document>,
    pub request: RequestInfo,
    pub read_only: bool,
    pub session_id: Option<String>,
    pub provider_version: Option<String>,
    message_address: String,
    ws: Arc<dyn WebSocketSink>,
    message_queue: Mutex<VecDeque<Vec<u8>>>,
    processing: Mutex<()>,
    fragment_buffers: Mutex<HashMap<String, FragmentBuffer>>,

    on_close_callbacks:
        RwLock<Vec<Arc<dyn Fn(Arc<Document>, Option<common::CloseEvent>) + Send + Sync>>>,
    before_handle_message_callback: RwLock<
        Option<
            Arc<
                dyn Fn(
                        String,
                        Vec<u8>,
                    ) -> std::pin::Pin<
                        Box<
                            dyn std::future::Future<
                                    Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                                > + Send,
                        >,
                    > + Send
                    + Sync,
            >,
        >,
    >,
    before_sync_callback: RwLock<
        Option<
            Arc<
                dyn Fn(
                        String,
                        u64,
                        Vec<u8>,
                    ) -> std::pin::Pin<
                        Box<
                            dyn std::future::Future<
                                    Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                                > + Send,
                        >,
                    > + Send
                    + Sync,
            >,
        >,
    >,
    stateless_callback: RwLock<
        Option<
            Arc<
                dyn Fn(
                        OnStatelessPayload,
                    ) -> std::pin::Pin<
                        Box<
                            dyn std::future::Future<
                                    Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                                > + Send,
                        >,
                    > + Send
                    + Sync,
            >,
        >,
    >,
    token_sync_callback: RwLock<
        Option<
            Arc<
                dyn Fn(
                        String,
                    ) -> std::pin::Pin<
                        Box<
                            dyn std::future::Future<
                                    Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                                > + Send,
                        >,
                    > + Send
                    + Sync,
            >,
        >,
    >,
    closed: std::sync::atomic::AtomicBool,
}

impl Connection {
    pub fn new(
        id: String,
        ws: Arc<dyn WebSocketSink>,
        request: RequestInfo,
        document: Arc<Document>,
        socket_id: String,
        context: Context,
        read_only: bool,
        session_id: Option<String>,
        provider_version: Option<String>,
    ) -> Arc<Self> {
        let message_address = match &session_id {
            Some(sid) => format!("{}\0{}", document.name, sid),
            None => document.name.clone(),
        };

        Arc::new(Self {
            id,
            socket_id,
            context: RwLock::new(context),
            document,
            request,
            read_only,
            session_id,
            provider_version,
            message_address,
            ws,
            message_queue: Mutex::new(VecDeque::new()),
            processing: Mutex::new(()),
            fragment_buffers: Mutex::new(HashMap::new()),
            on_close_callbacks: RwLock::new(Vec::new()),
            before_handle_message_callback: RwLock::new(None),
            before_sync_callback: RwLock::new(None),
            stateless_callback: RwLock::new(None),
            token_sync_callback: RwLock::new(None),
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn message_address(&self) -> &str {
        &self.message_address
    }

    pub async fn on_close(
        &self,
        callback: Arc<dyn Fn(Arc<Document>, Option<common::CloseEvent>) + Send + Sync>,
    ) {
        let mut cbs = self.on_close_callbacks.write().await;
        cbs.push(callback);
    }

    pub async fn set_stateless_callback(
        &self,
        callback: Arc<
            dyn Fn(
                    OnStatelessPayload,
                ) -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                            > + Send,
                    >,
                > + Send
                + Sync,
        >,
    ) {
        let mut cb = self.stateless_callback.write().await;
        *cb = Some(callback);
    }

    pub async fn set_before_handle_message(
        &self,
        callback: Arc<
            dyn Fn(
                    String,
                    Vec<u8>,
                ) -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                            > + Send,
                    >,
                > + Send
                + Sync,
        >,
    ) {
        let mut cb = self.before_handle_message_callback.write().await;
        *cb = Some(callback);
    }

    pub async fn set_before_sync(
        &self,
        callback: Arc<
            dyn Fn(
                    String,
                    u64,
                    Vec<u8>,
                ) -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                            > + Send,
                    >,
                > + Send
                + Sync,
        >,
    ) {
        let mut cb = self.before_sync_callback.write().await;
        *cb = Some(callback);
    }

    pub async fn set_token_sync_callback(
        &self,
        callback: Arc<
            dyn Fn(
                    String,
                ) -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                            > + Send,
                    >,
                > + Send
                + Sync,
        >,
    ) {
        let mut cb = self.token_sync_callback.write().await;
        *cb = Some(callback);
    }

    pub fn send(&self, message: &[u8]) {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }

        let state = self.ws.ready_state();
        if state == WsReadyState::Closing || state == WsReadyState::Closed {
            self.mark_closed();
            return;
        }

        if self.ws.send(message.to_vec()).is_err() {
            self.mark_closed();
        }
    }

    fn mark_closed(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn send_stateless(&self, payload: &str) {
        let msg = OutgoingMessage::new(self.message_address()).write_stateless(payload);
        self.send(&msg.to_vec());
    }

    pub fn request_token(&self) {
        let msg = OutgoingMessage::new(self.message_address()).write_token_sync_request();
        self.send(&msg.to_vec());
    }

    pub async fn close(self: &Arc<Self>, event: Option<common::CloseEvent>) {
        if self.document.has_connection(&self.id).await {
            self.document.remove_connection(&self.id).await;

            let cbs = self.on_close_callbacks.read().await;
            for cb in cbs.iter() {
                cb(self.document.clone(), event.clone());
            }

            let reason = event
                .as_ref()
                .map(|e| e.reason.as_str())
                .unwrap_or("Server closed the connection");

            let close_msg =
                OutgoingMessage::new(self.message_address()).write_close_message(reason);
            self.send(&close_msg.to_vec());
            self.mark_closed();
        }
    }

    async fn send_current_awareness(&self) {
        if !self.document.has_awareness_states() {
            return;
        }
        if let Ok(update_data) = self.document.encode_awareness_update_all() {
            let msg = OutgoingMessage::new(self.message_address())
                .create_awareness_update_message(&update_data);
            self.send(&msg.to_vec());
        }
    }

    pub async fn init_awareness(self: &Arc<Self>) {
        self.send_current_awareness().await;
    }

    pub async fn get_before_sync_callback(
        &self,
    ) -> Option<
        Arc<
            dyn Fn(
                    String,
                    u64,
                    Vec<u8>,
                ) -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                            > + Send,
                    >,
                > + Send
                + Sync,
        >,
    > {
        let cb = self.before_sync_callback.read().await;
        cb.clone()
    }

    pub async fn handle_message(self: &Arc<Self>, data: Vec<u8>) {
        let should_start_processing = {
            let mut queue = self.message_queue.lock().await;
            queue.push_back(data);
            queue.len() == 1
        };

        if should_start_processing {
            self.process_messages().await;
        }
    }

    async fn process_messages(self: &Arc<Self>) {
        let _guard = self.processing.lock().await;

        loop {
            let raw_update = {
                let queue = self.message_queue.lock().await;
                match queue.front() {
                    Some(data) => data.clone(),
                    None => break,
                }
            };

            let mut decoder = Decoder::new(&raw_update);
            let raw_key = match decoder.read_var_string() {
                Ok(k) => k,
                Err(_) => {
                    let mut queue = self.message_queue.lock().await;
                    queue.pop_front();
                    continue;
                }
            };

            let sep_idx = raw_key.find('\0');
            let document_name = match sep_idx {
                Some(idx) => &raw_key[..idx],
                None => &raw_key,
            };

            if document_name != self.document.name {
                let mut queue = self.message_queue.lock().await;
                queue.pop_front();
                continue;
            }

            {
                let cb = self.before_handle_message_callback.read().await;
                if let Some(ref callback) = *cb {
                    if let Err(e) = callback(self.id.clone(), raw_update.clone()).await {
                        tracing::error!(
                            "closing connection {} because of exception: {:?}",
                            self.socket_id,
                            e
                        );
                        self.close(Some(common::reset_connection())).await;
                        let mut queue = self.message_queue.lock().await;
                        queue.pop_front();
                        continue;
                    }
                }
            }

            // ── Inbound fragment reassembly (types 100/101/102) ──
            // Unconditional: whether the client fragments is decided by the client's own
            // chunk size, so the server always reassembles. Fragment frames are consumed
            // here and never reach MessageReceiver; a completed series yields the original
            // frame, which is dispatched exactly like a normal message.
            let apply_bytes: Vec<u8> = {
                let mut fdec = Decoder::new(&raw_update);
                let _ = fdec.read_var_string(); // address (already validated above)
                match fdec.read_var_uint() {
                    Ok(t) if t == MessageType::FragmentStart as u64 => {
                        if let Ok(id) = fdec.read_var_string() {
                            let mut bufs = self.fragment_buffers.lock().await;
                            if bufs.contains_key(&id) {
                                tracing::warn!("FragmentStart for already-active fragment: {}", id);
                            }
                            bufs.insert(id, FragmentBuffer::new());
                        }
                        self.message_queue.lock().await.pop_front();
                        continue;
                    }
                    Ok(t) if t == MessageType::FragmentData as u64 => {
                        let id = fdec.read_var_string();
                        let index = fdec.read_var_uint();
                        let chunk = fdec.read_var_uint8_array();
                        if let (Ok(id), Ok(index), Ok(chunk)) = (id, index, chunk) {
                            let mut bufs = self.fragment_buffers.lock().await;
                            match bufs.get_mut(&id) {
                                Some(buf) => buf.add_chunk(index, chunk),
                                None => tracing::warn!("FragmentData for unknown fragment: {}", id),
                            }
                        }
                        self.message_queue.lock().await.pop_front();
                        continue;
                    }
                    Ok(t) if t == MessageType::FragmentEnd as u64 => {
                        let combined = if let Ok(id) = fdec.read_var_string() {
                            let mut bufs = self.fragment_buffers.lock().await;
                            match bufs.get_mut(&id) {
                                Some(buf) => {
                                    buf.mark_end();
                                    if buf.is_complete() {
                                        let bytes = buf.combine();
                                        bufs.remove(&id);
                                        Some(bytes)
                                    } else {
                                        None
                                    }
                                }
                                None => {
                                    tracing::warn!("FragmentEnd for unknown fragment: {}", id);
                                    None
                                }
                            }
                        } else {
                            None
                        };
                        match combined {
                            Some(bytes) => bytes, // fall through and dispatch the reassembled frame
                            None => {
                                self.message_queue.lock().await.pop_front();
                                continue;
                            }
                        }
                    }
                    _ => raw_update.clone(), // normal (non-fragment) message
                }
            };

            let receiver = MessageReceiver::new(apply_bytes, None);
            let result = receiver
                .apply(
                    &self.document,
                    Some(&self.id),
                    self.message_address(),
                    self.read_only,
                    None,
                    self.before_sync_callback.read().await.as_ref().map(|cb| {
                        let cb = cb.clone();
                        let id = self.id.clone();
                        move |sync_type: u64,
                              payload: Vec<u8>|
                              -> std::pin::Pin<
                            Box<
                                dyn std::future::Future<
                                        Output = Result<
                                            (),
                                            Box<dyn std::error::Error + Send + Sync>,
                                        >,
                                    > + Send,
                            >,
                        > {
                            let cb = cb.clone();
                            let id = id.clone();
                            Box::pin(async move { cb(id, sync_type, payload).await })
                        }
                    }),
                )
                .await;

            if let Err(e) = result {
                let err_msg = e.to_string();

                if let Some(payload) = err_msg.strip_prefix("stateless:") {
                    let cb = self.stateless_callback.read().await;
                    if let Some(ref callback) = *cb {
                        let sp = OnStatelessPayload {
                            connection_id: self.id.clone(),
                            document_name: self.document.name.clone(),
                            document: self.document.clone(),
                            payload: payload.to_string(),
                        };
                        let _ = callback(sp).await;
                    }
                } else if let Some(token) = err_msg.strip_prefix("token_sync:") {
                    // Clone the callback out and drop the read guard before awaiting,
                    // so we don't hold the RwLock across close().
                    let callback = self.token_sync_callback.read().await.clone();
                    if let Some(callback) = callback {
                        if let Err(e) = callback(token.to_string()).await {
                            // matches TS ClientConnection.onTokenSyncCallback: a rejected
                            // token closes the connection with Unauthorized.
                            tracing::error!("onTokenSync rejected token: {:?}", e);
                            self.close(Some(common::unauthorized())).await;
                        }
                    }
                } else if err_msg == "close_requested" {
                    self.close(Some(common::CloseEvent {
                        code: 1000,
                        reason: "provider_initiated".to_string(),
                    }))
                    .await;
                } else {
                    tracing::error!(
                        "closing connection {} because of exception: {}",
                        self.socket_id,
                        err_msg
                    );
                    self.close(Some(common::reset_connection())).await;
                }
            }

            {
                let mut queue = self.message_queue.lock().await;
                queue.pop_front();
            }
        }
    }

    pub async fn wait_for_pending_messages(&self) {
        let _guard = self.processing.lock().await;
    }
}
