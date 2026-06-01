use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::document::Document;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum MessageType {
    Unknown = -1,
    Sync = 0,
    Awareness = 1,
    Auth = 2,
    QueryAwareness = 3,
    SyncReply = 4,
    Stateless = 5,
    BroadcastStateless = 6,
    Close = 7,
    SyncStatus = 8,
    Ping = 9,
    Pong = 10,
    FragmentStart = 100,
    FragmentData = 101,
    FragmentEnd = 102,
}

impl TryFrom<u64> for MessageType {
    type Error = ();
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageType::Sync),
            1 => Ok(MessageType::Awareness),
            2 => Ok(MessageType::Auth),
            3 => Ok(MessageType::QueryAwareness),
            4 => Ok(MessageType::SyncReply),
            5 => Ok(MessageType::Stateless),
            6 => Ok(MessageType::BroadcastStateless),
            7 => Ok(MessageType::Close),
            8 => Ok(MessageType::SyncStatus),
            9 => Ok(MessageType::Ping),
            10 => Ok(MessageType::Pong),
            100 => Ok(MessageType::FragmentStart),
            101 => Ok(MessageType::FragmentData),
            102 => Ok(MessageType::FragmentEnd),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AwarenessUpdate {
    pub added: Vec<u64>,
    pub updated: Vec<u64>,
    pub removed: Vec<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct ConnectionConfiguration {
    pub read_only: bool,
    pub is_authenticated: bool,
}

#[derive(Debug, Clone)]
pub enum TransactionOrigin {
    Connection(ConnectionTransactionOrigin),
    Redis,
    Local(LocalTransactionOrigin),
}

#[derive(Debug, Clone)]
pub struct ConnectionTransactionOrigin {
    pub connection_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct LocalTransactionOrigin {
    pub skip_store_hooks: bool,
    pub context: Option<Arc<dyn Any + Send + Sync>>,
}

pub fn is_transaction_origin(origin: &Option<TransactionOrigin>) -> bool {
    origin.is_some()
}

pub fn should_skip_store_hooks(origin: &Option<TransactionOrigin>) -> bool {
    match origin {
        Some(TransactionOrigin::Connection(_)) => false,
        Some(TransactionOrigin::Redis) => true,
        Some(TransactionOrigin::Local(local)) => local.skip_store_hooks,
        None => false,
    }
}

pub type Context = Arc<dyn Any + Send + Sync>;

pub fn empty_context() -> Context {
    Arc::new(())
}

#[derive(Debug, Clone, Default)]
pub struct RequestInfo {
    pub headers: HashMap<String, String>,
    pub parameters: HashMap<String, String>,
    pub url: String,
}

impl RequestInfo {
    pub fn from_url(url: &str) -> Self {
        let parameters = get_parameters(url);
        Self {
            headers: HashMap::new(),
            parameters,
            url: url.to_string(),
        }
    }

    pub fn localhost() -> Self {
        Self {
            headers: HashMap::new(),
            parameters: HashMap::new(),
            url: "http://localhost".to_string(),
        }
    }
}

pub fn get_parameters(url: &str) -> HashMap<String, String> {
    if let Some(query_pos) = url.find('?') {
        let query = &url[query_pos + 1..];
        url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect()
    } else {
        HashMap::new()
    }
}

// ──── Hook Payload Types ────

#[derive(Clone)]
pub struct OnConfigurePayload {
    pub version: String,
}

pub struct OnListenPayload {
    pub port: u16,
}

pub struct OnConnectPayload {
    pub context: Context,
    pub document_name: String,
    pub request: RequestInfo,
    pub socket_id: String,
    pub connection_config: ConnectionConfiguration,
    pub provider_version: Option<String>,
}

pub struct ConnectedPayload {
    pub context: Context,
    pub document_name: String,
    pub request: RequestInfo,
    pub socket_id: String,
    pub connection_config: ConnectionConfiguration,
    pub connection_id: String,
    pub provider_version: Option<String>,
}

pub struct OnAuthenticatePayload {
    pub context: Context,
    pub document_name: String,
    pub request: RequestInfo,
    pub socket_id: String,
    pub token: String,
    pub connection_config: ConnectionConfiguration,
    pub provider_version: Option<String>,
}

pub struct OnTokenSyncPayload {
    pub context: Context,
    pub document_name: String,
    pub document: Arc<Document>,
    pub request: RequestInfo,
    pub socket_id: String,
    pub token: String,
    pub connection_config: ConnectionConfiguration,
    pub connection_id: String,
}

pub struct OnCreateDocumentPayload {
    pub context: Context,
    pub document_name: String,
    pub request: RequestInfo,
    pub socket_id: String,
    pub connection_config: ConnectionConfiguration,
}

pub struct OnLoadDocumentPayload {
    pub context: Context,
    pub document: Arc<Document>,
    pub document_name: String,
    pub request: RequestInfo,
    pub socket_id: String,
    pub connection_config: ConnectionConfiguration,
}

pub struct AfterLoadDocumentPayload {
    pub context: Context,
    pub document: Arc<Document>,
    pub document_name: String,
    pub request: RequestInfo,
    pub socket_id: String,
    pub connection_config: ConnectionConfiguration,
}

pub struct BeforeHandleMessagePayload {
    pub clients_count: usize,
    pub context: Context,
    pub document: Arc<Document>,
    pub document_name: String,
    pub request: RequestInfo,
    pub update: Vec<u8>,
    pub socket_id: String,
    pub connection_id: String,
}

pub struct BeforeHandleAwarenessPayload {
    pub clients_count: usize,
    pub context: Option<Context>,
    pub document: Arc<Document>,
    pub document_name: String,
    pub request: RequestInfo,
    pub states: HashMap<u64, HashMap<String, serde_json::Value>>,
    pub socket_id: String,
    pub transaction_origin: Option<TransactionOrigin>,
    pub connection_id: Option<String>,
}

pub struct BeforeSyncPayload {
    pub clients_count: usize,
    pub context: Context,
    pub document: Arc<Document>,
    pub document_name: String,
    pub connection_id: String,
    pub sync_type: u64,
    pub payload: Vec<u8>,
}

pub struct BeforeBroadcastStatelessPayload {
    pub document_name: String,
    pub payload: String,
}

pub struct OnStatelessPayload {
    pub connection_id: String,
    pub document_name: String,
    pub document: Arc<Document>,
    pub payload: String,
}

#[derive(Clone)]
pub struct OnChangePayload {
    pub clients_count: usize,
    pub context: Context,
    pub document: Arc<Document>,
    pub document_name: String,
    pub request: RequestInfo,
    pub update: Vec<u8>,
    pub socket_id: String,
    pub transaction_origin: Option<TransactionOrigin>,
    pub connection_id: Option<String>,
}

#[derive(Clone)]
pub struct OnStoreDocumentPayload {
    pub clients_count: usize,
    pub document: Arc<Document>,
    pub last_context: Context,
    pub last_transaction_origin: Option<TransactionOrigin>,
    pub document_name: String,
}

pub type AfterStoreDocumentPayload = OnStoreDocumentPayload;

pub struct OnAwarenessUpdatePayload {
    pub document: Arc<Document>,
    pub document_name: String,
    pub transaction_origin: Option<TransactionOrigin>,
    pub connection_id: Option<String>,
    pub added: Vec<u64>,
    pub updated: Vec<u64>,
    pub removed: Vec<u64>,
}

#[derive(Clone)]
pub struct OnDisconnectPayload {
    pub clients_count: usize,
    pub context: Context,
    pub document: Arc<Document>,
    pub document_name: String,
    pub request: RequestInfo,
    pub socket_id: String,
}

pub struct OnRequestPayload {
    pub request: RequestInfo,
}

pub struct OnUpgradePayload {
    pub request: RequestInfo,
}

pub struct OnDestroyPayload {}

pub struct BeforeUnloadDocumentPayload {
    pub document_name: String,
    pub document: Arc<Document>,
}

pub struct AfterUnloadDocumentPayload {
    pub document_name: String,
}

// ──── Extension Trait ────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookName {
    OnConfigure,
    OnListen,
    OnUpgrade,
    OnConnect,
    Connected,
    OnAuthenticate,
    OnTokenSync,
    OnCreateDocument,
    OnLoadDocument,
    AfterLoadDocument,
    BeforeHandleMessage,
    BeforeHandleAwareness,
    BeforeBroadcastStateless,
    BeforeSync,
    OnStateless,
    OnChange,
    OnStoreDocument,
    AfterStoreDocument,
    OnAwarenessUpdate,
    OnRequest,
    OnDisconnect,
    BeforeUnloadDocument,
    AfterUnloadDocument,
    OnDestroy,
}

pub type HookResult = Result<Option<Context>, Box<dyn std::error::Error + Send + Sync>>;
pub type LoadDocumentResult =
    Result<Option<LoadedDocument>, Box<dyn std::error::Error + Send + Sync>>;

pub enum LoadedDocument {
    Update(Vec<u8>),
}

#[async_trait]
pub trait Extension: Send + Sync {
    fn priority(&self) -> i32 {
        100
    }

    fn name(&self) -> &str {
        "Extension"
    }

    async fn on_configure(&self, _payload: &OnConfigurePayload) -> HookResult {
        Ok(None)
    }

    async fn on_listen(&self, _payload: &OnListenPayload) -> HookResult {
        Ok(None)
    }

    async fn on_upgrade(&self, _payload: &OnUpgradePayload) -> HookResult {
        Ok(None)
    }

    async fn on_connect(&self, _payload: &OnConnectPayload) -> HookResult {
        Ok(None)
    }

    async fn connected(&self, _payload: &ConnectedPayload) -> HookResult {
        Ok(None)
    }

    async fn on_authenticate(&self, _payload: &OnAuthenticatePayload) -> HookResult {
        Ok(None)
    }

    async fn on_token_sync(&self, _payload: &OnTokenSyncPayload) -> HookResult {
        Ok(None)
    }

    async fn on_create_document(&self, _payload: &OnCreateDocumentPayload) -> HookResult {
        Ok(None)
    }

    async fn on_load_document(&self, _payload: &OnLoadDocumentPayload) -> LoadDocumentResult {
        Ok(None)
    }

    async fn after_load_document(&self, _payload: &AfterLoadDocumentPayload) -> HookResult {
        Ok(None)
    }

    async fn before_handle_message(&self, _payload: &BeforeHandleMessagePayload) -> HookResult {
        Ok(None)
    }

    async fn before_handle_awareness(
        &self,
        _payload: &mut BeforeHandleAwarenessPayload,
    ) -> HookResult {
        Ok(None)
    }

    async fn before_sync(&self, _payload: &BeforeSyncPayload) -> HookResult {
        Ok(None)
    }

    async fn before_broadcast_stateless(
        &self,
        _payload: &BeforeBroadcastStatelessPayload,
    ) -> HookResult {
        Ok(None)
    }

    async fn on_stateless(&self, _payload: &OnStatelessPayload) -> HookResult {
        Ok(None)
    }

    async fn on_change(&self, _payload: &OnChangePayload) -> HookResult {
        Ok(None)
    }

    async fn on_store_document(&self, _payload: &OnStoreDocumentPayload) -> HookResult {
        Ok(None)
    }

    async fn after_store_document(&self, _payload: &AfterStoreDocumentPayload) -> HookResult {
        Ok(None)
    }

    async fn on_awareness_update(&self, _payload: &OnAwarenessUpdatePayload) -> HookResult {
        Ok(None)
    }

    async fn on_request(&self, _payload: &OnRequestPayload) -> HookResult {
        Ok(None)
    }

    async fn on_disconnect(&self, _payload: &OnDisconnectPayload) -> HookResult {
        Ok(None)
    }

    async fn before_unload_document(&self, _payload: &BeforeUnloadDocumentPayload) -> HookResult {
        Ok(None)
    }

    async fn after_unload_document(&self, _payload: &AfterUnloadDocumentPayload) -> HookResult {
        Ok(None)
    }

    async fn on_destroy(&self, _payload: &OnDestroyPayload) -> HookResult {
        Ok(None)
    }
}

// ──── WebSocket Abstraction ────

#[async_trait]
pub trait WebSocketSink: Send + Sync {
    fn send(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn close(
        &self,
        code: u16,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn ready_state(&self) -> hocuspocus_common::WsReadyState;
}

// ──── Configuration ────

pub struct Configuration {
    pub name: Option<String>,
    pub timeout: u64,
    pub debounce: u64,
    pub max_debounce: u64,
    pub quiet: bool,
    pub unload_immediately: bool,
    pub extensions: Vec<Arc<dyn Extension>>,
    pub message_chunk_size: usize,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            name: None,
            timeout: 60_000,
            debounce: 2_000,
            max_debounce: 10_000,
            quiet: false,
            unload_immediately: true,
            extensions: Vec::new(),
            message_chunk_size: 0,
        }
    }
}
