use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use hocuspocus_common::{self as common, SkipFurtherHooksError};

use crate::client_connection::ClientConnection;
use crate::direct_connection::DirectConnection;
use crate::document::Document;
use crate::types::*;
use crate::util::Debouncer;

pub const VERSION: &str = "4.1.0";

pub struct Hocuspocus {
    pub configuration: RwLock<Configuration>,
    pub documents: RwLock<HashMap<String, Arc<Document>>>,
    loading_documents:
        RwLock<HashMap<String, Arc<tokio::sync::Mutex<Option<Result<Arc<Document>, String>>>>>>,
    unloading_documents: RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    pub debouncer: Debouncer,
}

impl Hocuspocus {
    pub fn new(configuration: Option<Configuration>) -> Arc<Self> {
        let config = configuration.unwrap_or_default();
        let instance = Arc::new(Self {
            configuration: RwLock::new(config),
            documents: RwLock::new(HashMap::new()),
            loading_documents: RwLock::new(HashMap::new()),
            unloading_documents: RwLock::new(HashMap::new()),
            debouncer: Debouncer::new(),
        });

        instance
    }

    pub async fn configure(self: &Arc<Self>, configuration: Configuration) {
        let mut config = self.configuration.write().await;

        config.name = configuration.name;
        config.timeout = configuration.timeout;
        config.debounce = configuration.debounce;
        config.max_debounce = configuration.max_debounce;
        config.quiet = configuration.quiet;
        config.unload_immediately = configuration.unload_immediately;

        let mut extensions = configuration.extensions;
        extensions.sort_by(|a, b| {
            let one = a.priority();
            let two = b.priority();
            two.cmp(&one) // descending: higher priority runs first
        });
        config.extensions = extensions;

        let on_configure_payload = OnConfigurePayload {
            version: VERSION.to_string(),
        };

        drop(config);
        let _ = self.hooks_on_configure(&on_configure_payload).await;
    }

    pub async fn get_documents_count(&self) -> usize {
        let docs = self.documents.read().await;
        docs.len()
    }

    pub async fn get_connections_count(&self) -> usize {
        let docs = self.documents.read().await;
        let mut unique_socket_ids = std::collections::HashSet::new();
        let mut total_direct = 0usize;

        for doc in docs.values() {
            let socket_ids = doc.get_connection_socket_ids().await;
            for sid in socket_ids {
                unique_socket_ids.insert(sid);
            }
            let dc = doc.direct_connections_count.read().await;
            total_direct += *dc;
        }

        unique_socket_ids.len() + total_direct
    }

    pub async fn flush_pending_stores(&self) {
        let docs = self.documents.read().await;
        for doc in docs.values() {
            let debounce_id = format!("onStoreDocument-{}", doc.name);
            let is_loading = *doc.is_loading.read().await;
            if !is_loading && self.debouncer.is_debounced(&debounce_id).await {
                self.debouncer.execute_now(&debounce_id).await;
            }
        }
    }

    pub async fn close_connections(&self, document_name: Option<&str>) {
        let docs = self.documents.read().await;
        for doc in docs.values() {
            if let Some(name) = document_name {
                if doc.name != name {
                    continue;
                }
            }
            doc.close_all_connections(&common::reset_connection()).await;
        }
    }

    pub fn handle_connection(
        self: &Arc<Self>,
        websocket: Arc<dyn WebSocketSink>,
        request: RequestInfo,
        default_context: Option<Context>,
    ) -> Arc<ClientConnection> {
        let timeout = {
            let config =
                tokio::task::block_in_place(|| self.configuration.blocking_read());
            config.timeout
        };

        let ctx = default_context.unwrap_or_else(empty_context);

        let client_connection = ClientConnection::new(
            websocket,
            request,
            self.clone(),
            timeout,
            ctx,
        );

        let hp = self.clone();
        let cc = client_connection.clone();
        tokio::spawn(async move {
            cc.on_close(Arc::new(move |document: Arc<Document>, _payload: OnDisconnectPayload| {
                let hp = hp.clone();
                let doc = document.clone();
                tokio::spawn(async move {
                    if doc.get_connections_count().await > 0 {
                        return;
                    }

                    let debounce_id = format!("onStoreDocument-{}", doc.name);
                    let is_loading = *doc.is_loading.read().await;
                    let is_debounced = hp.debouncer.is_debounced(&debounce_id).await;

                    if !is_loading && is_debounced {
                        let config = hp.configuration.read().await;
                        if config.unload_immediately {
                            drop(config);
                            hp.debouncer.execute_now(&debounce_id).await;
                        }
                    } else {
                        hp.unload_document(&doc).await;
                    }
                });
            })).await;
        });

        client_connection
    }

    pub async fn handle_document_update(
        self: &Arc<Self>,
        document: &Arc<Document>,
        origin: Option<TransactionOrigin>,
        update: Vec<u8>,
    ) {
        let connection_id = match &origin {
            Some(TransactionOrigin::Connection(c)) => Some(c.connection_id.clone()),
            _ => None,
        };

        let context = match &origin {
            Some(TransactionOrigin::Connection(_)) => empty_context(),
            Some(TransactionOrigin::Local(l)) => {
                l.context.clone().unwrap_or_else(empty_context)
            }
            _ => empty_context(),
        };

        let change_payload = OnChangePayload {
            clients_count: document.get_connections_count().await,
            document: document.clone(),
            document_name: document.name.clone(),
            request: RequestInfo::default(),
            socket_id: String::new(),
            update: update.clone(),
            transaction_origin: origin.clone(),
            connection_id: connection_id.clone(),
            context: context.clone(),
        };

        let _ = self.hooks_on_change(&change_payload).await;

        if should_skip_store_hooks(&origin) {
            return;
        }

        let store_payload = OnStoreDocumentPayload {
            clients_count: document.get_connections_count().await,
            document: document.clone(),
            last_context: context,
            last_transaction_origin: origin,
            document_name: document.name.clone(),
        };

        self.store_document_hooks(document, store_payload, false)
            .await;
    }

    pub async fn create_document(
        self: &Arc<Self>,
        document_name: &str,
        request: &RequestInfo,
        socket_id: &str,
        connection_config: &ConnectionConfiguration,
        context: Option<Context>,
    ) -> Result<Arc<Document>, Box<dyn std::error::Error + Send + Sync>> {
        if document_name.trim().is_empty() {
            return Err("Document name must not be empty".into());
        }

        // Check if already loaded
        {
            let docs = self.documents.read().await;
            if let Some(doc) = docs.get(document_name) {
                return Ok(doc.clone());
            }
        }

        // Check if currently loading
        {
            let loading = self.loading_documents.read().await;
            if let Some(lock) = loading.get(document_name) {
                let result = lock.lock().await;
                if let Some(ref res) = *result {
                    return res.clone().map_err(|e| e.into());
                }
            }
        }

        // Start loading
        let load_lock = Arc::new(tokio::sync::Mutex::new(None));
        {
            let mut loading = self.loading_documents.write().await;
            loading.insert(document_name.to_string(), load_lock.clone());
        }

        let result = self
            .load_document(document_name, request, socket_id, connection_config, context)
            .await;

        match result {
            Ok(doc) => {
                {
                    let mut docs = self.documents.write().await;
                    docs.insert(document_name.to_string(), doc.clone());
                }
                {
                    let mut loading = self.loading_documents.write().await;
                    loading.remove(document_name);
                }
                Ok(doc)
            }
            Err(e) => {
                {
                    let mut loading = self.loading_documents.write().await;
                    loading.remove(document_name);
                }
                Err(e)
            }
        }
    }

    async fn load_document(
        self: &Arc<Self>,
        document_name: &str,
        request: &RequestInfo,
        socket_id: &str,
        connection_config: &ConnectionConfiguration,
        context: Option<Context>,
    ) -> Result<Arc<Document>, Box<dyn std::error::Error + Send + Sync>> {
        let ctx = context.unwrap_or_else(empty_context);

        // onCreateDocument hook
        let create_payload = OnCreateDocumentPayload {
            context: ctx.clone(),
            document_name: document_name.to_string(),
            request: request.clone(),
            socket_id: socket_id.to_string(),
            connection_config: connection_config.clone(),
        };
        let _ = self.hooks_on_create_document(&create_payload).await;

        let document = Arc::new(Document::new(document_name));

        // onLoadDocument hook
        let load_payload = OnLoadDocumentPayload {
            context: ctx.clone(),
            document: document.clone(),
            document_name: document_name.to_string(),
            request: request.clone(),
            socket_id: socket_id.to_string(),
            connection_config: connection_config.clone(),
        };

        match self.hooks_on_load_document(&load_payload).await {
            Ok(Some(LoadedDocument::Update(update))) => {
                if let Err(e) = document.apply_update(&update) {
                    tracing::error!("Failed to apply loaded document update: {:?}", e);
                }
            }
            Ok(None) => {}
            Err(e) => {
                self.close_connections(Some(document_name)).await;
                return Err(e);
            }
        }

        {
            let mut is_loading = document.is_loading.write().await;
            *is_loading = false;
        }

        // Set up update handler
        let hp = self.clone();
        let _doc_ref = document.clone();
        document
            .set_on_update(Arc::new(move |doc, origin, update| {
                let hp = hp.clone();
                let doc = doc.clone();
                let update = update.clone();
                let origin = origin.clone();
                tokio::spawn(async move {
                    let mut ts = doc.last_change_time.write().await;
                    *ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    drop(ts);
                    hp.handle_document_update(&doc, origin, update).await;
                });
            }))
            .await;

        // afterLoadDocument hook
        let after_load_payload = AfterLoadDocumentPayload {
            context: ctx.clone(),
            document: document.clone(),
            document_name: document_name.to_string(),
            request: request.clone(),
            socket_id: socket_id.to_string(),
            connection_config: connection_config.clone(),
        };
        let _ = self.hooks_after_load_document(&after_load_payload).await;

        // Set up beforeBroadcastStateless callback
        let hp_bbs = self.clone();
        let doc_name = document_name.to_string();
        document
            .set_before_broadcast_stateless(Arc::new(move |_doc, payload| {
                let hp = hp_bbs.clone();
                let dn = doc_name.clone();
                let p = payload.clone();
                tokio::spawn(async move {
                    let payload = BeforeBroadcastStatelessPayload {
                        document_name: dn,
                        payload: p,
                    };
                    let _ = hp.hooks_before_broadcast_stateless(&payload).await;
                });
            }))
            .await;

        // Set up beforeHandleAwareness callback
        let hp_bha = self.clone();
        document
            .set_before_handle_awareness(Arc::new(
                move |doc,
                      update_data: Vec<u8>,
                      origin: Option<TransactionOrigin>| {
                    let hp = hp_bha.clone();
                    let doc = doc.clone();
                    let update_data = update_data.clone();
                    let origin = origin.clone();
                    Box::pin(async move {
                        let connection_id = match &origin {
                            Some(TransactionOrigin::Connection(c)) => {
                                Some(c.connection_id.clone())
                            }
                            _ => None,
                        };

                        let mut payload = BeforeHandleAwarenessPayload {
                            clients_count: doc.get_connections_count().await,
                            context: None,
                            document: doc.clone(),
                            document_name: doc.name.clone(),
                            request: RequestInfo::default(),
                            states: HashMap::new(),
                            socket_id: String::new(),
                            transaction_origin: origin,
                            connection_id,
                        };

                        let _ = hp.hooks_before_handle_awareness(&mut payload).await;
                        Ok(update_data)
                    })
                },
            ))
            .await;

        Ok(document)
    }

    pub async fn store_document_hooks(
        self: &Arc<Self>,
        document: &Arc<Document>,
        payload: OnStoreDocumentPayload,
        immediately: bool,
    ) {
        let debounce_id = format!("onStoreDocument-{}", document.name);
        let hp = self.clone();
        let doc = document.clone();
        let payload = payload.clone();

        let config = self.configuration.read().await;
        let debounce_ms = if immediately { 0 } else { config.debounce };
        let max_debounce_ms = config.max_debounce;
        drop(config);

        self.debouncer
            .debounce(
                &debounce_id,
                move || {
                    let hp = hp.clone();
                    let doc = doc.clone();
                    let payload = payload.clone();
                    async move {
                        {
                            let _guard = doc.save_mutex.lock().await;

                            match hp.hooks_on_store_document(&payload).await {
                                Ok(_) => {
                                    let _ = hp.hooks_after_store_document(&payload).await;
                                }
                                Err(e) => {
                                    if e.downcast_ref::<SkipFurtherHooksError>().is_some() {
                                        let hp2 = hp.clone();
                                        let doc2 = doc.clone();
                                        tokio::spawn(async move {
                                            if hp2.should_unload_document(&doc2).await {
                                                hp2.unload_document(&doc2).await;
                                            }
                                        });
                                        return;
                                    }
                                    tracing::error!(
                                        "Error during storeDocumentHooks. Document stays in memory: {:?}",
                                        e
                                    );
                                    return;
                                }
                            }
                        }

                        let hp2 = hp.clone();
                        let doc2 = doc.clone();
                        tokio::spawn(async move {
                            if hp2.should_unload_document(&doc2).await {
                                hp2.unload_document(&doc2).await;
                            }
                        });
                    }
                },
                debounce_ms,
                max_debounce_ms,
            )
            .await;
    }

    pub async fn should_unload_document(&self, document: &Arc<Document>) -> bool {
        let debounce_id = format!("onStoreDocument-{}", document.name);

        let has_pending_work = self.debouncer.is_debounced(&debounce_id).await
            || self.debouncer.is_currently_executing(&debounce_id).await;

        !has_pending_work && document.get_connections_count().await == 0
    }

    pub async fn unload_document(self: &Arc<Self>, document: &Arc<Document>) {
        let document_name = &document.name;

        if !self.should_unload_document(document).await {
            return;
        }

        {
            let docs = self.documents.read().await;
            if !docs.contains_key(document_name) {
                return;
            }
        }

        {
            let unloading = self.unloading_documents.read().await;
            if unloading.contains_key(document_name) {
                return;
            }
        }

        let unload_lock = Arc::new(tokio::sync::Mutex::new(()));
        {
            let mut unloading = self.unloading_documents.write().await;
            unloading.insert(document_name.to_string(), unload_lock.clone());
        }

        let _guard = unload_lock.lock().await;

        // beforeUnloadDocument hook
        let before_payload = BeforeUnloadDocumentPayload {
            document_name: document_name.clone(),
            document: document.clone(),
        };

        if let Err(_) = self.hooks_before_unload_document(&before_payload).await {
            let mut unloading = self.unloading_documents.write().await;
            unloading.remove(document_name);
            return;
        }

        if !self.should_unload_document(document).await {
            let mut unloading = self.unloading_documents.write().await;
            unloading.remove(document_name);
            return;
        }

        {
            let mut docs = self.documents.write().await;
            docs.remove(document_name);
        }

        document.destroy().await;

        let after_payload = AfterUnloadDocumentPayload {
            document_name: document_name.clone(),
        };
        let _ = self.hooks_after_unload_document(&after_payload).await;

        {
            let mut unloading = self.unloading_documents.write().await;
            unloading.remove(document_name);
        }
    }

    pub async fn open_direct_connection(
        self: &Arc<Self>,
        document_name: &str,
        context: Option<Context>,
    ) -> Result<DirectConnection, Box<dyn std::error::Error + Send + Sync>> {
        let connection_config = ConnectionConfiguration {
            is_authenticated: true,
            read_only: false,
        };

        let document = self
            .create_document(
                document_name,
                &RequestInfo::localhost(),
                &uuid::Uuid::new_v4().to_string(),
                &connection_config,
                context.clone(),
            )
            .await?;

        Ok(DirectConnection::new_async(document, self.clone(), context).await)
    }

    // ──── Hook Runners ────

    pub async fn hooks_on_configure(
        &self,
        payload: &OnConfigurePayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_configure(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_listen(&self, payload: &OnListenPayload) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_listen(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_upgrade(&self, payload: &OnUpgradePayload) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_upgrade(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_connect(&self, payload: &OnConnectPayload) -> HookResult {
        let config = self.configuration.read().await;
        let mut ctx = None;
        for ext in &config.extensions {
            let result = ext.on_connect(payload).await?;
            if result.is_some() {
                ctx = result;
            }
        }
        Ok(ctx)
    }

    pub async fn hooks_connected(&self, payload: &ConnectedPayload) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.connected(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_authenticate(&self, payload: &OnAuthenticatePayload) -> HookResult {
        let config = self.configuration.read().await;
        let mut ctx = None;
        for ext in &config.extensions {
            let result = ext.on_authenticate(payload).await?;
            if result.is_some() {
                ctx = result;
            }
        }
        Ok(ctx)
    }

    pub async fn hooks_on_token_sync(&self, payload: &OnTokenSyncPayload) -> HookResult {
        let config = self.configuration.read().await;
        let mut ctx = None;
        for ext in &config.extensions {
            let result = ext.on_token_sync(payload).await?;
            if result.is_some() {
                ctx = result;
            }
        }
        Ok(ctx)
    }

    pub async fn hooks_on_create_document(
        &self,
        payload: &OnCreateDocumentPayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_create_document(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_load_document(
        &self,
        payload: &OnLoadDocumentPayload,
    ) -> LoadDocumentResult {
        let config = self.configuration.read().await;
        let mut result = None;
        for ext in &config.extensions {
            let r = ext.on_load_document(payload).await?;
            if r.is_some() {
                result = r;
            }
        }
        Ok(result)
    }

    pub async fn hooks_after_load_document(
        &self,
        payload: &AfterLoadDocumentPayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.after_load_document(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_before_handle_message(
        &self,
        payload: &BeforeHandleMessagePayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.before_handle_message(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_before_handle_awareness(
        &self,
        payload: &mut BeforeHandleAwarenessPayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.before_handle_awareness(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_before_sync(&self, payload: &BeforeSyncPayload) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.before_sync(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_before_broadcast_stateless(
        &self,
        payload: &BeforeBroadcastStatelessPayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.before_broadcast_stateless(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_stateless(&self, payload: &OnStatelessPayload) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_stateless(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_change(&self, payload: &OnChangePayload) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_change(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_store_document(
        &self,
        payload: &OnStoreDocumentPayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_store_document(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_after_store_document(
        &self,
        payload: &AfterStoreDocumentPayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.after_store_document(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_awareness_update(
        &self,
        payload: &OnAwarenessUpdatePayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_awareness_update(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_request(&self, payload: &OnRequestPayload) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_request(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_disconnect(&self, payload: &OnDisconnectPayload) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_disconnect(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_before_unload_document(
        &self,
        payload: &BeforeUnloadDocumentPayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.before_unload_document(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_after_unload_document(
        &self,
        payload: &AfterUnloadDocumentPayload,
    ) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.after_unload_document(payload).await?;
        }
        Ok(None)
    }

    pub async fn hooks_on_destroy(&self, payload: &OnDestroyPayload) -> HookResult {
        let config = self.configuration.read().await;
        for ext in &config.extensions {
            ext.on_destroy(payload).await?;
        }
        Ok(None)
    }
}
