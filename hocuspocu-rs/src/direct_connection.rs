use std::sync::Arc;

use crate::document::Document;
use crate::hocuspocus::Hocuspocus;
use crate::types::*;

pub struct DirectConnection {
    document: Option<Arc<Document>>,
    instance: Arc<Hocuspocus>,
    context: Context,
}

impl DirectConnection {
    pub fn new(
        document: Arc<Document>,
        instance: Arc<Hocuspocus>,
        context: Option<Context>,
    ) -> Self {
        let ctx = context.unwrap_or_else(empty_context);

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(document.add_direct_connection());
        });

        Self {
            document: Some(document),
            instance,
            context: ctx,
        }
    }

    pub async fn new_async(
        document: Arc<Document>,
        instance: Arc<Hocuspocus>,
        context: Option<Context>,
    ) -> Self {
        let ctx = context.unwrap_or_else(empty_context);
        document.add_direct_connection().await;
        Self {
            document: Some(document),
            instance,
            context: ctx,
        }
    }

    pub async fn transact<F>(
        &self,
        transaction: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(&Document),
    {
        let doc = self.document.as_ref().ok_or("direct connection closed")?;

        // Wrap in a yrs transaction - the callback can use doc.doc().transact_mut()
        // to make changes, matching the TS behavior where document.transact() is used
        transaction(doc);
        Ok(())
    }

    pub fn document(&self) -> Option<&Arc<Document>> {
        self.document.as_ref()
    }

    pub async fn disconnect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(document) = self.document.take() {
            document.remove_direct_connection().await;

            let store_payload = OnStoreDocumentPayload {
                clients_count: document.get_connections_count().await,
                last_context: self.context.clone(),
                last_transaction_origin: Some(TransactionOrigin::Local(LocalTransactionOrigin {
                    skip_store_hooks: false,
                    context: Some(self.context.clone()),
                })),
                document: document.clone(),
                document_name: document.name.clone(),
            };

            self.instance
                .store_document_hooks(&document, store_payload, true)
                .await;

            if document.get_connections_count().await == 0 && !document.is_save_mutex_locked().await
            {
                let disconnect_payload = OnDisconnectPayload {
                    clients_count: 0,
                    context: self.context.clone(),
                    document: document.clone(),
                    document_name: document.name.clone(),
                    request: RequestInfo::localhost(),
                    socket_id: "server".to_string(),
                };

                let _ = self.instance.hooks_on_disconnect(&disconnect_payload).await;

                self.instance.unload_document(&document).await;
            }
        }

        Ok(())
    }
}
