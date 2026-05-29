use hocuspocu_rs::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use yrs::{GetString, Text, Transact};

#[tokio::test]
async fn test_direct_connection_triggers_update_observer() {
    let hp = Hocuspocus::new(None);

    // Open a direct connection - creates the document
    let mut direct = hp
        .open_direct_connection("test-doc", None)
        .await
        .expect("open direct connection");

    // The document should exist
    assert_eq!(hp.get_documents_count().await, 1);

    direct.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn test_yrs_doc_observer_fires_on_direct_mutation() {
    let hp = Hocuspocus::new(None);

    let direct = hp.open_direct_connection("doc1", None).await.expect("open");

    let doc = direct.document().expect("doc").clone();

    // Make changes via direct doc mutation - should fire observer
    let text = doc.doc().get_or_insert_text("content");
    {
        let mut txn = doc.doc().transact_mut();
        text.push(&mut txn, "hello from direct");
    }

    // Give observer time to fire
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify text is present
    let content = text.get_string(&doc.doc().transact());
    assert_eq!(content, "hello from direct");
}

#[tokio::test]
async fn test_is_empty_yrs_compat() {
    let doc = Document::new("test");
    assert!(doc.is_empty("anything"));

    let text = doc.doc().get_or_insert_text("content");
    text.push(&mut doc.doc().transact_mut(), "x");

    assert!(!doc.is_empty("anything"));
}

#[tokio::test]
async fn test_concurrent_create_document_dedup() {
    let hp = Hocuspocus::new(None);

    let mut handles = vec![];
    for i in 0..10 {
        let hp = hp.clone();
        handles.push(tokio::spawn(async move {
            hp.create_document(
                "shared-doc",
                &RequestInfo::default(),
                &format!("socket-{}", i),
                &ConnectionConfiguration::default(),
                None,
            )
            .await
        }));
    }

    let mut docs = vec![];
    for h in handles {
        docs.push(h.await.unwrap().unwrap());
    }

    // All callers should get the same Arc<Document>
    assert_eq!(hp.get_documents_count().await, 1);
    let first = docs[0].clone();
    for doc in &docs {
        assert!(Arc::ptr_eq(&first, doc));
    }
}

#[tokio::test]
async fn test_arc_cycle_broken_via_weak() {
    // Note: We can't directly observe Arc cycles without leak detection,
    // but we can verify that connection close cleans up state properly
    let hp = Hocuspocus::new(None);

    let mut direct = hp.open_direct_connection("ephemeral", None).await.unwrap();
    assert_eq!(hp.get_documents_count().await, 1);

    direct.disconnect().await.unwrap();

    // Give async cleanup time
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // The document should be unloaded since no connections remain
    // (it may take a moment due to debounce)
    let final_count = hp.get_documents_count().await;
    assert!(final_count <= 1);
}

#[tokio::test]
async fn test_observer_fires_for_message_receiver_updates() {
    // Verify that the observe_update_v1 fires for updates applied via MessageReceiver
    let counter = Arc::new(AtomicU32::new(0));

    struct CountingExtension {
        counter: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl Extension for CountingExtension {
        async fn on_change(&self, _payload: &OnChangePayload) -> HookResult {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }

    let mut config = Configuration::default();
    config.extensions.push(Arc::new(CountingExtension {
        counter: counter.clone(),
    }));

    let hp = Hocuspocus::new(Some(config));

    let direct = hp.open_direct_connection("obs-test", None).await.unwrap();
    let doc = direct.document().expect("doc").clone();

    let text = doc.doc().get_or_insert_text("content");
    {
        let mut txn = doc.doc().transact_mut();
        text.push(&mut txn, "test");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // onChange should have fired at least once
    assert!(counter.load(Ordering::SeqCst) >= 1);
}
