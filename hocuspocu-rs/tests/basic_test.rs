use hocuspocu_rs::*;
use hocuspocus_common::*;
use std::sync::Arc;

#[test]
fn test_message_types() {
    assert_eq!(MessageType::Sync as i8, 0);
    assert_eq!(MessageType::Awareness as i8, 1);
    assert_eq!(MessageType::Auth as i8, 2);
    assert_eq!(MessageType::QueryAwareness as i8, 3);
    assert_eq!(MessageType::SyncReply as i8, 4);
    assert_eq!(MessageType::Stateless as i8, 5);
    assert_eq!(MessageType::BroadcastStateless as i8, 6);
    assert_eq!(MessageType::Close as i8, 7);
    assert_eq!(MessageType::SyncStatus as i8, 8);
    assert_eq!(MessageType::Ping as i8, 9);
    assert_eq!(MessageType::Pong as i8, 10);
}

#[test]
fn test_message_type_from_u64() {
    assert_eq!(MessageType::try_from(0u64), Ok(MessageType::Sync));
    assert_eq!(MessageType::try_from(1u64), Ok(MessageType::Awareness));
    assert_eq!(MessageType::try_from(2u64), Ok(MessageType::Auth));
    assert_eq!(MessageType::try_from(10u64), Ok(MessageType::Pong));
    assert!(MessageType::try_from(99u64).is_err());
}

#[test]
fn test_close_events() {
    let reset = reset_connection();
    assert_eq!(reset.code, 4205);
    assert_eq!(reset.reason, "Reset Connection");

    let unauth = unauthorized();
    assert_eq!(unauth.code, 4401);

    let forb = forbidden();
    assert_eq!(forb.code, 4403);

    let timeout = connection_timeout();
    assert_eq!(timeout.code, 4408);

    let too_big = message_too_big();
    assert_eq!(too_big.code, 1009);
}

#[test]
fn test_routing_key() {
    let key = make_routing_key("doc1", "session123");
    assert_eq!(key, "doc1\0session123");

    let (name, session) = parse_routing_key(&key);
    assert_eq!(name, "doc1");
    assert_eq!(session, Some("session123"));

    let (name2, session2) = parse_routing_key("plain-doc");
    assert_eq!(name2, "plain-doc");
    assert_eq!(session2, None);
}

#[test]
fn test_connection_configuration_default() {
    let config = ConnectionConfiguration::default();
    assert!(!config.read_only);
    assert!(!config.is_authenticated);
}

#[test]
fn test_configuration_default() {
    let config = Configuration::default();
    assert_eq!(config.timeout, 60_000);
    assert_eq!(config.debounce, 2_000);
    assert_eq!(config.max_debounce, 10_000);
    assert!(!config.quiet);
    assert!(config.unload_immediately);
    assert!(config.extensions.is_empty());
}

#[test]
fn test_transaction_origin() {
    let conn_origin = Some(TransactionOrigin::Connection(ConnectionTransactionOrigin {
        connection_id: "test".to_string(),
    }));
    assert!(!should_skip_store_hooks(&conn_origin));

    let redis_origin = Some(TransactionOrigin::Redis);
    assert!(should_skip_store_hooks(&redis_origin));

    let local_origin = Some(TransactionOrigin::Local(LocalTransactionOrigin {
        skip_store_hooks: false,
        context: None,
    }));
    assert!(!should_skip_store_hooks(&local_origin));

    let local_skip = Some(TransactionOrigin::Local(LocalTransactionOrigin {
        skip_store_hooks: true,
        context: None,
    }));
    assert!(should_skip_store_hooks(&local_skip));

    assert!(!should_skip_store_hooks(&None));
}

#[test]
fn test_get_parameters() {
    let params = get_parameters("http://localhost:8080/ws?token=abc&room=test");
    assert_eq!(params.get("token"), Some(&"abc".to_string()));
    assert_eq!(params.get("room"), Some(&"test".to_string()));

    let empty = get_parameters("http://localhost:8080/ws");
    assert!(empty.is_empty());
}

#[test]
fn test_skip_further_hooks_error() {
    let err = SkipFurtherHooksError::new(None);
    assert_eq!(err.message, "Further hooks skipped");

    let err2 = SkipFurtherHooksError::new(Some("custom msg"));
    assert_eq!(err2.message, "custom msg");
}

#[test]
fn test_auth_message_type() {
    assert_eq!(AuthMessageType::Token as u8, 0);
    assert_eq!(AuthMessageType::PermissionDenied as u8, 1);
    assert_eq!(AuthMessageType::Authenticated as u8, 2);
}

#[test]
fn test_ws_ready_states() {
    assert_eq!(WsReadyState::Connecting as u8, 0);
    assert_eq!(WsReadyState::Open as u8, 1);
    assert_eq!(WsReadyState::Closing as u8, 2);
    assert_eq!(WsReadyState::Closed as u8, 3);
}

#[tokio::test]
async fn test_hocuspocus_new() {
    let hp = Hocuspocus::new(None);
    assert_eq!(hp.get_documents_count().await, 0);
    assert_eq!(hp.get_connections_count().await, 0);
}

#[tokio::test]
async fn test_hocuspocus_configure() {
    let hp = Hocuspocus::new(None);
    let config = Configuration {
        timeout: 30_000,
        debounce: 1_000,
        max_debounce: 5_000,
        ..Default::default()
    };
    hp.configure(config).await;
    let c = hp.configuration.read().await;
    assert_eq!(c.timeout, 30_000);
    assert_eq!(c.debounce, 1_000);
    assert_eq!(c.max_debounce, 5_000);
}

#[tokio::test]
async fn test_document_basic() {
    let doc = Document::new("test-doc");
    assert_eq!(doc.name, "test-doc");
    assert!(*doc.is_loading.read().await);
    assert!(!*doc.is_destroyed.read().await);
    assert_eq!(doc.get_connections_count().await, 0);
}

#[tokio::test]
async fn test_document_direct_connections() {
    let doc = Document::new("test");
    assert_eq!(doc.get_connections_count().await, 0);

    doc.add_direct_connection().await;
    assert_eq!(doc.get_connections_count().await, 1);

    doc.add_direct_connection().await;
    assert_eq!(doc.get_connections_count().await, 2);

    doc.remove_direct_connection().await;
    assert_eq!(doc.get_connections_count().await, 1);

    doc.remove_direct_connection().await;
    assert_eq!(doc.get_connections_count().await, 0);

    doc.remove_direct_connection().await;
    assert_eq!(doc.get_connections_count().await, 0);
}

#[tokio::test]
async fn test_document_yrs_operations() {
    let doc = Document::new("test");

    let sv = doc.encode_state_vector();
    assert!(!sv.is_empty());

    let update = doc.encode_state_as_update();
    assert!(!update.is_empty());
}

#[tokio::test]
async fn test_create_document_empty_name() {
    let hp = Hocuspocus::new(None);
    let result = hp
        .create_document(
            "",
            &RequestInfo::default(),
            "socket1",
            &ConnectionConfiguration::default(),
            None,
        )
        .await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err.to_string(), "Document name must not be empty");
}

#[tokio::test]
async fn test_create_document_whitespace_name() {
    let hp = Hocuspocus::new(None);
    let result = hp
        .create_document(
            "   ",
            &RequestInfo::default(),
            "socket1",
            &ConnectionConfiguration::default(),
            None,
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_and_retrieve_document() {
    let hp = Hocuspocus::new(None);
    let doc = hp
        .create_document(
            "my-doc",
            &RequestInfo::default(),
            "socket1",
            &ConnectionConfiguration::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(doc.name, "my-doc");
    assert_eq!(hp.get_documents_count().await, 1);

    let doc2 = hp
        .create_document(
            "my-doc",
            &RequestInfo::default(),
            "socket2",
            &ConnectionConfiguration::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(hp.get_documents_count().await, 1);
    assert_eq!(doc.name, doc2.name);
}

#[tokio::test]
async fn test_encoding_roundtrip() {
    use hocuspocu_rs::encoding::*;

    let mut buf = Vec::new();
    write_var_uint(&mut buf, 42);
    write_var_string(&mut buf, "hello world");
    write_var_uint8_array(&mut buf, &[1, 2, 3, 4]);

    let mut decoder = Decoder::new(&buf);
    assert_eq!(decoder.read_var_uint().unwrap(), 42);
    assert_eq!(decoder.read_var_string().unwrap(), "hello world");
    assert_eq!(decoder.read_var_uint8_array().unwrap(), vec![1, 2, 3, 4]);
    assert!(!decoder.has_content());
}

#[tokio::test]
async fn test_encoding_large_varuint() {
    use hocuspocu_rs::encoding::*;

    let mut buf = Vec::new();
    write_var_uint(&mut buf, 0);
    write_var_uint(&mut buf, 127);
    write_var_uint(&mut buf, 128);
    write_var_uint(&mut buf, 16383);
    write_var_uint(&mut buf, 16384);
    write_var_uint(&mut buf, u64::MAX);

    let mut decoder = Decoder::new(&buf);
    assert_eq!(decoder.read_var_uint().unwrap(), 0);
    assert_eq!(decoder.read_var_uint().unwrap(), 127);
    assert_eq!(decoder.read_var_uint().unwrap(), 128);
    assert_eq!(decoder.read_var_uint().unwrap(), 16383);
    assert_eq!(decoder.read_var_uint().unwrap(), 16384);
    assert_eq!(decoder.read_var_uint().unwrap(), u64::MAX);
}

#[tokio::test]
async fn test_outgoing_message_auth() {
    let msg = OutgoingMessage::new("test-doc").write_authenticated(false);
    let data = msg.to_vec();
    assert!(!data.is_empty());

    let msg2 = OutgoingMessage::new("test-doc").write_authenticated(true);
    let data2 = msg2.to_vec();
    assert!(!data2.is_empty());
}

#[tokio::test]
async fn test_outgoing_message_permission_denied() {
    let msg = OutgoingMessage::new("test-doc").write_permission_denied("not allowed");
    let data = msg.to_vec();
    assert!(!data.is_empty());
}

#[tokio::test]
async fn test_outgoing_message_sync_status() {
    let msg = OutgoingMessage::new("test-doc").write_sync_status(true);
    let data = msg.to_vec();
    assert!(!data.is_empty());
}

#[tokio::test]
async fn test_outgoing_message_close() {
    let msg = OutgoingMessage::new("test-doc").write_close_message("goodbye");
    let data = msg.to_vec();
    assert!(!data.is_empty());
}

#[tokio::test]
async fn test_outgoing_message_stateless() {
    let msg = OutgoingMessage::new("test-doc").write_stateless("custom payload");
    let data = msg.to_vec();
    assert!(!data.is_empty());
}

#[tokio::test]
async fn test_debouncer() {
    use hocuspocu_rs::util::Debouncer;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::time::Duration;

    let debouncer = Debouncer::new();
    let counter = Arc::new(AtomicU32::new(0));

    let c = counter.clone();
    debouncer
        .debounce(
            "test",
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            },
            0,
            10_000,
        )
        .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_debouncer_coalesces() {
    use hocuspocu_rs::util::Debouncer;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::time::Duration;

    let debouncer = Debouncer::new();
    let counter = Arc::new(AtomicU32::new(0));

    for _ in 0..5 {
        let c = counter.clone();
        debouncer
            .debounce(
                "test",
                move || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                },
                100,
                10_000,
            )
            .await;
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
