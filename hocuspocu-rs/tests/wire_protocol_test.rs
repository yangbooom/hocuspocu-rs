use hocuspocu_rs::encoding::*;
use hocuspocu_rs::*;
use yrs::sync::{Awareness, AwarenessUpdate as YrsAwarenessUpdate};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, GetString, ReadTxn, Text, Transact};

#[test]
fn test_lib0_varuint_encoding_compat() {
    let test_values: Vec<u64> = vec![0, 1, 127, 128, 255, 256, 16383, 16384, 2097151, 2097152];

    for val in test_values {
        let mut buf = Vec::new();
        write_var_uint(&mut buf, val);

        let mut decoder = Decoder::new(&buf);
        let decoded = decoder.read_var_uint().unwrap();
        assert_eq!(decoded, val, "varuint roundtrip failed for {}", val);
        assert!(
            !decoder.has_content(),
            "decoder should be empty after reading {}",
            val
        );
    }
}

#[test]
fn test_lib0_varstring_encoding_compat() {
    let test_strings = vec![
        "",
        "hello",
        "hello world",
        "Unicode: 한국어 日本語 中文",
        "Special chars: \n\t\r\\\"",
    ];

    for s in test_strings {
        let mut buf = Vec::new();
        write_var_string(&mut buf, s);

        let mut decoder = Decoder::new(&buf);
        let decoded = decoder.read_var_string().unwrap();
        assert_eq!(decoded, s, "varstring roundtrip failed for {:?}", s);
    }
}

#[test]
fn test_lib0_varuint8array_encoding_compat() {
    let test_arrays: Vec<Vec<u8>> = vec![vec![], vec![0], vec![1, 2, 3], vec![255; 1000]];

    for arr in test_arrays {
        let mut buf = Vec::new();
        write_var_uint8_array(&mut buf, &arr);

        let mut decoder = Decoder::new(&buf);
        let decoded = decoder.read_var_uint8_array().unwrap();
        assert_eq!(decoded, arr, "varuint8array roundtrip failed");
    }
}

#[test]
fn test_sync_step1_message_format() {
    let doc = Doc::new();
    let sv = {
        let txn = doc.transact();
        txn.state_vector().encode_v1()
    };

    let msg = OutgoingMessage::new("test-doc")
        .create_sync_message()
        .write_first_sync_step_for(&sv);

    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    let doc_name = decoder.read_var_string().unwrap();
    assert_eq!(doc_name, "test-doc");

    let msg_type = decoder.read_var_uint().unwrap();
    assert_eq!(msg_type, MessageType::Sync as u64);

    let sync_type = decoder.read_var_uint().unwrap();
    assert_eq!(sync_type, 0); // SyncStep1

    let sv_data = decoder.read_var_uint8_array().unwrap();
    assert_eq!(sv_data, sv);
}

#[test]
fn test_sync_step2_message_format() {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("content");
    text.push(&mut doc.transact_mut(), "hello");

    let update = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    };

    let msg = OutgoingMessage::new("my-doc")
        .create_sync_message()
        .write_sync_step2(&update);

    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    let doc_name = decoder.read_var_string().unwrap();
    assert_eq!(doc_name, "my-doc");

    let msg_type = decoder.read_var_uint().unwrap();
    assert_eq!(msg_type, MessageType::Sync as u64);

    let sync_type = decoder.read_var_uint().unwrap();
    assert_eq!(sync_type, 1); // SyncStep2

    let update_data = decoder.read_var_uint8_array().unwrap();
    assert_eq!(update_data, update);
}

#[test]
fn test_awareness_update_wire_format() {
    let awareness = Awareness::new(Doc::with_client_id(42));
    awareness
        .set_local_state(serde_json::json!({"user": {"name": "Alice", "color": "#ff0000"}}))
        .unwrap();

    let update = awareness.update().unwrap();
    let encoded = update.encode_v1();

    let decoded = YrsAwarenessUpdate::decode_v1(&encoded).unwrap();
    assert!(decoded.clients.contains_key(&42));
    assert_eq!(decoded.clients[&42].clock, 1);

    let state: serde_json::Value = serde_json::from_str(&decoded.clients[&42].json).unwrap();
    assert_eq!(state["user"]["name"], "Alice");
}

#[test]
fn test_awareness_message_wrapping() {
    let awareness = Awareness::new(Doc::with_client_id(1));
    awareness
        .set_local_state(serde_json::json!({"cursor": {"x": 10, "y": 20}}))
        .unwrap();

    let update = awareness.update().unwrap();
    let update_data = update.encode_v1();

    let msg = OutgoingMessage::new("doc1").create_awareness_update_message(&update_data);

    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    let doc_name = decoder.read_var_string().unwrap();
    assert_eq!(doc_name, "doc1");

    let msg_type = decoder.read_var_uint().unwrap();
    assert_eq!(msg_type, MessageType::Awareness as u64);

    let inner = decoder.read_var_uint8_array().unwrap();
    let decoded = YrsAwarenessUpdate::decode_v1(&inner).unwrap();
    assert!(decoded.clients.contains_key(&1));
}

#[test]
fn test_auth_message_format() {
    let msg = OutgoingMessage::new("doc1").write_authenticated(false);
    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    let doc_name = decoder.read_var_string().unwrap();
    assert_eq!(doc_name, "doc1");

    let msg_type = decoder.read_var_uint().unwrap();
    assert_eq!(msg_type, MessageType::Auth as u64);

    let auth_type = decoder.read_var_uint().unwrap();
    assert_eq!(
        auth_type,
        hocuspocus_common::AuthMessageType::Authenticated as u64
    );

    let scope = decoder.read_var_string().unwrap();
    assert_eq!(scope, "read-write");
}

#[test]
fn test_auth_readonly_message_format() {
    let msg = OutgoingMessage::new("doc1").write_authenticated(true);
    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    decoder.read_var_string().unwrap(); // doc name
    decoder.read_var_uint().unwrap(); // msg type

    let auth_type = decoder.read_var_uint().unwrap();
    assert_eq!(
        auth_type,
        hocuspocus_common::AuthMessageType::Authenticated as u64
    );

    let scope = decoder.read_var_string().unwrap();
    assert_eq!(scope, "readonly");
}

#[test]
fn test_permission_denied_message_format() {
    let msg = OutgoingMessage::new("doc1").write_permission_denied("not-authorized");
    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    decoder.read_var_string().unwrap(); // doc name
    let msg_type = decoder.read_var_uint().unwrap();
    assert_eq!(msg_type, MessageType::Auth as u64);

    let auth_type = decoder.read_var_uint().unwrap();
    assert_eq!(
        auth_type,
        hocuspocus_common::AuthMessageType::PermissionDenied as u64
    );

    let reason = decoder.read_var_string().unwrap();
    assert_eq!(reason, "not-authorized");
}

#[test]
fn test_sync_status_message_format() {
    let msg = OutgoingMessage::new("doc1").write_sync_status(true);
    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    decoder.read_var_string().unwrap(); // doc name
    let msg_type = decoder.read_var_uint().unwrap();
    assert_eq!(msg_type, MessageType::SyncStatus as u64);

    let saved = decoder.read_var_uint().unwrap();
    assert_eq!(saved, 1);
}

#[test]
fn test_stateless_message_format() {
    let msg = OutgoingMessage::new("doc1").write_stateless("my custom payload");
    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    decoder.read_var_string().unwrap(); // doc name
    let msg_type = decoder.read_var_uint().unwrap();
    assert_eq!(msg_type, MessageType::Stateless as u64);

    let payload = decoder.read_var_string().unwrap();
    assert_eq!(payload, "my custom payload");
}

#[test]
fn test_close_message_format() {
    let msg = OutgoingMessage::new("doc1").write_close_message("server shutting down");
    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    decoder.read_var_string().unwrap(); // doc name
    let msg_type = decoder.read_var_uint().unwrap();
    assert_eq!(msg_type, MessageType::Close as u64);

    let reason = decoder.read_var_string().unwrap();
    assert_eq!(reason, "server shutting down");
}

#[test]
fn test_session_aware_message_address() {
    let msg = OutgoingMessage::new("doc1\0session123").write_stateless("test");
    let data = msg.to_vec();
    let mut decoder = Decoder::new(&data);

    let addr = decoder.read_var_string().unwrap();
    assert_eq!(addr, "doc1\0session123");

    let (doc_name, session_id) = hocuspocus_common::parse_routing_key(&addr);
    assert_eq!(doc_name, "doc1");
    assert_eq!(session_id, Some("session123"));
}

#[tokio::test]
async fn test_document_yrs_sync_roundtrip() {
    let doc1 = Document::new("test");
    let text = doc1.doc().get_or_insert_text("content");
    text.push(&mut doc1.doc().transact_mut(), "hello world");

    let sv1 = doc1.encode_state_vector();
    let update1 = doc1.encode_state_as_update();

    let doc2 = Document::new("test");
    doc2.apply_update(&update1).unwrap();

    let text2 = doc2.doc().get_or_insert_text("content");
    let content = text2.get_string(&doc2.doc().transact());
    assert_eq!(content, "hello world");

    let sv2 = doc2.encode_state_vector();
    assert_eq!(sv1, sv2);
}

#[tokio::test]
async fn test_document_awareness_yrs_compat() {
    let doc = Document::new("test");

    assert!(!doc.has_awareness_states());

    doc.awareness
        .set_local_state(serde_json::json!({"user": "test"}))
        .unwrap();

    assert!(doc.has_awareness_states());

    let update = doc.encode_awareness_update_all().unwrap();
    assert!(!update.is_empty());

    let decoded = YrsAwarenessUpdate::decode_v1(&update).unwrap();
    assert!(!decoded.clients.is_empty());
}
