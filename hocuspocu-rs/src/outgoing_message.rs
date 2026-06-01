use crate::encoding;
use crate::types::MessageType;
use hocuspocus_common::{self as common};

pub struct OutgoingMessage {
    encoder: Vec<u8>,
    pub message_type: Option<MessageType>,
    pub category: Option<String>,
}

impl OutgoingMessage {
    pub fn new(document_name: &str) -> Self {
        let mut encoder = Vec::new();
        encoding::write_var_string(&mut encoder, document_name);
        Self {
            encoder,
            message_type: None,
            category: None,
        }
    }

    pub fn create_sync_message(mut self) -> Self {
        self.message_type = Some(MessageType::Sync);
        encoding::write_var_uint(&mut self.encoder, MessageType::Sync as u64);
        self
    }

    pub fn create_sync_reply_message(mut self) -> Self {
        self.message_type = Some(MessageType::SyncReply);
        encoding::write_var_uint(&mut self.encoder, MessageType::SyncReply as u64);
        self
    }

    pub fn create_awareness_update_message(mut self, update_data: &[u8]) -> Self {
        self.message_type = Some(MessageType::Awareness);
        self.category = Some("Update".to_string());
        encoding::write_var_uint(&mut self.encoder, MessageType::Awareness as u64);
        encoding::write_var_uint8_array(&mut self.encoder, update_data);
        self
    }

    pub fn write_query_awareness(mut self) -> Self {
        self.message_type = Some(MessageType::QueryAwareness);
        self.category = Some("Update".to_string());
        encoding::write_var_uint(&mut self.encoder, MessageType::QueryAwareness as u64);
        self
    }

    pub fn write_token_sync_request(mut self) -> Self {
        self.message_type = Some(MessageType::Auth);
        self.category = Some("TokenSync".to_string());
        encoding::write_var_uint(&mut self.encoder, MessageType::Auth as u64);
        common::write_token_sync_request(&mut self.encoder);
        self
    }

    pub fn write_authenticated(mut self, read_only: bool) -> Self {
        self.message_type = Some(MessageType::Auth);
        self.category = Some("Authenticated".to_string());
        encoding::write_var_uint(&mut self.encoder, MessageType::Auth as u64);
        let scope = if read_only { "readonly" } else { "read-write" };
        common::write_authenticated(&mut self.encoder, scope);
        self
    }

    pub fn write_permission_denied(mut self, reason: &str) -> Self {
        self.message_type = Some(MessageType::Auth);
        self.category = Some("PermissionDenied".to_string());
        encoding::write_var_uint(&mut self.encoder, MessageType::Auth as u64);
        common::write_permission_denied(&mut self.encoder, reason);
        self
    }

    pub fn write_first_sync_step_for(mut self, state_vector: &[u8]) -> Self {
        self.category = Some("SyncStep1".to_string());
        encoding::write_var_uint(&mut self.encoder, 0); // messageYjsSyncStep1
        encoding::write_var_uint8_array(&mut self.encoder, state_vector);
        self
    }

    pub fn write_sync_step2(mut self, update: &[u8]) -> Self {
        self.category = Some("SyncStep2".to_string());
        encoding::write_var_uint(&mut self.encoder, 1); // messageYjsSyncStep2
        encoding::write_var_uint8_array(&mut self.encoder, update);
        self
    }

    pub fn write_update(mut self, update: &[u8]) -> Self {
        self.category = Some("Update".to_string());
        encoding::write_var_uint(&mut self.encoder, 2); // messageYjsUpdate
        encoding::write_var_uint8_array(&mut self.encoder, update);
        self
    }

    pub fn write_stateless(mut self, payload: &str) -> Self {
        self.category = Some("Stateless".to_string());
        encoding::write_var_uint(&mut self.encoder, MessageType::Stateless as u64);
        encoding::write_var_string(&mut self.encoder, payload);
        self
    }

    pub fn write_broadcast_stateless(mut self, payload: &str) -> Self {
        self.category = Some("Stateless".to_string());
        encoding::write_var_uint(&mut self.encoder, MessageType::BroadcastStateless as u64);
        encoding::write_var_string(&mut self.encoder, payload);
        self
    }

    pub fn write_sync_status(mut self, update_saved: bool) -> Self {
        self.category = Some("SyncStatus".to_string());
        encoding::write_var_uint(&mut self.encoder, MessageType::SyncStatus as u64);
        encoding::write_var_uint(&mut self.encoder, if update_saved { 1 } else { 0 });
        self
    }

    pub fn write_close_message(mut self, reason: &str) -> Self {
        self.message_type = Some(MessageType::Close);
        encoding::write_var_uint(&mut self.encoder, MessageType::Close as u64);
        encoding::write_var_string(&mut self.encoder, reason);
        self
    }

    pub fn write_fragment_start(mut self, unique_id: &str) -> Self {
        self.message_type = Some(MessageType::FragmentStart);
        encoding::write_var_uint(&mut self.encoder, MessageType::FragmentStart as u64);
        encoding::write_var_string(&mut self.encoder, unique_id);
        self
    }

    pub fn write_fragment_data(mut self, unique_id: &str, index: usize, chunk: &[u8]) -> Self {
        self.message_type = Some(MessageType::FragmentData);
        encoding::write_var_uint(&mut self.encoder, MessageType::FragmentData as u64);
        encoding::write_var_string(&mut self.encoder, unique_id);
        encoding::write_var_uint(&mut self.encoder, index as u64);
        encoding::write_var_uint8_array(&mut self.encoder, chunk);
        self
    }

    pub fn write_fragment_end(mut self, unique_id: &str) -> Self {
        self.message_type = Some(MessageType::FragmentEnd);
        encoding::write_var_uint(&mut self.encoder, MessageType::FragmentEnd as u64);
        encoding::write_var_string(&mut self.encoder, unique_id);
        self
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.encoder
    }
}
