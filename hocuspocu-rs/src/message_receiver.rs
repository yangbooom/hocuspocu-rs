use std::sync::Arc;

use hocuspocus_common::AuthMessageType;
use yrs::updates::decoder::Decode;
use yrs::{ReadTxn, Transact};

use crate::document::Document;
use crate::encoding::Decoder;
use crate::outgoing_message::OutgoingMessage;
use crate::types::*;

const MESSAGE_YJS_SYNC_STEP1: u64 = 0;
const MESSAGE_YJS_SYNC_STEP2: u64 = 1;
const MESSAGE_YJS_UPDATE: u64 = 2;

pub struct MessageReceiver {
    data: Vec<u8>,
    default_transaction_origin: Option<TransactionOrigin>,
}

impl MessageReceiver {
    pub fn new(data: Vec<u8>, default_transaction_origin: Option<TransactionOrigin>) -> Self {
        Self {
            data,
            default_transaction_origin,
        }
    }

    pub async fn apply(
        &self,
        document: &Arc<Document>,
        connection_id: Option<&str>,
        message_address: &str,
        read_only: bool,
        reply: Option<&(dyn Fn(Vec<u8>) + Send + Sync)>,
        before_sync: Option<
            impl Fn(
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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut decoder = Decoder::new(&self.data);

        let _raw_key = decoder.read_var_string()?;
        let msg_type = decoder.read_var_uint()?;

        match MessageType::try_from(msg_type) {
            Ok(MessageType::Sync) | Ok(MessageType::SyncReply) => {
                let request_first_sync = msg_type != MessageType::SyncReply as u64;

                self.read_sync_message(
                    &mut decoder,
                    document,
                    connection_id,
                    message_address,
                    read_only,
                    reply,
                    request_first_sync,
                    &before_sync,
                )
                .await?;
            }
            Ok(MessageType::Awareness) => {
                let update = decoder.read_var_uint8_array()?;

                let origin = if let Some(conn_id) = connection_id {
                    Some(TransactionOrigin::Connection(ConnectionTransactionOrigin {
                        connection_id: conn_id.to_string(),
                    }))
                } else {
                    self.default_transaction_origin.clone()
                };

                document
                    .apply_awareness_update(&update, origin, connection_id)
                    .await?;
            }
            Ok(MessageType::QueryAwareness) => {
                if let Ok(update_data) = document.encode_awareness_update_all() {
                    let msg = OutgoingMessage::new(message_address)
                        .create_awareness_update_message(&update_data);

                    if let Some(reply_fn) = reply {
                        reply_fn(msg.to_vec());
                    }
                }
            }
            Ok(MessageType::Stateless) => {
                let payload = decoder.read_var_string()?;
                return Err(format!("stateless:{}", payload).into());
            }
            Ok(MessageType::BroadcastStateless) => {
                if connection_id.is_some() {
                    return Err(
                        "BroadcastStateless is a server-internal opcode and cannot be sent from a client"
                            .into(),
                    );
                }
                let payload = decoder.read_var_string()?;
                document.broadcast_stateless(&payload, None).await;
            }
            Ok(MessageType::Close) => {
                return Err("close_requested".into());
            }
            Ok(MessageType::Auth) => {
                let auth_type = decoder.read_var_uint()?;
                if auth_type == AuthMessageType::Token as u64 {
                    let token = decoder.read_var_string()?;
                    return Err(format!("token_sync:{}", token).into());
                }
                tracing::error!(
                    "Received an authentication message on a connection that is already fully authenticated."
                );
            }
            _ => {
                tracing::error!(
                    "Unable to handle message of type {}: no handler defined!",
                    msg_type
                );
            }
        }

        Ok(())
    }

    async fn read_sync_message<F>(
        &self,
        decoder: &mut Decoder<'_>,
        document: &Arc<Document>,
        connection_id: Option<&str>,
        message_address: &str,
        read_only: bool,
        reply: Option<&(dyn Fn(Vec<u8>) + Send + Sync)>,
        request_first_sync: bool,
        before_sync: &Option<F>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(
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
    {
        let sync_type = decoder.read_var_uint()?;

        // Call beforeSync hook if available
        if let Some(ref callback) = before_sync {
            let payload = decoder.peek_var_uint8_array().unwrap_or_default();
            callback(sync_type, payload).await?;
        }

        match sync_type {
            MESSAGE_YJS_SYNC_STEP1 => {
                let remote_sv_data = decoder.read_var_uint8_array()?;
                let remote_sv = yrs::StateVector::decode_v1(&remote_sv_data)?;

                let update = {
                    let txn = document.doc().transact();
                    txn.encode_state_as_update_v1(&remote_sv)
                };

                let sync_step2 = OutgoingMessage::new(message_address)
                    .create_sync_message()
                    .write_sync_step2(&update);

                if let Some(conn_id) = connection_id {
                    document
                        .send_to_connection(conn_id, &sync_step2.to_vec())
                        .await;
                } else if let Some(reply_fn) = reply {
                    reply_fn(sync_step2.to_vec());
                }

                if request_first_sync {
                    let sv = document.encode_state_vector();
                    let sync_msg = if connection_id.is_some() {
                        OutgoingMessage::new(message_address)
                            .create_sync_message()
                            .write_first_sync_step_for(&sv)
                    } else {
                        OutgoingMessage::new(message_address)
                            .create_sync_reply_message()
                            .write_first_sync_step_for(&sv)
                    };

                    if let Some(conn_id) = connection_id {
                        document
                            .send_to_connection(conn_id, &sync_msg.to_vec())
                            .await;
                    } else if let Some(reply_fn) = reply {
                        reply_fn(sync_msg.to_vec());
                    }
                }
            }
            MESSAGE_YJS_SYNC_STEP2 | MESSAGE_YJS_UPDATE => {
                if read_only {
                    let update_data = decoder.read_var_uint8_array()?;

                    // A read-only Update(2) is always rejected outright (matches TS
                    // MessageReceiver.ts messageYjsUpdate branch: unconditional
                    // writeSyncStatus(false), no snapshot check). Only SyncStep2 gets
                    // the snapshotContainsUpdate optimization below.
                    if sync_type == MESSAGE_YJS_UPDATE {
                        let ack_msg =
                            OutgoingMessage::new(message_address).write_sync_status(false);
                        if let Some(conn_id) = connection_id {
                            document
                                .send_to_connection(conn_id, &ack_msg.to_vec())
                                .await;
                        }
                        return Ok(());
                    }

                    // Read-only SyncStep2: ack positively only if the update contains
                    // no novel data (matches TS Y.snapshotContainsUpdate behavior).
                    let contains_new = match yrs::encode_state_vector_from_update_v1(&update_data) {
                        Ok(update_sv_bytes) => {
                            match yrs::StateVector::decode_v1(&update_sv_bytes) {
                                Ok(update_sv) => {
                                    let doc_sv = {
                                        let txn = document.doc().transact();
                                        txn.state_vector()
                                    };
                                    update_sv
                                        .iter()
                                        .any(|(client, clock)| *clock > doc_sv.get(client))
                                }
                                Err(_) => true, // assume novel on decode failure
                            }
                        }
                        Err(_) => true,
                    };
                    let ack_msg =
                        OutgoingMessage::new(message_address).write_sync_status(!contains_new);
                    if let Some(conn_id) = connection_id {
                        document
                            .send_to_connection(conn_id, &ack_msg.to_vec())
                            .await;
                    }
                    return Ok(());
                }

                let update_data = decoder.read_var_uint8_array()?;

                // apply_update triggers observe_update_v1 which handles
                // broadcasting to connections and firing onChange/onStoreDocument hooks
                document.apply_update(&update_data)?;

                if let Some(conn_id) = connection_id {
                    let ack = OutgoingMessage::new(message_address).write_sync_status(true);
                    document.send_to_connection(conn_id, &ack.to_vec()).await;
                }
            }
            _ => {
                return Err(format!(
                    "Received a sync message with an unknown type: {}",
                    sync_type
                )
                .into());
            }
        }

        Ok(())
    }
}
