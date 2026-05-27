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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut decoder = Decoder::new(&self.data);

        let _raw_key = decoder.read_var_string()?;
        let msg_type = decoder.read_var_uint()?;

        match MessageType::try_from(msg_type) {
            Ok(MessageType::Sync) | Ok(MessageType::SyncReply) => {
                let is_sync_reply = msg_type == MessageType::SyncReply as u64;
                let request_first_sync = !is_sync_reply;

                self.read_sync_message(
                    &mut decoder,
                    document,
                    connection_id,
                    message_address,
                    read_only,
                    reply,
                    request_first_sync,
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
                    let mut msg = Vec::new();
                    crate::encoding::write_var_string(&mut msg, message_address);
                    crate::encoding::write_var_uint(&mut msg, MessageType::Awareness as u64);
                    crate::encoding::write_var_uint8_array(&mut msg, &update_data);

                    if let Some(reply_fn) = reply {
                        reply_fn(msg);
                    }
                }
            }
            Ok(MessageType::Stateless) => {
                let payload = decoder.read_var_string()?;
                // Handled by connection callback, no direct handling here
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

    async fn read_sync_message(
        &self,
        decoder: &mut Decoder<'_>,
        document: &Arc<Document>,
        connection_id: Option<&str>,
        message_address: &str,
        read_only: bool,
        reply: Option<&(dyn Fn(Vec<u8>) + Send + Sync)>,
        request_first_sync: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sync_type = decoder.read_var_uint()?;

        match sync_type {
            MESSAGE_YJS_SYNC_STEP1 => {
                let remote_sv_data = decoder.read_var_uint8_array()?;
                let remote_sv = yrs::StateVector::decode_v1(&remote_sv_data)?;

                let update = {
                    let txn = document.doc().transact();
                    txn.encode_state_as_update_v1(&remote_sv)
                };

                // Send SyncStep2 (our diff based on their state vector)
                let sync_step2 = OutgoingMessage::new(message_address)
                    .create_sync_message()
                    .write_sync_step2(&update);

                if let Some(conn_id) = connection_id {
                    document.send_to_connection(conn_id, &sync_step2.to_vec()).await;
                } else if let Some(reply_fn) = reply {
                    reply_fn(sync_step2.to_vec());
                }

                // Also send our SyncStep1 to get their updates
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
                        document.send_to_connection(conn_id, &sync_msg.to_vec()).await;
                    } else if let Some(reply_fn) = reply {
                        reply_fn(sync_msg.to_vec());
                    }
                }
            }
            MESSAGE_YJS_SYNC_STEP2 => {
                if read_only {
                    let _update_data = decoder.read_var_uint8_array()?;
                    let ack_msg = OutgoingMessage::new(message_address).write_sync_status(false);
                    if let Some(conn_id) = connection_id {
                        document.send_to_connection(conn_id, &ack_msg.to_vec()).await;
                    }
                    return Ok(());
                }

                let update_data = decoder.read_var_uint8_array()?;
                let origin = connection_id.map(|id| {
                    TransactionOrigin::Connection(ConnectionTransactionOrigin {
                        connection_id: id.to_string(),
                    })
                });

                document.apply_update(&update_data)?;

                if let Some(conn_id) = connection_id {
                    let ack = OutgoingMessage::new(message_address).write_sync_status(true);
                    document.send_to_connection(conn_id, &ack.to_vec()).await;
                }

                document.handle_update(update_data, origin).await;
            }
            MESSAGE_YJS_UPDATE => {
                if read_only {
                    if let Some(conn_id) = connection_id {
                        let ack =
                            OutgoingMessage::new(message_address).write_sync_status(false);
                        document.send_to_connection(conn_id, &ack.to_vec()).await;
                    }
                    return Ok(());
                }

                let update_data = decoder.read_var_uint8_array()?;
                let origin = connection_id.map(|id| {
                    TransactionOrigin::Connection(ConnectionTransactionOrigin {
                        connection_id: id.to_string(),
                    })
                });

                document.apply_update(&update_data)?;

                if let Some(conn_id) = connection_id {
                    let ack = OutgoingMessage::new(message_address).write_sync_status(true);
                    document.send_to_connection(conn_id, &ack.to_vec()).await;
                }

                document.handle_update(update_data, origin).await;
            }
            _ => {
                return Err(format!("Received a sync message with an unknown type: {}", sync_type).into());
            }
        }

        Ok(())
    }
}
