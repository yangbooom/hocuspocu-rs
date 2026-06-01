// The async hook/callback plumbing uses boxed `Fn -> Pin<Box<dyn Future>>` types
// pervasively; factoring every one into a named alias hurts readability more than
// it helps, so we allow the complexity lints crate-wide.
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

pub mod client_connection;
pub mod connection;
pub mod direct_connection;
pub mod document;
pub mod encoding;
pub mod fragment;
pub mod hocuspocus;
pub mod incoming_message;
pub mod message_receiver;
pub mod outgoing_message;
pub mod server;
pub mod types;
pub mod util;

pub use client_connection::ClientConnection;
pub use connection::Connection;
pub use direct_connection::DirectConnection;
pub use document::Document;
pub use fragment::FragmentBuffer;
pub use hocuspocus::Hocuspocus;
pub use incoming_message::IncomingMessage;
pub use message_receiver::MessageReceiver;
pub use outgoing_message::OutgoingMessage;
pub use server::{Server, ServerConfiguration};
pub use types::*;

/// Shared protocol types (the `hocuspocu-rs-common` crate), re-exported so a
/// dependency on `hocuspocu-rs` alone is enough: e.g. `hocuspocu_rs::common::CloseEvent`.
pub use hocuspocus_common as common;
