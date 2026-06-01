use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::accept_hdr_async_with_config;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, WebSocketConfig};

use hocuspocus_common::WsReadyState;

use crate::hocuspocus::Hocuspocus;
use crate::types::*;

pub struct ServerConfiguration {
    pub port: u16,
    pub address: String,
    pub stop_on_signals: bool,
    pub config: Configuration,
}

impl Default for ServerConfiguration {
    fn default() -> Self {
        Self {
            port: 80,
            address: "0.0.0.0".to_string(),
            stop_on_signals: true,
            config: Configuration::default(),
        }
    }
}

pub struct Server {
    pub hocuspocus: Arc<Hocuspocus>,
    configuration: RwLock<ServerConfiguration>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl Server {
    pub fn new(configuration: Option<ServerConfiguration>) -> Arc<Self> {
        let config = configuration.unwrap_or_default();
        let hocuspocus = Hocuspocus::new(Some(config.config));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        Arc::new(Self {
            hocuspocus,
            configuration: RwLock::new(ServerConfiguration {
                port: config.port,
                address: config.address,
                stop_on_signals: config.stop_on_signals,
                config: Configuration::default(),
            }),
            shutdown_tx,
            shutdown_rx,
        })
    }

    pub fn with_config(config: ServerConfiguration) -> Arc<Self> {
        let port = config.port;
        let address = config.address.clone();
        let stop_on_signals = config.stop_on_signals;
        let hocuspocus = Hocuspocus::new(Some(config.config));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        Arc::new(Self {
            hocuspocus,
            configuration: RwLock::new(ServerConfiguration {
                port,
                address,
                stop_on_signals,
                config: Configuration::default(),
            }),
            shutdown_tx,
            shutdown_rx,
        })
    }

    // The accept_hdr_async callback must return `Result<Response, ErrorResponse>`,
    // and `ErrorResponse` (http::Response<Option<String>>) is a large Err variant
    // we don't control — the signature is dictated by tungstenite's Callback trait.
    #[allow(clippy::result_large_err)]
    pub async fn listen(
        self: &Arc<Self>,
        port: Option<u16>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (addr, actual_port) = {
            let mut config = self.configuration.write().await;
            if let Some(p) = port {
                config.port = p;
            }
            (format!("{}:{}", config.address, config.port), config.port)
        };

        let listener = TcpListener::bind(&addr).await?;

        let on_listen_payload = OnListenPayload { port: actual_port };
        let _ = self.hocuspocus.hooks_on_listen(&on_listen_payload).await;

        let mut shutdown_rx = self.shutdown_rx.clone();
        let server = self.clone();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            let server = server.clone();
                            tokio::spawn(async move {
                                // Capture the upgrade request's URL + headers during the
                                // handshake so they reach connection hooks (matches TS
                                // getParameters / requestHeaders behavior).
                                let captured: Arc<std::sync::Mutex<(String, HashMap<String, String>)>> =
                                    Arc::new(std::sync::Mutex::new((String::new(), HashMap::new())));
                                let cap = captured.clone();
                                // Yjs frames are small, but tungstenite eagerly allocates a
                                // 128 KiB read buffer per connection by default, which dominates
                                // per-connection memory. A 16 KiB buffer cuts that ~8x. The
                                // message-size limits (max_message_size 64 MiB) are unchanged, so
                                // large initial syncs are still received fully, just in more reads.
                                let ws_config = WebSocketConfig::default()
                                    .read_buffer_size(16 * 1024)
                                    .write_buffer_size(16 * 1024);
                                let ws_stream = match accept_hdr_async_with_config(
                                    stream,
                                    move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                                        let url = req.uri().to_string();
                                        let mut headers = HashMap::new();
                                        for (k, v) in req.headers().iter() {
                                            if let Ok(val) = v.to_str() {
                                                headers.insert(k.as_str().to_lowercase(), val.to_string());
                                            }
                                        }
                                        if let Ok(mut g) = cap.lock() {
                                            *g = (url, headers);
                                        }
                                        Ok(resp)
                                    },
                                    Some(ws_config),
                                )
                                .await
                                {
                                    Ok(ws) => ws,
                                    Err(e) => {
                                        tracing::error!("WebSocket handshake failed: {:?}", e);
                                        return;
                                    }
                                };

                                let (url, headers) = captured
                                    .lock()
                                    .map(|g| g.clone())
                                    .unwrap_or_default();
                                let parameters = crate::types::get_parameters(&url);
                                let request = RequestInfo { headers, parameters, url };

                                server.handle_websocket(ws_stream, request).await;
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to accept connection: {:?}", e);
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_websocket(
        self: &Arc<Self>,
        ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        request: RequestInfo,
    ) {
        let (mut write, mut read) = ws_stream.split();
        let state = Arc::new(std::sync::RwLock::new(WsReadyState::Open));

        // One writer task owns the sink half and drains an ordered command channel.
        // This replaces the old per-message `tokio::spawn` + `Mutex` sink: hand-off
        // is now a non-blocking channel push (lower latency, no lock contention) and
        // frames reach each client in exact send() order. Bursts already queued are
        // coalesced with feed() + a single flush; once the queue drains we flush and
        // park on the next command, so idle messages still go out immediately.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WriterCmd>();
        {
            let state = state.clone();
            tokio::spawn(async move {
                'outer: while let Some(mut cmd) = rx.recv().await {
                    loop {
                        match cmd {
                            WriterCmd::Send(data) => {
                                if write.feed(Message::Binary(data.into())).await.is_err() {
                                    break 'outer;
                                }
                            }
                            WriterCmd::Close { code, reason } => {
                                let _ = write.flush().await;
                                let frame = CloseFrame {
                                    code: CloseCode::from(code),
                                    reason: reason.into(),
                                };
                                let _ = write.send(Message::Close(Some(frame))).await;
                                break 'outer;
                            }
                        }
                        match rx.try_recv() {
                            Ok(next) => cmd = next,
                            Err(_) => break,
                        }
                    }
                    if write.flush().await.is_err() {
                        break;
                    }
                }
                if let Ok(mut s) = state.write() {
                    *s = WsReadyState::Closed;
                }
            });
        }

        let ws_sink = Arc::new(TungsteniteWebSocketSink {
            tx,
            state: state.clone(),
        });

        let client_connection = self
            .hocuspocus
            .handle_connection(ws_sink.clone(), request, None);

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    client_connection.handle_message(data.to_vec()).await;
                }
                Ok(Message::Close(frame)) => {
                    let event = frame.map(|f| hocuspocus_common::CloseEvent {
                        code: f.code.into(),
                        reason: f.reason.to_string(),
                    });
                    client_connection.handle_close(event).await;
                    break;
                }
                Ok(Message::Ping(_)) => {}
                Err(e) => {
                    tracing::error!("WebSocket error: {:?}", e);
                    client_connection
                        .handle_close(Some(hocuspocus_common::reset_connection()))
                        .await;
                    break;
                }
                _ => {}
            }
        }

        let mut s = state.write().unwrap();
        *s = WsReadyState::Closed;
    }

    pub async fn destroy(self: &Arc<Self>) {
        let _ = self.shutdown_tx.send(true);

        if self.hocuspocus.get_documents_count().await == 0 {
            let _ = self.hocuspocus.hooks_on_destroy(&OnDestroyPayload {}).await;
            return;
        }

        self.hocuspocus.close_connections(None).await;
        self.hocuspocus.flush_pending_stores().await;

        let mut attempts = 0;
        while self.hocuspocus.get_documents_count().await > 0 && attempts < 100 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            attempts += 1;
        }

        let _ = self.hocuspocus.hooks_on_destroy(&OnDestroyPayload {}).await;
    }

    pub async fn url(&self) -> String {
        let config = self.configuration.read().await;
        format!("{}:{}", config.address, config.port)
    }

    pub async fn websocket_url(&self) -> String {
        format!("ws://{}", self.url().await)
    }

    pub async fn http_url(&self) -> String {
        format!("http://{}", self.url().await)
    }
}

/// A command queued to a connection's writer task, in send() order.
enum WriterCmd {
    Send(Vec<u8>),
    Close { code: u16, reason: String },
}

struct TungsteniteWebSocketSink {
    tx: tokio::sync::mpsc::UnboundedSender<WriterCmd>,
    state: Arc<std::sync::RwLock<WsReadyState>>,
}

impl WebSocketSink for TungsteniteWebSocketSink {
    fn send(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let state = *self.state.read().unwrap();
        if state == WsReadyState::Closing || state == WsReadyState::Closed {
            return Ok(());
        }
        // Non-blocking, ordered hand-off to the writer task. A send error means the
        // writer is gone (socket dead) — surface it so the caller marks the
        // connection closed (Connection::send checks this).
        self.tx
            .send(WriterCmd::Send(data))
            .map_err(|_| "websocket writer task closed".into())
    }

    fn close(
        &self,
        code: u16,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        {
            let mut s = self.state.write().unwrap();
            *s = WsReadyState::Closing;
        }
        // Queued behind any already-buffered frames so they flush before the close
        // frame; a send error just means the writer already exited (already closed).
        let _ = self.tx.send(WriterCmd::Close {
            code,
            reason: reason.to_string(),
        });
        Ok(())
    }

    fn ready_state(&self) -> WsReadyState {
        *self.state.read().unwrap()
    }
}
