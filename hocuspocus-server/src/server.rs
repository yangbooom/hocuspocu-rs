use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::accept_async;

use hocuspocus_common::WsReadyState;

use crate::hocuspocus::{Hocuspocus, VERSION};
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
        let _hocuspocus = Hocuspocus::new(Some(config.config));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        Arc::new(Self {
            hocuspocus: Hocuspocus::new(None),
            configuration: RwLock::new(ServerConfiguration::default()),
            shutdown_tx,
            shutdown_rx,
        })
    }

    pub fn with_config(config: ServerConfiguration) -> Arc<Self> {
        let hocuspocus = Hocuspocus::new(Some(config.config));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        Arc::new(Self {
            hocuspocus,
            configuration: RwLock::new(ServerConfiguration {
                port: 80,
                address: "0.0.0.0".to_string(),
                stop_on_signals: true,
                config: Configuration::default(),
            }),
            shutdown_tx,
            shutdown_rx,
        })
    }

    pub async fn listen(self: &Arc<Self>, port: Option<u16>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (addr, actual_port) = {
            let mut config = self.configuration.write().await;
            if let Some(p) = port {
                config.port = p;
            }
            (format!("{}:{}", config.address, config.port), config.port)
        };

        let listener = TcpListener::bind(&addr).await?;

        let config = self.configuration.read().await;
        if !config.config.quiet {
            self.show_start_screen(actual_port).await;
        }
        drop(config);

        let on_listen_payload = OnListenPayload {
            port: actual_port,
        };
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
                                let ws_stream = match accept_async(stream).await {
                                    Ok(ws) => ws,
                                    Err(e) => {
                                        tracing::error!("WebSocket handshake failed: {:?}", e);
                                        return;
                                    }
                                };

                                server.handle_websocket(ws_stream).await;
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
    ) {
        let (write, mut read) = ws_stream.split();
        let write = Arc::new(tokio::sync::Mutex::new(write));

        let ws_sink = Arc::new(TungsteniteWebSocketSink {
            sender: write.clone(),
            state: Arc::new(RwLock::new(WsReadyState::Open)),
        });

        let request = RequestInfo::default();

        let client_connection = self.hocuspocus.handle_connection(
            ws_sink.clone(),
            request,
            None,
        );

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
                Ok(Message::Ping(_)) => {
                    // tungstenite handles pong automatically
                }
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

        let mut state = ws_sink.state.write().await;
        *state = WsReadyState::Closed;
    }

    pub async fn destroy(self: &Arc<Self>) {
        let _ = self.shutdown_tx.send(true);

        if self.hocuspocus.get_documents_count().await == 0 {
            let _ = self
                .hocuspocus
                .hooks_on_destroy(&OnDestroyPayload {})
                .await;
            return;
        }

        self.hocuspocus.close_connections(None).await;
        self.hocuspocus.flush_pending_stores().await;

        // Wait for documents to unload
        let mut attempts = 0;
        while self.hocuspocus.get_documents_count().await > 0 && attempts < 100 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            attempts += 1;
        }

        let _ = self
            .hocuspocus
            .hooks_on_destroy(&OnDestroyPayload {})
            .await;
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

    async fn show_start_screen(&self, port: u16) {
        let config = self.configuration.read().await;
        let name = config
            .config
            .name
            .as_ref()
            .map(|n| format!(" ({})", n))
            .unwrap_or_default();

        println!();
        println!("  Hocuspocus v{}{} running at:", VERSION, name);
        println!();
        println!("  > HTTP: http://{}:{}", config.address, port);
        println!("  > WebSocket: ws://{}:{}", config.address, port);

        let extensions: Vec<String> = config
            .config
            .extensions
            .iter()
            .map(|ext| ext.name().to_string())
            .filter(|n| n != "Extension")
            .collect();

        if !extensions.is_empty() {
            println!();
            println!("  Extensions:");
            for name in &extensions {
                println!("  - {}", name);
            }
        }

        println!();
        println!("  Ready.");
        println!();
    }
}

struct TungsteniteWebSocketSink {
    sender: Arc<
        tokio::sync::Mutex<
            futures_util::stream::SplitSink<
                tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
                Message,
            >,
        >,
    >,
    state: Arc<RwLock<WsReadyState>>,
}

impl WebSocketSink for TungsteniteWebSocketSink {
    fn send(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sender = self.sender.clone();
        let state = self.state.clone();
        tokio::spawn(async move {
            let current_state = *state.read().await;
            if current_state == WsReadyState::Closing || current_state == WsReadyState::Closed {
                return;
            }
            let mut sender = sender.lock().await;
            let _ = sender.send(Message::Binary(data.into())).await;
        });
        Ok(())
    }

    fn close(
        &self,
        code: u16,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sender = self.sender.clone();
        let state = self.state.clone();
        let reason = reason.to_string();
        tokio::spawn(async move {
            let mut s = state.write().await;
            *s = WsReadyState::Closing;
            let mut sender = sender.lock().await;
            let close_frame = tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(
                    code,
                ),
                reason: reason.into(),
            };
            let _ = sender.send(Message::Close(Some(close_frame))).await;
            drop(sender);
            let mut s = state.write().await;
            *s = WsReadyState::Closed;
        });
        Ok(())
    }

    fn ready_state(&self) -> WsReadyState {
        // Synchronous access - use try_read
        match self.state.try_read() {
            Ok(state) => *state,
            Err(_) => WsReadyState::Open,
        }
    }
}
