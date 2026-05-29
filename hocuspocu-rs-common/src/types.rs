#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WsReadyState {
    Connecting = 0,
    Open = 1,
    Closing = 2,
    Closed = 3,
}
