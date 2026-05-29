#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseEvent {
    pub code: u16,
    pub reason: String,
}

impl CloseEvent {
    pub const fn new(code: u16, _reason: &'static str) -> Self {
        Self {
            code,
            reason: String::new(),
        }
    }
}

pub fn message_too_big() -> CloseEvent {
    CloseEvent {
        code: 1009,
        reason: "Message Too Big".into(),
    }
}

pub fn reset_connection() -> CloseEvent {
    CloseEvent {
        code: 4205,
        reason: "Reset Connection".into(),
    }
}

pub fn unauthorized() -> CloseEvent {
    CloseEvent {
        code: 4401,
        reason: "Unauthorized".into(),
    }
}

pub fn forbidden() -> CloseEvent {
    CloseEvent {
        code: 4403,
        reason: "Forbidden".into(),
    }
}

pub fn connection_timeout() -> CloseEvent {
    CloseEvent {
        code: 4408,
        reason: "Connection Timeout".into(),
    }
}
