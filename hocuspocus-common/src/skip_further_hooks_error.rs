use std::fmt;

#[derive(Debug)]
pub struct SkipFurtherHooksError {
    pub message: String,
}

impl SkipFurtherHooksError {
    pub fn new(message: Option<&str>) -> Self {
        Self {
            message: message.unwrap_or("Further hooks skipped").to_string(),
        }
    }
}

impl fmt::Display for SkipFurtherHooksError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SkipFurtherHooksError: {}", self.message)
    }
}

impl std::error::Error for SkipFurtherHooksError {}
