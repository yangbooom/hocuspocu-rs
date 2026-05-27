use crate::encoding::{self, Decoder};

pub struct IncomingMessage<'a> {
    decoder: Decoder<'a>,
    encoder: Vec<u8>,
}

impl<'a> IncomingMessage<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            decoder: Decoder::new(data),
            encoder: Vec::new(),
        }
    }

    pub fn read_var_uint(&mut self) -> std::io::Result<u64> {
        self.decoder.read_var_uint()
    }

    pub fn read_var_string(&mut self) -> std::io::Result<String> {
        self.decoder.read_var_string()
    }

    pub fn read_var_uint8_array(&mut self) -> std::io::Result<Vec<u8>> {
        self.decoder.read_var_uint8_array()
    }

    pub fn peek_var_uint8_array(&mut self) -> std::io::Result<Vec<u8>> {
        self.decoder.peek_var_uint8_array()
    }

    pub fn has_content(&self) -> bool {
        self.decoder.has_content()
    }

    pub fn remaining(&self) -> &[u8] {
        self.decoder.remaining()
    }

    pub fn write_var_uint(&mut self, value: u64) {
        encoding::write_var_uint(&mut self.encoder, value);
    }

    pub fn write_var_string(&mut self, s: &str) {
        encoding::write_var_string(&mut self.encoder, s);
    }

    pub fn encoder_to_vec(&self) -> Vec<u8> {
        self.encoder.clone()
    }

    pub fn encoder_len(&self) -> usize {
        self.encoder.len()
    }
}
