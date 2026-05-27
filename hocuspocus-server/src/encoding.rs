use std::io::{self};

pub fn write_var_uint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value > 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

pub fn write_var_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_var_uint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

pub fn write_var_uint8_array(buf: &mut Vec<u8>, data: &[u8]) {
    write_var_uint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

pub struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn set_position(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub fn has_content(&self) -> bool {
        self.pos < self.data.len()
    }

    pub fn remaining(&self) -> &'a [u8] {
        &self.data[self.pos..]
    }

    pub fn read_var_uint(&mut self) -> io::Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        loop {
            if self.pos >= self.data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected end of data",
                ));
            }
            let byte = self.data[self.pos];
            self.pos += 1;
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift >= 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "varuint too large",
                ));
            }
        }
        Ok(result)
    }

    pub fn read_var_string(&mut self) -> io::Result<String> {
        let len = self.read_var_uint()? as usize;
        if self.pos + len > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "string data truncated",
            ));
        }
        let s = std::str::from_utf8(&self.data[self.pos..self.pos + len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .to_string();
        self.pos += len;
        Ok(s)
    }

    pub fn read_var_uint8_array(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_var_uint()? as usize;
        if self.pos + len > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "array data truncated",
            ));
        }
        let data = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(data)
    }

    pub fn peek_var_uint8_array(&mut self) -> io::Result<Vec<u8>> {
        let saved_pos = self.pos;
        let result = self.read_var_uint8_array();
        self.pos = saved_pos;
        result
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if self.pos + buf.len() > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "not enough data",
            ));
        }
        buf.copy_from_slice(&self.data[self.pos..self.pos + buf.len()]);
        self.pos += buf.len();
        Ok(())
    }
}
