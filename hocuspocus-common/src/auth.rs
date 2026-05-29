use std::io::Cursor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthMessageType {
    Token = 0,
    PermissionDenied = 1,
    Authenticated = 2,
}

impl TryFrom<u64> for AuthMessageType {
    type Error = ();
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(AuthMessageType::Token),
            1 => Ok(AuthMessageType::PermissionDenied),
            2 => Ok(AuthMessageType::Authenticated),
            _ => Err(()),
        }
    }
}

pub fn write_authentication(buf: &mut Vec<u8>, auth: &str) {
    write_var_uint(buf, AuthMessageType::Token as u64);
    write_var_string(buf, auth);
}

pub fn write_permission_denied(buf: &mut Vec<u8>, reason: &str) {
    write_var_uint(buf, AuthMessageType::PermissionDenied as u64);
    write_var_string(buf, reason);
}

pub fn write_authenticated(buf: &mut Vec<u8>, scope: &str) {
    write_var_uint(buf, AuthMessageType::Authenticated as u64);
    write_var_string(buf, scope);
}

pub fn write_token_sync_request(buf: &mut Vec<u8>) {
    write_var_uint(buf, AuthMessageType::Token as u64);
}

fn write_var_uint(buf: &mut Vec<u8>, mut value: u64) {
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

fn write_var_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_var_uint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

pub fn read_var_uint(cursor: &mut Cursor<&[u8]>) -> Option<u64> {
    use std::io::Read;
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8];
        cursor.read_exact(&mut byte).ok()?;
        result |= ((byte[0] & 0x7F) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    Some(result)
}

pub fn read_var_string(cursor: &mut Cursor<&[u8]>) -> Option<String> {
    let len = read_var_uint(cursor)? as usize;
    let pos = cursor.position() as usize;
    let data = cursor.get_ref();
    if pos + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[pos..pos + len]).ok()?.to_string();
    cursor.set_position((pos + len) as u64);
    Some(s)
}
