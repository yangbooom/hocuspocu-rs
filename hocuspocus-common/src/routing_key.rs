const SEPARATOR: char = '\0';

pub fn make_routing_key(document_name: &str, session_id: &str) -> String {
    format!("{}{}{}", document_name, SEPARATOR, session_id)
}

pub fn parse_routing_key(key: &str) -> (&str, Option<&str>) {
    match key.find(SEPARATOR) {
        Some(idx) => (&key[..idx], Some(&key[idx + 1..])),
        None => (key, None),
    }
}
