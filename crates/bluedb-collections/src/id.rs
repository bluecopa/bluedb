use std::time::{SystemTime, UNIX_EPOCH};

/// A 24-hex-character ObjectId-like id: 4-byte big-endian seconds + 8 random bytes.
/// The time prefix makes ids roughly creation-ordered (sortable as text).
pub fn new_object_id() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    let mut bytes = [0u8; 12];
    bytes[0..4].copy_from_slice(&secs.to_be_bytes());
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut bytes[4..12]);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn object_id_is_24_lowercase_hex_and_unique() {
        let a = new_object_id();
        let b = new_object_id();
        assert_eq!(a.len(), 24, "{a}");
        assert!(a
            .bytes()
            .all(|c: u8| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_ne!(a, b);
    }
    #[test]
    fn object_ids_sort_by_creation_time() {
        let a = new_object_id();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let b = new_object_id();
        assert!(
            a < b,
            "ObjectIds should be roughly time-ordered: {a} !< {b}"
        );
    }
}
