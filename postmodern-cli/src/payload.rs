//! Payload handling for YAML display.

use rmpv::Value;

/// Maximum binary length to display as hex (longer values show `<N bytes>`).
const HEX_DISPLAY_THRESHOLD: usize = 512;

/// Minimum length for byte array heuristic (fallback for legacy data without `serde_bytes`).
const BYTE_ARRAY_THRESHOLD: usize = 10_000;

/// Prepares an rmpv Value for YAML serialization by converting binary data to strings.
///
/// - Binary up to 512 bytes: displayed as hex string `0xaabb...`
/// - Binary over 512 bytes: displayed as `<N bytes>`
/// - Arrays that look like byte arrays (>10k integers 0-255): displayed as `<N bytes>`
/// - Ext values: displayed as `<ext TAG: N bytes>`
pub fn prepare_for_yaml(v: &Value) -> Value {
    match v {
        Value::Binary(b) => {
            if b.len() <= HEX_DISPLAY_THRESHOLD {
                let mut hex = String::with_capacity(2 + b.len() * 2);
                hex.push_str("0x");
                for byte in b {
                    hex.push_str(&format!("{byte:02x}"));
                }
                Value::String(hex.into())
            } else {
                Value::String(format!("<{} bytes>", b.len()).into())
            }
        }
        Value::Array(arr) => {
            if looks_like_byte_array(arr) {
                Value::String(format!("<{} bytes>", arr.len()).into())
            } else {
                Value::Array(arr.iter().map(prepare_for_yaml).collect())
            }
        }
        Value::Map(map) => Value::Map(
            map.iter()
                .map(|(k, v)| (prepare_for_yaml(k), prepare_for_yaml(v)))
                .collect(),
        ),
        Value::Ext(tag, data) => {
            Value::String(format!("<ext {}: {} bytes>", tag, data.len()).into())
        }
        // Primitives pass through unchanged
        Value::Nil => Value::Nil,
        Value::Boolean(b) => Value::Boolean(*b),
        Value::Integer(n) => Value::Integer(*n),
        Value::F32(n) => Value::F32(*n),
        Value::F64(n) => Value::F64(*n),
        Value::String(s) => Value::String(s.clone()),
    }
}

/// Checks if an array looks like serialized binary data (all elements are integers 0-255).
fn looks_like_byte_array(arr: &[Value]) -> bool {
    if arr.len() < BYTE_ARRAY_THRESHOLD {
        return false;
    }
    arr.iter().all(|v| match v {
        Value::Integer(n) => n.as_u64().is_some_and(|n| n <= 255),
        _ => false,
    })
}

/// Decodes msgpack bytes into an rmpv Value.
pub fn from_msgpack(bytes: &[u8]) -> Result<Value, rmpv::decode::Error> {
    rmpv::decode::read_value(&mut &bytes[..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_binary_as_hex() {
        let value = Value::Binary(vec![0xde, 0xad, 0xbe, 0xef]);
        let prepared = prepare_for_yaml(&value);
        assert_eq!(prepared, Value::String("0xdeadbeef".into()));
    }

    #[test]
    fn large_binary_summarized() {
        let value = Value::Binary(vec![0u8; 1024]);
        let prepared = prepare_for_yaml(&value);
        assert_eq!(prepared, Value::String("<1024 bytes>".into()));
    }

    #[test]
    fn nested_structure() {
        let value = Value::Map(vec![(
            Value::String("items".into()),
            Value::Array(vec![Value::Integer(1.into()), Value::Integer(2.into())]),
        )]);
        let prepared = prepare_for_yaml(&value);
        // Should pass through unchanged (no binaries)
        assert_eq!(prepared, value);
    }
}
