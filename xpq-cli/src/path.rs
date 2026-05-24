//! Path-based value extraction from msgpack values.
//!
//! Supports dot/bracket notation: `items[0].job.pdf`

use rmpv::Value;

/// Error when walking a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    /// Path syntax is invalid.
    Parse(String),
    /// Path is valid but doesn't exist in the value.
    NotFound,
}

impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathError::Parse(msg) => write!(f, "invalid path: {msg}"),
            PathError::NotFound => write!(f, "path not found"),
        }
    }
}

impl std::error::Error for PathError {}

/// Extracts a value at the given path.
///
/// Path syntax:
/// - `key` or `.key` — access map key
/// - `[0]` — access array index
/// - Chained: `items[0].job.pdf`
///
/// Returns the value at the path, or an error if the path is invalid or not found.
pub fn get<'a>(value: &'a Value, path: &str) -> Result<&'a Value, PathError> {
    let segments = parse(path)?;

    if segments.is_empty() {
        return Ok(value);
    }

    let mut current = value;
    for segment in segments {
        current = match (current, segment) {
            (Value::Map(m), Segment::Key(k)) => m
                .iter()
                .find(|(key, _)| key_matches(key, &k))
                .map(|(_, v)| v)
                .ok_or(PathError::NotFound)?,
            (Value::Array(a), Segment::Index(i)) => a.get(i).ok_or(PathError::NotFound)?,
            _ => return Err(PathError::NotFound),
        };
    }

    Ok(current)
}

/// Path segment.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Key(String),
    Index(usize),
}

/// Parses a path string into segments.
fn parse(path: &str) -> Result<Vec<Segment>, PathError> {
    let mut segments = Vec::new();
    let mut chars = path.chars().peekable();

    // Skip leading dot if present.
    if chars.peek() == Some(&'.') {
        chars.next();
    }

    while chars.peek().is_some() {
        match chars.peek() {
            Some('[') => {
                chars.next(); // consume '['
                let mut num = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        break;
                    }
                    if !c.is_ascii_digit() {
                        return Err(PathError::Parse(format!(
                            "expected digit in index, got '{c}'"
                        )));
                    }
                    num.push(c);
                    chars.next();
                }
                if chars.next() != Some(']') {
                    return Err(PathError::Parse("unclosed bracket".into()));
                }
                if num.is_empty() {
                    return Err(PathError::Parse("empty index".into()));
                }
                let index: usize = num
                    .parse()
                    .map_err(|_| PathError::Parse(format!("invalid index: {num}")))?;
                segments.push(Segment::Index(index));
            }
            Some('.') => {
                chars.next(); // consume '.'
                if chars.peek().is_none()
                    || chars.peek() == Some(&'.')
                    || chars.peek() == Some(&'[')
                {
                    return Err(PathError::Parse("expected key after '.'".into()));
                }
            }
            Some(_) => {
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                if key.is_empty() {
                    return Err(PathError::Parse("empty key".into()));
                }
                segments.push(Segment::Key(key));
            }
            None => break,
        }
    }

    Ok(segments)
}

/// Checks if a map key matches the expected string.
fn key_matches(key: &Value, expected: &str) -> bool {
    match key {
        Value::String(s) => s.as_str() == Some(expected),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: Vec<(&str, Value)>) -> Value {
        Value::Map(
            pairs
                .into_iter()
                .map(|(k, v)| (Value::String(k.into()), v))
                .collect(),
        )
    }

    fn arr(items: Vec<Value>) -> Value {
        Value::Array(items)
    }

    fn int(n: i64) -> Value {
        Value::Integer(n.into())
    }

    fn str(s: &str) -> Value {
        Value::String(s.into())
    }

    #[test]
    fn empty_path_returns_root() {
        let v = int(42);
        assert_eq!(get(&v, "").unwrap(), &v);
    }

    #[test]
    fn simple_key() {
        let v = map(vec![("foo", int(1))]);
        assert_eq!(get(&v, "foo").unwrap(), &int(1));
        assert_eq!(get(&v, ".foo").unwrap(), &int(1));
    }

    #[test]
    fn simple_index() {
        let v = arr(vec![int(10), int(20)]);
        assert_eq!(get(&v, "[0]").unwrap(), &int(10));
        assert_eq!(get(&v, "[1]").unwrap(), &int(20));
    }

    #[test]
    fn nested_path() {
        let v = map(vec![("items", arr(vec![map(vec![("name", str("test"))])]))]);
        assert_eq!(get(&v, "items[0].name").unwrap(), &str("test"));
    }

    #[test]
    fn not_found() {
        let v = map(vec![("foo", int(1))]);
        assert_eq!(get(&v, "bar"), Err(PathError::NotFound));
        assert_eq!(get(&v, "foo[0]"), Err(PathError::NotFound));
    }

    #[test]
    fn parse_errors() {
        assert!(matches!(get(&int(1), "["), Err(PathError::Parse(_))));
        assert!(matches!(get(&int(1), "[]"), Err(PathError::Parse(_))));
        assert!(matches!(get(&int(1), "[a]"), Err(PathError::Parse(_))));
        assert!(matches!(get(&int(1), "foo."), Err(PathError::Parse(_))));
        assert!(matches!(get(&int(1), ".."), Err(PathError::Parse(_))));
    }
}
