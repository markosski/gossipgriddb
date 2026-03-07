
/// Utility functions for encoding and decoding string keys for SSTable storage.
pub(crate) fn escape_key_component(component: &str) -> String {
    let mut escaped = String::with_capacity(component.len());
    for ch in component.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '/' => escaped.push_str("\\/"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

/// Splits an encoded key into its first component (partition key) and the remaining string (range key)
pub(crate) fn split_encoded_key(encoded: &str) -> Option<(&str, Option<&str>)> {
    let mut escaped = false;

    for (idx, ch) in encoded.char_indices() {
        match ch {
            '\\' if !escaped => escaped = true,
            '/' if !escaped => return Some((&encoded[..idx], Some(&encoded[idx + 1..]))),
            _ => escaped = false,
        }
    }

    if escaped { None } else { Some((encoded, None)) }
}

pub(crate) fn unescape_key_component(component: &str) -> Option<String> {
    let mut unescaped = String::with_capacity(component.len());
    let mut escaped = false;

    for ch in component.chars() {
        if escaped {
            match ch {
                '\\' | '/' => unescaped.push(ch),
                _ => return None,
            }
            escaped = false;
            continue;
        }

        match ch {
            '\\' => escaped = true,
            _ => unescaped.push(ch),
        }
    }

    if escaped { None } else { Some(unescaped) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn foo_test() {
        let foobar = "foobar";
        let foo = &foobar[..3];

        assert_eq!(foo, "foo");
    }
}