use crate::{DataStoreError, PartitionKey, RangeKey, StorageKey, item::Item};

const KEY_SEPARATOR: char = '/';
const KEY_SEPARATOR_BYTE: u8 = b'/';

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

pub(crate) fn encode_storage_key(key: &StorageKey) -> Vec<u8> {
    let mut encoded = escape_key_component(key.partition_key.value());
    if let Some(range_key) = &key.range_key {
        encoded.push(KEY_SEPARATOR);
        encoded.push_str(&escape_key_component(range_key.value()));
    }
    encoded.into_bytes()
}

pub(crate) fn decode_storage_key(encoded: &[u8]) -> Result<StorageKey, DataStoreError> {
    let encoded = std::str::from_utf8(encoded).map_err(|err| {
        DataStoreError::StorageKeyParsingError(format!("invalid UTF-8 key bytes: {err}"))
    })?;

    let (raw_pk, raw_rk) = split_encoded_key(encoded).ok_or_else(|| {
        DataStoreError::StorageKeyParsingError("invalid escaped key encoding".to_string())
    })?;

    let partition_key = PartitionKey(unescape_key_component(raw_pk).ok_or_else(|| {
        DataStoreError::StorageKeyParsingError("invalid partition key escape sequence".to_string())
    })?);

    let range_key = match raw_rk {
        Some(raw_rk) => Some(RangeKey(unescape_key_component(raw_rk).ok_or_else(
            || {
                DataStoreError::StorageKeyParsingError(
                    "invalid range key escape sequence".to_string(),
                )
            },
        )?)),
        None => None,
    };

    Ok(StorageKey::new(partition_key, range_key))
}

pub(crate) fn encoded_partition_prefix(partition_key: &PartitionKey) -> Vec<u8> {
    escape_key_component(partition_key.value()).into_bytes()
}

pub(crate) fn matches_partition_key(encoded_key: &[u8], encoded_partition_prefix: &[u8]) -> bool {
    encoded_key == encoded_partition_prefix
        || encoded_key
            .strip_prefix(encoded_partition_prefix)
            .is_some_and(|suffix| suffix.first() == Some(&KEY_SEPARATOR_BYTE))
}
