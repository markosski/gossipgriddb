use crate::item::ItemEntry;
use mlua::{Lua, serde::LuaSerdeExt};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;

/// Converts an ItemEntry to a Lua-compatible HashMap
fn item_entry_to_lua_value(entry: &ItemEntry) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    map.insert(
        "storage_key".to_string(),
        Value::String(entry.storage_key.to_string()),
    );

    // Try to parse message as JSON
    let message_json: Value = serde_json::from_slice(&entry.item.message).unwrap_or_else(|_| {
        // If not JSON, represent as a raw string if it's UTF-8, else null
        match String::from_utf8(entry.item.message.clone()) {
            Ok(s) => Value::String(s),
            Err(_) => Value::Null,
        }
    });

    map.insert("data".to_string(), message_json);
    map.insert(
        "hlc".to_string(),
        serde_json::to_value(&entry.item.hlc).unwrap_or(Value::Null),
    );
    map
}

/// Execute a Lua script with lazy iteration over items.
///
/// Items are pulled one at a time via `next_item()` function exposed to Lua,
/// avoiding upfront collection and reducing memory copying.
///
/// # Lua Usage
/// ```lua
/// local item = next_item()
/// while item ~= nil do
///     -- process item (has: storage_key, data, hlc)
///     item = next_item()
/// end
/// ```
pub fn execute_lua<I>(items: I, script: &str) -> Result<Value, String>
where
    I: Iterator<Item = ItemEntry> + 'static,
{
    let lua = Lua::new();

    // Wrap iterator in RefCell for interior mutability
    // The closure is the sole owner, so no Rc needed
    let items = RefCell::new(items);

    // Create next_item() function that Lua can call to pull items lazily
    let next_item = lua
        .create_function(move |lua_ctx, ()| {
            let mut iter = items.borrow_mut();
            match iter.next() {
                Some(entry) => {
                    let map = item_entry_to_lua_value(&entry);
                    lua_ctx.to_value(&map)
                }
                None => Ok(mlua::Value::Nil),
            }
        })
        .map_err(|e| e.to_string())?;

    let globals = lua.globals();
    globals
        .set("next_item", next_item)
        .map_err(|e| e.to_string())?;

    let result: mlua::Value = lua.load(script).eval().map_err(|e| e.to_string())?;

    let json_result = lua.from_value::<Value>(result).map_err(|e| e.to_string())?;

    Ok(json_result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::HLC;
    use crate::item::{Item, ItemStatus};
    use crate::store::{PartitionKey, RangeKey, StorageKey};

    #[test]
    fn test_execute_lua_sum() {
        let items = vec![
            ItemEntry::new(
                StorageKey::new(
                    PartitionKey("p1".to_string()),
                    Some(RangeKey("r1".to_string())),
                ),
                Item {
                    message: b"{\"val\": 10}".to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::default(),
                },
            ),
            ItemEntry::new(
                StorageKey::new(
                    PartitionKey("p1".to_string()),
                    Some(RangeKey("r2".to_string())),
                ),
                Item {
                    message: b"{\"val\": 20}".to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::default(),
                },
            ),
        ];

        // New lazy iteration pattern
        let script = r#"
            local sum = 0
            local item = next_item()
            while item ~= nil do
                sum = sum + item.data.val
                item = next_item()
            end
            return sum
        "#;

        let result = execute_lua(items.into_iter(), script).unwrap();
        assert_eq!(result, Value::from(30));
    }

    #[test]
    fn test_execute_lua_empty_iterator() {
        let items: Vec<ItemEntry> = vec![];

        let script = r#"
            local count = 0
            local item = next_item()
            while item ~= nil do
                count = count + 1
                item = next_item()
            end
            return count
        "#;

        let result = execute_lua(items.into_iter(), script).unwrap();
        assert_eq!(result, Value::from(0));
    }

    #[test]
    fn test_execute_lua_collect_keys() {
        let items = vec![
            ItemEntry::new(
                StorageKey::new(
                    PartitionKey("p1".to_string()),
                    Some(RangeKey("a".to_string())),
                ),
                Item {
                    message: b"{}".to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::default(),
                },
            ),
            ItemEntry::new(
                StorageKey::new(
                    PartitionKey("p1".to_string()),
                    Some(RangeKey("b".to_string())),
                ),
                Item {
                    message: b"{}".to_vec(),
                    status: ItemStatus::Active,
                    hlc: HLC::default(),
                },
            ),
        ];

        let script = r#"
            local keys = {}
            local item = next_item()
            while item ~= nil do
                table.insert(keys, item.storage_key)
                item = next_item()
            end
            return keys
        "#;

        let result = execute_lua(items.into_iter(), script).unwrap();
        let keys: Vec<String> = serde_json::from_value(result).unwrap();
        assert_eq!(keys.len(), 2);
    }
}
