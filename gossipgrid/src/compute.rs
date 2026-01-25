use crate::item::ItemEntry;
use mlua::{Lua, serde::LuaSerdeExt};
use serde_json::Value;
use std::collections::HashMap;

pub fn execute_lua(items: Vec<ItemEntry>, script: &str) -> Result<Value, String> {
    let lua = Lua::new();

    // Restricted environment (minimal globals)
    // mlua by default provides a mostly standard environment, but we can further restrict it if needed.
    // For now, let's keep it simple but ensure we are not exposing dangerous crates if we were using a more complex setup.

    let lua_items: Vec<HashMap<String, Value>> = items
        .into_iter()
        .map(|entry| {
            let mut map = HashMap::new();
            map.insert(
                "storage_key".to_string(),
                Value::String(entry.storage_key.to_string()),
            );

            // Try to parse message as JSON
            let message_json: Value =
                serde_json::from_slice(&entry.item.message).unwrap_or_else(|_| {
                    // If not JSON, represent as base64 string or just a raw string if possible
                    // For now, let's just use the raw bytes converted to a string if it's UTF-8, else null
                    match String::from_utf8(entry.item.message.clone()) {
                        Ok(s) => Value::String(s),
                        Err(_) => Value::Null, // Or we could use base64
                    }
                });

            map.insert("data".to_string(), message_json);
            map.insert(
                "hlc".to_string(),
                serde_json::to_value(&entry.item.hlc).unwrap_or(Value::Null),
            );
            map
        })
        .collect();

    let globals = lua.globals();
    let lua_items_table = lua.to_value(&lua_items).map_err(|e| e.to_string())?;
    globals
        .set("items", lua_items_table)
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

        let script = r#"
            local sum = 0
            for i, item in ipairs(items) do
                sum = sum + item.data.val
            end
            return sum
        "#;

        let result = execute_lua(items, script).unwrap();
        assert_eq!(result, Value::from(30));
    }
}
