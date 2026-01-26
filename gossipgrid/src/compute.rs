use crate::item::ItemEntry;
use mlua::{Lua, LuaOptions, StdLib, serde::LuaSerdeExt};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;

/// Maximum script size in bytes
const MAX_SCRIPT_SIZE: usize = 16 * 1024;

/// Instruction count limit before timeout (prevents infinite loops)
const INSTRUCTION_LIMIT: u32 = 1_000_000;

/// Memory limit in bytes
const MEMORY_LIMIT: usize = 16 * 1024 * 1024;

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
/// # Security
///
/// The Lua environment is sandboxed with the following protections:
/// - Only safe standard libraries: `string`, `table`, `math`, `base` (subset)
/// - No access to: `io`, `os`, `debug`, `package`, `ffi`
/// - Dangerous base functions removed: `dofile`, `loadfile`, `load`, `require`
/// - Instruction limit to prevent infinite loops
/// - Memory limit to prevent OOM attacks
/// - Script size limit
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
    // Validate script size
    if script.len() > MAX_SCRIPT_SIZE {
        return Err(format!(
            "Script too large: {} bytes (max: {} bytes)",
            script.len(),
            MAX_SCRIPT_SIZE
        ));
    }

    // Create sandboxed Lua environment with only safe libraries
    // Excludes: IO, OS, DEBUG, PACKAGE, FFI (dangerous capabilities)
    // Note: Base functions (print, pairs, ipairs, type, etc.) are always available
    let lua = Lua::new_with(
        StdLib::STRING | StdLib::TABLE | StdLib::MATH,
        LuaOptions::default(),
    )
    .map_err(|e| format!("Failed to create Lua sandbox: {e}"))?;

    // Set memory limit
    lua.set_memory_limit(MEMORY_LIMIT)
        .map_err(|e| format!("Failed to set memory limit: {e}"))?;

    // Set instruction limit hook to prevent infinite loops
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(INSTRUCTION_LIMIT),
        |_lua, _debug| {
            Err(mlua::Error::RuntimeError(
                "Script execution exceeded instruction limit".into(),
            ))
        },
    );

    // Remove dangerous base functions that could be used for code injection
    let globals = lua.globals();
    for dangerous_fn in ["dofile", "loadfile", "load", "require", "collectgarbage"] {
        globals
            .set(dangerous_fn, mlua::Value::Nil)
            .map_err(|e| format!("Failed to remove {dangerous_fn}: {e}"))?;
    }

    // Wrap iterator in RefCell for interior mutability
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

    // Security tests

    #[test]
    fn test_sandbox_blocks_os_execute() {
        let items: Vec<ItemEntry> = vec![];
        let script = r#"os.execute("echo pwned")"#;

        let result = execute_lua(items.into_iter(), script);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("nil"));
    }

    #[test]
    fn test_sandbox_blocks_io_open() {
        let items: Vec<ItemEntry> = vec![];
        let script = r#"io.open("/etc/passwd")"#;

        let result = execute_lua(items.into_iter(), script);
        assert!(result.is_err());
    }

    #[test]
    fn test_sandbox_blocks_loadfile() {
        let items: Vec<ItemEntry> = vec![];
        let script = r#"loadfile("/tmp/evil.lua")"#;

        let result = execute_lua(items.into_iter(), script);
        assert!(result.is_err());
    }

    #[test]
    fn test_sandbox_blocks_require() {
        let items: Vec<ItemEntry> = vec![];
        let script = r#"require("os")"#;

        let result = execute_lua(items.into_iter(), script);
        assert!(result.is_err());
    }

    #[test]
    fn test_script_size_limit() {
        let items: Vec<ItemEntry> = vec![];
        // Create a script larger than MAX_SCRIPT_SIZE
        let script = "x".repeat(MAX_SCRIPT_SIZE + 1);

        let result = execute_lua(items.into_iter(), &script);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Script too large"));
    }

    #[test]
    fn test_infinite_loop_protection() {
        let items: Vec<ItemEntry> = vec![];
        let script = r#"while true do end"#;

        let result = execute_lua(items.into_iter(), script);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("instruction limit"));
    }
}
