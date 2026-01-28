//! Function Registry for pre-registered Lua scripts.
//!
//! Allows registering named Lua scripts that can be executed on item queries
//! by referencing them by name via the `fn` query parameter.

use std::collections::HashMap;
use std::sync::RwLock;

/// A registered Lua function that can be executed on items.
#[derive(Debug, Clone)]
pub struct RegisteredFunction {
    /// Name of the function
    pub name: String,
    /// Lua script source code
    pub script: String,
}

/// Registry for pre-registered Lua functions.
///
/// Functions are stored by name and can be looked up for execution.
/// Thread-safe via RwLock.
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, RegisteredFunction>>,
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry {
    /// Create a new empty function registry.
    pub fn new() -> Self {
        FunctionRegistry {
            functions: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new function.
    ///
    /// If a function with the same name already exists, it will be replaced.
    pub fn register(&self, name: String, script: String) -> Result<(), String> {
        let mut functions = self
            .functions
            .write()
            .map_err(|e| format!("Failed to acquire write lock: {e}"))?;

        functions.insert(name.clone(), RegisteredFunction { name, script });
        Ok(())
    }

    /// Get a function by name.
    pub fn get(&self, name: &str) -> Option<RegisteredFunction> {
        let functions = self.functions.read().ok()?;
        functions.get(name).cloned()
    }

    /// List all registered function names.
    pub fn list(&self) -> Vec<String> {
        self.functions
            .read()
            .map(|f| f.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Remove a function by name.
    ///
    /// Returns true if the function was removed, false if it didn't exist.
    pub fn remove(&self, name: &str) -> Result<bool, String> {
        let mut functions = self
            .functions
            .write()
            .map_err(|e| format!("Failed to acquire write lock: {e}"))?;

        Ok(functions.remove(name).is_some())
    }

    /// Get the number of registered functions.
    pub fn len(&self) -> usize {
        self.functions.read().map(|f| f.len()).unwrap_or(0)
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get() {
        let registry = FunctionRegistry::new();

        registry
            .register("sum".to_string(), "return 42".to_string())
            .unwrap();

        let func = registry.get("sum").unwrap();
        assert_eq!(func.name, "sum");
        assert_eq!(func.script, "return 42");
    }

    #[test]
    fn test_list_functions() {
        let registry = FunctionRegistry::new();

        registry
            .register("fn1".to_string(), "return 1".to_string())
            .unwrap();
        registry
            .register("fn2".to_string(), "return 2".to_string())
            .unwrap();

        let names = registry.list();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"fn1".to_string()));
        assert!(names.contains(&"fn2".to_string()));
    }

    #[test]
    fn test_remove_function() {
        let registry = FunctionRegistry::new();

        registry
            .register("temp".to_string(), "return 0".to_string())
            .unwrap();
        assert!(registry.get("temp").is_some());

        let removed = registry.remove("temp").unwrap();
        assert!(removed);
        assert!(registry.get("temp").is_none());

        // Removing non-existent should return false
        let removed_again = registry.remove("temp").unwrap();
        assert!(!removed_again);
    }

    #[test]
    fn test_get_nonexistent() {
        let registry = FunctionRegistry::new();
        assert!(registry.get("nonexistent").is_none());
    }
}
