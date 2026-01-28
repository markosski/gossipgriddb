# Compute (Server-Side Lua)

GossipGridDB supports server-side computation using Lua scripts that execute on the node where data resides, eliminating network transfer of raw data.

## Performance

| Items | Execution Time | Per Item |
|-------|----------------|----------|
| 1,000 | ~2ms | **~2μs** |
| 10,000 | ~20ms | **~2μs** |

> **Planning Guidance**: At ~2μs per item, processing 100K items takes ~200ms.

## Providing Functions

Functions are provided to the node at startup via a JSON configuration file using the `--functions` flag.

### Configuration Format

The configuration file is a JSON array of objects, where each object defines a named function:

```json
[
  {
    "name": "sum_amounts",
    "script": "local sum = 0; local item = next_item(); while item ~= nil do if item.data and item.data.amount then sum = sum + item.data.amount end; item = next_item(); end; return sum"
  }
]
```

### Starting a Node with Functions

```bash
gossipgrid start --functions functions.json
```

## API

### List Registered Functions

Returns a list of all functions loaded from the configuration file.

```bash
curl -XGET http://127.0.0.1:3001/functions
```

### Execute Function on Items

Add `?fn=<function_name>` to any GET request:

```bash
curl -XGET "http://127.0.0.1:3001/items/user_123?fn=sum_amounts"
```

## Lua Script Pattern

Items are processed lazily via the `next_item()` function:

```lua
local result = 0
local item = next_item()
while item ~= nil do
    -- Available fields:
    -- item.storage_key: string (partition_key/range_key)
    -- item.data: parsed JSON object from message
    -- item.hlc: {timestamp, counter}
    
    result = result + item.data.amount
    item = next_item()
end
return result
```

### Example: Count Items

```lua
local count = 0
local item = next_item()
while item ~= nil do
    count = count + 1
    item = next_item()
end
return count
```

### Example: Find Max Value

```lua
local max = nil
local item = next_item()
while item ~= nil do
    if max == nil or item.data.value > max then
        max = item.data.value
    end
    item = next_item()
end
return max
```

### Example: Collect Keys

```lua
local keys = {}
local item = next_item()
while item ~= nil do
    table.insert(keys, item.storage_key)
    item = next_item()
end
return keys
```

## Security

The Lua environment is sandboxed with the following protections:

| Protection | Details |
|------------|---------|
| **Allowed libraries** | `string`, `table`, `math`, base functions |
| **Blocked libraries** | `io`, `os`, `debug`, `package`, `ffi` |
| **Blocked functions** | `dofile`, `loadfile`, `load`, `require` |
| **Instruction limit** | 1,000,000 instructions (prevents infinite loops) |
| **Memory limit** | 16 MB |
| **Script size limit** | 16 KB |

## Internals

- **Thread-local VMs**: Lua VMs are reused per Tokio worker thread for performance
- **Direct table construction**: Items are converted directly to Lua tables, avoiding serde overhead
- **Lazy iteration**: Items are pulled one-at-a-time via `next_item()`, allowing early termination
