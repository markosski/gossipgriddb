## ADDED Requirements

### Requirement: HTTP Batch Write Endpoint
The system SHALL expose an HTTP endpoint (`POST /items/batch`) that accepts an array of `ItemCreateUpdate` objects. The endpoint MUST group the items by their respective leader node, process local items natively, and proxy remote items to their respective leaders concurrently. The endpoint MUST wait for all subset operations to complete before returning a comprehensive response to the client.

#### Scenario: Submitting a mixed batch of local and remote items
- **WHEN** a client sends a batch request containing items belonging to partitions where the current node is the leader, and partitions where other nodes are the leaders
- **THEN** the local node processes its items in a single IO transaction, concurrently proxies the other items to the remote leaders, and returns a combined result only after all operations finish

#### Scenario: Submitting a valid batch to a single node
- **WHEN** a batch of items is submitted via `POST /items/batch`
- **THEN** the system will write the items and return a list of successfully saved items in the response envelope

### Requirement: Smart Client Batch Write Method
The smart client (`gossipgrid-client`) SHALL provide a `put_batch` method to submit multiple items efficiently. The client MUST logically group the input items by their target leader node, based on the `partition_key` topology, and send independent HTTP batch requests directly to each responsible leader node concurrently.

#### Scenario: Using put_batch with items for multiple partitions
- **WHEN** a user calls `client.put_batch(items)` with items that span three different partition leaders
- **THEN** the smart client groups the items into three sub-batches and executes three concurrent HTTP POST requests directly to those three leader nodes, aggregating the final result back to the user
