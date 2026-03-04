## MODIFIED Requirements

### Requirement: Client Routing
The smart client SHALL route requests directly to the correct node based on the partition key. The client MUST exclude nodes in the `Leaving` state from write routing and adapt its routing table when topology changes due to node join or leave events.

#### Scenario: Read operation routing
- **WHEN** a client performs a read operation (GET) using the smart client
- **THEN** the request is routed directly to a known replica for that partition

#### Scenario: Write operation routing
- **WHEN** a client performs a write or delete operation using the smart client
- **THEN** the request is routed directly to the known leader for that partition, excluding any node in the `Leaving` state

#### Scenario: Routing adapts to node join
- **WHEN** a new node joins the cluster and partition assignments change
- **THEN** the client MUST update its routing table to reflect the new assignments and route subsequent requests accordingly

#### Scenario: Routing adapts to node leave
- **WHEN** a node transitions to `Leaving` state
- **THEN** the client MUST immediately stop routing new write requests to that node and re-route to the newly elected leader for affected partitions

#### Scenario: Read routing during node leave
- **WHEN** a node is in the `Leaving` state and has not yet shut down
- **THEN** the client MAY continue routing read requests to that node until it fully disconnects

## ADDED Requirements

### Requirement: Direct API Request Redirection During Migration
When a node receives a request for a partition it no longer owns due to a topology change (node join or leave), the node SHALL respond with an HTTP `301 Moved Permanently` status code including a `Location` header pointing to the correct node for that partition. This enables direct API users who do not use the smart client to reach the correct node.

#### Scenario: Write request arrives at a node that no longer leads the partition
- **WHEN** a direct API user sends a write request to a node that is no longer the leader for the target partition due to a recent topology change
- **THEN** the node MUST respond with HTTP `301` and a `Location` header containing the URL of the current leader for that partition

#### Scenario: Read request arrives at a node that no longer holds the partition
- **WHEN** a direct API user sends a read request to a node that no longer holds a replica for the target partition
- **THEN** the node MUST respond with HTTP `301` and a `Location` header containing the URL of a current replica for that partition

#### Scenario: Request arrives at a Leaving node
- **WHEN** a direct API user sends a write request to a node in the `Leaving` state
- **THEN** the node MUST respond with HTTP `301` and a `Location` header pointing to the new leader for that partition

#### Scenario: Redirect includes topology version
- **WHEN** a node responds with a `301` redirect
- **THEN** the response MUST include the current topology version in a response header so that clients can detect and cache updated routing information
