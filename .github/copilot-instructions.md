---
applyTo: "**/*.rs"
---

# Project Overview
This project is a Rust-based codebase and aims at building a library for distributed, gossip-based key-value cache that can be embedded into other Rust-based applications. 

## Core Libraries and Frameworks
- Tokio for asynchronous programming
- Serde for serialization and deserialization
- Actix for exposing API endpoints to interact with the cache

## Coding Standards
- Follow Rust's official style guidelines as outlined in the Rust Book.
- Use `cargo fmt` to format code consistently.
- Write clear and concise documentation comments for all public functions and modules.
- Ensure code is modular and follows the single responsibility principle.
- Write unit tests for all critical components and ensure high test coverage.
- Use meaningful variable and function names that convey intent.
- Avoid using `unwrap` and `expect` in favor of proper error handling.
- Limit data copying by using references and smart pointers where appropriate.
- Use pattern matching effectively to handle different cases in a clear manner.
- Leverage Rust's powerful type system to enforce invariants at compile time.
- Focus on performance optimizations, especially in the gossip protocol implementation, to ensure low latency and high throughput.

