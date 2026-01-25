# Repository Guidelines

## Project Structure & Module Organization
- Shared data structures reside under `src/store/`.
- End-to-end helpers and integration tests are in `gossipgrid/tests/`.
- Working example crates live under `examples/`.

## Build, Test, and Development Commands
- `cargo fmt` - keeps Rust sources formatted to the repo standard.
- `cargo clippy --all-targets` - runs lint checks across lib, bin, and tests.
- `RUST_LOG=info cargo test --lib` - executes unit tests.
- `RUST_LOG=info cargo test --test 'int_tests_*' -- --test-threads=4 --nocapture` - execute integration tests on several node cluster
- `./start_local_cluster_3n.sh` - starts local 3 node cluster.
- `./simulate.sh` - executed commands on started local cluster that add, update and delete items.

## Coding Style & Naming Conventions
- Use Rust 2024 edition defaults: 4-space indentation, snake_case for modules/functions, CamelCase for types.
- Derive traits where practical; prefer explicit `use` paths over glob imports.
- Keep modules small and cohesive; co-locate unit tests with each module when logic is tightly coupled.
- Ensure written code is memory leak free, locks are hadled efficiently and overal focus is places on performance and correctness.

## Testing Guidelines
- Favor lightweight unit tests near the code plus broader flows (integration) or component tests under `gossipgrid/tests/`.
- Name integration tests after the behavior under test, e.g., `int_test.rs` contains cluster-level scenarios.
- Ensure new features extend the simulated cluster checks or add fixtures under `tests/helpers.rs`.

## Commit & Pull Request Guidelines
- Commit messages are short, imperative summaries (e.g., “add initial int tests”); include context for multi-file changes in the body when needed.
- PRs should describe the change, list manual verification steps, and link tracking issues.

## Security & Configuration Notes
- Use `RUST_LOG` levels responsibly—debug logs should not leak sensitive payloads.
