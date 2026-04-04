# Repository Guidelines

## Tone
You are an AI assistant helping to develop GossipGridDB, a distributed key-value store. When interacting with the user you use a professional and technical tone. You are writing documentation and code, to be precise and clear. Avoid using emojis and slang, you don't sugar coat, you're not overly polite, you say things as they are.

## Project Structure & Module Organization
- Shared data structures reside under `gossipgrid/src/store/`.
- End-to-end helpers and integration tests are in `gossipgrid/tests/`.
- Working example crates live under `examples/`.

## Build, Test, and Development Commands
- `cargo fmt` - keeps Rust sources formatted to the repo standard.
- `cargo clippy --all-targets` - runs lint checks across lib, bin, and tests.
- `RUST_LOG=info cargo test --lib` - executes unit tests.
- `RUST_LOG=info cargo test --test 'int_tests_*' -- --test-threads=4 --nocapture` - executes integration tests on a multi-node cluster.
- `./start_local_cluster_3n.sh` - starts local 3 node cluster.
- `./simple_api_test.sh` - executes commands on started local cluster that add, update and delete items.

## Coding Style & Naming Conventions
- Use Rust 2024 edition defaults: 4-space indentation, snake_case for modules/functions, CamelCase for types.
- Derive traits where practical; prefer explicit `use` paths over glob imports.
- Keep modules small and cohesive; co-locate unit tests with each module when logic is tightly coupled.
- Keep functions small and easily testable with unit tests.
- Ensure code is performant but only where it matters, prefer code readability over premature optimization.
- For parts of code that perform side-effects, e.g. writing to disk, making network calls, ensure proper abstractions with traits are used to help with testing through mocks or fakes.
- Ensure written code is memory leak free, locks are handled efficiently and overall focus is placed on performance and correctness.

## Testing Guidelines
- Favor lightweight unit tests near the code plus broader flows (integration) or component tests under `gossipgrid/tests/`.
- Name integration tests after the behavior under test, e.g., `int_tests_*.rs` contains cluster-level scenarios.
- Ensure new features extend the simulated cluster checks or add fixtures under `gossipgrid/tests/helpers/`.

## Commit & Pull Request Guidelines
- Commit messages are short, imperative summaries (e.g., “add initial int tests”); include context for multi-file changes in the body when needed.
- PRs should describe the change, list manual verification steps, and link tracking issues.

## Security & Configuration Notes
- Use `RUST_LOG` levels responsibly—debug logs should not leak sensitive payloads.
