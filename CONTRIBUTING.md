# Contributing to GossipGrid

Thank you for your interest in contributing to GossipGrid! We welcome contributions from the community.

## How to Contribute

1.  **Fork the repository** on GitHub.
2.  **Create a new branch** for your feature or bug fix.
3.  **Make your changes**.
4.  **Run tests** to ensure everything is working correctly:
    ```bash
    cargo test --lib
    RUST_LOG=info cargo test --test 'int_tests_*' -- --test-threads=1 --nocapture
    ```
5.  **Submit a pull request**.

## Development Guidelines

- **Code Style**: We use `rustfmt` to maintain a consistent code style. Please run `cargo fmt` before submitting your PR.
- **Linting**: We use `clippy` for linting. Please ensure your code is free of clippy warnings by running `cargo clippy`.
- **Documentation**: All public APIs should be documented with doc comments.

## Reporting Issues

If you find a bug or have a feature request, please [open an issue](https://github.com/markosski/gossipgrid/issues) on GitHub.

## License

By contributing to GossipGrid, you agree that your contributions will be licensed under the project's [MIT OR Apache-2.0](LICENSE) license.
