default:
  @just 2>/dev/null || echo "Usage: just <recipe>"

# Run all tests
test:
  cargo test --workspace

# Format code
fmt:
  cargo fmt --all

# Lint
lint:
  cargo clippy --all -- -D warnings

# Check
check:
  cargo check --workspace
