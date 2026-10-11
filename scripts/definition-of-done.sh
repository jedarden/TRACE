#!/usr/bin/env bash
# Definition of done for TRACE's standalone Rust packages and browser client.
#
#   scripts/definition-of-done.sh          full checks
#   scripts/definition-of-done.sh --fast   formatting, Clippy, and client tests

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

MODE="full"
if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [--fast]" >&2
  exit 2
fi
if [[ $# -eq 1 ]]; then
  case "$1" in
    --fast) MODE="fast" ;;
    *)
      echo "Usage: $0 [--fast]" >&2
      exit 2
      ;;
  esac
fi

RUST_PACKAGES=(analytics collector compactor flusher iceberg syncer)

echo "NEEDLE_VERIFICATION_GATE: definition-of-done"

for package in "${RUST_PACKAGES[@]}"; do
  manifest="$package/Cargo.toml"
  echo "=== $package: format ==="
  cargo fmt --manifest-path "$manifest" -- --check
  echo "=== $package: Clippy ==="
  cargo clippy --manifest-path "$manifest" --all-targets
done

echo "=== browser client tests ==="
npm test --prefix client

if [[ "$MODE" == "full" ]]; then
  for package in "${RUST_PACKAGES[@]}"; do
    echo "=== $package: tests ==="
    cargo test --manifest-path "$package/Cargo.toml"
  done
fi
