#!/usr/bin/env bash
set -euo pipefail

if ! command -v container >/dev/null 2>&1; then
  echo "skip: apple containerization CLI (container) not available on this runner"
  exit 0
fi

container --help >/dev/null

if ! container system start >/dev/null 2>&1; then
  echo "skip: failed to start container runtime on this runner"
  exit 0
fi

if ! container ls >/dev/null 2>&1; then
  echo "skip: container runtime not healthy enough for smoke"
  container system stop >/dev/null 2>&1 || true
  exit 0
fi

echo "apple containerization CLI is available and runtime started"

client_bin="${MESHFS_TEST_CLIENT_BIN:-target/debug/meshfs}"
server_bin="${MESHFS_TEST_SERVER_BIN:-target/debug/meshfs-control-plane}"

if [[ ! -x "$client_bin" || ! -x "$server_bin" ]]; then
  echo "skip: binaries missing (client=$client_bin server=$server_bin)"
  container system stop >/dev/null 2>&1 || true
  exit 0
fi

MESHFS_TEST_CLIENT_BIN="$client_bin" \
MESHFS_TEST_SERVER_BIN="$server_bin" \
cargo test -p meshfs-integration-tests --test core -- --nocapture

container system stop >/dev/null 2>&1 || true
