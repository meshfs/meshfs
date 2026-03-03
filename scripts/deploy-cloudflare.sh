#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Backward-compatible alias.
exec "${ROOT_DIR}/scripts/deploy-provider.sh" --provider cloudflare-workers-free-tier "$@"
