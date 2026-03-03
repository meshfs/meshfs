#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROVIDER=""

usage() {
  cat <<USAGE
Usage:
  $(basename "$0") --provider <provider_id> [provider options...]

Available providers:
  - cloudflare-workers-free-tier

Examples:
  $(basename "$0") --provider cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN>

To see provider-specific options:
  ${ROOT_DIR}/scripts/providers/deploy-cloudflare-workers-free-tier.sh --help
USAGE
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${1:-}" != "--provider" ]]; then
  echo "error: --provider is required as the first argument" >&2
  usage
  exit 1
fi

PROVIDER="${2:-}"
if [[ -z "${PROVIDER}" ]]; then
  echo "error: provider id is empty" >&2
  usage
  exit 1
fi
shift 2

case "${PROVIDER}" in
  cloudflare-workers-free-tier|cloudflare-workers-free)
    exec "${ROOT_DIR}/scripts/providers/deploy-cloudflare-workers-free-tier.sh" "$@"
    ;;
  *)
    echo "error: unknown provider '${PROVIDER}'" >&2
    usage
    exit 1
    ;;
esac
