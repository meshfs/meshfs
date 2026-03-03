#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUST_WORKER_DIR="${ROOT_DIR}/crates/meshfs-control-plane-runtime-cloudflare-workers"
CONFIG_PATH="${RUST_WORKER_DIR}/wrangler.generated.jsonc"
SCHEMA_PATH="${ROOT_DIR}/deploy/providers/cloudflare-workers-free-tier/d1/schema.sql"
WRANGLER_VERSION="${WRANGLER_VERSION:-4.69.0}"

WORKER_NAME="meshfs-oss-edge"
ACCOUNT_ID=""
TOKEN=""
COMPAT_DATE="$(date +%F)"
ENABLE_D1="yes"
D1_DATABASE_ID=""
D1_DATABASE_NAME=""
ENABLE_R2="yes"
R2_BUCKET_NAME=""

usage() {
  cat <<USAGE
Usage:
  $(basename "$0") --token <cloudflare_api_token> [--account-id <account_id>] [--name <worker_name>] [--compat-date YYYY-MM-DD] [--d1-database-id <database_id> --d1-database-name <database_name>] [--d1-database-name <database_name>] [--no-d1] [--r2-bucket-name <bucket_name>] [--no-r2]

Example:
  $(basename "$0") --token "<CLOUDFLARE_API_TOKEN>"

This deployment targets Cloudflare Workers Free tier (zero-cost bootstrap).
By default:
  - D1 metadata is enabled and auto-provisioned.
  - R2 object storage is enabled and auto-provisioned.
  - Wrangler is executed through npx (no local npm install required).
  - Rust Worker runtime is compiled from:
    ${RUST_WORKER_DIR}

Required API token permissions:
  - Account: Workers Scripts:Edit
  - User: Memberships:Read (only needed when --account-id is omitted)
  - User: User Details:Read (only needed when --account-id is omitted)
  - Account: D1:Edit (required unless --no-d1)
  - Account: Workers R2 Storage:Edit (required unless --no-r2)

Optional permissions (only for extended setup):
  - Zone: Workers Routes:Edit
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: missing required command '$1'" >&2
    exit 1
  fi
}

ensure_wasm_target() {
  if ! rustup target list --installed | grep -q '^wasm32-unknown-unknown$'; then
    echo "Installing Rust target wasm32-unknown-unknown..."
    rustup target add wasm32-unknown-unknown >/dev/null
  fi
}

ensure_worker_build() {
  if ! command -v worker-build >/dev/null 2>&1; then
    echo "Installing worker-build..."
    cargo install worker-build --locked >/dev/null
  fi
}

cf_api() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local url="https://api.cloudflare.com/client/v4${path}"

  if [[ -n "${body}" ]]; then
    curl -fsSL -X "${method}" "${url}" \
      -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      -H "Content-Type: application/json" \
      --data "${body}"
  else
    curl -fsSL -X "${method}" "${url}" \
      -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      -H "Content-Type: application/json"
  fi
}

default_d1_database_name() {
  local cleaned
  cleaned="$(printf '%s' "${WORKER_NAME}" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//')"
  if [[ -z "${cleaned}" ]]; then
    cleaned="meshfs-oss-edge"
  fi
  cleaned="${cleaned:0:40}"
  printf '%s' "${cleaned}-metadata"
}

default_r2_bucket_name() {
  local cleaned
  cleaned="$(printf '%s' "${WORKER_NAME}" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//')"
  if [[ -z "${cleaned}" ]]; then
    cleaned="meshfs-oss-edge"
  fi
  cleaned="${cleaned:0:50}"
  printf '%s' "${cleaned}-objects"
}

resolve_or_create_d1_database() {
  local list_json
  local create_payload
  local create_json

  list_json="$(cf_api GET "/accounts/${CLOUDFLARE_ACCOUNT_ID}/d1/database?per_page=1000")"
  D1_DATABASE_ID="$(printf '%s' "${list_json}" | node -e '
    const fs = require("fs");
    const payload = JSON.parse(fs.readFileSync(0, "utf8"));
    const targetName = process.argv[1];
    if (!payload.success || !Array.isArray(payload.result)) process.exit(2);
    const hit = payload.result.find((x) => x.name === targetName);
    process.stdout.write(hit ? String(hit.uuid || hit.id || "") : "");
  ' "${D1_DATABASE_NAME}")" || {
    echo "error: failed to list D1 databases" >&2
    exit 1
  }

  if [[ -n "${D1_DATABASE_ID}" ]]; then
    echo "Reusing D1 database: ${D1_DATABASE_NAME} (${D1_DATABASE_ID})"
    return
  fi

  echo "Creating D1 database: ${D1_DATABASE_NAME}"
  create_payload="$(node -e '
    const name = process.argv[1];
    process.stdout.write(JSON.stringify({ name }));
  ' "${D1_DATABASE_NAME}")"
  create_json="$(cf_api POST "/accounts/${CLOUDFLARE_ACCOUNT_ID}/d1/database" "${create_payload}")"
  D1_DATABASE_ID="$(printf '%s' "${create_json}" | node -e '
    const fs = require("fs");
    const payload = JSON.parse(fs.readFileSync(0, "utf8"));
    if (!payload.success || !payload.result) process.exit(2);
    process.stdout.write(String(payload.result.uuid || payload.result.id || ""));
  ')" || {
    echo "error: failed to create D1 database" >&2
    exit 1
  }

  if [[ -z "${D1_DATABASE_ID}" ]]; then
    echo "error: created D1 database but did not receive database id" >&2
    exit 1
  fi
}

resolve_or_create_r2_bucket() {
  local list_json
  local create_payload

  list_json="$(cf_api GET "/accounts/${CLOUDFLARE_ACCOUNT_ID}/r2/buckets?per_page=1000")"
  local existing
  existing="$(printf '%s' "${list_json}" | node -e '
    const fs = require("fs");
    const payload = JSON.parse(fs.readFileSync(0, "utf8"));
    const targetName = process.argv[1];
    if (!payload.success) process.exit(2);
    const result = payload.result;
    const buckets = Array.isArray(result)
      ? result
      : (Array.isArray(result?.buckets) ? result.buckets : []);
    const hit = buckets.find((x) => x && x.name === targetName);
    process.stdout.write(hit ? String(hit.name) : "");
  ' "${R2_BUCKET_NAME}")" || {
    echo "error: failed to list R2 buckets" >&2
    exit 1
  }

  if [[ -n "${existing}" ]]; then
    echo "Reusing R2 bucket: ${R2_BUCKET_NAME}"
    return
  fi

  echo "Creating R2 bucket: ${R2_BUCKET_NAME}"
  create_payload="$(node -e '
    const name = process.argv[1];
    process.stdout.write(JSON.stringify({ name }));
  ' "${R2_BUCKET_NAME}")"

  local create_json
  create_json="$(cf_api POST "/accounts/${CLOUDFLARE_ACCOUNT_ID}/r2/buckets" "${create_payload}")"
  local create_ok
  create_ok="$(printf '%s' "${create_json}" | node -e '
    const fs = require("fs");
    const payload = JSON.parse(fs.readFileSync(0, "utf8"));
    process.stdout.write(payload.success ? "yes" : "no");
  ')" || {
    echo "error: failed to create R2 bucket" >&2
    exit 1
  }

  if [[ "${create_ok}" != "yes" ]]; then
    echo "error: R2 bucket creation request failed" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --token)
      TOKEN="${2:-}"
      shift 2
      ;;
    --account-id)
      ACCOUNT_ID="${2:-}"
      shift 2
      ;;
    --name)
      WORKER_NAME="${2:-}"
      shift 2
      ;;
    --compat-date)
      COMPAT_DATE="${2:-}"
      shift 2
      ;;
    --d1-database-id)
      D1_DATABASE_ID="${2:-}"
      shift 2
      ;;
    --d1-database-name)
      D1_DATABASE_NAME="${2:-}"
      shift 2
      ;;
    --no-d1)
      ENABLE_D1="no"
      shift
      ;;
    --r2-bucket-name)
      R2_BUCKET_NAME="${2:-}"
      shift 2
      ;;
    --no-r2)
      ENABLE_R2="no"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1'" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${TOKEN}" ]]; then
  echo "error: --token is required" >&2
  usage
  exit 1
fi

require_cmd curl
require_cmd node
require_cmd npx
require_cmd cargo
require_cmd rustup

export CLOUDFLARE_API_TOKEN="${TOKEN}"

# Validate token shape early.
verify_json="$(curl -fsSL "https://api.cloudflare.com/client/v4/user/tokens/verify" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
  -H "Content-Type: application/json")"

verify_ok="$(printf '%s' "${verify_json}" | node -e '
  const fs = require("fs");
  const payload = JSON.parse(fs.readFileSync(0, "utf8"));
  process.stdout.write(payload.success ? "yes" : "no");
')"

if [[ "${verify_ok}" != "yes" ]]; then
  echo "error: token verification failed" >&2
  exit 1
fi

if [[ -z "${ACCOUNT_ID}" ]]; then
  echo "Resolving Cloudflare account id from token memberships..."
  memberships_json="$(curl -fsSL "https://api.cloudflare.com/client/v4/memberships" \
    -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
    -H "Content-Type: application/json")"

  ACCOUNT_ID="$(printf '%s' "${memberships_json}" | node -e '
    const fs = require("fs");
    const payload = JSON.parse(fs.readFileSync(0, "utf8"));
    if (!payload.success || !Array.isArray(payload.result) || payload.result.length === 0) {
      process.exit(2);
    }
    const active = payload.result.find((x) => x.status === "accepted") || payload.result[0];
    process.stdout.write(active.account.id);
  ')" || {
    echo "error: failed to infer account id; provide --account-id explicitly" >&2
    exit 1
  }
fi

export CLOUDFLARE_ACCOUNT_ID="${ACCOUNT_ID}"

echo "Using account: ${CLOUDFLARE_ACCOUNT_ID}"
echo "Worker name: ${WORKER_NAME}"

if [[ "${ENABLE_D1}" == "yes" ]]; then
  if [[ -n "${D1_DATABASE_ID}" && -z "${D1_DATABASE_NAME}" ]]; then
    echo "error: --d1-database-name is required when --d1-database-id is provided" >&2
    exit 1
  fi

  if [[ -z "${D1_DATABASE_NAME}" ]]; then
    D1_DATABASE_NAME="$(default_d1_database_name)"
  fi

  if [[ -z "${D1_DATABASE_ID}" ]]; then
    resolve_or_create_d1_database
  fi

  echo "D1 metadata: enabled"
  echo "D1 database: ${D1_DATABASE_NAME} (${D1_DATABASE_ID})"
else
  echo "D1 metadata: disabled (--no-d1)"
fi

if [[ "${ENABLE_R2}" == "yes" ]]; then
  if [[ -z "${R2_BUCKET_NAME}" ]]; then
    R2_BUCKET_NAME="$(default_r2_bucket_name)"
  fi
  resolve_or_create_r2_bucket
  echo "R2 object store: enabled"
  echo "R2 bucket: ${R2_BUCKET_NAME}"
else
  echo "R2 object store: disabled (--no-r2)"
fi

if [[ "${ENABLE_D1}" != "yes" && "${ENABLE_R2}" != "yes" ]]; then
  echo "warning: both D1 and R2 are disabled; deployment will run with ephemeral state" >&2
fi

echo "Generating Wrangler config at ${CONFIG_PATH}"
ENABLE_D1_JSON="${ENABLE_D1}" \
ENABLE_R2_JSON="${ENABLE_R2}" \
WORKER_NAME_JSON="${WORKER_NAME}" \
COMPAT_DATE_JSON="${COMPAT_DATE}" \
D1_DATABASE_ID_JSON="${D1_DATABASE_ID}" \
D1_DATABASE_NAME_JSON="${D1_DATABASE_NAME}" \
R2_BUCKET_NAME_JSON="${R2_BUCKET_NAME}" \
node -e '
  const fs = require("fs");
  const outputPath = process.argv[1];
  const cfg = {
    name: process.env.WORKER_NAME_JSON,
    main: "build/worker/shim.mjs",
    compatibility_date: process.env.COMPAT_DATE_JSON,
    workers_dev: true
  };
  if (process.env.ENABLE_D1_JSON === "yes") {
    cfg.d1_databases = [
      {
        binding: "MESHFS_DB",
        database_id: process.env.D1_DATABASE_ID_JSON,
        database_name: process.env.D1_DATABASE_NAME_JSON
      }
    ];
  }
  if (process.env.ENABLE_R2_JSON === "yes") {
    cfg.r2_buckets = [
      {
        binding: "MESHFS_R2",
        bucket_name: process.env.R2_BUCKET_NAME_JSON
      }
    ];
  }
  fs.writeFileSync(outputPath, JSON.stringify(cfg, null, 2) + "\n");
' "${CONFIG_PATH}"

ensure_wasm_target
ensure_worker_build

echo "Building Rust Worker runtime..."
(
  cd "${RUST_WORKER_DIR}"
  worker-build --release
)

if [[ "${ENABLE_D1}" == "yes" ]]; then
  echo "Applying D1 schema from ${SCHEMA_PATH}..."
  npx --yes "wrangler@${WRANGLER_VERSION}" d1 execute MESHFS_DB --remote --yes --file "${SCHEMA_PATH}" --config "${CONFIG_PATH}"
fi

echo "Deploying Rust Worker runtime to Cloudflare Workers..."
npx --yes "wrangler@${WRANGLER_VERSION}" deploy --config "${CONFIG_PATH}"

echo "Deployment finished."
