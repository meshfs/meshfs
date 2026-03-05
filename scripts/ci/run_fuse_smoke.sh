#!/usr/bin/env bash
set -euo pipefail

os="${1:-}"
if [[ -z "$os" ]]; then
  echo "usage: $0 <linux|macos>" >&2
  exit 1
fi

client_bin="${MESHFS_TEST_CLIENT_BIN:-target/debug/meshfs}"
server_bin="${MESHFS_TEST_SERVER_BIN:-target/debug/meshfs-control-plane}"

if [[ ! -x "$client_bin" || ! -x "$server_bin" ]]; then
  echo "skip: binaries missing (client=$client_bin server=$server_bin)"
  exit 0
fi

is_mounted() {
  local mount_dir="$1"
  if [[ "$os" == "linux" ]]; then
    mount | grep -E " on ${mount_dir} type .*fuse" >/dev/null 2>&1
  else
    mount | grep -E "on ${mount_dir} \(" >/dev/null 2>&1
  fi
}

unmount_target() {
  local mount_dir="$1"
  if [[ "$os" == "linux" ]]; then
    if command -v fusermount3 >/dev/null 2>&1; then
      fusermount3 -u "$mount_dir" || true
    elif command -v fusermount >/dev/null 2>&1; then
      fusermount -u "$mount_dir" || true
    else
      umount "$mount_dir" || true
    fi
  else
    umount "$mount_dir" || true
  fi
}

if [[ "$os" == "linux" ]]; then
  if [[ ! -e /dev/fuse ]]; then
    echo "skip: /dev/fuse is unavailable on this runner"
    exit 0
  fi
  if ! command -v fusermount3 >/dev/null 2>&1 && ! command -v fusermount >/dev/null 2>&1; then
    echo "skip: fusermount tool is unavailable"
    exit 0
  fi
elif [[ "$os" == "macos" ]]; then
  if [[ ! -d /Library/Filesystems/macfuse.fs ]]; then
    echo "skip: macFUSE is not installed on this runner"
    exit 0
  fi
else
  echo "unsupported os: $os" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
home_dir="$tmp_dir/home"
mount_dir="$tmp_dir/mount"
mkdir -p "$home_dir" "$mount_dir"

server_log="$tmp_dir/server.log"
mount_log="$tmp_dir/mount.log"

cleanup() {
  set +e
  unmount_target "$mount_dir"
  if [[ -n "${mount_pid:-}" ]]; then
    kill "$mount_pid" >/dev/null 2>&1 || true
    wait "$mount_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "${server_pid:-}" ]]; then
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
  fi
  echo "fuse smoke logs: $tmp_dir"
}
trap cleanup EXIT

port="$(python3 - <<'PY'
import socket
s=socket.socket()
s.bind(("127.0.0.1",0))
print(s.getsockname()[1])
s.close()
PY
)"

server_url="http://127.0.0.1:$port"

MESHFS_BIND_ADDR="127.0.0.1:$port" \
MESHFS_METADATA_SQLITE_PATH="$tmp_dir/metadata.db" \
MESHFS_JWT_SECRET="meshfs-fuse-smoke-secret" \
MESHFS_DEV_AUTO_APPROVE="true" \
"$server_bin" >"$server_log" 2>&1 &
server_pid=$!

for _ in $(seq 1 80); do
  if curl -fsS "$server_url/healthz" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$server_pid" >/dev/null 2>&1; then
    echo "control plane exited early" >&2
    cat "$server_log" >&2 || true
    exit 1
  fi
  sleep 0.25
done

HOME="$home_dir" BROWSER="true" "$client_bin" --server "$server_url" login \
  --auto-activate --user-id fuse-smoke-user --tenant-id fuse-smoke-tenant

HOME="$home_dir" "$client_bin" --server "$server_url" mount \
  --remote "$server_url" --target "$mount_dir" --auto-unmount >"$mount_log" 2>&1 &
mount_pid=$!

mounted="0"
for _ in $(seq 1 40); do
  if is_mounted "$mount_dir"; then
    mounted="1"
    break
  fi
  if ! kill -0 "$mount_pid" >/dev/null 2>&1; then
    echo "meshfs mount process exited early" >&2
    cat "$mount_log" >&2 || true
    exit 1
  fi
  sleep 0.25
done

if [[ "$mounted" != "1" ]]; then
  echo "mount point did not become active: $mount_dir" >&2
  cat "$mount_log" >&2 || true
  exit 1
fi

echo "fuse smoke payload" > "$mount_dir/fuse-smoke.txt"
actual="$(cat "$mount_dir/fuse-smoke.txt")"
if [[ "$actual" != "fuse smoke payload" ]]; then
  echo "unexpected file content from fuse mount: $actual" >&2
  exit 1
fi

echo "fuse smoke test passed"
