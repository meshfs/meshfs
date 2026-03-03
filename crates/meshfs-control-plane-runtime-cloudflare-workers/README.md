# meshfs-control-plane-runtime-cloudflare-workers

Worker runtime baseline for MeshFS OSS direct-worker mode.

## Scope in this repository

- Rust/Wasm `fetch` entrypoint in `rustwasm-worker-template` style.
- Direct Worker runtime routes backed by `meshfs-control-plane-core`.
- D1 snapshot persistence for control-plane metadata (`meshfs_metadata_snapshot`).
- R2-backed object store adapter for blob payloads (`MESHFS_R2`).
- Capability detection for Cloudflare bindings (`MESHFS_DB` D1, `MESHFS_R2` bucket).

## Notes

- This crate is included in the workspace and compiles on native hosts with a non-wasm stub.
- `sync/ws` is implemented as a stateless cursor protocol over WebSocket (server-side periodic pull from D1 + push incremental events, with heartbeat frames).
- `sync/stream` is implemented as snapshot-mode SSE (returns current incremental backlog from cursor and closes).
