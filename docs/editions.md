# MeshFS Editions

This project uses an open-core model:

- **MeshFS OSS (Open Source)**: free to use, community-supported, optimized for zero-cost bootstrap.
- **MeshFS Commercial**: paid offering with production-grade durability and enterprise operations.

## MeshFS OSS

Included in this repository:

- Rust workspace with control plane/client/store/type crates.
- Local development server (`meshfs-control-plane`) and CLI with login + sync support.
- Provider-oriented deployment structure and unified CLI deployment entry (`meshfs deploy ...`).
- Cloudflare Workers free-tier provider (`cloudflare-workers-free-tier`) as one optional deployment target.
- Cloudflare free-tier provider uses D1 metadata by default (auto-provisioned by `meshfs deploy`), with `--no-d1` opt-out.
- Cloudflare free-tier provider is aligned to direct-worker runtime mode (no gateway mode in OSS path).
- Canonical control-plane naming and runtime/provider map is defined in `docs/control-plane-architecture.md`.
- Public docs and code under open-source licensing.

Current OSS constraints:

- Cloudflare Free-tier deployment is an edge bootstrap service, not full production durability.
- No enterprise SLA.
- Cloudflare provides platform-level edge HA, but OSS MeshFS does not include managed multi-region failover for stateful data.
- OSS transport is HTTP/WebSocket oriented and does not include a managed native TCP transport stack.
- `meshfs mount` in OSS supports kernel-level FUSE mode when built with `--features fuse`.
- Local client state is persisted with SQLite; Rust control-plane metadata uses normalized SQLite tables with transactional writes by default.
- OSS includes refresh-token auth flow, tenant rate limiting, tenant quota guard, and audit event records; enterprise policy/compliance features remain out of scope.
- No advanced billing and policy automation.

## MeshFS Commercial (outside this repository)

Planned paid capabilities:

- Production persistent metadata backend and managed operations.
- Advanced version retention tiers and policy controls.
- Cross-region disaster recovery with operational runbooks.
- Optional native TCP client/server transport for performance-sensitive deployments.
- Team/enterprise controls, support, and uptime commitments.
