# Control Plane Architecture and Naming

This document defines canonical naming for MeshFS control-plane architecture.

## Design Rule

MeshFS should use one business core and multiple runtime/provider adapters.

- Avoid one full control-plane implementation per provider.
- Keep business logic in a shared core.
- Keep cloud/runtime differences in adapters.

## Canonical Components

### Core

- `meshfs_control_plane_core`
  - Purpose: shared business logic (metadata, sync semantics, versioning, auth logic).
  - Status: **implemented baseline** (runtime-independent business core crate in workspace).

### Runtime adapters

- `meshfs_control_plane_runtime_native`
  - Purpose: native server runtime (self-hosted process).
  - Status: **implemented baseline** (`meshfs-control-plane` crate depends on core).

- `meshfs_control_plane_runtime_cloudflare_workers`
  - Purpose: Cloudflare Workers runtime shape.
  - Status: **implemented** (`meshfs-control-plane-runtime-cloudflare-workers` crate; direct fetch path with core API routes + D1 snapshot + R2 adapter + stateless cursor-based `sync/ws` push channel + snapshot-mode `sync/stream` SSE).

### Metadata adapters

- `meshfs_control_plane_adapter_metadata_sqlite`
  - Status: **implemented**.

- `meshfs_control_plane_adapter_metadata_cloudflare_d1`
  - Status: **implemented baseline** (worker runtime snapshot adapter + D1 schema).

- `meshfs_control_plane_adapter_metadata_postgres`
  - Status: **planned**.

### Object adapters

- `meshfs_control_plane_adapter_object_cloudflare_r2`
  - Status: **implemented baseline** (worker runtime adapter).

- `meshfs_control_plane_adapter_object_aws_s3`
  - Status: **planned**.

- `meshfs_control_plane_adapter_object_s3_compatible`
  - Status: **implemented in native runtime path**.

## Provider IDs

Provider IDs are deployment presets, not business logic boundaries.

### Implemented now

- `cloudflare-workers-free-tier`
  - Runtime target: `meshfs_control_plane_runtime_cloudflare_workers`
  - Cost target: zero-cost bootstrap
  - Default metadata binding: D1
  - Provider doc: [providers/cloudflare-workers-free-tier.md](providers/cloudflare-workers-free-tier.md)

### Future provider presets

- `selfhost-native` ([doc](providers/selfhost-native.md))
- `cloudflare-containers` ([doc](providers/cloudflare-containers.md))
- `aws-ecs` ([doc](providers/aws-ecs.md))
- `aws-ec2` ([doc](providers/aws-ec2.md))
- `gcp-cloud-run` ([doc](providers/gcp-cloud-run.md))
- `azure-container-apps` ([doc](providers/azure-container-apps.md))

All future providers are **documentation-only plans** at this stage.
