# Provider Docs

This directory holds one review document per provider ID.

## Provider Matrix

| Provider | Status | 控制面 (Control Plane) | 数据库 (DB) | kv/queue | 存储 (Object Storage) |
| --- | --- | --- | --- | --- | --- |
| `cloudflare-workers-free-tier` ([doc](cloudflare-workers-free-tier.md)) | Implemented | Cloudflare Workers (Rust/Wasm runtime) | Cloudflare D1 (default) | None (stateless cursor push; no managed queue) | Cloudflare R2 (default) |
| `selfhost-native` ([doc](selfhost-native.md)) | Planned | Native runtime (self-hosted process/container) | PostgreSQL (primary), SQLite (fallback) | TBD (Redis/Valkey candidate) | S3-compatible (MinIO/Ceph/S3 endpoint) |
| `cloudflare-containers` ([doc](cloudflare-containers.md)) | Planned | Cloudflare Workers + Containers | D1 or external PostgreSQL (TBD) | Cloudflare Queues (candidate) | Cloudflare R2 |
| `aws-ecs` ([doc](aws-ecs.md)) | Planned | ECS service (Fargate-first) | Amazon RDS PostgreSQL | SQS (candidate), optional Redis cache | Amazon S3 |
| `aws-ec2` ([doc](aws-ec2.md)) | Planned | Native runtime or container on EC2 | PostgreSQL (self-managed or RDS) | TBD (SQS / RabbitMQ / Redis candidate) | Amazon S3 or S3-compatible |
| `gcp-cloud-run` ([doc](gcp-cloud-run.md)) | Planned | Google Cloud Run service | Cloud SQL PostgreSQL | Pub/Sub or Cloud Tasks (candidate) | Google Cloud Storage |
| `azure-container-apps` ([doc](azure-container-apps.md)) | Planned | Azure Container Apps service | Azure Database for PostgreSQL | Azure Service Bus / Queue Storage (candidate) | Azure Blob Storage |

## Implemented

- `cloudflare-workers-free-tier`: [cloudflare-workers-free-tier.md](cloudflare-workers-free-tier.md)

## Planned (documentation-only)

- `selfhost-native`: [selfhost-native.md](selfhost-native.md)
- `cloudflare-containers`: [cloudflare-containers.md](cloudflare-containers.md)
- `aws-ecs`: [aws-ecs.md](aws-ecs.md)
- `aws-ec2`: [aws-ec2.md](aws-ec2.md)
- `gcp-cloud-run`: [gcp-cloud-run.md](gcp-cloud-run.md)
- `azure-container-apps`: [azure-container-apps.md](azure-container-apps.md)
