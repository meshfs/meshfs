# azure-container-apps

Status: planned (documentation-only, not implemented yet).

## Positioning

Managed container app platform on Azure for serverless-style operations with container flexibility.

## Expected runtime and deployment shape

- Platform target: Azure Container Apps
- Suggested deploy shape: containerized control plane service with managed revision rollout

## Suggested default backends

- Metadata: Azure Database for PostgreSQL
- Objects: Azure Blob Storage (S3-compatible abstraction required in adapter layer)

## Review focus

- Revision strategy and traffic splitting for zero-downtime upgrades
- AuthN/AuthZ boundary with managed identity
- Performance and cost baseline against `gcp-cloud-run`
