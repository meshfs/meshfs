# gcp-cloud-run

Status: planned (documentation-only, not implemented yet).

## Positioning

Managed container runtime on GCP with scale-to-zero and minimal cluster operations.

## Expected runtime and deployment shape

- Platform target: Google Cloud Run
- Suggested deploy shape: containerized control plane service behind Cloud Run HTTP endpoint

## Suggested default backends

- Metadata: Cloud SQL PostgreSQL
- Objects: Google Cloud Storage (S3-compatible abstraction required in adapter layer)

## Review focus

- Connection pooling strategy for Cloud SQL
- Request timeout and streaming compatibility for sync endpoints
- Cost and latency profile versus `aws-ecs` and `azure-container-apps`
