# aws-ecs

Status: planned (documentation-only, not implemented yet).

## Positioning

Managed AWS container orchestration path with lower ops burden than raw EC2.

## Expected runtime and deployment shape

- Platform target: AWS ECS (prefer Fargate-first profile)
- Suggested deploy shape: containerized control plane service + managed task definition/service

## Suggested default backends

- Metadata: Amazon RDS PostgreSQL
- Objects: Amazon S3

## Review focus

- Service discovery and internal/external ingress model
- Task scaling policy for sync and stream traffic
- IAM boundary and secret management model
