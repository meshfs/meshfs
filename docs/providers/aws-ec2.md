# aws-ec2

Status: planned (documentation-only, not implemented yet).

## Positioning

Low-level AWS path for teams needing full host control, custom networking, or special runtime dependencies.

## Expected runtime and deployment shape

- Platform target: AWS EC2
- Suggested deploy shape: native binary or container on provisioned instances

## Suggested default backends

- Metadata: PostgreSQL (self-managed or RDS)
- Objects: Amazon S3 or S3-compatible endpoint

## Review focus

- Instance bootstrap and AMI strategy
- Auto-recovery and rolling upgrade mechanics
- Operational burden compared with `aws-ecs`
