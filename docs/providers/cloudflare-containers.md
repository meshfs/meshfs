# cloudflare-containers

Status: planned (documentation-only, not implemented yet).

## Positioning

Cloudflare container provider for workloads that need a Linux/container runtime model, similar to Cloud Run style deployment.

## Expected runtime and deployment shape

- Platform target: Cloudflare Containers (integrated with Workers)
- Suggested deploy shape: Worker front door + container class/instances
- Plan assumption: paid Workers plan requirement (Containers beta)

## Suggested default backends

- Metadata: D1 or external Postgres (to be decided)
- Objects: R2

## Review focus

- Runtime boundary between Worker request layer and container workload
- Cold start and autoscaling behavior under sync traffic
- Cost profile versus `cloudflare-workers-free-tier`
