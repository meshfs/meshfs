# Provider Planning Guide

This document is the provider-planning and review entry for MeshFS deployment options.

## Quick choose

- Want fastest zero-cost bootstrap now: use `cloudflare-workers-free-tier`.
- Want self-hosted control first: track `selfhost-native`.
- Want managed container runtimes on cloud platforms: review `cloudflare-containers`, `aws-ecs`, `gcp-cloud-run`, and `azure-container-apps`.

## Current provider

- `cloudflare-workers-free-tier` (implemented): [providers/cloudflare-workers-free-tier.md](providers/cloudflare-workers-free-tier.md)

## Planned providers

- `selfhost-native`: [providers/selfhost-native.md](providers/selfhost-native.md)
- `cloudflare-containers`: [providers/cloudflare-containers.md](providers/cloudflare-containers.md)
- `aws-ecs`: [providers/aws-ecs.md](providers/aws-ecs.md)
- `aws-ec2`: [providers/aws-ec2.md](providers/aws-ec2.md)
- `gcp-cloud-run`: [providers/gcp-cloud-run.md](providers/gcp-cloud-run.md)
- `azure-container-apps`: [providers/azure-container-apps.md](providers/azure-container-apps.md)

## Related docs

- Provider map and dispatch model: [deployment-providers.md](deployment-providers.md)
- Control-plane naming and runtime/provider boundaries: [control-plane-architecture.md](control-plane-architecture.md)
