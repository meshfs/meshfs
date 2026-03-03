# Cloudflare Workers Free Tier Provider

This provider deploys the MeshFS OSS Rust direct-worker runtime to Cloudflare Workers Free tier.

## Deploy

From repository root:

```bash
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN>
```

Default behavior:
- D1 metadata is enabled.
- Script auto-reuses an existing D1 database named `<worker-name>-metadata` or creates it.
- R2 object storage is enabled.
- Script auto-reuses an existing R2 bucket named `<worker-name>-objects` or creates it.
- Script uses `npx wrangler` directly (no local `node_modules` install required).
- Script applies `d1/schema.sql` automatically.
- Worker deploys from Rust runtime crate `crates/meshfs-control-plane-runtime-cloudflare-workers`.

Deploy with custom D1 database name:

```bash
./scripts/deploy-provider.sh \
  --provider cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN> \
  --d1-database-name <DATABASE_NAME>
```

Deploy with custom R2 bucket name:

```bash
./scripts/deploy-provider.sh \
  --provider cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN> \
  --r2-bucket-name <BUCKET_NAME>
```

Deploy without D1:

```bash
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --no-d1
```

Deploy without R2:

```bash
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --no-r2
```

## Required token permissions

- Account: `Workers Scripts:Edit`
- User: `Memberships:Read` (only needed when `--account-id` is omitted)
- User: `User Details:Read` (only needed when `--account-id` is omitted)
- Account: `D1:Edit` (required unless using `--no-d1`)
- Account: `Workers R2 Storage:Edit` (required unless using `--no-r2`)

## D1 schema

- Schema file: `deploy/providers/cloudflare-workers-free-tier/d1/schema.sql`
- Applied automatically during deployment when D1 is enabled.

## Endpoints

- `GET /healthz`
- `GET /edition`
- `GET /capabilities`
- `GET /sync/pull`
- `GET /sync/stream`
- `GET /sync/ws`
