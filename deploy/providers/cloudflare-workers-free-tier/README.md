# Cloudflare Workers Free Tier Provider

This provider deploys the MeshFS OSS Rust direct-worker runtime to Cloudflare Workers Free tier.

## Deploy

From repository root:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN>
```

Default behavior:
- D1 metadata is enabled.
- `meshfs deploy` auto-reuses an existing D1 database named `<worker-name>-metadata` or creates it.
- R2 object storage is enabled.
- `meshfs deploy` auto-reuses an existing R2 bucket named `<worker-name>-objects` or creates it.
- `meshfs deploy` uploads Worker modules via Cloudflare API (no `wrangler` required).
- `meshfs deploy` applies `d1/schema.sql` automatically.
- Worker deploys from Rust runtime crate `crates/meshfs-control-plane-runtime-cloudflare-workers`.
- Prebuilt bundle default path: `deploy/providers/cloudflare-workers-free-tier/worker-bundle/`.

Deploy with custom D1 database name:

```bash
meshfs deploy cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN> \
  --d1-database-name <DATABASE_NAME>
```

Deploy with custom R2 bucket name:

```bash
meshfs deploy cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN> \
  --r2-bucket-name <BUCKET_NAME>
```

Deploy without D1:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --no-d1
```

Deploy without R2:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --no-r2
```

Use custom prebuilt bundle:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --worker-bundle <BUNDLE_DIR>
```

Fallback to local worker build:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --build-worker-local
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
