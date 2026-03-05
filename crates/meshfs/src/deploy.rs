use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context};
use chrono::Utc;
use clap::{Args, Subcommand};
use reqwest::{multipart, Client};
use serde_json::{json, Value};

const DEFAULT_WORKER_NAME: &str = "meshfs-oss-edge";
const DEFAULT_WORKER_BUNDLE_RELATIVE: &str =
    "deploy/providers/cloudflare-workers-free-tier/worker-bundle";
const CLOUDFLARE_API_BASE: &str = "https://api.cloudflare.com/client/v4";

#[derive(Debug, Subcommand)]
pub enum DeployCommand {
    #[command(name = "cloudflare-workers-free-tier")]
    CloudflareWorkersFreeTier(DeployCloudflareWorkersFreeTierArgs),
}

#[derive(Debug, Args)]
pub struct DeployCloudflareWorkersFreeTierArgs {
    #[arg(long)]
    token: String,
    #[arg(long)]
    account_id: Option<String>,
    #[arg(long, default_value = DEFAULT_WORKER_NAME)]
    name: String,
    #[arg(long)]
    compat_date: Option<String>,
    #[arg(long)]
    d1_database_id: Option<String>,
    #[arg(long)]
    d1_database_name: Option<String>,
    #[arg(long, default_value_t = false)]
    no_d1: bool,
    #[arg(long)]
    r2_bucket_name: Option<String>,
    #[arg(long, default_value_t = false)]
    no_r2: bool,
    #[arg(long, default_value = ".")]
    repo_root: PathBuf,
    #[arg(long)]
    worker_bundle: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    build_worker_local: bool,
}

#[derive(Debug)]
struct WorkerBundleFiles {
    index_js: PathBuf,
    index_bg_wasm: PathBuf,
}

pub async fn run_deploy(client: &Client, command: DeployCommand) -> anyhow::Result<()> {
    match command {
        DeployCommand::CloudflareWorkersFreeTier(args) => {
            run_cloudflare_workers_free_tier(client, args).await
        }
    }
}

async fn run_cloudflare_workers_free_tier(
    client: &Client,
    args: DeployCloudflareWorkersFreeTierArgs,
) -> anyhow::Result<()> {
    validate_deploy_args(&args)?;

    let repo_root = args
        .repo_root
        .canonicalize()
        .with_context(|| format!("failed to resolve repo root {}", args.repo_root.display()))?;
    let runtime_dir = repo_root.join("crates/meshfs-control-plane-runtime-cloudflare-workers");
    let schema_path = repo_root.join("deploy/providers/cloudflare-workers-free-tier/d1/schema.sql");

    if !runtime_dir.is_dir() {
        bail!(
            "runtime directory not found: {} (run from repo root or pass --repo-root)",
            runtime_dir.display()
        );
    }
    if !args.no_d1 && !schema_path.is_file() {
        bail!(
            "D1 schema not found: {} (run from repo root or pass --repo-root)",
            schema_path.display()
        );
    }
    let bundle_dir = resolve_bundle_dir(&args, &repo_root);

    verify_cloudflare_token(client, &args.token).await?;

    let account_id = match args.account_id {
        Some(account_id) => account_id,
        None => {
            println!("Resolving Cloudflare account id from token memberships...");
            resolve_account_id(client, &args.token).await?
        }
    };

    let worker_name = args.name.clone();
    let compat_date = args
        .compat_date
        .clone()
        .unwrap_or_else(|| Utc::now().format("%Y-%m-%d").to_string());
    println!("Using account: {account_id}");
    println!("Worker name: {worker_name}");

    let mut d1_database_name = args.d1_database_name.clone();
    let mut d1_database_id = args.d1_database_id.clone();
    if !args.no_d1 {
        if d1_database_name.is_none() {
            d1_database_name = Some(default_d1_database_name(&worker_name));
        }
        if d1_database_id.is_none() {
            d1_database_id = Some(
                resolve_or_create_d1_database(
                    client,
                    &args.token,
                    &account_id,
                    d1_database_name
                        .as_deref()
                        .context("d1 database name missing after defaulting")?,
                )
                .await?,
            );
        }
        println!(
            "D1 metadata: enabled\nD1 database: {} ({})",
            d1_database_name
                .as_deref()
                .context("d1 database name missing")?,
            d1_database_id
                .as_deref()
                .context("d1 database id missing")?
        );
    } else {
        println!("D1 metadata: disabled (--no-d1)");
    }

    let mut r2_bucket_name = args.r2_bucket_name.clone();
    if !args.no_r2 {
        if r2_bucket_name.is_none() {
            r2_bucket_name = Some(default_r2_bucket_name(&worker_name));
        }
        resolve_or_create_r2_bucket(
            client,
            &args.token,
            &account_id,
            r2_bucket_name
                .as_deref()
                .context("r2 bucket name missing")?,
        )
        .await?;
        println!(
            "R2 object store: enabled\nR2 bucket: {}",
            r2_bucket_name
                .as_deref()
                .context("r2 bucket name missing")?
        );
    } else {
        println!("R2 object store: disabled (--no-r2)");
    }

    if args.no_d1 && args.no_r2 {
        eprintln!("warning: both D1 and R2 are disabled; deployment will run with ephemeral state");
    }

    let worker_bundle = prepare_worker_bundle(&bundle_dir, args.build_worker_local, &runtime_dir)?;
    println!(
        "Using worker bundle: {}",
        worker_bundle
            .index_js
            .parent()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<unknown>".to_string())
    );

    if !args.no_d1 {
        println!("Applying D1 schema from {}...", schema_path.display());
        let schema_sql = fs::read_to_string(&schema_path)
            .with_context(|| format!("failed to read D1 schema {}", schema_path.display()))?;
        apply_d1_schema(
            client,
            &args.token,
            &account_id,
            d1_database_id
                .as_deref()
                .context("d1 database id missing before schema apply")?,
            &schema_sql,
        )
        .await?;
    }

    println!("Deploying Rust Worker runtime to Cloudflare Workers via API...");
    let index_js = fs::read(&worker_bundle.index_js).with_context(|| {
        format!(
            "failed to read worker JS bundle {}",
            worker_bundle.index_js.display()
        )
    })?;
    let index_bg_wasm = fs::read(&worker_bundle.index_bg_wasm).with_context(|| {
        format!(
            "failed to read worker wasm bundle {}",
            worker_bundle.index_bg_wasm.display()
        )
    })?;
    upload_worker_script(
        client,
        &args.token,
        &account_id,
        &worker_name,
        &compat_date,
        d1_database_id.as_deref(),
        r2_bucket_name.as_deref(),
        index_js,
        index_bg_wasm,
    )
    .await?;

    println!("Deployment finished.");
    Ok(())
}

fn resolve_bundle_dir(args: &DeployCloudflareWorkersFreeTierArgs, repo_root: &Path) -> PathBuf {
    if let Some(path) = &args.worker_bundle {
        if path.is_absolute() {
            return path.clone();
        }
        return repo_root.join(path);
    }
    repo_root.join(DEFAULT_WORKER_BUNDLE_RELATIVE)
}

fn prepare_worker_bundle(
    preferred_bundle_dir: &Path,
    build_worker_local: bool,
    runtime_dir: &Path,
) -> anyhow::Result<WorkerBundleFiles> {
    if let Some(found) = find_worker_bundle_files(preferred_bundle_dir) {
        return Ok(found);
    }

    let runtime_build_dir = runtime_dir.join("build");
    if let Some(found) = find_worker_bundle_files(&runtime_build_dir) {
        return Ok(found);
    }

    if !build_worker_local {
        bail!(
            "worker bundle not found at {} (or {}). Provide --worker-bundle <path> or run with --build-worker-local",
            preferred_bundle_dir.display(),
            runtime_build_dir.display()
        );
    }

    ensure_wasm_target_installed()?;
    ensure_worker_build_installed()?;

    println!("Building Rust Worker runtime bundle locally...");
    run_command(
        "worker-build",
        &["--release"],
        Some(runtime_dir),
        &[],
        "failed to build rust worker runtime",
    )?;

    if let Some(found) = find_worker_bundle_files(&runtime_build_dir) {
        return Ok(found);
    }

    bail!(
        "worker build completed but bundle files are missing in {}",
        runtime_build_dir.display()
    )
}

fn find_worker_bundle_files(dir: &Path) -> Option<WorkerBundleFiles> {
    let index_js = dir.join("index.js");
    let index_bg_wasm = dir.join("index_bg.wasm");
    if index_js.is_file() && index_bg_wasm.is_file() {
        Some(WorkerBundleFiles {
            index_js,
            index_bg_wasm,
        })
    } else {
        None
    }
}

fn command_exists(name: &str) -> bool {
    match Command::new(name)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(_) => true,
        Err(err) if err.kind() == ErrorKind::NotFound => false,
        Err(_) => false,
    }
}

fn ensure_wasm_target_installed() -> anyhow::Result<()> {
    require_command("rustup")?;
    let output = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .context("failed to check rustup installed targets")?;
    if !output.status.success() {
        bail!("failed to list rust targets via rustup");
    }
    let installed = String::from_utf8_lossy(&output.stdout);
    if installed
        .lines()
        .any(|line| line.trim() == "wasm32-unknown-unknown")
    {
        return Ok(());
    }
    println!("Installing Rust target wasm32-unknown-unknown...");
    run_command(
        "rustup",
        &["target", "add", "wasm32-unknown-unknown"],
        None,
        &[],
        "failed to install wasm32 target",
    )
}

fn ensure_worker_build_installed() -> anyhow::Result<()> {
    require_command("cargo")?;
    if command_exists("worker-build") {
        return Ok(());
    }
    println!("Installing worker-build...");
    run_command(
        "cargo",
        &["install", "worker-build", "--locked"],
        None,
        &[],
        "failed to install worker-build",
    )
}

fn require_command(name: &str) -> anyhow::Result<()> {
    if command_exists(name) {
        return Ok(());
    }
    bail!("missing required command '{name}'");
}

fn run_command(
    program: &str,
    args: &[&str],
    cwd: Option<&Path>,
    envs: &[(&str, &str)],
    error_context: &str,
) -> anyhow::Result<()> {
    let owned_args = args
        .iter()
        .map(|arg| (*arg).to_string())
        .collect::<Vec<_>>();
    run_command_owned(program, owned_args, cwd, envs, error_context)
}

fn run_command_owned(
    program: &str,
    args: Vec<String>,
    cwd: Option<&Path>,
    envs: &[(&str, &str)],
    error_context: &str,
) -> anyhow::Result<()> {
    let mut cmd = Command::new(program);
    cmd.args(&args).stdin(Stdio::null());
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    for (key, value) in envs {
        cmd.env(key, value);
    }

    let status = cmd
        .status()
        .with_context(|| format!("{error_context}: failed to spawn {}", program))?;
    if !status.success() {
        bail!("{error_context}: {} exited with status {}", program, status);
    }
    Ok(())
}

async fn verify_cloudflare_token(client: &Client, token: &str) -> anyhow::Result<()> {
    let path = "/user/tokens/verify";
    let _ = cf_api(client, token, "GET", path, None).await?;
    Ok(())
}

async fn resolve_account_id(client: &Client, token: &str) -> anyhow::Result<String> {
    let value = cf_api(client, token, "GET", "/memberships", None).await?;
    let memberships = value
        .get("result")
        .and_then(Value::as_array)
        .context("cloudflare memberships response missing result array")?;
    let pick = memberships
        .iter()
        .find(|entry| entry.get("status").and_then(Value::as_str) == Some("accepted"))
        .or_else(|| memberships.first())
        .context("cloudflare memberships is empty")?;
    let account_id = pick
        .get("account")
        .and_then(|v| v.get("id"))
        .and_then(Value::as_str)
        .context("cloudflare memberships missing account id")?;
    Ok(account_id.to_string())
}

async fn resolve_or_create_d1_database(
    client: &Client,
    token: &str,
    account_id: &str,
    database_name: &str,
) -> anyhow::Result<String> {
    let list_path = format!("/accounts/{account_id}/d1/database?per_page=1000");
    let value = cf_api(client, token, "GET", &list_path, None).await?;
    let list = value
        .get("result")
        .and_then(Value::as_array)
        .context("cloudflare D1 list response missing result array")?;
    for item in list {
        if item.get("name").and_then(Value::as_str) == Some(database_name) {
            let db_id = item
                .get("uuid")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .context("matched D1 database missing id/uuid")?;
            println!("Reusing D1 database: {database_name} ({db_id})");
            return Ok(db_id.to_string());
        }
    }

    println!("Creating D1 database: {database_name}");
    let create_path = format!("/accounts/{account_id}/d1/database");
    let create = cf_api(
        client,
        token,
        "POST",
        &create_path,
        Some(json!({ "name": database_name })),
    )
    .await?;
    let db_id = create
        .get("result")
        .and_then(|v| v.get("uuid").or_else(|| v.get("id")))
        .and_then(Value::as_str)
        .context("created D1 database but did not receive id")?;
    Ok(db_id.to_string())
}

async fn resolve_or_create_r2_bucket(
    client: &Client,
    token: &str,
    account_id: &str,
    bucket_name: &str,
) -> anyhow::Result<()> {
    let list_path = format!("/accounts/{account_id}/r2/buckets?per_page=1000");
    let value = cf_api(client, token, "GET", &list_path, None).await?;
    let result = value
        .get("result")
        .context("cloudflare R2 list missing result")?;
    let buckets = if let Some(items) = result.as_array() {
        items.clone()
    } else {
        result
            .get("buckets")
            .and_then(Value::as_array)
            .cloned()
            .context("cloudflare R2 list missing buckets array")?
    };

    if buckets
        .iter()
        .any(|bucket| bucket.get("name").and_then(Value::as_str) == Some(bucket_name))
    {
        println!("Reusing R2 bucket: {bucket_name}");
        return Ok(());
    }

    println!("Creating R2 bucket: {bucket_name}");
    let create_path = format!("/accounts/{account_id}/r2/buckets");
    let _ = cf_api(
        client,
        token,
        "POST",
        &create_path,
        Some(json!({ "name": bucket_name })),
    )
    .await?;
    Ok(())
}

async fn apply_d1_schema(
    client: &Client,
    token: &str,
    account_id: &str,
    database_id: &str,
    schema_sql: &str,
) -> anyhow::Result<()> {
    apply_d1_schema_with_base(
        client,
        token,
        CLOUDFLARE_API_BASE,
        account_id,
        database_id,
        schema_sql,
    )
    .await
}

async fn apply_d1_schema_with_base(
    client: &Client,
    token: &str,
    base_url: &str,
    account_id: &str,
    database_id: &str,
    schema_sql: &str,
) -> anyhow::Result<()> {
    let query_path = format!("/accounts/{account_id}/d1/database/{database_id}/query");
    let result = cf_api_with_base(
        client,
        token,
        "POST",
        base_url,
        &query_path,
        Some(json!({ "sql": schema_sql })),
    )
    .await?;
    validate_d1_query_result(&result)
}

fn validate_d1_query_result(result: &Value) -> anyhow::Result<()> {
    let Some(rows) = result.get("result").and_then(Value::as_array) else {
        return Ok(());
    };

    for (idx, row) in rows.iter().enumerate() {
        if row.get("success").and_then(Value::as_bool) == Some(false) {
            let err = row
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown d1 query error");
            bail!("D1 query statement #{idx} failed: {err}");
        }
    }
    Ok(())
}

async fn upload_worker_script(
    client: &Client,
    token: &str,
    account_id: &str,
    worker_name: &str,
    compat_date: &str,
    d1_database_id: Option<&str>,
    r2_bucket_name: Option<&str>,
    index_js: Vec<u8>,
    index_bg_wasm: Vec<u8>,
) -> anyhow::Result<()> {
    upload_worker_script_with_base(
        client,
        token,
        CLOUDFLARE_API_BASE,
        account_id,
        worker_name,
        compat_date,
        d1_database_id,
        r2_bucket_name,
        index_js,
        index_bg_wasm,
    )
    .await
}

async fn upload_worker_script_with_base(
    client: &Client,
    token: &str,
    base_url: &str,
    account_id: &str,
    worker_name: &str,
    compat_date: &str,
    d1_database_id: Option<&str>,
    r2_bucket_name: Option<&str>,
    index_js: Vec<u8>,
    index_bg_wasm: Vec<u8>,
) -> anyhow::Result<()> {
    let metadata = worker_metadata(compat_date, d1_database_id, r2_bucket_name);
    let metadata_text =
        serde_json::to_string(&metadata).context("failed to serialize worker metadata")?;

    let form = multipart::Form::new()
        .part(
            "metadata",
            multipart::Part::text(metadata_text)
                .mime_str("application/json")
                .context("failed to build metadata multipart part")?,
        )
        .part(
            "index.js",
            multipart::Part::bytes(index_js)
                .file_name("index.js")
                .mime_str("application/javascript+module")
                .context("failed to build index.js multipart part")?,
        )
        .part(
            "index_bg.wasm",
            multipart::Part::bytes(index_bg_wasm)
                .file_name("index_bg.wasm")
                .mime_str("application/wasm")
                .context("failed to build index_bg.wasm multipart part")?,
        );

    let path = format!("/accounts/{account_id}/workers/scripts/{worker_name}");
    let url = format!("{base_url}{path}");
    let response = client
        .put(url)
        .bearer_auth(token)
        .multipart(form)
        .send()
        .await
        .with_context(|| format!("cloudflare worker upload request failed: PUT {path}"))?;

    let status = response.status();
    let text = response
        .text()
        .await
        .with_context(|| format!("cloudflare worker upload response read failed: PUT {path}"))?;
    if !status.is_success() {
        bail!(
            "cloudflare worker upload failed: PUT {} -> HTTP {}: {}",
            path,
            status,
            text
        );
    }

    let value: Value = serde_json::from_str(&text)
        .with_context(|| format!("cloudflare worker upload returned non-json body: PUT {path}"))?;
    if value.get("success").and_then(Value::as_bool) != Some(true) {
        let errors = value
            .get("errors")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|err| err.get("message").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .unwrap_or_else(|| "unknown cloudflare api error".to_string());
        bail!("cloudflare worker upload returned success=false for PUT {path}: {errors}");
    }

    Ok(())
}

fn worker_metadata(
    compat_date: &str,
    d1_database_id: Option<&str>,
    r2_bucket_name: Option<&str>,
) -> Value {
    let mut bindings = Vec::new();
    if let Some(database_id) = d1_database_id {
        bindings.push(json!({
            "name": "MESHFS_DB",
            "type": "d1",
            "id": database_id
        }));
    }
    if let Some(bucket_name) = r2_bucket_name {
        bindings.push(json!({
            "name": "MESHFS_R2",
            "type": "r2_bucket",
            "bucket_name": bucket_name
        }));
    }

    json!({
        "main_module": "index.js",
        "compatibility_date": compat_date,
        "bindings": bindings
    })
}

async fn cf_api(
    client: &Client,
    token: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> anyhow::Result<Value> {
    cf_api_with_base(client, token, method, CLOUDFLARE_API_BASE, path, body).await
}

async fn cf_api_with_base(
    client: &Client,
    token: &str,
    method: &str,
    base_url: &str,
    path: &str,
    body: Option<Value>,
) -> anyhow::Result<Value> {
    let url = format!("{base_url}{path}");
    let builder = match method {
        "GET" => client.get(&url),
        "POST" => client.post(&url),
        "PUT" => client.put(&url),
        other => return Err(anyhow!("unsupported method: {other}")),
    };
    let builder = builder
        .bearer_auth(token)
        .header("Content-Type", "application/json");
    let response = if let Some(body) = body {
        builder.json(&body).send().await
    } else {
        builder.send().await
    }
    .with_context(|| format!("cloudflare api request failed: {method} {path}"))?;

    let http_status = response.status();
    let text = response
        .text()
        .await
        .with_context(|| format!("cloudflare api response body read failed: {method} {path}"))?;
    if !http_status.is_success() {
        bail!(
            "cloudflare api failed: {} {} -> HTTP {}: {}",
            method,
            path,
            http_status,
            text
        );
    }

    let value: Value = serde_json::from_str(&text)
        .with_context(|| format!("cloudflare api returned non-json body: {method} {path}"))?;
    if value.get("success").and_then(Value::as_bool) != Some(true) {
        let errors = value
            .get("errors")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|err| err.get("message").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .unwrap_or_else(|| "unknown cloudflare api error".to_string());
        bail!("cloudflare api returned success=false for {method} {path}: {errors}");
    }

    Ok(value)
}

fn default_d1_database_name(worker_name: &str) -> String {
    format!("{}-metadata", sanitize_name(worker_name, 40))
}

fn default_r2_bucket_name(worker_name: &str) -> String {
    format!("{}-objects", sanitize_name(worker_name, 50))
}

fn sanitize_name(raw: &str, max_len: usize) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for ch in raw.chars() {
        let c = ch.to_ascii_lowercase();
        if c.is_ascii_alphanumeric() {
            out.push(c);
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    let trimmed = out.trim_matches('-');
    let mut normalized = if trimmed.is_empty() {
        DEFAULT_WORKER_NAME.to_string()
    } else {
        trimmed.to_string()
    };
    if normalized.len() > max_len {
        normalized.truncate(max_len);
        normalized = normalized.trim_matches('-').to_string();
    }
    if normalized.is_empty() {
        DEFAULT_WORKER_NAME.to_string()
    } else {
        normalized
    }
}

fn validate_deploy_args(args: &DeployCloudflareWorkersFreeTierArgs) -> anyhow::Result<()> {
    if args.no_d1 && (args.d1_database_id.is_some() || args.d1_database_name.is_some()) {
        bail!("--no-d1 cannot be used together with --d1-database-id/--d1-database-name");
    }
    if args.no_r2 && args.r2_bucket_name.is_some() {
        bail!("--no-r2 cannot be used together with --r2-bucket-name");
    }
    if args.d1_database_id.is_some() && args.d1_database_name.is_none() {
        bail!("--d1-database-name is required when --d1-database-id is provided");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::any;
    use axum::{Json, Router};
    use reqwest::Client;
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::{
        apply_d1_schema_with_base, cf_api_with_base, default_d1_database_name,
        default_r2_bucket_name, prepare_worker_bundle, resolve_bundle_dir, sanitize_name,
        upload_worker_script_with_base, validate_d1_query_result, validate_deploy_args,
        worker_metadata, DeployCloudflareWorkersFreeTierArgs, DEFAULT_WORKER_BUNDLE_RELATIVE,
        DEFAULT_WORKER_NAME,
    };

    fn base_args() -> DeployCloudflareWorkersFreeTierArgs {
        DeployCloudflareWorkersFreeTierArgs {
            token: "token".to_string(),
            account_id: None,
            name: DEFAULT_WORKER_NAME.to_string(),
            compat_date: None,
            d1_database_id: None,
            d1_database_name: None,
            no_d1: false,
            r2_bucket_name: None,
            no_r2: false,
            repo_root: PathBuf::from("."),
            worker_bundle: None,
            build_worker_local: false,
        }
    }

    #[test]
    fn validate_deploy_args_rejects_invalid_combinations() {
        let mut args = base_args();
        args.no_d1 = true;
        args.d1_database_name = Some("db".to_string());
        assert!(validate_deploy_args(&args).is_err());

        let mut args = base_args();
        args.no_r2 = true;
        args.r2_bucket_name = Some("bucket".to_string());
        assert!(validate_deploy_args(&args).is_err());

        let mut args = base_args();
        args.d1_database_id = Some("id".to_string());
        assert!(validate_deploy_args(&args).is_err());
    }

    #[test]
    fn validate_deploy_args_accepts_valid_combinations() {
        let args = base_args();
        assert!(validate_deploy_args(&args).is_ok());

        let mut args = base_args();
        args.d1_database_id = Some("id".to_string());
        args.d1_database_name = Some("name".to_string());
        assert!(validate_deploy_args(&args).is_ok());

        let mut args = base_args();
        args.no_d1 = true;
        args.no_r2 = true;
        assert!(validate_deploy_args(&args).is_ok());
    }

    #[test]
    fn sanitize_name_normalizes_and_truncates() {
        assert_eq!(sanitize_name("MeshFS OSS Edge", 40), "meshfs-oss-edge");
        assert_eq!(sanitize_name("___", 40), DEFAULT_WORKER_NAME);
        assert_eq!(sanitize_name("abc--DEF---ghi", 20), "abc-def-ghi");
        assert_eq!(sanitize_name("a".repeat(80).as_str(), 10), "aaaaaaaaaa");
    }

    #[test]
    fn default_names_have_expected_suffixes() {
        assert_eq!(
            default_d1_database_name("MeshFS OSS Edge"),
            "meshfs-oss-edge-metadata"
        );
        assert_eq!(
            default_r2_bucket_name("MeshFS OSS Edge"),
            "meshfs-oss-edge-objects"
        );
    }

    #[test]
    fn worker_metadata_contains_bindings() {
        let metadata = worker_metadata("2026-03-04", Some("db-id"), Some("bucket"));
        assert_eq!(metadata["main_module"], "index.js");
        assert_eq!(metadata["compatibility_date"], "2026-03-04");
        assert_eq!(metadata["bindings"].as_array().map(Vec::len), Some(2));
        assert_eq!(metadata["bindings"][0]["name"], "MESHFS_DB");
        assert_eq!(metadata["bindings"][1]["name"], "MESHFS_R2");
    }

    #[test]
    fn worker_metadata_without_bindings_is_valid() {
        let metadata = worker_metadata("2026-03-04", None, None);
        assert_eq!(metadata["bindings"].as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn resolve_bundle_dir_prefers_explicit_path() {
        let mut args = base_args();
        args.worker_bundle = Some(PathBuf::from("my/bundle"));
        let root = PathBuf::from("/tmp/repo");
        assert_eq!(resolve_bundle_dir(&args, &root), root.join("my/bundle"));

        let mut args = base_args();
        args.worker_bundle = Some(PathBuf::from("/opt/bundle"));
        assert_eq!(
            resolve_bundle_dir(&args, &root),
            PathBuf::from("/opt/bundle")
        );
    }

    #[test]
    fn resolve_bundle_dir_uses_default_relative_dir() {
        let args = base_args();
        let root = PathBuf::from("/tmp/repo");
        assert_eq!(
            resolve_bundle_dir(&args, &root),
            root.join(DEFAULT_WORKER_BUNDLE_RELATIVE)
        );
    }

    #[test]
    fn validate_d1_query_result_rejects_failed_statement() {
        let value = json!({
            "result": [
                {"success": true},
                {"success": false, "error": "near syntax error"}
            ]
        });
        let err = validate_d1_query_result(&value).expect_err("should fail");
        assert!(err.to_string().contains("near syntax error"));
    }

    #[test]
    fn validate_d1_query_result_accepts_successful_statements() {
        let value = json!({"result": [{"success": true}]});
        assert!(validate_d1_query_result(&value).is_ok());
    }

    async fn start_cf_api_test_server() -> (String, tokio::task::JoinHandle<()>) {
        async fn ok_handler() -> impl IntoResponse {
            Json(json!({
                "success": true,
                "result": { "ok": true }
            }))
        }

        async fn success_false_handler() -> impl IntoResponse {
            Json(json!({
                "success": false,
                "errors": [{ "message": "bad token" }]
            }))
        }

        async fn http_fail_handler() -> impl IntoResponse {
            (StatusCode::FORBIDDEN, "forbidden")
        }

        async fn invalid_json_handler() -> impl IntoResponse {
            (StatusCode::OK, "not-json")
        }

        let app = Router::new()
            .route("/ok", any(ok_handler))
            .route("/success-false", any(success_false_handler))
            .route("/http-fail", any(http_fail_handler))
            .route("/invalid-json", any(invalid_json_handler));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("get listener addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve test app");
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn cf_api_with_base_handles_success_response() {
        let (base_url, handle) = start_cf_api_test_server().await;
        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("build client");

        let resp = cf_api_with_base(&client, "token", "GET", &base_url, "/ok", None)
            .await
            .expect("cf api success");
        assert_eq!(resp["result"]["ok"], true);

        handle.abort();
    }

    #[tokio::test]
    async fn cf_api_with_base_handles_error_cases() {
        let (base_url, handle) = start_cf_api_test_server().await;
        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("build client");

        let http_err = cf_api_with_base(&client, "token", "GET", &base_url, "/http-fail", None)
            .await
            .expect_err("http failure should error");
        assert!(http_err.to_string().contains("HTTP 403"));

        let success_false_err =
            cf_api_with_base(&client, "token", "GET", &base_url, "/success-false", None)
                .await
                .expect_err("success=false should error");
        assert!(success_false_err.to_string().contains("bad token"));

        let invalid_json_err =
            cf_api_with_base(&client, "token", "GET", &base_url, "/invalid-json", None)
                .await
                .expect_err("invalid json should error");
        assert!(invalid_json_err
            .to_string()
            .contains("returned non-json body"));

        handle.abort();
    }

    #[test]
    fn worker_bundle_files_are_detected() {
        let dir = std::env::temp_dir().join(format!(
            "meshfs-bundle-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("create dir");
        fs::write(dir.join("index.js"), "export default {};").expect("write js");
        fs::write(dir.join("index_bg.wasm"), [0x00, 0x61]).expect("write wasm");

        let bundle = super::find_worker_bundle_files(&dir);
        assert!(bundle.is_some());

        let _ = fs::remove_file(dir.join("index.js"));
        let _ = fs::remove_file(dir.join("index_bg.wasm"));
        let _ = fs::remove_dir(&dir);
    }

    fn temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "meshfs-deploy-path-test-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn prepare_worker_bundle_prefers_prebuilt_dir() {
        let preferred = temp_dir("preferred");
        let runtime = temp_dir("runtime");
        fs::write(preferred.join("index.js"), "export default {};").expect("write js");
        fs::write(preferred.join("index_bg.wasm"), [0x00, 0x61]).expect("write wasm");

        let bundle = prepare_worker_bundle(&preferred, false, &runtime).expect("resolve bundle");
        assert_eq!(bundle.index_js, preferred.join("index.js"));
        assert_eq!(bundle.index_bg_wasm, preferred.join("index_bg.wasm"));
    }

    #[test]
    fn prepare_worker_bundle_falls_back_to_runtime_build_dir() {
        let preferred = temp_dir("preferred-missing");
        let runtime = temp_dir("runtime-with-build");
        let build = runtime.join("build");
        fs::create_dir_all(&build).expect("create build dir");
        fs::write(build.join("index.js"), "export default {};").expect("write js");
        fs::write(build.join("index_bg.wasm"), [0x00, 0x61]).expect("write wasm");

        let bundle = prepare_worker_bundle(&preferred, false, &runtime).expect("resolve bundle");
        assert_eq!(bundle.index_js, build.join("index.js"));
        assert_eq!(bundle.index_bg_wasm, build.join("index_bg.wasm"));
    }

    #[test]
    fn prepare_worker_bundle_errors_when_missing_and_no_local_build() {
        let preferred = temp_dir("preferred-empty");
        let runtime = temp_dir("runtime-empty");
        let err = prepare_worker_bundle(&preferred, false, &runtime).expect_err("should fail");
        assert!(err.to_string().contains("worker bundle not found"));
    }

    #[derive(Default)]
    struct DeployApiState {
        last_body: Mutex<Option<String>>,
    }

    async fn d1_query_ok_handler(
        State(state): State<Arc<DeployApiState>>,
        body: Bytes,
    ) -> impl IntoResponse {
        let body_text = String::from_utf8(body.to_vec()).unwrap_or_default();
        *state.last_body.lock().expect("lock state") = Some(body_text);
        Json(json!({
            "success": true,
            "result": [{"success": true}]
        }))
    }

    async fn d1_query_stmt_fail_handler() -> impl IntoResponse {
        Json(json!({
            "success": true,
            "result": [{"success": false, "error": "stmt failed"}]
        }))
    }

    async fn upload_ok_handler() -> impl IntoResponse {
        Json(json!({
            "success": true,
            "result": {"id": "ok"}
        }))
    }

    #[tokio::test]
    async fn apply_d1_schema_sends_sql_payload() {
        let state = Arc::new(DeployApiState::default());
        let app = Router::new()
            .route(
                "/accounts/{account_id}/d1/database/{database_id}/query",
                any(d1_query_ok_handler),
            )
            .with_state(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let base_url = format!("http://{addr}");
        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("build client");
        let schema_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY);";

        apply_d1_schema_with_base(&client, "token", &base_url, "acc", "db", schema_sql)
            .await
            .expect("apply d1 schema");

        let body = state.last_body.lock().expect("lock").clone().expect("body");
        assert!(body.contains("CREATE TABLE t"));

        handle.abort();
    }

    #[tokio::test]
    async fn apply_d1_schema_reports_statement_error() {
        let state = Arc::new(DeployApiState::default());
        let app = Router::new().route(
            "/accounts/{account_id}/d1/database/{database_id}/query",
            any(d1_query_stmt_fail_handler),
        );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app.with_state(state))
                .await
                .expect("serve");
        });

        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("build client");
        let err = apply_d1_schema_with_base(
            &client,
            "token",
            &format!("http://{addr}"),
            "acc",
            "db",
            "BAD SQL",
        )
        .await
        .expect_err("statement should fail");
        assert!(err.to_string().contains("stmt failed"));

        handle.abort();
    }

    #[tokio::test]
    async fn upload_worker_script_accepts_success() {
        let app = Router::new().route(
            "/accounts/{account_id}/workers/scripts/{script_name}",
            any(upload_ok_handler),
        );
        let state = Arc::new(DeployApiState::default());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app.with_state(state))
                .await
                .expect("serve");
        });

        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("build client");

        let upload = upload_worker_script_with_base(
            &client,
            "token",
            &format!("http://{addr}"),
            "acc",
            "worker",
            "2026-03-04",
            Some("db"),
            Some("bucket"),
            b"export default {};".to_vec(),
            vec![0x00, 0x61, 0x73, 0x6d],
        )
        .await;
        assert!(upload.is_ok());

        handle.abort();
    }

    async fn upload_success_false_handler() -> impl IntoResponse {
        Json(json!({
            "success": false,
            "errors": [{"message": "upload denied"}]
        }))
    }

    #[tokio::test]
    async fn upload_worker_script_reports_success_false() {
        let app = Router::new().route(
            "/accounts/{account_id}/workers/scripts/{script_name}",
            any(upload_success_false_handler),
        );
        let state = Arc::new(DeployApiState::default());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app.with_state(state))
                .await
                .expect("serve");
        });

        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("build client");

        let err = upload_worker_script_with_base(
            &client,
            "token",
            &format!("http://{addr}"),
            "acc",
            "worker",
            "2026-03-04",
            None,
            None,
            b"export default {};".to_vec(),
            vec![0x00, 0x61, 0x73, 0x6d],
        )
        .await
        .expect_err("success=false should fail");
        assert!(err.to_string().contains("upload denied"));

        handle.abort();
    }
}
