# MeshFS User Guide

本指南面向普通用户（非部署平台管理员、非服务器管理员），目标是让你完成两件事：

1. 部署自己的第一个 MeshFS 服务端（最短路径）。
2. 用 `meshfs` 完成登录与同步。

## 1. 5 分钟完成第一次服务端部署（Cloudflare Free Tier）

当前 OSS 路径里，最简单的首个服务端部署方式是 `cloudflare-workers-free-tier`。

如果你使用 GitHub Release 下载包，发布包内会包含预编译 worker bundle，可直接部署。

### 1.1 你需要准备

- 一个 Cloudflare 账号。
- 一个 API Token，至少包含这些权限：
  - Account: `Workers Scripts:Edit`
  - User: `Memberships:Read`（不传 `--account-id` 时需要）
  - User: `User Details:Read`（不传 `--account-id` 时需要）
  - Account: `D1:Edit`（除非你用 `--no-d1`）
  - Account: `Workers R2 Storage:Edit`（除非你用 `--no-r2`）
- 本地命令依赖：
  - `meshfs`
  - 部署时默认走预编译 worker bundle（无需 `node/npx/cargo/rustup`）。
  - 只有在你显式使用 `--build-worker-local` 时，才需要 `cargo`、`rustup`、`worker-build`。

### 1.2 执行部署

在仓库根目录运行（目录中应包含 `crates/` 和 `deploy/`）：

```bash
meshfs deploy cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN>
```

默认会自动处理：
- Worker 发布
- D1 元数据库创建/复用
- R2 存储桶创建/复用
- 从 `deploy/providers/cloudflare-workers-free-tier/worker-bundle/` 读取预编译 worker bundle（如存在）

### 1.3 记录服务端 URL

部署完成后，命令输出会给出可访问地址信息。把它记为 `<SERVER_URL>`，后续客户端命令都用它。

可选健康检查：

```bash
curl <SERVER_URL>/healthz
```

## 2. 客户端登录

```bash
meshfs --server <SERVER_URL> login --auto-activate
```

如果你需要显式指定用户和租户：

```bash
meshfs --server <SERVER_URL> login \
  --auto-activate \
  --user-id <USER_ID> \
  --tenant-id <TENANT_ID>
```

## 3. 开始同步（最常用）

单次同步（执行一次后退出）：

```bash
meshfs --server <SERVER_URL> sync --once --target ./meshfs-mirror
```

持续同步（长期运行）：

```bash
meshfs --server <SERVER_URL> sync --target ./meshfs-mirror
```

## 4. 多设备使用（普通用户常见）

在另一台设备上重复两步即可：

1. `login` 到同一个 `<SERVER_URL>`
2. `sync --target <本地目录>`

这样可以让多个设备围绕同一个租户数据进行同步。

## 5. 可选：把 MeshFS 挂载成目录

如果你要用挂载目录方式访问：

```bash
meshfs mount --remote <SERVER_URL> --target ./meshfs-mount
```

可选参数：
- `--read-only`
- `--auto-unmount`
- `--allow-other`

## 6. 常见问题

### 6.1 部署时报 token 权限不足

优先检查 API Token 是否包含第 1.1 节列出的权限，尤其是 D1 与 R2 相关权限。

### 6.2 不想自动推断 Cloudflare account id

在部署命令里显式传入：

```bash
meshfs deploy cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN> \
  --account-id <CLOUDFLARE_ACCOUNT_ID>
```

如果你的目录里没有预编译 bundle，也可显式启用本地构建：

```bash
meshfs deploy cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN> \
  --build-worker-local
```

### 6.3 `sync` 连不上服务端

- 确认 `<SERVER_URL>` 可以 `curl <SERVER_URL>/healthz`。
- 确认登录使用的是同一个 `<SERVER_URL>`。
- 确认本机网络可访问 Workers 域名。
