# caddy-pgstore

PostgreSQL-backed storage module for Caddy/CertMagic.

本仓库使用单一 `main` 分支。日常开发从 `main` 创建 `feature/*`、`fix/*`、`docs/*` 或
`chore/*` 分支，通过 PR 合并；不长期维护 `develop` 或 `release/*` 分支。

## GitHub Actions 与发布

指向 `main` 的 PR 和 push 会运行 Go 格式检查、`go vet`、带 PostgreSQL 服务的 race tests，
并构建一个包含 `caddy.storage.postgres` 的 Caddy 二进制。

创建并推送严格格式的 SemVer tag 后，Release workflow 会创建 GitHub Release，并构建
`linux/amd64` 的 Caddy 镜像推送到腾讯云 TCR：

```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

发布 workflow 需要以下 GitHub 配置：

- Repository variable `TCR_IMAGE`：完整镜像路径，不包含 tag，例如
  `ccr.ccs.tencentyun.com/<namespace>/caddy-pgstore`。
- Repository secrets `TCR_USERNAME` 和 `TCR_PASSWORD`：TCR 长期访问凭证。

镜像 Dockerfile 位于 `docker/Dockerfile`，使用当前 tag 的源码通过 xcaddy 构建；运维仓库的
Caddyfile 仍由部署系统提供。

首次迁移分支时，先将当前代码提交到 `main` 并推送，再在 GitHub 仓库设置中将 `main` 设为默认
分支、启用分支保护并要求 CI，通过后再冻结或删除 `develop`。

Build a Caddy binary with the module:

```bash
go run github.com/caddyserver/xcaddy/cmd/xcaddy@v0.4.5 build \
    --with github.com/c2hy/caddy-pgstore=.
```

配置统一使用 `DATABASE_URL`（或 Caddy 的 `database_url` 字段）。模块会创建两张表，
并使用 PostgreSQL lease row 协调多实例证书申请。

```caddyfile
{
    storage postgres {
        database_url {$DATABASE_URL}
        schema caddy
        auto_create true
    }
}
```

详细的数据模型、并发语义、维护任务和测试策略见
[架构设计文档](docs/architecture.md)。

`DATABASE_URL` should be supplied at install/runtime through the environment or a secret
manager; do not commit it. `auto_create true` needs schema DDL permissions. For a least-privilege
runtime role, create the schema/tables first and set `auto_create false`.

验证并启动：

```bash
export DATABASE_URL='postgresql://<user>:<password>@<host>/<database>?sslmode=require&channel_binding=require'
./caddy list-modules | grep caddy.storage.postgres
./caddy validate --config /path/to/Caddyfile
./caddy run --config /path/to/Caddyfile
```

`validate` 和首次启动会连接数据库；`auto_create true` 时会幂等创建配置的 schema、对象表和
lease 锁表。多台 Caddy 必须使用同一个 `DATABASE_URL` 和 `schema`。
