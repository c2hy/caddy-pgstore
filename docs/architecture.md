# caddy-pgstore 架构设计

状态：V1 implementation  
日期：2026-09-02

## 1. 结论

本项目实现一个小而专一的 Caddy storage module：使用 PostgreSQL 持久化
CertMagic 的证书、私钥、OCSP 等对象，并使用 PostgreSQL 行记录实现可过期、可续租的
分布式锁。

项目不实现 ACME、证书签发、续期策略或 TLS 内存缓存。上述能力仍由 Caddy/CertMagic
负责。本项目的责任边界只有两部分：

1. 完整实现 `certmagic.Storage` 的键值与隐式目录语义；
2. 以原子 SQL 实现 `Lock`、`TryLock`、`Unlock` 和 `RenewLockLease`。

锁方案确定为：

> PostgreSQL lease row + 每次获取唯一的 owner token + 原子 UPSERT + TTL + 主动心跳 +
> CertMagic 显式续租 + 成功释放锁后限频异步清理过期 lease。

不使用 session-scoped `pg_advisory_lock`，因此锁等待期间和持锁期间都不独占连接池连接。

## 2. 设计依据与边界

CertMagic 的 `Storage` 是带文件系统路径语义的 KV 接口。实现必须并发安全、响应
context cancellation；`Load`、`List`、`Stat` 在目标不存在时应返回
`fs.ErrNotExist`。`Locker` 用于跨节点协调证书签发等高层操作，`Lock` 必须等待到成功、
context 取消或发生错误。[Storage 接口源码](https://github.com/caddyserver/certmagic/blob/master/storage.go)

最新版接口还提供两个可选能力：非阻塞的 `TryLocker` 和按调用方给定时长续租的
`LockLeaseRenewer`。CertMagic 会在证书获取/续期的重试路径中调用后者。
[CertMagic 续租调用](https://github.com/caddyserver/certmagic/blob/master/config.go#L473-L493)

Caddy 侧模块注册在 `caddy.storage` namespace，并通过 `caddy.StorageConverter` 转换为
`certmagic.Storage`。Caddy 配置重载时，新旧模块实例可能短暂重叠，因此连接池、心跳
goroutine 和本地锁状态必须属于具体模块实例并有明确清理流程。
[Caddy 模块生命周期](https://caddyserver.com/docs/extending-caddy)

### 2.1 项目负责

- PostgreSQL schema 的创建/校验；
- KV 操作及隐式目录行为；
- 分布式互斥、超时接管、续租、安全释放和异步过期 lease 维护；
- Caddy JSON/Caddyfile 配置适配；
- 连接池生命周期、错误包装和必要日志；
- 接口一致性、并发、故障恢复和双 Caddy 集成测试。

### 2.2 项目不负责

- ACME client、challenge、issuer 或证书生命周期策略；
- TLS handshake 热路径缓存；证书仍由 CertMagic 缓存在内存中；
- 通用对象存储 API；只保证 CertMagic 产生的合法、无文件/目录冲突的 key；
- 跨区域强一致数据库架构、PostgreSQL HA、备份及密钥托管；
- 在 lease 丢失后对既有写操作提供 fencing。当前 `certmagic.Storage` 的 `Store` 不接收
  fencing token，接口层无法实现严格 fencing；设计目标是在 PostgreSQL 可达且 lease
  未丢失时保证互斥，并可靠检测所有权丢失。

## 3. 质量属性

优先级从高到低：

1. **并发正确性**：同名有效 lease 最多一个 owner；旧 owner 永远不能续租或删除新
   owner 的 lease。
2. **数据安全**：单次对象写原子覆盖，不返回部分内容，不在日志暴露 DSN、私钥或值。
3. **故障可恢复**：进程崩溃或连接中断后，其他实例能在 TTL 后接管。
4. **接口一致性**：行为尽量与 CertMagic `FileStorage` 一致，尤其是目录、排序和
   `fs.ErrNotExist`。
5. **实现克制**：固定两张业务表，不引入队列、通知系统或长期占用连接的锁。

## 4. 总体结构

```text
Caddy JSON / Caddyfile
        │
        ▼
Postgres module
  Provision / Validate / Cleanup
        │ creates and owns
        ▼
Storage
  ├── object operations ───────► certmagic_objects
  ├── distributed lock SQL ────► certmagic_locks
  ├── async expired-lock reaper
  │     └── at most one task per module instance
  └── local lock registry
        ├── one acquisition token per held name
        ├── serialize same-name callers in one instance
        └── heartbeat lifecycle
```

### 4.1 建议代码布局

```text
.
├── go.mod
├── module.go              # Caddy 注册、配置、Provision/Validate/Cleanup
├── caddyfile.go           # Caddyfile 解析
├── storage.go             # Storage 类型和 Store/Load/Delete/Exists/List/Stat
├── lock.go                # Lock/TryLock/Unlock/RenewLockLease、本地 registry
├── maintenance.go         # 成功释放锁后触发的限频异步过期 lease 清理
├── schema.go              # 固定 DDL 与 schema identifier 处理
├── errors.go              # fs.ErrNotExist 包装、ErrLockLost 等
├── *_test.go              # 单元与契约测试
├── integration_test.go    # 真实 PostgreSQL 并发/故障测试
├── testdata/
└── docs/
    └── architecture.md
```

保持单一 Go package，暂不抽象 repository/DAO 层。SQL 与所实现的方法放在一起，更容易审查
每个操作的原子性。

## 5. 数据模型

V1 仅包含两张业务表：

```sql
CREATE TABLE IF NOT EXISTS <schema>.certmagic_objects (
    key         text        PRIMARY KEY,
    value       bytea       NOT NULL,
    modified_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS <schema>.certmagic_locks (
    key        text        PRIMARY KEY,
    owner      uuid        NOT NULL,
    expires_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS certmagic_locks_expires_at_idx
    ON <schema>.certmagic_locks (expires_at);
```

设计约束：

- `key` 原样保存，不进行大小写转换或路径清洗；CertMagic key 是大小写敏感的逻辑路径；
- 对象表只保存 terminal key，目录由 key 的 `/` 分量隐式推导；
- `modified_at`、lease 过期判断与续租都使用 PostgreSQL 时钟，避免 Caddy 节点间时钟偏差；
- `owner` 是**每次获取生成的新 UUID**，不是进程 ID、主机名或模块实例 UUID；
- schema 名只允许配置期校验后的 PostgreSQL identifier，并始终安全 quote；值全部使用参数；
- `expires_at` B-tree index 服务于 V1 的异步过期 lease 清理；抢锁仍只通过主键定位；
- 清理任务删除已经失效的行，过期行即使尚未被清理，也可以由同 key 的原子 UPSERT 直接
  接管，因此维护不是抢锁正确性或可用性的前置条件。

### 5.1 时间函数

使用 `clock_timestamp()` 而非应用服务器时间；该函数返回调用时的数据库实际时间，而
`now()` 固定为事务开始时间。[PostgreSQL 时间函数](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)
单条 statement 内多次读取的微小差异不影响正确性；过期边界统一采用
`expires_at <= clock_timestamp()`。

Go duration 以整数微秒传入，并在 SQL 中乘 `interval '1 microsecond'`，避免拼接 interval
字符串和浮点精度问题。

## 6. 对象存储语义

### 6.1 Store

一条 UPSERT 完成创建或覆盖：

```sql
INSERT INTO certmagic_objects (key, value, modified_at)
VALUES ($1, $2, clock_timestamp())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value,
    modified_at = EXCLUDED.modified_at;
```

不使用先查后写。PostgreSQL statement 成功即表示完整值已可见，失败则不暴露部分值。

### 6.2 Load

按主键查询 `value`。无行时返回包装了 key 上下文、且满足
`errors.Is(err, fs.ErrNotExist)` 的错误。

### 6.3 Delete

删除精确 key 及其整个隐式子树，单 statement 完成：

```sql
DELETE FROM certmagic_objects
WHERE key = $1 OR starts_with(key, $1 || '/');
```

删除不存在的 key 返回 `nil`。不用未转义的 `LIKE`，避免 key 中 `_`、`%` 被当成通配符。

### 6.4 Exists

精确对象或至少一个后代存在即为 true：

```sql
SELECT EXISTS (
    SELECT 1
    FROM certmagic_objects
    WHERE key = $1 OR starts_with(key, $1 || '/')
);
```

该接口只能返回 `bool`，数据库错误无法向上传递。实现返回 false，并记录一次不含敏感值的
错误日志。其余方法不吞数据库错误。

### 6.5 List

先按前缀读取所有 terminal key，再在 Go 中按 `/` 推导目录并去重、字典序排序：

- `recursive=false`：只返回 path 的直接子对象或直接子目录；
- `recursive=true`：返回全部后代目录和对象；
- path 仅作为隐式目录存在时可以正常列举；
- path 不存在时返回 `fs.ErrNotExist`；
- path 是对象且没有后代时返回空 slice；
- 空 path 表示 storage root。

例：仅存有 `a/b/c.crt` 和 `a/d.json` 时：

```text
List("a", false) -> ["a/b", "a/d.json"]
List("a", true)  -> ["a/b", "a/b/c.crt", "a/d.json"]
```

这一实现优先保证与文件存储相同的目录可见性。数据量达到需要分页或服务端分段聚合之前，
不引入复杂 SQL。

### 6.6 Stat

先查精确对象：返回 key、`octet_length(value)`、`modified_at`、`IsTerminal=true`。若精确
对象不存在但存在后代，则返回隐式目录：key、size 0、零值 modified、
`IsTerminal=false`。两者都不存在时返回 `fs.ErrNotExist`。

### 6.7 Key 约束

入口校验空 key、开头/结尾 `/` 等违反 CertMagic contract 的输入。V1 不支持同时存在
`a` 对象与 `a/...` 后代；CertMagic 自身产生的 key 不会构造这种冲突。若未来将本模块
开放为通用 KV，需要通过独立的 namespace 模型或 serializable transaction 正式保证此
不变量，不能只增加一组有竞态的“查询后插入”检查。

## 7. 分布式锁设计

### 7.1 核心不变量

对任意 lock key：

1. PostgreSQL 中最多一行，主键冲突作为竞争串行化点；
2. 有效 lease 不能被另一个 owner 覆盖；
3. 仅 `expires_at <= database_now` 的 lease 可被原子接管；
4. 续租和释放必须同时匹配 `key + owner`；
5. owner token 每次 acquisition 唯一；
6. 同一 `Storage` 实例内，同名调用在本地 registry 先串行化；lease 丢失也不能让旧任务
   与该实例的新任务共享/覆盖 owner 状态；
7. 不在事务或连接上等待锁，不在整个 critical section 持有数据库连接。

### 7.2 原子抢锁

`tryAcquire` 只执行一条语句：

```sql
INSERT INTO certmagic_locks (key, owner, expires_at)
VALUES (
    $1,
    $2,
    clock_timestamp() + ($3::bigint * interval '1 microsecond')
)
ON CONFLICT (key) DO UPDATE
SET owner = EXCLUDED.owner,
    expires_at = EXCLUDED.expires_at
WHERE expires_at <= clock_timestamp()
RETURNING owner;
```

结果语义：

- 返回一行：插入成功，或原子接管了过期 lease；
- 返回零行：已有未过期 owner，属于正常竞争而不是数据库错误；
- SQL error：立即向调用方返回，不把存储故障伪装成锁竞争。

这里没有 `SELECT -> 应用判断 -> UPDATE` 的 TOCTOU 窗口。PostgreSQL 明确保证
`ON CONFLICT DO UPDATE` 在高并发下得到原子的 insert-or-update 结果；当冲突更新的
`WHERE` 不成立时，`RETURNING` 不返回该行。
[PostgreSQL INSERT/ON CONFLICT](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)

### 7.3 Lock

流程：

1. 在本地 registry 为 name 创建 reservation；已有 reservation 时等待其 release channel；
2. 为本次 acquisition 生成随机 UUID；
3. 调用原子 `tryAcquire`；
4. 未获得时，使用带抖动的 `lock_poll_interval` 等待后重试；
5. 每次数据库调用及等待都检查 caller context；
6. 成功后把 token 与 heartbeat cancel/done 状态记录到 reservation，启动心跳并返回。

如果 context 取消或数据库报错，必须移除 reservation 并唤醒本实例的等待者。

轮询而非 `LISTEN/NOTIFY` 是刻意选择：锁竞争低频，轮询逻辑容易验证；通知存在丢失、重连
和额外连接生命周期问题。默认 1 秒轮询，并加小比例随机抖动，避免多个实例整齐惊群。

### 7.4 TryLock

本地已有 reservation 时直接返回 `(false, nil)`；否则创建 reservation、生成 token，只执行
一次原子 `tryAcquire`：

- 成功则启动心跳并返回 `(true, nil)`；
- 正常竞争返回 `(false, nil)` 并清理 reservation；
- 数据库错误返回 `(false, err)`。

注意：并发测试中 `Lock` 是阻塞接口，所有调用最终都可以依次成功；要断言的是 critical
section 的最大同时进入数为 1。只有同一时刻并发调用 `TryLock` 才应恰好一个返回 true。

### 7.5 心跳与 TTL

默认：

- `lock_ttl = 5m`；
- `lock_poll_interval = 1s`；
- heartbeat interval 从 TTL 派生为 `TTL / 3`，设合理上下限，不额外暴露配置项。

成功获取后启动每锁一个轻量 heartbeat。续租 SQL 必须匹配 token：

```sql
UPDATE certmagic_locks
SET expires_at = GREATEST(
    expires_at,
    clock_timestamp() + ($3::bigint * interval '1 microsecond')
)
WHERE key = $1 AND owner = $2
RETURNING key;
```

`GREATEST` 防止默认心跳把 CertMagic 显式设置的更长 lease 缩短。连续数据库故障不立即放弃
本地 reservation；若在 TTL 内恢复且尚未被接管，带 owner 条件的 UPDATE 可以继续保持
lease。若返回零行，标记 `ErrLockLost`、停止心跳并记录错误，但保留本地 reservation 直到
原调用 `Unlock`，避免同一实例中旧任务和新任务重叠。

主动心跳覆盖没有触发 `RenewLockLease` 的同步/交互路径；显式续租则允许 CertMagic 根据
重试周期和证书获取 timeout 扩大 lease。两者使用同一 token 和同一 guarded UPDATE。

### 7.6 RenewLockLease

从本地 registry 读取 name 对应 acquisition token，再执行上述 guarded UPDATE：

- 更新一行：成功；
- 更新零行：返回 `ErrLockLost`；
- 本地不存在 reservation：返回 `ErrLockNotHeld`；
- duration 非正：返回参数错误。

绝不能只按 key 续租，也不能在 lease 已被其他实例接管后重新写回旧 owner。

### 7.7 Unlock

先取消 heartbeat 并等待它退出，避免“DELETE 后心跳复活”的竞态，然后执行：

```sql
DELETE FROM certmagic_locks
WHERE key = $1 AND owner = $2
RETURNING key;
```

最后清理本地 reservation 并唤醒等待者。无论 SQL 成功还是失败，都不能无限保留本地
reservation；错误需返回给 CertMagic 记录，数据库中的残留 lease 可由 TTL 回收。仅当
guarded DELETE 成功删除当前 owner 的行之后，调用 `maybeReapExpiredLocks()`；该调用只尝试
调度后台维护，不等待维护 SQL，也不改变 `Unlock` 的成功结果。

返回零行表示 lease 已丢失或被外部删除，返回 `ErrLockLost`。最关键的故障序列是：

```text
A(token=a) 获得锁 -> A 失联 -> lease 过期
B(token=b) 原子接管 -> A 恢复后迟到 Unlock(token=a)
DELETE ... WHERE owner=a -> 0 rows，B 的锁保持不变
```

### 7.8 异步过期 lease 维护

V1 实现“成功释放后触发、单实例限频”的事件式维护。清理不放在 `Lock`/`TryLock` 入口，
原因是：

- 过期行清理只控制历史 lock key 占用的空间，不参与当前 key 的抢锁正确性；当前过期行
  本来就能被 UPSERT 原子接管；
- 申请入口可能有多个竞争者，入口触发会让失败的 `TryLock` 和等待中的 `Lock` 也制造维护
  压力；
- 后台 DELETE 可能先占连接池连接或锁住当前过期行，从而间接增加首个抢锁 SQL 的延迟；
- 成功 `Unlock` 表示实例确实完成过一次临界区，且当前 owner 行已经删除，此时证书操作的
  主流程已经结束，是更安全的维护边界。

每次 owner-guarded `Unlock` 成功后调用 `maybeReapExpiredLocks()`。满足限频条件时，它启动
一个不阻塞 `Unlock` 返回的后台任务；清理与刚释放的 key 无关，一次删除表中所有在数据库
时钟下已过期的 lease：

```sql
DELETE FROM certmagic_locks
WHERE expires_at <= clock_timestamp();
```

任务调度规则：

1. 每个模块实例持有受 mutex 保护的 `reaperRunning`、`lastReaperAttempt` 和 maintenance wait group；
2. 首次成功 Unlock 可立即触发；之后同一实例两次 reaper 启动至少间隔 10 分钟；该常量
   不是互斥参数，V1 不暴露配置；
3. 未到时间或已有任务运行时立即返回，不排队、不创建 goroutine；
4. 通过限频检查后再 compare-and-swap，取得执行权时记录 attempt 时间，然后启动 goroutine；
5. 清理使用模块生命周期派生的 maintenance context 和 `operation_timeout`，不继承 Unlock
   context；
6. SQL 错误仅记录日志，不返回给 `Unlock`，也不改变已经成功的释放结果；失败后等待下一
   限频窗口重试，避免数据库故障期间产生日志和查询风暴；
7. 不设置周期 timer；没有证书锁活动时不产生后台查询，下一次成功 Unlock 会重新判断。

限频使用 Go 单调时钟即可，因为它只影响空间维护频率，不参与 lease 正确性。多个 Caddy
实例仍可能各运行一个 DELETE；PostgreSQL 行锁与条件重检保证结果安全：

- 有效或刚续租成功、`expires_at` 已在未来的行不会被删除；
- 清理先删除已过期行时，随后抢锁可正常 INSERT；
- 抢锁先接管过期行时，清理在获得行锁后重检条件，不会删除新的有效 lease；
- 过期 owner 的迟到 heartbeat 若在清理后执行，会因 owner 行不存在而得到 0 rows 并进入
  `ErrLockLost`；lease 既然已经过期，这正是预期语义。

清理是空间维护，不参与互斥正确性。即使进程在 Unlock 前崩溃或任务持续失败，过期行仍能
被同 key 的原子 UPSERT 接管；影响仅是历史 lock key 行暂时保留。一旦集群再次成功完成任意
一个锁生命周期，就有机会在临界区之外执行下一次维护。

### 7.9 lease 的保证边界

这是 failure-detection lease，不是共识协议。网络分区超过 TTL 时，数据库必须允许其他节点
接管，旧节点上的业务代码也可能暂时继续执行。由于 CertMagic 在获得锁后会重新检查工作
是否仍需执行，证书操作本身具有一定幂等防护；但无法承诺线性一致的 fenced write。

因此生产默认 TTL 要远高于正常数据库抖动，监控必须告警 heartbeat/renew 的失败。缩短
TTL 只用于测试，不应被当成提升吞吐的手段。

## 8. Caddy 模块与配置

模块 ID：`caddy.storage.postgres`；Caddyfile 中的短名为 `postgres`。

建议 V1 配置字段：

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `database_url` | 空 | PostgreSQL URL；字段为空时读取 `DATABASE_URL` |
| `schema` | `public` | 两张表所在 schema，也是多套独立 storage 的隔离边界 |
| `auto_create` | `true` | 幂等创建 schema/table；关闭时只做兼容性探测 |
| `max_open_conns` | `10` | 连接池上限，不随锁数量增长 |
| `max_idle_conns` | `2` | 空闲连接数上限（通过 pgxpool `AfterRelease` best-effort enforced） |
| `conn_max_lifetime` | `30m` | 连接最大寿命 |
| `operation_timeout` | `10s` | 单次数据库操作的内部上限；仍服从更短 caller deadline |
| `lock_ttl` | `5m` | 初始 lease 与后台心跳续租时长 |
| `lock_poll_interval` | `1s` | `Lock` 竞争重试间隔 |

Caddyfile 示例：

```caddyfile
{
    storage postgres {
        database_url {$DATABASE_URL}
        schema caddy
        auto_create true
        max_open_conns 10
        lock_ttl 5m
        lock_poll_interval 1s
    }
}
```

JSON 示例：

```json
{
  "storage": {
    "module": "postgres",
    "database_url": "{env.DATABASE_URL}",
    "schema": "caddy",
    "auto_create": true,
    "max_open_conns": 10,
    "lock_ttl": "5m",
    "lock_poll_interval": "1s"
  }
}
```

`Provision` 中展开 `{env.*}` placeholder，设置默认值，打开并 ping 连接池，按配置执行 DDL
或 schema 探测，再构造 `Storage`。不得输出完整 DSN。`Validate` 拒绝非法 schema、非正
duration、负连接数以及 `max_idle_conns > max_open_conns`。

`Cleanup` 先取消 maintenance context 和全部 heartbeat，对本实例仍持有的 token 做有界
best-effort guarded unlock，等待 heartbeat 与 reaper goroutine 退出后关闭连接池。配置重载
期间的新旧实例使用不同 acquisition token，互不误删锁。

## 9. 数据库权限与安全

表中会保存证书私钥，数据库应按密钥存储而非普通缓存对待：

- Caddy 到 PostgreSQL 使用 TLS 并验证服务端证书；
- DSN 通过环境或 secret mount 注入，不写入仓库、不打印日志；
- runtime role 最小权限为 schema `USAGE`，两表 `SELECT/INSERT/UPDATE/DELETE`；
- 若 `auto_create=true`，启动身份还需 `CREATE`/DDL 权限；生产可由独立 migration role 建表
  后关闭 `auto_create`；
- PostgreSQL 数据盘、WAL 和备份均加密并限制访问；
- 日志不记录 `value`、完整 DSN 或 owner token；lock key 是否记录由日志级别控制，因为它
  可能含域名；
- 备份恢复必须同时恢复对象表；lock 表是临时协调状态，跨时间点恢复后应清空或等待其
  `expires_at`，不能把旧 lease 当成真实运行状态。

## 10. 错误模型与可观测性

定义稳定的 sentinel/category：

- `fs.ErrNotExist`：对象或目录不存在；
- `ErrLockNotHeld`：本实例没有成功获取过该 name；
- `ErrLockLost`：数据库行不存在或 owner 已改变；
- 配置错误、schema 错误、数据库错误保留原始 cause，使用 `%w` 包装。

日志只覆盖对运维有价值的边界：连接/初始化失败、heartbeat 连续失败、lease lost、模块
cleanup 失败和异步 reaper 失败。正常 Store/Load、成功 reaper 和每次 lock poll 不逐条
打印，避免高噪声和域名泄露。reaper 失败不得传播到已经成功的 Unlock。

V1 不自建 metrics subsystem。先使用 Caddy 日志和 PostgreSQL 自身连接/查询监控；如果生产
数据证明需要，再增加等待时长、竞争次数、续租失败数等指标。

## 11. 测试架构

### 11.1 接口与对象测试

- `Store -> Load -> overwrite -> Load`，校验字节完全一致和 modified 更新；
- 缺失 `Load/List/Stat` 满足 `errors.Is(err, fs.ErrNotExist)`；
- `Delete` 幂等并能删除整个 prefix，不能误删 `a` 相邻的 `ab`；
- key 包含 `_`、`%` 时 prefix 操作无通配符误匹配；
- `Exists/Stat/List` 覆盖 terminal、implicit directory、root、递归/非递归；
- 用同一数据集对比 PostgreSQL Storage 与 `FileStorage` 的可观察结果；
- context 取消与超时能够终止所有阻塞操作；
- `go test -race` 无数据竞争、无 goroutine 泄漏。

### 11.2 锁并发测试

所有关键测试必须用至少两个独立 `Storage` 实例连接同一数据库，以模拟不同 Caddy 进程：

- 100 goroutine 并发 `TryLock("same-key")`，恰好一个 true；
- 100 goroutine 依次 `Lock` 并进入 critical section，记录到的最大并发数始终为 1；
- 不同 key 可以并发持有；
- A 持有时 B 的 `Lock` 阻塞，取消 B context 后及时返回；
- A lease 过期且心跳被故障注入停止后，B 可接管；
- B 接管后 A 的迟到 `Unlock`/`RenewLockLease` 都失败且不影响 B；
- 同一进程内旧 lease 丢失时，本地 reservation 仍阻止第二个同名任务覆盖 token；
- 显式续租使用给定 duration，默认 heartbeat 不会缩短它；
- Unlock 与 heartbeat 并发时，锁不会在释放后复活；
- `Lock`、失败的 `TryLock` 和内部 poll 均不触发 reaper；成功 Unlock 才有触发机会；
- 首次成功 Unlock 可立即触发，之后 10 分钟内的 Unlock 不重复启动维护；
- 大量并发 Unlock 时，每个模块实例最多一个 reaper SQL/goroutine 在运行；
- reaper 只删除过期 lease，不删除有效、刚续租或刚被新 owner 接管的 lease；
- 人为阻塞或令 reaper SQL 失败时，Unlock 仍保持成功且及时返回；
- 模块 Cleanup 会取消并等待 reaper，不发生关闭连接池后的后台查询；
- Caddy A 进程被强制终止后，Caddy B 在 TTL 后接管；
- 数据库连接断开、恢复、query timeout、模块 cleanup 均不泄漏连接或 goroutine。

测试使用短 TTL 仅为加速，生产默认值另做配置测试。并发断言基于数据库可观察状态和
critical section 计数，不基于 goroutine 调度顺序。

### 11.3 集成与兼容矩阵

- PostgreSQL：CI 覆盖项目声明支持的最老版本和最新稳定版本；初始建议 14+；
- Go/Caddy/CertMagic：以 `go.mod` 固定版本，CI 再覆盖允许的最小与最新兼容组合；
- 使用真实 PostgreSQL 容器执行 integration tests，不用 mock 证明 SQL 原子性；
- Caddyfile adapt、JSON provision、reload、cleanup 都有测试；
- 使用 Pebble 或等价本地 ACME 测试服务跑两 Caddy 节点，验证首次签发与续期只有一个节点
  执行实际订单；生产 CA 仅用于最终小流量验收，避免测试触发 rate limit。

## 12. 迁移与上线

### 12.1 数据迁移

Caddy 自带 experimental `storage export/import`，可在旧、新配置之间通过 tar stream 转移
storage 内容：[Caddy storage 命令](https://caddyserver.com/docs/command-line#caddy-storage)。

建议流程：

1. 备份原 storage 与 PostgreSQL；
2. 用旧配置 export；
3. 用新 PostgreSQL storage 配置 import；
4. 对导入前后 key 数、关键 `.crt/.key/.json` 文件及随机 hash 抽样校验；
5. 先启动单个 Caddy，观察一个完整维护周期；
6. 再加入第二实例，执行首次签发和续期演练；
7. 保留可回滚的旧 storage，只读保存至观察期结束。

锁表不参与迁移。

### 12.2 分阶段上线

- 阶段 A：单 Caddy + PostgreSQL storage，验证 KV、连接稳定性和备份；
- 阶段 B：双 Caddy + staging/local ACME，验证争锁、崩溃接管和续期；
- 阶段 C：双 Caddy + production CA，小批域名灰度；
- 阶段 D：扩大范围，建立 heartbeat failure、DB 可用性、证书到期时间告警。

回滚前应停止所有使用 PostgreSQL storage 的 Caddy，export PostgreSQL 内容并 import 回旧
storage，再启动单实例，避免两个 storage 同时各自签发。

## 13. 实施阶段与完成标准

### Phase 1：骨架与 KV

- Go module、Caddy module 注册、配置解析、连接池与 DDL；
- 六个对象方法及 `fs.ErrNotExist`/目录契约；
- 单元测试、FileStorage parity test、Caddy adapt test。

完成标准：接口 guard 编译通过；对象测试和 race test 全绿。

### Phase 2：锁

- 本地 reservation registry；
- 原子 acquire、阻塞 Lock、TryLock；
- owner-guarded Renew/Unlock；
- heartbeat、TTL、成功释放后限频异步清理、模块 cleanup；
- 多实例并发和故障注入测试。

完成标准：本节 11.2 的测试稳定重复运行，无互斥违规和 goroutine/连接泄漏。

### Phase 3：Caddy 端到端与发布

- xcaddy 构建验证；
- 两 Caddy + PostgreSQL + Pebble 场景；
- README 配置、权限、迁移、运维说明；
- CI 版本矩阵、lint、race、integration job；
- 版本兼容策略和 changelog。

完成标准：首次签发、续期、进程崩溃接管、配置 reload 均通过；安全检查确认日志和配置示例
不含真实凭据。

## 14. 明确不采用的方案

### `pg_advisory_lock`

session advisory lock 绑定连接生命周期。若基于连接池，Lock 和 Unlock 可能落在不同连接；
若长期持有 `sql.Conn`，每个 CertMagic lock 又会长期占一个连接，并引入断线、重连和 cleanup
状态机。相较 lease row，没有收益足以覆盖复杂度。

### `SELECT` 后再 `INSERT/UPDATE`

检查与写入分离会产生 TOCTOU race。所有接管条件必须放在同一条 UPSERT 的冲突更新
predicate 内。

### 只按 key 删除或续租

会让过期 owner 破坏新 owner 的有效 lease，是不可接受的生产级并发错误。

### PostgreSQL `LISTEN/NOTIFY`

V1 的锁争用低、等待粒度为秒，简单的带抖动轮询足够。通知机制会新增专用连接、断线重订阅
和丢通知后的兜底轮询，暂不值得。

### 在 V1 引入通用 migration framework、缓存或 metrics

当前只有固定两表、对象规模小且证书热路径由 CertMagic 内存缓存承担。先用幂等 DDL、直接
查询和现有监控交付可验证的最小系统，再由数据驱动扩展。

## 15. 待实现时验证的细节

以下不是架构悬而未决项，但在编码第一步必须用固定依赖版本确认：

- 所选 Caddy/CertMagic tag 的精确接口签名与 Go 最低版本；
- `List` 对 root、terminal path 的行为与该版本 `FileStorage` 的 parity；
- Caddy `Cleanup` 与 storage export/import 命令中的模块生命周期；
- PostgreSQL driver 对 UUID、`[]byte`、duration 微秒参数及 cancellation 的编码行为；
- 自动 DDL 在两个 Caddy 同时首次启动时的幂等性。

一旦 `go.mod` 建立，文档和 CI 以固定 tag 为准，不跟随 `master` 静默漂移。
