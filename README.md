# rc-notifier

一个面向企业内部业务系统的 HTTP(S) 通知投递服务 MVP。业务系统只负责提交通知；服务在请求持久化后返回，并由异步 Worker 尽可能可靠地投递到预先注册的外部供应商地址。

提交作业时，应按题目要求将 GitHub 仓库命名为 `rc_{your_nickname}`。

## 快速运行

要求：Docker Compose。

```bash
docker compose up --build
```

Compose 会启动 PostgreSQL、API、Worker 和一个演示接收端。演示接收端第一次返回 `503`，用于展示自动重试。

提交通知：

```bash
curl -i -X POST http://localhost:8080/v1/destinations/demo/notifications -H "X-Caller-ID: orders" -H "Idempotency-Key: order-20260813-001" -H "Content-Type: application/json" --data "{\"orderId\":\"20260813-001\",\"paid\":true}"
```

成功持久化后返回 `202 Accepted`：

```json
{
  "id": "4bd95a38-f36d-4bfd-ab12-0418784e465b",
  "destinationId": "demo",
  "destinationVersion": 1,
  "status": "pending",
  "attemptCount": 0,
  "maxAttempts": 5,
  "created": true
}
```

查询状态：

```bash
curl -H "X-Caller-ID: orders" http://localhost:8080/v1/notifications/4bd95a38-f36d-4bfd-ab12-0418784e465b
```

重复提交相同的 `X-Caller-ID`、`Idempotency-Key`、目标、Content-Type 和 Body，会返回已有记录及 `200 OK`。同一个幂等键对应不同内容时返回 `409 Conflict`。

> `docker-compose.yml` 为了访问容器内演示接收端而设置了 `ALLOW_PRIVATE_DESTINATIONS=true`。生产环境应保持默认值 `false`。

## 问题理解

业务系统的核心诉求不是同步获得供应商响应，而是把通知责任可靠地移交给内部服务。系统因此把“接收”和“投递”解耦：

1. API 校验请求并将通知持久化。
2. 数据库事务提交成功后才返回 `202`。
3. Worker 独立领取、投递并记录结果。
4. 临时失败延迟重试；永久失败或重试耗尽后进入 `dead`。

这里的“可靠”不是保证外部系统最终一定收到。若供应商永久下线、持续拒绝请求或配置错误，通知最终会进入 `dead`，等待运营处理。

## 整体架构

```text
                         +-----------------------+
Business System -------> | Notification HTTP API |
                         +-----------+-----------+
                                     |
                              durable transaction
                                     |
                                     v
                         +-----------------------+
                         |      PostgreSQL       |
                         | jobs + leases + audit |
                         +-----------+-----------+
                                     ^
                              claim / complete
                                     |
                         +-----------+-----------+
                         | Delivery Worker Pool  |
                         +-----------+-----------+
                                     |
                              HTTP(S), no redirect
                                     v
                         +-----------------------+
                         | External Supplier API |
                         +-----------------------+
```

API 和 Worker 可以分别横向扩容。Worker 使用 `FOR UPDATE SKIP LOCKED` 领取任务，同一条通知通过租约令牌避免过期 Worker 覆盖新 Worker 的结果。

## 核心流程

### 1. 接收与幂等

接口：

```text
POST /v1/destinations/{destinationId}/notifications
X-Caller-ID: <由内部网关注入的调用方标识>
Idempotency-Key: <业务事件唯一键>
Content-Type: <最终供应商请求的 Content-Type>

<最终序列化 Body>
```

API 不接受调用方传入 URL。它只接受 `destinationId`，再读取已注册且启用的目标配置，避免把服务变成任意网络代理。

幂等约束为：

```text
UNIQUE(caller_id, idempotency_key)
```

数据库同时保存目标、Content-Type 和 Body 的 SHA-256。重复键内容相同则返回原通知；内容不同则返回 `409`，避免静默覆盖。

### 2. 目标快照

目标配置包含：

- URL、HTTP Method
- 非敏感静态 Header
- 一个敏感 Header 的环境变量引用
- 请求超时
- 最大尝试次数

通知创建时会复制目标版本和投递参数。之后即使启用了新版本，已经排队的通知仍按原快照投递，避免配置变更悄悄改变历史请求。

敏感值不存入数据库；数据库只保存如 `CRM_AUTH_HEADER` 的引用，Worker 在投递时从环境变量读取。生产环境可用实现同一接口的 Secret Manager Provider 替换。

### 3. Worker 租约

Worker 在短事务内：

1. 锁定一条到期任务。
2. 将状态改为 `processing`。
3. 增加尝试次数并创建 attempt 审计记录。
4. 写入 `lease_owner`、随机 `lease_token` 和 `lease_until`。
5. 提交事务。

HTTP 调用在数据库事务之外执行。租约时长至少比目标请求超时多 5 秒，降低正常慢请求被重复领取的概率。

若 Worker 在完成前崩溃，租约到期后其他 Worker 会重新领取任务，并把旧 attempt 标为 `lease_expired`。

### 4. 结果分类

| 结果 | 处理 |
|---|---|
| `2xx` | 成功，状态改为 `succeeded` |
| 网络错误、超时 | 重试 |
| `408`、`425`、`429` | 重试 |
| `5xx` | 重试 |
| 其他 `3xx`、`4xx` | 视为永久失败，进入 `dead` |
| 配置、Header、Secret 错误 | 进入 `dead` |

重试使用指数退避和 full jitter，并在不超过配置上限的前提下尊重 `Retry-After`。

## 可靠性语义

本系统提供 **at-least-once（至少一次尝试）**，不承诺 exactly-once。

关键重复窗口：

```text
供应商已处理请求
        |
Worker 在写入 succeeded 前崩溃
        |
租约到期后再次投递
```

通用 HTTP 无法消除这个窗口。服务会把稳定的业务 `Idempotency-Key` 转发给供应商；若供应商支持幂等，可在对方进一步去重。若供应商不支持，业务事件设计必须能够容忍重复。

`202 Accepted` 的准确含义是：

- 通知已经在本服务数据库中持久化；
- 本服务会按照重试策略处理它；
- 不代表供应商已经接收；
- 不代表永久不可用的供应商最终一定能接收。

### 与上游业务事务的一致性

本服务的保证从 API 成功提交开始。它无法自动保证“业务数据库提交”和“通知创建”原子一致。

需要该保证的业务系统应使用自己的 transactional outbox：

1. 在同一业务事务内写业务数据和 outbox。
2. 独立 Publisher 使用稳定幂等键调用本服务。
3. 成功后标记 outbox 已发送。

## 系统边界

### MVP 负责

- 通知持久化后再响应；
- 调用方范围内的提交幂等；
- 版本化目标配置和请求快照；
- 多 Worker 安全领取与崩溃恢复；
- 超时、失败分类、退避重试；
- `dead` 状态和 attempt 审计；
- 状态查询、健康检查和结构化日志；
- 禁止重定向、限制 Body、阻止默认访问私网地址。

### MVP 明确不负责

| 不做的内容 | 原因 |
|---|---|
| exactly-once | 通用 HTTP 在远端成功、本地未记录时存在不可消除的不确定窗口 |
| 供应商 Body 模板或字段映射 | MVP 把 Body 视为不透明字节，避免演变为工作流/ETL 平台 |
| 任意动态 Header | 静态 Header 属于目标配置；动态 Header 会扩大安全和幂等边界 |
| 全局或按业务键排序 | 重试会导致乱序；无明确业务需求时不引入分区和阻塞 |
| 自动事务性接入上游数据库 | 跨业务系统无法由本服务单方面解决，应使用上游 outbox |
| 目标配置 UI、审批流 | 第一版使用受控 SQL/迁移管理，先验证核心投递模型 |
| 自动回放 API | 第一版由受控运维操作回放，避免无鉴权管理接口 |
| 多区域一致性 | MVP 单 PostgreSQL 主库；多区域会显著增加租约和数据一致性复杂度 |
| 长期归档、删除策略 | 需要结合真实合规要求决定，MVP 不臆测保留期 |
| 完整认证系统 | 依赖内部 API Gateway/mTLS；网关必须覆盖注入 `X-Caller-ID` 并限制调用方可访问的 destination |

## 关键工程决策与取舍

### PostgreSQL 同时作为状态库和 MVP 队列

选择原因：

- API 接收只需一次数据库事务，不存在“写数据库成功、发消息失败”的双写；
- 幂等、状态查询、租约和 attempt 审计都需要持久化数据；
- `FOR UPDATE SKIP LOCKED` 足以支持第一版多个 Worker；
- 运维组件少，便于观察真实流量后再决定是否引入消息队列。

不使用 PostgreSQL 队列的替代方案是 RabbitMQ/SQS/Kafka 加状态数据库。该方案在高吞吐、流量削峰、分区消费方面更强，但第一版会增加部署、双写/outbox、消息与状态对账等复杂度。

若完全不使用中间件，只使用内存 Channel，进程崩溃会丢任务，不满足核心可靠性要求；使用本地文件或 SQLite 则限制多实例扩展和高可用。因此 MVP 选择 PostgreSQL 是有意的复杂度折中。

### Go

本题没有既有代码栈。Go 被用于实现小型 API 和并发 Worker，主要考虑：

- 标准库 HTTP 能覆盖大部分需求；
- 静态类型有利于约束任务状态和失败分类；
- 单二进制部署简单；
- Goroutine 适合有限并发的 I/O Worker。

语言不是架构前提；在已有 Java、C#、Node.js 等团队栈中，应优先沿用团队熟悉的技术。

### 不透明 Body

业务系统提交最终序列化 Body，服务不理解业务字段。这样可以支持不同供应商格式，同时保持职责单一。代价是业务系统仍需知道供应商 payload 契约；未来只有在重复映射需求被证实时才增加受版本控制的 Adapter。

### 不保证排序

并发 Worker 和失败重试都可能改变投递顺序。MVP 明确不保证顺序，以避免单个失败任务阻塞同一目标的后续任务。若库存等场景不能容忍乱序，事件应携带版本号，由接收方拒绝旧版本；未来也可按业务聚合键分区并串行消费。

## 安全设计

- 调用方不能提交 URL，只能引用注册目标；
- 生产默认拒绝 loopback、私网、链路本地、保留地址等网络范围；
- DNS 解析后按实际 IP 建连，降低 DNS rebinding 风险；
- 禁止 HTTP Redirect，避免跳转绕过目标校验；
- 默认仅允许 TLS 1.2 及以上的 HTTPS；
- Header 名和值经过校验，`Host`、`Content-Length`、`Content-Type`、`Idempotency-Key` 等由服务管理；
- Secret 值不进入数据库或日志；
- Body 默认上限为 1 MiB；
- 响应 Body 不记录，避免意外采集供应商敏感数据；
- `X-Caller-ID` 不是认证机制，必须由受信任网关在完成认证后覆盖注入。

## 目标配置

新建目标示例：

```sql
INSERT INTO destinations (
    id,
    version,
    active,
    url,
    method,
    headers,
    secret_header_name,
    secret_env_key,
    timeout_ms,
    max_attempts
)
VALUES (
    'crm',
    1,
    true,
    'https://supplier.example.com/contact-events',
    'POST',
    '{"X-Integration-Version":"2026-08"}'::jsonb,
    'Authorization',
    'CRM_AUTH_HEADER',
    5000,
    8
);
```

Worker 环境中配置：

```text
CRM_AUTH_HEADER=Bearer <secret>
```

发布新版本应在一个事务中停用旧版本并创建新版本：

```sql
BEGIN;

UPDATE destinations
SET active = false
WHERE id = 'crm' AND active = true;

INSERT INTO destinations (
    id, version, active, url, method, headers,
    secret_header_name, secret_env_key, timeout_ms, max_attempts
)
VALUES (
    'crm', 2, true, 'https://supplier.example.com/v2/contact-events',
    'POST', '{}'::jsonb, 'Authorization', 'CRM_AUTH_HEADER', 5000, 8
);

COMMIT;
```

## 失败运营

查询死信：

```sql
SELECT
    id,
    caller_id,
    destination_id,
    attempt_count,
    last_status_code,
    last_error_code,
    last_error_message,
    updated_at
FROM notifications
WHERE status = 'dead'
ORDER BY updated_at DESC;
```

修复目标配置或 Secret 后，可受控回放一条通知：

```sql
UPDATE notifications
SET status = 'pending',
    next_attempt_at = now(),
    max_attempts = GREATEST(max_attempts, attempt_count + 5),
    last_error_code = NULL,
    last_error_message = NULL,
    updated_at = now()
WHERE id = '<notification-id>'
  AND status = 'dead';
```

不重置 `attempt_count`，因此 attempt 编号继续递增且审计历史保持完整。生产环境应将 `dead` 日志/指标接入告警平台；本 MVP 未绑定特定监控供应商。

## 演进路线

在真实数据表明当前方案不足时按问题演进：

1. **数据库轮询成为瓶颈**：引入 transactional outbox 和 SQS/RabbitMQ/Kafka；PostgreSQL 继续保存幂等和可查询状态。
2. **单个供应商故障拖累整体**：增加每目标并发上限、令牌桶限流和 circuit breaker。
3. **Body 体积或保留量增长**：Body 存对象存储，数据库只保存校验和与引用，并增加分区/保留策略。
4. **要求顺序**：增加 partition key，同一 key 单线程消费，并定义 head-of-line blocking 策略。
5. **供应商映射重复**：增加版本化 Adapter，但仍保持原始事件与渲染结果可审计。
6. **跨区域需求**：根据 RPO/RTO 选择单写多读、区域队列或按租户分区，避免直接构造全局锁。
7. **运营规模增长**：增加带 RBAC 和审计日志的目标管理、死信查看和回放控制台。

## API

| Method | Path | 说明 |
|---|---|---|
| `POST` | `/v1/destinations/{destinationId}/notifications` | 创建或幂等读取通知 |
| `GET` | `/v1/notifications/{notificationId}` | 查询属于当前调用方的通知 |
| `GET` | `/health/live` | 进程存活 |
| `GET` | `/health/ready` | 数据库可用 |

主要状态：

| 状态 | 含义 |
|---|---|
| `pending` | 等待首次投递或重试 |
| `processing` | 已被 Worker 租约领取 |
| `succeeded` | 收到供应商 `2xx` |
| `dead` | 永久失败、配置失败或尝试耗尽 |

## 配置

### API

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `DATABASE_URL` | 必填 | PostgreSQL DSN |
| `LISTEN_ADDR` | `:8080` | 监听地址 |
| `MAX_BODY_BYTES` | `1048576` | 最大请求 Body |

### Worker

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `DATABASE_URL` | 必填 | PostgreSQL DSN |
| `WORKER_ID` | 主机名加随机后缀 | Worker 标识 |
| `WORKER_CONCURRENCY` | `4` | 同进程并发循环数 |
| `WORKER_POLL_INTERVAL` | `500ms` | 空队列/错误后的轮询间隔 |
| `WORKER_LEASE_DURATION` | `30s` | 基础租约时长 |
| `RETRY_BACKOFF_BASE` | `1s` | 退避基数 |
| `RETRY_BACKOFF_MAX` | `1h` | 单次最大退避 |
| `ALLOW_PRIVATE_DESTINATIONS` | `false` | 是否允许私网目标，仅本地演示建议开启 |

## 测试

要求 Go 1.25+：

```bash
go test ./...
go vet ./...
```

Store 集成测试需要一个专用 PostgreSQL 数据库：

```bash
TEST_DATABASE_URL=postgres://notifier:notifier@localhost:5432/notifier_test?sslmode=disable go test -count=1 -v ./internal/store -args -require-integration-database
```

集成测试会创建独立 schema，并在结束时清理。

完整 Docker Compose 能力测试：

```bash
RUN_COMPOSE_E2E=1 go test -count=1 -v ./e2e -run TestComposeMVP
```

覆盖重点：

- API 接收、参数限制、状态码和调用方隔离；
- Body/Header/Secret 组装、HTTP 失败分类、超时、Redirect 和 SSRF 阻断；
- 指数退避上限和 `Retry-After`；
- Worker 成功、重试、最终死信和优雅停机；
- PostgreSQL 迁移幂等、并发提交去重、目标快照、并发领取、租约恢复、旧 Worker 隔离和 attempt 审计；
- Docker 容器构建、`503 -> retry -> 204`、HTTP 幂等、`409` 和 Worker 停机期间持久排队。

GitHub Actions 会在每次 push 和 pull request 中运行真实 PostgreSQL 测试、race detector、`go vet` 和 Docker Compose 能力测试。

## 目录

```text
cmd/api                 API 进程
cmd/worker              Worker 进程
cmd/demo-receiver       本地演示接收端
internal/api            HTTP 接口
internal/store          PostgreSQL 状态、租约、迁移
internal/delivery       HTTP 投递、安全校验、退避
internal/worker         并发处理循环
deploy/postgres         Compose 演示数据
AI_USAGE.md             AI 使用说明
```
