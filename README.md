# 企业登记变更多部门协同分发服务

企业登记变更受理后，按涉及范围批量分发给税务、社保、公积金、行业主管等承办部门。
各部门订阅接收职责内变更，留下已看记录；超时未签收的变更自动重投并升级；
连续变更按真实先后裁决，部分失败触发补偿回滚。

## 系统结构

```
cmd/
  gateway/          接入网关（主服务，监听 48443）
  upstream-mock/    可独立运行的上游部门模拟服务
internal/
  domain/           领域模型（change、enterprise、department、event）
  orchestrator/     业务编排（dispatch、compensation、resolver）
  store/            持久化（SQLite + 追加写事件日志 + 迁移）
  transport/        对外接入（HTTP 路由、SSE 订阅流、中间件）
  config/           配置解析与校验
  scheduler/        后台周期任务（超时重投、压实、补偿重试）
  upstream/         上游客户端（熔断器、多上游选择、留痕）
  clock/            可注入时钟
  errorsx/          错误链与分类
migrations/         建表 SQL
```

## 数据模型

| 表名 | 说明 | 关联 |
|------|------|------|
| enterprises | 企业登记主表 | — |
| changes | 登记变更记录 | enterprise_id → enterprises.id |
| dispatch_tasks | 分发任务 | change_id → changes.id |
| event_log | 追加写事件日志 | change_id → changes.id |
| subscriber_offsets | 消费位点 | (subscriber_id, topic) |
| audit_records | 审计记录 | entity_id 关联业务实体 |
| dead_letters | 死信 | change_id → changes.id |
| compensation_records | 补偿记录 | change_id → changes.id |
| upstream_traces | 上游请求留痕 | — |

所有变更先追加写入 event_log，状态由重放日志得到。支持截断（Truncate）与压实（Compact）。

## 启动方法

```bash
# 构建全部
go build ./...

# 启动上游模拟服务（端口 48444）
go run ./cmd/upstream-mock

# 启动接入网关（端口 48443）
go run ./cmd/gateway

# 指定配置文件
go run ./cmd/gateway -config /path/to/config.yaml
```

服务端口 48443 运行示例：

```bash
# 注册企业
curl -X POST http://localhost:48443/api/v1/enterprises \
  -H "Content-Type: application/json" \
  -d '{"name":"示例科技","legal_representative":"张三","unified_credit_code":"91110000MA0ABCDE1","registered_capital":"500万元","business_scope":"软件开发","industry_code":"I6510"}'

# 提交变更
curl -X POST http://localhost:48443/api/v1/changes \
  -H "Content-Type: application/json" \
  -d '{"enterprise_id":"ent-xxx","change_type":"legal_representative","new_value":"李四","submitted_by":"clerk1"}'

# 分发变更
curl -X POST http://localhost:48443/api/v1/changes/chg-xxx/dispatch \
  -H "Content-Type: application/json" \
  -d '{"operator":"op1"}'

# 部门签收
curl -X POST http://localhost:48443/api/v1/dispatch/disp-xxx/ack \
  -H "Content-Type: application/json" \
  -d '{"acked_by":"tax_clerk"}'

# 部门订阅事件流
curl http://localhost:48443/api/v1/departments/tax/subscribe?subscriber_id=sub-1

# 导出对账单
curl http://localhost:48443/api/v1/export/reconciliation?department_code=tax

# 查看积压
curl http://localhost:48443/api/v1/backlog
```

## 配置项

通过 `config.yaml` 或环境变量（`REGDISPATCH_` 前缀）覆盖：

| 配置键 | 环境变量 | 默认值 | 说明 |
|--------|----------|--------|------|
| server.port | REGDISPATCH_PORT | 48443 | HTTP 端口 |
| storage.data_dir | REGDISPATCH_DATA_DIR | ./data | 数据目录 |
| storage.db_path | REGDISPATCH_DB_PATH | ./data/regdispatch.db | 数据库路径 |
| dispatch.max_retries | REGDISPATCH_MAX_RETRIES | 5 | 最大重投次数 |
| dispatch.ack_timeout | REGDISPATCH_DISPATCH_TIMEOUT | 60s | 签收超时 |
| dispatch.retry_base_delay | REGDISPATCH_RETRY_BASE_DELAY | 2s | 重投基础退避 |
| upstream.mock_url | — | http://127.0.0.1:48444 | 上游模拟服务地址 |

## 迁移方式

建表 SQL 位于 `migrations/001_init.sql`，同时通过 `//go:embed schema.sql` 内嵌到二进制。
启动时自动执行迁移并记录版本到 `schema_version` 表，重启后幂等跳过。

## 主要 API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /api/v1/enterprises | 注册企业 |
| GET | /api/v1/enterprises | 分页查询企业 |
| POST | /api/v1/changes | 提交变更 |
| GET | /api/v1/changes | 分页筛选变更 |
| POST | /api/v1/changes/{id}/dispatch | 批量分发变更 |
| POST | /api/v1/changes/{id}/revoke | 撤销变更 |
| POST | /api/v1/changes/{id}/compensate | 触发补偿回滚 |
| POST | /api/v1/changes/{id}/resolve | 按时间裁决排序 |
| GET | /api/v1/departments/{code}/dispatches | 部门待办列表 |
| GET | /api/v1/departments/{code}/subscribe | SSE 订阅流 |
| POST | /api/v1/dispatch/{id}/ack | 签收 |
| POST | /api/v1/dispatch/{id}/complete | 办结 |
| POST | /api/v1/dispatch/{id}/fail | 退回 |
| GET | /api/v1/audit | 审计记录查询 |
| GET | /api/v1/dead-letters | 死信列表 |
| POST | /api/v1/dead-letters/{id}/redeliver | 人工重投 |
| GET | /api/v1/export/reconciliation | 导出对账单 |
| GET | /api/v1/backlog | 部门积压统计 |
| GET | /api/v1/admin/upstreams | 上游状态 |
| GET | /healthz | 存活检查 |
| GET | /readyz | 就绪检查 |

## 测试命令

```bash
# 单元测试
go test -timeout=300s -count=1 ./...

# 竞态检测
go test -race -timeout=420s -count=1 ./...
```

## 核心业务能力

- **追加写事件日志**：所有变更先写入 event_log，状态由重放得到，支持截断与压实
- **多部门批量分发**：按变更类型确定涉及部门，批量创建分发任务并转发上游
- **自研消费组模型**：按主题订阅、消费位点、签收确认、超时重投、死信
- **乱序裁决**：按事件时间（非到达时间）排序，迟到变更回填到正确位置
- **部分失败补偿**：已办成不重复执行，未办成退回变更前状态，补偿幂等可重试
- **状态机**：显式跃迁表 + 非法跃迁拒绝
- **熔断与多上游**：circuit breaker 半开恢复、round-robin 选择、请求留痕
- **后台任务**：Ticker 驱动、指数退避重试、最大重试后死信、可优雅停止
- **审计追溯**：按时间与责任人可查询
- **重启恢复**：重启后未完成接着办、已完成不重复
