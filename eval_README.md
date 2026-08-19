# 评测说明

## 项目用途

企业登记变更多部门协同分发服务：受理员登记变更后按涉及范围批量分发给税务、社保、公积金、行业主管等部门；各部门订阅接收、签收处理；超时重投升级；连续变更按真实先后裁决；部分失败补偿回滚。

## 数据目录

默认 `./data`，可通过环境变量 `REGDISPATCH_DATA_DIR` 或配置文件 `storage.data_dir` 覆盖。
数据库文件默认 `./data/regdispatch.db`，可通过 `REGDISPATCH_DB_PATH` 覆盖。

## 标准命令

```bash
# 构建
go build ./...

# 运行网关（端口 48443）
go run ./cmd/gateway

# 运行上游模拟服务（端口 48444）
go run ./cmd/upstream-mock

# 测试
go test -timeout=300s -count=1 ./...
go test -race -timeout=420s -count=1 ./...

# 代码检查
go vet ./...
go fmt ./...
```

## 服务端口 48443 运行示例

```bash
# 启动服务
go run ./cmd/gateway -config config.yaml

# 健康检查
curl http://localhost:48443/healthz
curl http://localhost:48443/readyz

# 注册企业
curl -X POST http://localhost:48443/api/v1/enterprises \
  -H "Content-Type: application/json" \
  -d '{"name":"示例公司","legal_representative":"张三","unified_credit_code":"91110000MA0ABCDE1","registered_capital":"500万","business_scope":"软件","industry_code":"I6510"}'

# 提交并分发变更
curl -X POST http://localhost:48443/api/v1/changes \
  -H "Content-Type: application/json" \
  -d '{"enterprise_id":"ent-xxx","change_type":"legal_representative","new_value":"李四","submitted_by":"clerk1"}'

curl -X POST http://localhost:48443/api/v1/changes/chg-xxx/dispatch \
  -H "Content-Type: application/json" -d '{"operator":"op1"}'
```

## Docker 构建

```bash
# 构建 linux/amd64 镜像
./build_eval_docker.sh regdispatch linux/amd64

# 构建 linux/arm64 镜像
./build_eval_docker.sh regdispatch linux/arm64
```

镜像基于 `golang:1.26`，单阶段构建，保留完整 Go 工具链，离线可编译。
