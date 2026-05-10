# IC-Flow Platform 部署指南

## 1. Docker 部署

### 构建镜像

```bash
# 在项目根目录
docker build -t icflow-platform:latest .
```

### 使用 Docker Compose

```bash
# 启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down

# 重新构建并启动
docker-compose up -d --build
```

**服务列表**:

| 服务 | 端口 | 说明 |
|------|------|------|
| icflow-api | 8000 | REST API 服务 |
| redis | 6379 | 消息总线后端 |
| postgres | 5432 | 知识库存储 |

### 验证部署

```bash
# 健康检查
curl http://localhost:8000/api/v1/health

# 触发工作流
curl -X POST http://localhost:8000/api/v1/workflow/run \
  -H "Content-Type: application/json" \
  -d '{"template_name": "drc_repair"}'
```

---

## 2. Kubernetes 部署

### 前置条件

- Kubernetes 1.24+
- kubectl 已配置
- 可选：Ingress Controller (如 nginx-ingress)

### 部署步骤

```bash
# 1. 创建命名空间和配置
kubectl apply -f deploy/k8s/configmap.yaml

# 2. 部署 Redis
kubectl apply -f deploy/k8s/service.yaml

# 3. 部署 API 服务
kubectl apply -f deploy/k8s/deployment.yaml

# 4. 验证部署
kubectl get pods -n icflow
kubectl get services -n icflow

# 5. 测试
kubectl port-forward -n icflow service/icflow-api 8000:8000
curl http://localhost:8000/api/v1/health
```

### K8s 资源清单

| 文件 | 说明 |
|------|------|
| `deploy/k8s/configmap.yaml` | 配置映射 + 命名空间 + ServiceAccount |
| `deploy/k8s/deployment.yaml` | API 部署 + HPA 自动扩缩 |
| `deploy/k8s/service.yaml` | API/Redis/PostgreSQL 服务暴露 |

### 水平扩缩

HPA 配置在 `deployment.yaml` 中，默认：
- 最小副本: 2
- 最大副本: 10
- CPU 阈值: 70%
- 内存阈值: 80%

---

## 3. 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `ICFLOW_CONFIG` | `/app/config/config.yaml` | 配置文件路径 |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `REDIS_HOST` | `localhost` | Redis 主机 |
| `REDIS_PORT` | `6379` | Redis 端口 |
| `POSTGRES_HOST` | `localhost` | PostgreSQL 主机 |
| `POSTGRES_PORT` | `5432` | PostgreSQL 端口 |
| `POSTGRES_DB` | `icflow` | 数据库名 |
| `POSTGRES_USER` | `icflow` | 数据库用户 |
| `API_WORKERS` | `4` | API worker 数 |
| `DEFAULT_WORKFLOW_TIMEOUT` | `600` | 工作流默认超时(秒) |
| `MAX_CONCURRENT_WORKFLOWS` | `50` | 最大并发工作流数 |

---

## 4. 监控与日志

### 内置健康检查

API 服务提供 `/api/v1/health` 端点，返回：
- 服务状态
- 运行时间
- 引擎在线数量
- 版本信息

### 日志

默认输出到 stdout/stderr，日志格式：
```
2026-05-02 10:00:00 [INFO] icflow.orchestrator: 工作流完成: wf_xxx
```

### Prometheus 集成（规划中）

在 `config.yaml` 中配置：
```yaml
monitoring:
  prometheus:
    enabled: true
    metrics_path: /metrics
```

---

## 5. 故障排查

### API 服务无法启动
```bash
# 检查日志
docker-compose logs icflow-api

# 检查配置
docker-compose exec icflow-api cat /app/config/config.yaml
```

### Redis 连接失败
```bash
# 检查 Redis 健康状态
docker-compose exec redis redis-cli ping
# 应返回 PONG
```

### 工作流执行失败
```bash
# 查询工作流状态获取失败详情
curl http://localhost:8000/api/v1/workflow/{workflow_id}

# 检查引擎日志
docker-compose logs icflow-api | grep "ERROR"
```

---

## 6. 生产环境建议

### 安全
- 启用 mTLS 认证（Extension Protocol）
- 设置数据库密码为强密码
- 配置网络策略限制服务间访问

### 高可用
- API 服务至少 2 副本
- Redis 使用主从模式
- PostgreSQL 配置流复制

### 性能
- 根据负载调整 `API_WORKERS` 和 `MAX_CONCURRENT_WORKFLOWS`
- 使用 Redis 消息总线实现跨进程事件分发
- 配置 HPA 自动扩缩
