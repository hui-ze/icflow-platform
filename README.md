# IC-Flow Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![Tests](https://img.shields.io/badge/tests-115%20passing-brightgreen.svg)]()
[![Version](https://img.shields.io/badge/version-0.1.0--alpha-orange.svg)]()

**IC‑Flow Platform** 是一个基于事件驱动架构的智能协同平台，专为**半导体设计流程自动化与知识管理**而设计。它提供了 DRC/LVS 违例自动修复、EDA 工具集成、设计知识管理等核心能力，帮助芯片设计团队提升效率、保护经验知识。

---

## 核心理念

| 理念 | 说明 |
|------|------|
| **Flow‑Driven** | 以事件流（Flow event）为核心，实现任务的自驱动与协同 |
| **Extensible** | 通过 Extension Protocol 提供灵活的第三方扩展能力 |
| **Knowledge‑Centric** | 内置混合存储（向量DB + 图DB + 关系DB）的知识管理系统 |
| **Production‑Ready** | 容器化部署，支持 Kubernetes，具备完整的监控与灾备方案 |

---

## 核心组件

```
┌────────────────────────────────────────────────────────────┐
│                   REST API (FastAPI)                         │
│   POST /workflow/run  │  GET /workflow/{id}  │  /health     │
└──────────────────────────┬─────────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────────┐
│                 Flow Orchestrator                            │
│        工作流编排引擎（多步设计流程自动化）                    │
└──────────────────────────┬─────────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────────┐
│                   Message Bus (发布-订阅)                    │
└──┬──────────┬──────────┬──────────┬────────────────────────┘
   │          │          │          │
┌──▼─────┐ ┌─▼──────┐ ┌─▼──────┐ ┌▼───────────┐
│  DRC   │ │  LVS   │ │  EDA   │ │  知识管理   │
│ 修复   │ │ 修复   │ │ 适配器  │ │  引擎      │
│ 引擎   │ │ 引擎   │ │ 引擎   │ │            │
└────────┘ └────────┘ └────────┘ └────────────┘
   │                                            │
   └──────────────┬─────────────────────────────┘
                  │
┌─────────────────▼──────────────────────────────────────────┐
│                  Extension System                           │
│          Extension + Registry + Protocol (插件协议)          │
└────────────────────────────────────────────────────────────┘
```

### 组件说明

| 组件 | 说明 | 状态 |
|------|------|------|
| **Flow Event System** | 事件驱动模型，承载业务数据的基本单位 | ✅ 稳定 |
| **Flow Engine** | 执行特定任务的处理单元（基类 + 4个引擎实现） | ✅ 稳定 |
| **Flow Message Bus** | 事件发布‑订阅中枢（Memory 实现 + Redis 规划） | ✅ 稳定 |
| **Flow Orchestrator** | 工作流编排引擎（支持自定义多步模板） | ✅ 新增 |
| **Extension Protocol** | 扩展与平台间的通信协议 | ✅ 稳定 |
| **REST API** | FastAPI 服务，提供工作流触发与查询 | ✅ 新增 |

---

## 快速开始

### 安装

```bash
# 克隆仓库
git clone https://github.com/icflow/platform.git
cd platform

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/macOS
# 或
venv\Scripts\activate     # Windows

# 安装依赖
pip install -r requirements.txt
```

### 运行演示

```bash
# 端到端自动化工作流演示
python demo_end_to_end.py
```

### 启动 API 服务

```bash
# 开发模式
uvicorn icflow.api.main:app --host 0.0.0.0 --port 8000 --reload

# 访问 API 文档
# http://localhost:8000/docs
```

### 触发修复工作流

```bash
curl -X POST http://localhost:8000/api/v1/workflow/run \
  -H "Content-Type: application/json" \
  -d '{"template_name": "drc_repair"}'
```

---

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | API 根信息 |
| GET | `/api/v1/health` | 健康检查 |
| POST | `/api/v1/workflow/run` | 触发修复工作流 |
| GET | `/api/v1/workflow/{id}` | 查询工作流状态 |
| GET | `/api/v1/workflows/active` | 列出活跃工作流 |
| GET | `/api/v1/engines` | 引擎状态概览 |

---

## 部署

### Docker

```bash
# 构建并启动
docker-compose up -d --build

# 验证
curl http://localhost:8000/api/v1/health
```

### Kubernetes

```bash
kubectl apply -f deploy/k8s/
kubectl port-forward -n icflow service/icflow-api 8000:8000
```

详见 [部署指南](docs/deployment.md)。

---

## 项目状态

```
✅ 115 个测试全部通过
✅ 4 个核心业务引擎（DRC/LVS/EDA适配器/知识管理）
✅ 流程编排引擎（工作流自动化）
✅ REST API 服务（FastAPI）
✅ MemoryMessageBus 消息总线
✅ Extension 扩展系统
✅ 演示脚本（基础/EDA/端到端）
✅ Docker + K8s 部署方案
📋 文档（架构/API/用户指南/部署）
📋 设计文档（第1-5章架构设计）
```

---

## 文档索引

| 文档 | 路径 | 说明 |
|------|------|------|
| 设计文档 | [design/IC-Flow-Platform-Design.md](design/IC-Flow-Platform-Design.md) | 完整架构设计 |
| 架构文档 | [docs/architecture.md](docs/architecture.md) | 组件架构与事件流 |
| API 文档 | [docs/api.md](docs/api.md) | REST API 详细说明 |
| 用户指南 | [docs/user_guide.md](docs/user_guide.md) | 编程使用与扩展开发 |
| 部署指南 | [docs/deployment.md](docs/deployment.md) | Docker/K8s 部署 |

---

## 测试

```bash
# 运行全部测试
pytest

# 带覆盖率
pytest --cov=src/icflow

# 指定测试文件
pytest tests/test_workflow_orchestrator.py -v
```

当前测试覆盖：**115 个测试用例**（单元测试 + 集成测试 + 接口兼容性测试）

---

## 技术栈

| 领域 | 技术 | 说明 |
|------|------|------|
| 运行时 | Python 3.10+ asyncio | 全异步编程 |
| 数据模型 | Pydantic v2 | 事件模型验证 |
| API | FastAPI + Uvicorn | REST API 服务 |
| 消息总线 | Memory / Redis | 进程内/跨进程事件分发 |
| 知识存储 | PostgreSQL / Qdrant / Neo4j | 混合存储（规划） |
| 容器化 | Docker / Docker Compose | 开发部署 |
| 编排 | Kubernetes + HPA | 生产部署 |
| 监控 | Prometheus + Grafana | 可观测性（规划） |

---

## 路线图

- [x] 核心事件模型与引擎框架
- [x] DRC/LVS 修复引擎
- [x] EDA 工具适配器引擎
- [x] 知识管理引擎
- [x] 流程编排引擎
- [x] REST API 服务
- [x] Docker / K8s 部署
- [ ] Redis 消息总线实现
- [ ] 知识管理持久化存储集成
- [ ] 可观测性（Metrics / Tracing）
- [ ] 前端 Dashboard
- [ ] CI/CD 自动化

---

## 许可证

本项目基于 MIT 许可证发布 - 详见 [LICENSE](./LICENSE) 文件。
