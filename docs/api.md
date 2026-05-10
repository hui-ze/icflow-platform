# IC-Flow Platform API 文档

## 基础信息

- **Base URL**: `http://localhost:8000`
- **文档服务**: Swagger UI: `/docs` | ReDoc: `/redoc`
- **版本**: v0.1.0
- **数据格式**: JSON

## 端点总览

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | API根信息 |
| GET | `/api/v1/health` | 健康检查 |
| POST | `/api/v1/workflow/run` | 触发修复工作流 |
| GET | `/api/v1/workflow/{workflow_id}` | 查询工作流状态 |
| GET | `/api/v1/workflows/active` | 列出活跃工作流 |
| GET | `/api/v1/engines` | 引擎状态概览 |

---

## 1. 健康检查

```
GET /api/v1/health
```

**响应 200**:
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "uptime": 123.45,
  "engines_online": 1,
  "engines": [
    {
      "engine_id": "flow_orchestrator",
      "engine_name": "流程编排引擎",
      "running": true,
      "stats": { ... }
    }
  ]
}
```

---

## 2. 触发工作流

```
POST /api/v1/workflow/run
```

**请求体**:
```json
{
  "template_name": "drc_repair",
  "task_id": "task_001",
  "violation_data": {
    "violation_type": "min_width",
    "violation_id": "vio_001",
    "location": {
      "layer": "M1",
      "x": 100,
      "y": 200
    },
    "rule_description": "Minimum width violation: 0.08um < 0.1um"
  }
}
```

**响应 200**:
```json
{
  "workflow_id": "wf_a1b2c3d4e5f6",
  "status": "started",
  "message": "工作流已启动"
}
```

**响应 400** (无效模板):
```json
{
  "detail": "工作流模板不存在: invalid_template"
}
```

**响应 503** (系统繁忙):
```json
{
  "detail": "并发工作流已达上限 (50)"
}
```

---

## 3. 查询工作流状态

```
GET /api/v1/workflow/{workflow_id}
```

**响应 200**:
```json
{
  "workflow_id": "wf_a1b2c3d4e5f6",
  "template_name": "drc_repair",
  "status": "completed",
  "steps": [
    {"step_id": "violation_analysis", "name": "违例分析", "status": "completed"},
    {"step_id": "repair_strategy", "name": "修复策略选择", "status": "completed"},
    {"step_id": "tool_execution", "name": "EDA工具执行", "status": "completed"},
    {"step_id": "result_verification", "name": "结果验证", "status": "completed", "error": null},
    {"step_id": "knowledge_capture", "name": "知识入库", "status": "completed"}
  ],
  "started_at": "2026-05-02T10:00:00",
  "completed_at": "2026-05-02T10:00:05"
}
```

**响应 404**:
```json
{
  "detail": "工作流不存在: wf_unknown"
}
```

**status 取值说明**:
- `pending` — 等待执行
- `running` — 执行中
- `completed` — 全部步骤成功
- `failed` — 有步骤失败
- `timeout` — 步骤超时
- `cancelled` — 已取消

---

## 4. 列出活跃工作流

```
GET /api/v1/workflows/active
```

**响应 200**:
```json
[
  {
    "workflow_id": "wf_a1b2c3d4e5f6",
    "template_name": "drc_repair",
    "status": "running",
    "steps": [...],
    "started_at": "2026-05-02T10:00:00"
  }
]
```

---

## 5. 引擎状态

```
GET /api/v1/engines
```

**响应 200**:
```json
[
  {
    "engine_id": "flow_orchestrator",
    "engine_name": "流程编排引擎",
    "running": true,
    "stats": {
      "active_workflows": 3,
      "completed_workflows": 10,
      "workflow_templates": ["drc_repair"],
      "events_processed": 25
    }
  }
]
```
