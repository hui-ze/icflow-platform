# IC-Flow Platform 用户指南

## 快速开始

### 1. 环境准备

```bash
# Python 3.10+
python --version

# 克隆项目
git clone <repository-url>
cd chipflow

# 安装依赖
pip install -r requirements.txt
```

### 2. 运行演示

```bash
# 基础功能演示
python demo_basic_usage.py

# EDA工具适配器演示
python demo_eda_tool_adapter.py

# 端到端自动化工作流演示
python demo_end_to_end.py
```

### 3. 启动 API 服务

```bash
# 开发模式（热重载）
uvicorn icflow.api.main:app --host 0.0.0.0 --port 8000 --reload

# 生产模式
uvicorn icflow.api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

启动后访问 `http://localhost:8000/docs` 查看 Swagger 文档。

### 4. 触发工作流

```bash
curl -X POST http://localhost:8000/api/v1/workflow/run \
  -H "Content-Type: application/json" \
  -d '{
    "template_name": "drc_repair",
    "violation_data": {
      "violation_type": "min_width",
      "violation_id": "vio_001",
      "location": {"layer": "M1", "x": 100, "y": 200}
    }
  }'
```

---

## 编程使用

### 创建引擎并处理事件

```python
import asyncio
from src.icflow.engines.drc_repair import DRCRepairMasterEngine
from src.icflow.message_bus.memory import MemoryMessageBus
from src.icflow.core.concrete_events import create_drc_violation_event

async def main():
    # 1. 创建消息总线
    bus = MemoryMessageBus()
    await bus.start()
    
    # 2. 创建并注册引擎
    drc = DRCRepairMasterEngine({"default_tool": "calibre"})
    drc.message_bus = bus
    await drc.start()
    
    # 3. 发布事件
    event = create_drc_violation_event(
        task_id="task_001",
        violation_id="vio_001",
        violation_type="min_width",
        location={"layer": "M1", "x": 100, "y": 200, "width": 0.08, "height": 0.5},
        rule_description="Minimum width violation",
    )
    await bus.publish(event)
    
    # 4. 等待处理
    await asyncio.sleep(1)
    
    # 5. 查看结果
    print(f"修复历史: {len(drc.repair_history)}")
    
    # 6. 清理
    await drc.stop()
    await bus.stop()

asyncio.run(main())
```

### 使用工作流编排

```python
from src.icflow.engines.workflow_orchestrator import FlowOrchestrator

async def run_workflow():
    orchestrator = FlowOrchestrator()
    
    # 启动工作流
    wid = await orchestrator.start_workflow(
        template_name="drc_repair",
        initial_data={
            "task_id": "task_001",
            "violation_type": "min_width",
        },
    )
    print(f"工作流已启动: {wid}")
    
    # 查询状态
    status = orchestrator.get_workflow_status(wid)
    print(f"状态: {status['status']}")
```

### 自定义工作流模板

```python
from src.icflow.engines.workflow_orchestrator import (
    FlowOrchestrator, WorkflowStep
)

async def custom_step(ctx):
    print(f"执行自定义步骤...")
    ctx.record_step_result("custom", {"result": "ok"})
    return True

orchestrator = FlowOrchestrator()
orchestrator.register_workflow_template("custom_flow", [
    WorkflowStep(step_id="custom", name="自定义步骤", handler=custom_step),
])
```

---

## 扩展开发

### 创建自定义引擎

```python
from src.icflow.core.flow_engine import FlowEngine, FlowEvent

class MyCustomEngine(FlowEngine):
    engine_id = "my_custom_engine"
    subscribed_event_types = ["my.event.type"]
    
    async def process(self, event: FlowEvent):
        print(f"处理事件: {event.event_type}")
        # 业务逻辑
        return None
```

### 创建扩展

```python
from src.icflow.extensions.base import Extension, ExtensionCapability

class MyExtension(Extension):
    extension_id = "my_extension"
    capabilities = [ExtensionCapability.EVENT_PROCESSING]
    
    async def process_request(self, request):
        return {"result": "processed", "data": request}
```

---

## 常见问题

### Q: 如何配置工具路径？
A: 在创建 EDA 引擎时传入配置：
```python
engine = EDAToolAdapterEngine({
    "tool_paths": {
        "calibre": "/tools/calibre/bin/calibre",
        "icv": "/tools/icv/bin/icv",
    }
})
```

### Q: 工作流超时怎么办？
A: 可通过引擎配置调整：
```python
orchestrator = FlowOrchestrator({
    "default_timeout": 1200,  # 默认超时（秒）
    "max_concurrent_workflows": 100,
})
```

### Q: 如何查看事件处理统计？
A: 调用引擎的 `get_stats()` 方法：
```python
stats = engine.get_stats()
print(f"已处理事件: {stats['events_processed']}")
```

### Q: 如何添加新的违例类型？
A: 扩展修复策略映射：
```python
drc_engine.repair_strategies["new_violation_type"] = "custom_strategy"
```
