"""
IC-Flow Platform 端到端自动化工作流演示

展示完整的芯片设计流程自动化：
1. 启动消息总线 + 所有引擎
2. 流程编排引擎监听并协调工作流
3. DRC违例事件触发自动修复流程
4. EDA工具适配器执行模拟工具
5. 知识管理引擎捕获知识
6. 工作流状态查询

运行方式：
    python demo_end_to_end.py

依赖：需要先安装依赖 pip install -r requirements.txt
"""

import asyncio
import logging
import sys
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo")

# 抑制引擎内部调试日志
logging.getLogger("src.icflow").setLevel(logging.WARNING)


async def demo_end_to_end_workflow():
    """
    端到端自动化工作流演示
    
    流程步骤：
    1. 初始化基础设施（消息总线）
    2. 启动所有引擎（DRC修复 / EDA适配器 / 知识管理 / 流程编排）
    3. 流程编排引擎注册默认工作流模板
    4. 发布DRC违例事件 → 流程编排引擎自动启动工作流
    5. 工作流执行：违例分析 → 修复策略 → EDA工具执行 → 结果验证 → 知识入库
    6. 查询工作流状态
    7. 查询知识库
    8. 清理关闭
    """
    print("=" * 70)
    print("  IC-Flow Platform - 端到端自动化工作流演示")
    print("=" * 70)
    print()
    
    # ========== 1. 初始化基础设施 ==========
    print("[1/8] 初始化消息总线...")
    from src.icflow.message_bus.memory import MemoryMessageBus
    bus = MemoryMessageBus()
    await bus.start()
    print("  ✅ 消息总线已启动")
    
    # ========== 2. 启动所有引擎 ==========
    print("[2/8] 启动引擎...")
    
    from src.icflow.engines.drc_repair import DRCRepairMasterEngine
    from src.icflow.engines.eda_tool_adapter import EDAToolAdapterEngine
    from src.icflow.engines.knowledge_management import KnowledgeManagementEngine
    from src.icflow.engines.workflow_orchestrator import FlowOrchestrator
    
    from src.icflow.core.concrete_events import (
        DesignFlowEventTypes,
        ToolExecutionEventTypes,
        KnowledgeCaptureEventTypes,
        create_drc_violation_event,
    )
    
    # DRC修复引擎
    drc_engine = DRCRepairMasterEngine({"default_tool": "calibre"})
    drc_engine.message_bus = bus
    for et in drc_engine.subscribed_event_types:
        await bus.subscribe(et, lambda e: asyncio.create_task(drc_engine.process(e)))
    await drc_engine.start()
    print("  ✅ DRC修复引擎已启动")
    
    # EDA工具适配器引擎
    eda_engine = EDAToolAdapterEngine({
        "tool_paths": {"calibre": "/tools/calibre/bin/calibre"},
        "default_timeout": 30,
    })
    eda_engine.message_bus = bus
    for et in eda_engine.subscribed_event_types:
        await bus.subscribe(et, lambda e: asyncio.create_task(eda_engine.process(e)))
    await eda_engine.start()
    print("  ✅ EDA工具适配器引擎已启动")
    
    # 知识管理引擎
    km_engine = KnowledgeManagementEngine()
    km_engine.message_bus = bus
    for et in km_engine.subscribed_event_types:
        await bus.subscribe(et, lambda e: asyncio.create_task(km_engine.process(e)))
    await km_engine.start()
    print("  ✅ 知识管理引擎已启动")
    
    # 流程编排引擎
    orchestrator = FlowOrchestrator({
        "default_timeout": 600,
        "max_concurrent_workflows": 10,
    })
    orchestrator.message_bus = bus
    for et in orchestrator.subscribed_event_types:
        await bus.subscribe(et, lambda e: asyncio.create_task(orchestrator.process(e)))
    await orchestrator.start()
    print("  ✅ 流程编排引擎已启动")
    
    # ========== 3. 查看工作流模板 ==========
    print("[3/8] 查看工作流模板...")
    templates = orchestrator.list_workflow_templates()
    print(f"  已注册模板: {', '.join(templates)}")
    
    template = orchestrator.get_workflow_template("drc_repair")
    if template:
        print(f"  drc_repair 模板步骤 ({len(template)}步):")
        for i, step in enumerate(template, 1):
            deps = f" (依赖: {', '.join(step.depends_on)})" if step.depends_on else ""
            print(f"    {i}. [{step.step_id}] {step.name}{deps}")
    
    # ========== 4. 发布DRC违例事件 - 触发工作流 ==========
    print()
    print("[4/8] 发布DRC违例事件，触发自动修复工作流...")
    
    drc_event = create_drc_violation_event(
        task_id="demo_task_001",
        violation_id="vio_demo_001",
        violation_type="min_width",
        location={"layer": "M1", "x": 100, "y": 200, "width": 0.08, "height": 0.5},
        rule_description="Minimum width violation: 0.08um < 0.1um (foundry rule W1)",
        source="demo_script",
    )
    print(f"  违例信息: {drc_event.payload['violation_type']} @ "
          f"layer={drc_event.payload['location']['layer']}")
    
    # 通过消息总线发布事件
    await bus.publish(drc_event)
    print("  ✅ DRC违例事件已发布到消息总线")
    
    # ========== 5. 等待工作流执行 ==========
    print()
    print("[5/8] 等待工作流自动执行...")
    print("  工作流步骤:" + " → ".join(
        ["违例分析", "修复策略", "EDA工具执行", "结果验证", "知识入库"]
    ))
    
    # 轮询等待工作流出现在活跃列表中
    workflow_id = None
    for i in range(20):
        active = orchestrator.list_active_workflows()
        if active:
            workflow_id = active[0]["workflow_id"]
            print(f"  ⏳ 工作流已启动: {workflow_id}")
            break
        await asyncio.sleep(0.3)
    else:
        print("  ⚠️ 工作流未在预期时间内启动")
    
    # 等待工作流完成（总耗时约0.8s，加上编排本身延迟）
    if workflow_id:
        for i in range(30):
            status = orchestrator.get_workflow_status(workflow_id)
            if status and status.get("status") in ("completed", "failed", "timeout"):
                print(f"  ✅ 工作流完成! 最终状态: {status['status']}")
                
                # 打印每个步骤的状态
                print("  步骤执行详情:")
                for step in status.get("steps", []):
                    icon = "✅" if step["status"] == "completed" else "❌" if step["status"] in ("failed", "timeout") else "⏳"
                    err = f" - {step['error']}" if step.get("error") else ""
                    print(f"    {icon} [{step['step_id']}] {step['name']}: {step['status']}{err}")
                break
            await asyncio.sleep(0.3)
        else:
            print("  ⚠️ 工作流未在预期时间内完成")
    
    # ========== 6. 查询工作流历史 ==========
    print()
    print("[6/8] 查询工作流历史...")
    stats = orchestrator.get_stats()
    print(f"  活跃工作流: {stats['active_workflows']}")
    print(f"  已完成工作流: {stats['completed_workflows']}")
    if orchestrator.workflow_history:
        h = orchestrator.workflow_history[-1]
        print(f"  最近工作流: {h['workflow_id']} ({h['status']})")
        print(f"  模板: {h['template_name']}")
    
    # ========== 7. 查询知识库 ==========
    print()
    print("[7/8] 查询知识库...")
    print(f"  知识库条目数: {km_engine.knowledge_count}")
    if km_engine.knowledge_count > 0:
        # 列出最近存储的知识
        for kid, kdata in list(km_engine.knowledge_store.items())[-3:]:
            ktype = kdata.get("knowledge_type", "unknown")
            source = kdata.get("source", "unknown")
            print(f"  📚 [{kid}] type={ktype}, source={source}")
    
    # ========== 8. 清理关闭 ==========
    print()
    print("[8/8] 清理关闭...")
    
    await orchestrator.stop()
    await drc_engine.stop()
    await eda_engine.stop()
    await km_engine.stop()
    await bus.stop()
    
    print("  ✅ 所有引擎已关闭")
    print()
    print("=" * 70)
    print("  端到端流程演示完成! ✅")
    print("  完整链路: DRC违例 → 流程编排 → EDA工具 → 知识入库")
    print("=" * 70)


def main():
    """入口函数"""
    start = time.time()
    asyncio.run(demo_end_to_end_workflow())
    elapsed = time.time() - start
    print(f"\n总耗时: {elapsed:.2f}秒")


if __name__ == "__main__":
    main()
