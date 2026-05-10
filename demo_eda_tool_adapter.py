"""
EDA工具适配器引擎演示
展示EDA工具适配器引擎的基本功能
"""

import asyncio
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demo_eda_tool_adapter():
    """演示EDA工具适配器引擎"""
    print("=== EDA工具适配器引擎演示 ===")
    
    from src.icflow.engines.eda_tool_adapter import EDAToolAdapterEngine
    from src.icflow.core.concrete_events import create_tool_started_event
    from src.icflow.message_bus.memory import MemoryMessageBus
    
    # 创建消息总线
    message_bus = MemoryMessageBus()
    await message_bus.start()
    
    # 创建EDA工具适配器引擎
    config = {
        "tool_paths": {
            "calibre": "/tools/calibre/bin/calibre",
            "icv": "/tools/icv/bin/icv",
            "innovus": "/tools/innovus/bin/innovus",
        },
        "default_timeout": 30,  # 30秒超时
    }
    
    engine = EDAToolAdapterEngine(config)
    engine.message_bus = message_bus
    
    # 启动引擎
    await engine.start()
    print(f"引擎启动: {engine.engine_name} (ID: {engine.engine_id})")
    
    # 订阅工具执行结果事件
    tool_results = []
    async def tool_result_handler(event):
        tool_results.append(event)
        event_type = event.event_type
        tool_name = event.payload.get("tool_name", "unknown")
        status = event.payload.get("execution_status", "unknown")
        print(f"  收到工具事件: {tool_name} - {event_type} - 状态: {status}")
        
        if "completed" in event_type:
            print(f"    退出码: {event.payload.get('exit_code')}")
            if "parsed_output" in event.payload:
                output = event.payload["parsed_output"]
                print(f"    输出摘要: {output.get('summary', {})}")
    
    # 订阅所有工具执行事件
    await message_bus.subscribe("tool_execution.*", tool_result_handler)
    
    # 订阅设计流程事件
    design_events = []
    async def design_event_handler(event):
        design_events.append(event)
        phase = event.payload.get("phase", "unknown")
        print(f"  收到设计流程事件: {phase}")
    
    await message_bus.subscribe("design_flow.*", design_event_handler)
    
    # 测试1: 发送Calibre DRC工具执行事件
    print("\n--- 测试1: 发送Calibre DRC工具执行事件 ---")
    calibre_event = create_tool_started_event(
        tool_name="calibre",
        command_line="-drc -hier -turbo input.gds output.rep",
        source="demo-client"
    )
    
    # 添加任务ID用于跟踪
    calibre_event.payload["task_id"] = "test-task-001"
    calibre_event.metadata["correlation_id"] = "demo-correlation-001"
    
    print(f"发送工具开始事件: calibre")
    await message_bus.publish(calibre_event)
    
    # 等待事件处理
    await asyncio.sleep(1)
    
    # 测试2: 发送ICV LVS工具执行事件
    print("\n--- 测试2: 发送ICV LVS工具执行事件 ---")
    icv_event = create_tool_started_event(
        tool_name="icv",
        command_line="-lvs -flat layout.gds schematic.cdl",
        source="demo-client"
    )
    
    icv_event.payload["task_id"] = "test-task-002"
    icv_event.metadata["correlation_id"] = "demo-correlation-002"
    
    print(f"发送工具开始事件: icv")
    await message_bus.publish(icv_event)
    
    # 等待事件处理
    await asyncio.sleep(1)
    
    # 测试3: 发送未知工具事件（应触发错误处理）
    print("\n--- 测试3: 发送未知工具事件 ---")
    unknown_event = create_tool_started_event(
        tool_name="unknown_tool",
        command_line="-arg value",
        source="demo-client"
    )
    
    unknown_event.payload["task_id"] = "test-task-003"
    unknown_event.metadata["correlation_id"] = "demo-correlation-003"
    
    print(f"发送工具开始事件: unknown_tool")
    await message_bus.publish(unknown_event)
    
    # 等待事件处理
    await asyncio.sleep(1)
    
    # 显示统计信息
    print("\n=== 演示统计 ===")
    print(f"收到的工具结果事件: {len(tool_results)} 个")
    print(f"收到的设计流程事件: {len(design_events)} 个")
    
    if engine.active_tasks:
        print(f"活动任务: {len(engine.active_tasks)} 个")
    else:
        print("所有任务已完成")
    
    # 停止引擎
    await engine.stop()
    await message_bus.stop()
    
    print("\n演示完成！")
    
    return {
        "engine": engine,
        "tool_results": tool_results,
        "design_events": design_events,
    }


async def demo_eda_tool_adapter_integration():
    """演示EDA工具适配器引擎与DRC修复引擎的集成"""
    print("\n=== EDA工具适配器引擎集成演示 ===")
    
    from src.icflow.engines.eda_tool_adapter import EDAToolAdapterEngine
    from src.icflow.engines.drc_repair import DRCRepairMasterEngine
    from src.icflow.core.concrete_events import create_drc_violation_event
    from src.icflow.message_bus.memory import MemoryMessageBus
    
    # 创建消息总线
    message_bus = MemoryMessageBus()
    await message_bus.start()
    
    # 创建EDA工具适配器引擎
    eda_config = {
        "tool_paths": {
            "calibre": "/tools/calibre/bin/calibre",
        },
        "default_timeout": 30,
    }
    
    eda_engine = EDAToolAdapterEngine(eda_config)
    eda_engine.message_bus = message_bus
    
    # 创建DRC修复引擎
    drc_config = {
        "default_tool": "calibre",
        "repair_strategies": {
            "spacing": "widen_spacing",
            "width": "increase_width",
        }
    }
    
    drc_engine = DRCRepairMasterEngine(drc_config)
    drc_engine.message_bus = message_bus
    
    # 启动两个引擎
    await eda_engine.start()
    await drc_engine.start()
    
    print(f"启动引擎: {eda_engine.engine_name}")
    print(f"启动引擎: {drc_engine.engine_name}")
    
    # 订阅所有工具执行事件
    tool_events = []
    async def tool_event_handler(event):
        tool_events.append(event)
        tool_name = event.payload.get("tool_name", "unknown")
        event_type = event.event_type
        print(f"  工具事件: {tool_name} - {event_type}")
    
    await message_bus.subscribe("tool_execution.*", tool_event_handler)
    
    # 订阅知识捕获事件
    knowledge_events = []
    async def knowledge_handler(event):
        knowledge_events.append(event)
        print(f"  知识事件: {event.event_type}")
    
    await message_bus.subscribe("knowledge_capture.*", knowledge_handler)
    
    # 发送DRC违例事件
    print("\n--- 发送DRC违例事件 ---")
    drc_violation_event = create_drc_violation_event(
        violation_type="min_spacing",
        violation_value=0.05,
        rule_value=0.1,
        location={"x": 100, "y": 200, "layer": "M1"},
        design_file="/path/to/design.gds",
        source="drc_checker"
    )
    
    drc_violation_event.payload["task_id"] = "integration-task-001"
    
    print(f"发送DRC违例事件: {drc_violation_event.payload['violation_type']}")
    await message_bus.publish(drc_violation_event)
    
    # 等待事件处理
    await asyncio.sleep(2)
    
    # 显示统计信息
    print("\n=== 集成演示统计 ===")
    print(f"收到的工具事件: {len(tool_events)} 个")
    print(f"收到的知识事件: {len(knowledge_events)} 个")
    
    # 停止引擎
    await drc_engine.stop()
    await eda_engine.stop()
    await message_bus.stop()
    
    print("集成演示完成！")
    
    return {
        "tool_events": tool_events,
        "knowledge_events": knowledge_events,
    }


async def main():
    """主演示函数"""
    print("EDA工具适配器引擎演示")
    print("=" * 50)
    
    try:
        # 演示1: 基本功能
        result1 = await demo_eda_tool_adapter()
        
        # 演示2: 集成演示
        result2 = await demo_eda_tool_adapter_integration()
        
        print("\n" + "=" * 50)
        print("所有演示完成！")
        
    except Exception as e:
        print(f"演示出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())