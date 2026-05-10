"""
IC-Flow Platform 基本使用演示
"""

import asyncio
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demo_flow_event():
    """演示Flow Event基本用法"""
    print("=== Flow Event 演示 ===")
    
    from src.icflow.core.flow_event import FlowEvent, FlowEventBuilder, create_event
    
    # 方法1：直接创建
    event1 = FlowEvent(
        event_type="test.event",
        source="demo",
        payload={"message": "Hello, IC-Flow!"}
    )
    print(f"事件1: {event1.event_type} | ID: {event1.event_id}")
    print(f"  负载: {event1.payload}")
    
    # 方法2：使用构建器
    event2 = FlowEventBuilder() \
        .with_type("demo.event") \
        .with_source("demo-engine", "flow_engine") \
        .with_payload({"data": 123, "status": "success"}) \
        .with_metadata({"priority": 5, "tags": ["demo", "test"]}) \
        .build()
    print(f"\n事件2: {event2.event_type} | 优先级: {event2.get_priority()}")
    print(f"  标签: {event2.get_tags()}")
    
    # 方法3：使用辅助函数
    event3 = create_event(
        event_type="quick.event",
        source="demo-client",
        payload={"action": "test"}
    )
    print(f"\n事件3: {event3.event_type} | 来源: {event3.source}")
    
    return event1, event2, event3


async def demo_flow_engine():
    """演示Flow Engine基本用法"""
    print("\n=== Flow Engine 演示 ===")
    
    from src.icflow.core.flow_engine import SimpleFlowEngine
    from src.icflow.core.flow_event import FlowEvent
    from src.icflow.message_bus.memory import MemoryMessageBus
    
    # 创建消息总线
    message_bus = MemoryMessageBus()
    await message_bus.start()
    
    # 创建简单引擎
    async def process_echo(event):
        """处理函数：回声"""
        message = event.payload.get("message", "")
        return event.copy(
            payload={"echo": f"Echo: {message}", "original": event.payload}
        )
    
    engine = SimpleFlowEngine(
        engine_id="echo-engine",
        process_callback=process_echo,
        subscribed_event_types=["echo.*"]
    )
    engine.message_bus = message_bus
    
    # 启动引擎
    await engine.start()
    print(f"引擎启动: {engine.engine_id}")
    
    # 订阅响应事件
    responses = []
    async def response_handler(event):
        responses.append(event)
        print(f"  收到响应: {event.payload.get('echo')}")
    
    await message_bus.subscribe("echo.response", response_handler)
    
    # 发送测试事件
    test_event = FlowEvent(
        event_type="echo.test",
        source="demo-client",
        payload={"message": "Hello from demo!"}
    )
    
    print(f"发送事件: {test_event.event_type}")
    result = await engine.process_event(test_event)
    
    if result:
        print(f"引擎返回: {result.payload}")
    
    # 等待事件处理
    await asyncio.sleep(0.2)
    
    # 显示统计信息
    print(f"\n引擎统计:")
    stats = engine.get_stats()
    for key, value in stats.items():
        if key not in ["subscribed_event_types", "engine_description"]:
            print(f"  {key}: {value}")
    
    # 清理
    await engine.stop()
    await message_bus.stop()
    
    return engine, responses


async def demo_extension():
    """演示Extension基本用法"""
    print("\n=== Extension 演示 ===")
    
    from src.icflow.extensions.base import Extension, ExtensionCapability
    from src.icflow.message_bus.memory import MemoryMessageBus
    
    # 创建自定义扩展
    class DemoExtension(Extension):
        extension_id = "demo-extension"
        name = "Demo Extension"
        description = "演示扩展功能"
        capabilities = [ExtensionCapability.EVENT_PROCESSING, ExtensionCapability.TOOL_CALL]
        
        async def process_request(self, request):
            operation = request.get("operation", "echo")
            data = request.get("data", {})
            
            if operation == "echo":
                return {"result": f"Echo: {data}"}
            elif operation == "reverse":
                text = data.get("text", "")
                return {"result": text[::-1]}
            else:
                return {"error": f"未知操作: {operation}"}
    
    # 创建消息总线
    message_bus = MemoryMessageBus()
    await message_bus.start()
    
    # 创建并启动扩展
    extension = DemoExtension()
    extension.message_bus = message_bus
    await extension.start()
    
    print(f"扩展启动: {extension.info.name}")
    print(f"  能力: {[c.value for c in extension.capabilities]}")
    
    # 测试扩展功能
    test_requests = [
        {"operation": "echo", "data": "Hello, Extension!"},
        {"operation": "reverse", "data": {"text": "IC-Flow"}},
    ]
    
    results = []
    for req in test_requests:
        try:
            result = await extension.handle_request(req)
            results.append(result)
            print(f"  请求: {req['operation']} -> 结果: {result['result']}")
        except Exception as e:
            print(f"  请求失败: {e}")
    
    # 显示扩展信息
    print(f"\n扩展统计:")
    stats = extension.get_stats()
    for key in ["extension_id", "name", "version", "running", "requests_processed"]:
        if key in stats:
            print(f"  {key}: {stats[key]}")
    
    # 清理
    await extension.stop()
    await message_bus.stop()
    
    return extension, results


async def demo_integration():
    """演示集成使用"""
    print("\n=== 集成演示 ===")
    
    from src.icflow.core.flow_engine import FlowEngineRegistry
    from src.icflow.extensions.base import ExtensionRegistry
    from src.icflow.message_bus.memory import MemoryMessageBus
    from src.icflow.examples import EchoEngine, CalculatorExtension
    
    # 创建消息总线
    message_bus = MemoryMessageBus()
    
    # 创建注册表
    engine_registry = FlowEngineRegistry()
    extension_registry = ExtensionRegistry()
    
    # 创建并注册组件
    echo_engine = EchoEngine(prefix="集成演示: ")
    echo_engine.message_bus = message_bus
    engine_registry.register(echo_engine)
    
    calculator = CalculatorExtension()
    calculator.message_bus = message_bus
    extension_registry.register(calculator)
    
    # 启动所有组件
    await message_bus.start()
    await engine_registry.start_all()
    await extension_registry.start_all()
    
    print("所有组件启动完成")
    print(f"  - 引擎: {len(engine_registry.get_all())} 个")
    print(f"  - 扩展: {len(extension_registry.get_all())} 个")
    
    # 使用计算器扩展
    calc_result = await calculator.handle_request({
        "operation": "add",
        "operands": [10, 20, 30]
    })
    print(f"计算器结果: 10 + 20 + 30 = {calc_result['result']}")
    
    # 停止所有组件
    await extension_registry.stop_all()
    await engine_registry.stop_all()
    await message_bus.stop()
    
    print("所有组件已停止")
    
    return {
        "engines": engine_registry.get_all(),
        "extensions": extension_registry.get_all(),
        "calculator_result": calc_result
    }


async def main():
    """主演示函数"""
    print("IC-Flow Platform 基本使用演示")
    print("=" * 50)
    
    try:
        # 演示1: Flow Event
        events = await demo_flow_event()
        
        # 演示2: Flow Engine
        engine, responses = await demo_flow_engine()
        
        # 演示3: Extension
        extension, ext_results = await demo_extension()
        
        # 演示4: 集成
        integration_result = await demo_integration()
        
        print("\n" + "=" * 50)
        print("演示完成！")
        print(f"创建了 {len(events)} 个事件")
        print(f"引擎处理了 {engine.get_stats().get('events_processed', 0)} 个事件")
        print(f"扩展处理了 {extension.get_stats().get('requests_processed', 0)} 个请求")
        
    except Exception as e:
        print(f"演示出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())