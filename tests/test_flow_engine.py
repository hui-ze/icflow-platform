"""
Flow Engine 测试
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock

from src.icflow.core.flow_engine import (
    FlowEngine, 
    SimpleFlowEngine, 
    FlowEngineRegistry
)
from src.icflow.core.flow_event import FlowEvent


class TestFlowEngine(FlowEngine):
    """用于测试的FlowEngine实现"""
    
    engine_id = "test-engine"
    
    async def process(self, event):
        return event.copy(payload={"processed": True})


@pytest.mark.asyncio
async def test_flow_engine_creation():
    """测试FlowEngine创建"""
    engine = TestFlowEngine()
    
    assert engine.engine_id == "test-engine"
    assert engine.engine_name == "Test Engine"
    assert engine.engine_version == "1.0.0"
    assert engine.subscribed_event_types == []
    assert engine.max_concurrent_tasks == 10
    assert not engine.is_running()


@pytest.mark.asyncio
async def test_flow_engine_start_stop():
    """测试引擎启动和停止"""
    engine = TestFlowEngine()
    
    # 启动引擎
    await engine.start()
    assert engine.is_running()
    
    # 再次启动应该发出警告但不报错
    await engine.start()
    assert engine.is_running()
    
    # 停止引擎
    await engine.stop()
    assert not engine.is_running()
    
    # 再次停止应该发出警告但不报错
    await engine.stop()
    assert not engine.is_running()


@pytest.mark.asyncio
async def test_flow_engine_with_message_bus():
    """测试带消息总线的引擎"""
    mock_bus = AsyncMock()
    engine = TestFlowEngine()
    engine.message_bus = mock_bus
    
    await engine.start()
    
    # 检查是否发送了心跳事件
    assert mock_bus.publish.called
    call_args = mock_bus.publish.call_args[0][0]
    assert call_args.event_type == "engine.heartbeat"
    
    await engine.stop()


@pytest.mark.asyncio
async def test_flow_engine_process_event():
    """测试引擎处理事件"""
    engine = TestFlowEngine()
    await engine.start()
    
    test_event = FlowEvent(
        event_type="test.event",
        source="test-client",
        payload={"data": "test"}
    )
    
    result = await engine.process_event(test_event)
    
    assert result is not None
    assert result.event_type == test_event.event_type
    assert result.payload["processed"] == True
    
    await engine.stop()


@pytest.mark.asyncio
async def test_flow_engine_subscribed_events():
    """测试事件订阅匹配"""
    engine = TestFlowEngine()
    engine.subscribed_event_types = ["test.*", "specific.event"]
    
    # 匹配通配符
    matching_event = FlowEvent(event_type="test.something")
    assert engine._should_handle_event(matching_event) == True
    
    # 匹配具体事件
    specific_event = FlowEvent(event_type="specific.event")
    assert engine._should_handle_event(specific_event) == True
    
    # 不匹配的事件
    non_matching_event = FlowEvent(event_type="other.event")
    assert engine._should_handle_event(non_matching_event) == False


@pytest.mark.asyncio
async def test_flow_engine_stats():
    """测试引擎统计信息"""
    engine = TestFlowEngine()
    await engine.start()
    
    stats = engine.get_stats()
    
    assert "engine_id" in stats
    assert "engine_name" in stats
    assert "engine_version" in stats
    assert "running" in stats
    assert stats["running"] == True
    assert "events_processed" in stats
    assert "start_time" in stats
    
    await engine.stop()
    
    stats = engine.get_stats()
    assert stats["running"] == False


@pytest.mark.asyncio
async def test_simple_flow_engine():
    """测试SimpleFlowEngine"""
    mock_callback = AsyncMock(return_value={"result": "success"})
    
    engine = SimpleFlowEngine(
        engine_id="simple-engine",
        process_callback=mock_callback,
        subscribed_event_types=["test.*"]
    )
    
    await engine.start()
    
    test_event = FlowEvent(event_type="test.event")
    result = await engine.process_event(test_event)
    
    # 检查回调是否被调用
    assert mock_callback.called
    # 检查返回的事件
    assert result.payload["result"] == "success"
    
    await engine.stop()


@pytest.mark.asyncio
async def test_simple_flow_engine_sync_callback():
    """测试SimpleFlowEngine同步回调"""
    def sync_callback(event):
        return event.copy(payload={"sync": True})
    
    engine = SimpleFlowEngine(
        engine_id="sync-engine",
        process_callback=sync_callback
    )
    
    await engine.start()
    
    test_event = FlowEvent(event_type="test.event")
    result = await engine.process_event(test_event)
    
    assert result.payload["sync"] == True
    
    await engine.stop()


@pytest.mark.asyncio
async def test_flow_engine_registry():
    """测试FlowEngineRegistry"""
    registry = FlowEngineRegistry()
    
    # 创建并注册引擎
    engine1 = TestFlowEngine()
    engine1.engine_id = "engine-1"
    
    engine2 = TestFlowEngine()
    engine2.engine_id = "engine-2"
    engine2.subscribed_event_types = ["test.*"]
    
    registry.register(engine1)
    registry.register(engine2)
    
    # 获取引擎
    assert registry.get("engine-1") == engine1
    assert registry.get("engine-2") == engine2
    assert registry.get("non-existent") is None
    
    # 获取所有引擎
    all_engines = registry.get_all()
    assert len(all_engines) == 2
    assert engine1 in all_engines
    assert engine2 in all_engines
    
    # 按事件类型获取引擎
    test_engines = registry.get_by_event_type("test.something")
    assert len(test_engines) == 2
    assert engine1 in test_engines
    assert engine2 in test_engines
    
    # 注销引擎
    unregistered = registry.unregister("engine-1")
    assert unregistered == engine1
    assert registry.get("engine-1") is None
    assert len(registry.get_all()) == 1
    
    # 清空注册表
    registry.clear()
    assert len(registry.get_all()) == 0


@pytest.mark.asyncio
async def test_flow_engine_registry_start_stop():
    """测试注册表启动和停止所有引擎"""
    registry = FlowEngineRegistry()
    
    # 创建并注册引擎
    engine1 = TestFlowEngine()
    engine1.engine_id = "engine-1"
    
    engine2 = TestEngineWithMessageBus()
    engine2.engine_id = "engine-2"
    
    registry.register(engine1)
    registry.register(engine2)
    
    # 启动所有引擎
    await registry.start_all()
    assert engine1.is_running()
    assert engine2.is_running()
    
    # 停止所有引擎
    await registry.stop_all()
    assert not engine1.is_running()
    assert not engine2.is_running()


class TestEngineWithMessageBus(FlowEngine):
    """带有消息总线模拟的测试引擎"""
    
    engine_id = "bus-engine"
    
    async def process(self, event):
        # 模拟使用消息总线发布事件
        if self.message_bus:
            await self.message_bus.publish(
                event.copy(event_type="response.event")
            )
        return event


@pytest.mark.asyncio
async def test_flow_engine_lifecycle_context():
    """测试引擎生命周期上下文管理器"""
    engine = TestFlowEngine()
    
    async with engine.lifecycle() as running_engine:
        assert running_engine.is_running()
        assert running_engine == engine
    
    assert not engine.is_running()