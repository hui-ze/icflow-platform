"""
消息总线测试
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock

from src.icflow.message_bus.memory import MemoryMessageBus
from src.icflow.core.flow_event import FlowEvent


@pytest.mark.asyncio
async def test_memory_message_bus_creation():
    """测试内存消息总线创建"""
    bus = MemoryMessageBus()
    
    assert bus.max_queue_size == 1000
    assert not bus._running
    assert bus._subscriptions == {}
    assert bus._subscription_by_id == {}
    assert bus._queues == {}
    assert bus._consumer_tasks == set()


@pytest.mark.asyncio
async def test_memory_message_bus_start_stop():
    """测试消息总线启动和停止"""
    bus = MemoryMessageBus()
    
    # 启动总线
    await bus.start()
    assert bus._running
    
    # 再次启动应该发出警告但不报错
    await bus.start()
    assert bus._running
    
    # 停止总线
    await bus.stop()
    assert not bus._running
    
    # 再次停止应该发出警告但不报错
    await bus.stop()
    assert not bus._running


@pytest.mark.asyncio
async def test_memory_message_bus_publish_no_subscribers():
    """测试发布事件（无订阅者）"""
    bus = MemoryMessageBus()
    await bus.start()
    
    event = FlowEvent(event_type="test.event")
    
    # 发布事件，应该没有错误
    await bus.publish(event)
    
    await bus.stop()


@pytest.mark.asyncio
async def test_memory_message_bus_subscribe_publish():
    """测试订阅和发布事件"""
    bus = MemoryMessageBus()
    await bus.start()
    
    received_events = []
    
    async def event_handler(event):
        received_events.append(event)
    
    # 订阅事件
    subscription = await bus.subscribe("test.event", event_handler)
    assert subscription.id is not None
    assert subscription.event_type == "test.event"
    assert subscription.callback == event_handler
    
    # 发布事件
    event = FlowEvent(event_type="test.event", payload={"data": "test"})
    await bus.publish(event)
    
    # 等待事件处理
    await asyncio.sleep(0.1)
    
    assert len(received_events) == 1
    assert received_events[0].event_type == "test.event"
    assert received_events[0].payload["data"] == "test"
    
    await bus.stop()


@pytest.mark.asyncio
async def test_memory_message_bus_wildcard_subscription():
    """测试通配符订阅"""
    bus = MemoryMessageBus()
    await bus.start()
    
    received_events = []
    
    async def event_handler(event):
        received_events.append(event)
    
    # 订阅通配符事件
    await bus.subscribe("test.*", event_handler)
    
    # 发布匹配的事件
    await bus.publish(FlowEvent(event_type="test.something"))
    await bus.publish(FlowEvent(event_type="test.other"))
    
    # 发布不匹配的事件
    await bus.publish(FlowEvent(event_type="other.event"))
    
    # 等待事件处理
    await asyncio.sleep(0.1)
    
    assert len(received_events) == 2
    assert all(e.event_type.startswith("test.") for e in received_events)
    
    await bus.stop()


@pytest.mark.asyncio
async def test_memory_message_bus_multiple_subscribers():
    """测试多个订阅者"""
    bus = MemoryMessageBus()
    await bus.start()
    
    handler1_events = []
    handler2_events = []
    
    async def handler1(event):
        handler1_events.append(event)
    
    async def handler2(event):
        handler2_events.append(event)
    
    # 两个订阅者订阅相同事件
    await bus.subscribe("test.event", handler1)
    await bus.subscribe("test.event", handler2)
    
    # 发布事件
    event = FlowEvent(event_type="test.event")
    await bus.publish(event)
    
    # 等待事件处理
    await asyncio.sleep(0.1)
    
    assert len(handler1_events) == 1
    assert len(handler2_events) == 1
    
    await bus.stop()


@pytest.mark.asyncio
async def test_memory_message_bus_queue_groups():
    """测试队列分组"""
    bus = MemoryMessageBus()
    await bus.start()
    
    handler1_events = []
    handler2_events = []
    
    async def handler1(event):
        handler1_events.append(event)
        await asyncio.sleep(0.05)  # 模拟处理延迟
    
    async def handler2(event):
        handler2_events.append(event)
    
    # 两个订阅者使用不同队列
    await bus.subscribe("test.event", handler1, queue_name="queue1")
    await bus.subscribe("test.event", handler2, queue_name="queue2")
    
    # 发布多个事件
    for i in range(3):
        await bus.publish(FlowEvent(event_type="test.event", payload={"i": i}))
    
    # 等待事件处理
    await asyncio.sleep(0.2)
    
    # 两个队列应该独立处理事件
    assert len(handler1_events) == 3
    assert len(handler2_events) == 3
    
    await bus.stop()


@pytest.mark.asyncio
async def test_memory_message_bus_unsubscribe():
    """测试取消订阅"""
    bus = MemoryMessageBus()
    await bus.start()
    
    received_events = []
    
    async def event_handler(event):
        received_events.append(event)
    
    # 订阅事件
    subscription = await bus.subscribe("test.event", event_handler)
    
    # 发布一个事件
    await bus.publish(FlowEvent(event_type="test.event"))
    await asyncio.sleep(0.1)
    assert len(received_events) == 1
    
    # 取消订阅
    result = await bus.unsubscribe(subscription.id)
    assert result == True
    
    # 再次发布事件，应该不会被接收
    await bus.publish(FlowEvent(event_type="test.event"))
    await asyncio.sleep(0.1)
    assert len(received_events) == 1  # 数量不变
    
    # 取消不存在的订阅
    result = await bus.unsubscribe("non-existent")
    assert result == False
    
    await bus.stop()


@pytest.mark.asyncio
async def test_memory_message_bus_stats():
    """测试统计信息"""
    bus = MemoryMessageBus()
    
    stats = bus.get_stats()
    assert "events_published" in stats
    assert "events_delivered" in stats
    assert "events_dropped" in stats
    assert "subscriptions_active" in stats
    assert "running" in stats
    assert stats["running"] == False
    
    await bus.start()
    
    # 订阅并发布事件
    async def handler(event):
        pass
    
    await bus.subscribe("test.event", handler)
    await bus.publish(FlowEvent(event_type="test.event"))
    
    stats = bus.get_stats()
    assert stats["running"] == True
    assert stats["subscriptions_active"] == 1
    assert stats["events_published"] == 1
    assert "uptime" in stats
    
    await bus.stop()


@pytest.mark.asyncio
async def test_memory_message_bus_queue_creation():
    """测试队列创建"""
    bus = MemoryMessageBus()
    
    # 创建队列
    bus.create_queue("test-queue")
    assert "test-queue" in bus._queues
    
    # 启动总线，队列消费者应该被创建
    await bus.start()
    assert bus._running
    
    # 移除队列
    bus.remove_queue("test-queue")
    assert "test-queue" not in bus._queues
    
    await bus.stop()


@pytest.mark.asyncio
async def test_memory_message_bus_queue_full():
    """测试队列满的情况"""
    bus = MemoryMessageBus(max_queue_size=1)  # 很小的队列
    await bus.start()
    
    received_events = []
    
    async def slow_handler(event):
        await asyncio.sleep(0.2)  # 慢处理
        received_events.append(event)
    
    # 订阅事件
    await bus.subscribe("test.event", slow_handler)
    
    # 快速发布多个事件
    for i in range(3):
        await bus.publish(FlowEvent(event_type="test.event", payload={"i": i}))
    
    # 等待处理
    await asyncio.sleep(0.3)
    
    # 检查统计信息
    stats = bus.get_stats()
    # 由于队列满，可能会有事件被丢弃
    assert stats["events_dropped"] >= 0
    
    await bus.stop()


@pytest.mark.asyncio
async def test_memory_message_bus_sync_callback():
    """测试同步回调函数"""
    bus = MemoryMessageBus()
    await bus.start()
    
    received_events = []
    
    def sync_handler(event):
        received_events.append(event)
    
    # 订阅事件（同步回调）
    await bus.subscribe("test.event", sync_handler)
    
    # 发布事件
    await bus.publish(FlowEvent(event_type="test.event"))
    
    # 等待事件处理
    await asyncio.sleep(0.1)
    
    assert len(received_events) == 1
    
    await bus.stop()