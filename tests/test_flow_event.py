"""
Flow Event 测试
"""

import pytest
import time
from src.icflow.core.flow_event import FlowEvent, FlowEventBuilder, EventTypes, create_event


def test_flow_event_creation():
    """测试FlowEvent创建"""
    event = FlowEvent(event_type="test.event")
    
    assert event.event_type == "test.event"
    assert isinstance(event.event_id, str)
    assert isinstance(event.timestamp, float)
    assert isinstance(event.created_at, str)
    assert event.source == "system"
    assert event.source_type == "unknown"
    assert event.payload == {}
    assert event.metadata == {}
    assert event.context == {}


def test_flow_event_with_data():
    """测试带数据的FlowEvent"""
    event = FlowEvent(
        event_type="test.event",
        source="test-source",
        source_type="test",
        payload={"key": "value"},
        metadata={"priority": 5},
        context={"trace_id": "123"}
    )
    
    assert event.event_type == "test.event"
    assert event.source == "test-source"
    assert event.source_type == "test"
    assert event.payload["key"] == "value"
    assert event.metadata["priority"] == 5
    assert event.context["trace_id"] == "123"


def test_flow_event_age():
    """测试事件年龄计算"""
    event = FlowEvent(event_type="test.event")
    # 事件刚创建，年龄应该很小
    assert 0 <= event.age < 0.1


def test_flow_event_priority():
    """测试事件优先级"""
    event = FlowEvent(event_type="test.event")
    
    # 默认优先级为0
    assert event.get_priority() == 0
    
    # 设置优先级
    event.set_priority(10)
    assert event.get_priority() == 10
    assert event.metadata["priority"] == 10


def test_flow_event_tags():
    """测试事件标签"""
    event = FlowEvent(event_type="test.event")
    
    # 初始标签为空列表
    assert event.get_tags() == []
    
    # 添加标签
    event.add_tag("important")
    event.add_tag("test")
    event.add_tag("important")  # 重复标签不应重复添加
    
    assert set(event.get_tags()) == {"important", "test"}
    assert event.metadata["tags"] == ["important", "test"]


def test_flow_event_to_from_dict():
    """测试事件字典转换"""
    original = FlowEvent(
        event_type="test.event",
        source="test",
        payload={"data": 123},
        metadata={"tags": ["test"]}
    )
    
    # 转换为字典
    data = original.to_dict()
    assert isinstance(data, dict)
    assert data["event_type"] == "test.event"
    assert data["payload"]["data"] == 123
    
    # 从字典创建
    restored = FlowEvent.from_dict(data)
    assert restored.event_type == original.event_type
    assert restored.payload == original.payload
    assert restored.metadata == original.metadata
    # 注意：由于时间戳差异，event_id会不同


def test_flow_event_copy():
    """测试事件复制"""
    original = FlowEvent(
        event_type="original.event",
        source="original",
        payload={"data": "original"}
    )
    
    # 复制并更新
    copy = original.copy(
        event_type="copy.event",
        payload={"data": "updated"}
    )
    
    assert copy.event_type == "copy.event"
    assert copy.payload["data"] == "updated"
    assert copy.source == "original"  # 未修改的字段保持不变
    assert copy.event_id != original.event_id  # ID应该不同
    assert copy.timestamp > original.timestamp  # 时间戳应该更新


def test_flow_event_builder():
    """测试FlowEventBuilder"""
    builder = FlowEventBuilder()
    event = builder \
        .with_type("test.event") \
        .with_source("test-engine", "flow_engine") \
        .with_payload({"key": "value"}) \
        .with_metadata({"priority": 1}) \
        .with_context({"trace": "123"}) \
        .build()
    
    assert event.event_type == "test.event"
    assert event.source == "test-engine"
    assert event.source_type == "flow_engine"
    assert event.payload["key"] == "value"
    assert event.metadata["priority"] == 1
    assert event.context["trace"] == "123"


def test_flow_event_builder_missing_type():
    """测试缺少事件类型的FlowEventBuilder"""
    builder = FlowEventBuilder()
    with pytest.raises(ValueError, match="事件类型（event_type）必须设置"):
        builder.with_source("test").build()


def test_create_event_helper():
    """测试create_event辅助函数"""
    event = create_event(
        event_type="test.event",
        source="test-engine",
        payload={"data": 123},
        metadata={"priority": 5},
        context={"trace": "456"}
    )
    
    assert event.event_type == "test.event"
    assert event.source == "test-engine"
    assert event.source_type == "flow_engine"
    assert event.payload["data"] == 123
    assert event.metadata["priority"] == 5
    assert event.context["trace"] == "456"


def test_event_types_constants():
    """测试事件类型常量"""
    assert EventTypes.SYSTEM_STARTUP == "system.startup"
    assert EventTypes.TASK_COMPLETED == "task.completed"
    assert EventTypes.ENGINE_HEARTBEAT == "engine.heartbeat"
    assert EventTypes.KNOWLEDGE_CREATED == "knowledge.created"
    
    # 确保常量是字符串
    assert isinstance(EventTypes.SYSTEM_STARTUP, str)