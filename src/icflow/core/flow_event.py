"""
Flow Event 核心模块
定义事件驱动架构中的基本事件单位
"""

import uuid
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, ConfigDict


class FlowEvent(BaseModel):
    """Flow Event - 事件驱动架构中的基本数据单位"""
    
    # 事件标识
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="事件唯一标识符"
    )
    event_type: str = Field(
        ...,
        description="事件类型，用于路由和处理决策"
    )
    
    # 时间信息
    timestamp: float = Field(
        default_factory=time.time,
        description="事件创建时间戳（Unix时间戳）"
    )
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="事件创建时间（ISO格式）"
    )
    
    # 来源信息
    source: str = Field(
        default="system",
        description="事件来源标识，通常为 Flow Engine ID"
    )
    source_type: str = Field(
        default="unknown",
        description="来源类型，如 'flow_engine', 'extension', 'api' 等"
    )
    
    # 业务数据
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="事件携带的业务数据"
    )
    
    # 元数据
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="事件元数据，如优先级、标签等"
    )
    
    # 上下文信息
    context: Dict[str, Any] = Field(
        default_factory=dict,
        description="事件上下文，用于关联事件链、跟踪等"
    )
    
    # Pydantic 配置
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_encoders={
            datetime: lambda v: v.isoformat(),
        }
    )
    
    def __init__(self, **data):
        """初始化事件，确保必要的字段被设置"""
        super().__init__(**data)
        
        # 如果提供了 timestamp 但未提供 created_at，则根据 timestamp 生成
        if "timestamp" in data and "created_at" not in data:
            self.created_at = datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()
    
    @property
    def age(self) -> float:
        """获取事件年龄（秒）"""
        return time.time() - self.timestamp
    
    def get_priority(self) -> int:
        """获取事件优先级，默认返回 0"""
        return self.metadata.get("priority", 0)
    
    def set_priority(self, priority: int) -> None:
        """设置事件优先级"""
        self.metadata["priority"] = priority
    
    def get_tags(self) -> List[str]:
        """获取事件标签列表"""
        return self.metadata.get("tags", [])
    
    def add_tag(self, tag: str) -> None:
        """添加事件标签"""
        if "tags" not in self.metadata:
            self.metadata["tags"] = []
        if tag not in self.metadata["tags"]:
            self.metadata["tags"].append(tag)
    
    def to_dict(self) -> Dict[str, Any]:
        """将事件转换为字典（用于序列化）"""
        return self.model_dump()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FlowEvent":
        """从字典创建事件"""
        return cls(**data)
    
    def copy(self, **updates) -> "FlowEvent":
        """创建事件的副本，可选更新部分字段"""
        data = self.to_dict()
        data.update(updates)
        # 生成新的事件ID
        data["event_id"] = str(uuid.uuid4())
        data["timestamp"] = time.time()
        data["created_at"] = datetime.now(timezone.utc).isoformat()
        return self.from_dict(data)


class FlowEventBuilder:
    """Flow Event 构建器，提供更友好的创建接口"""
    
    def __init__(self):
        self._event_type = None
        self._source = "system"
        self._source_type = "unknown"
        self._payload = {}
        self._metadata = {}
        self._context = {}
    
    def with_type(self, event_type: str) -> "FlowEventBuilder":
        """设置事件类型"""
        self._event_type = event_type
        return self
    
    def with_source(self, source: str, source_type: str = "flow_engine") -> "FlowEventBuilder":
        """设置事件来源"""
        self._source = source
        self._source_type = source_type
        return self
    
    def with_payload(self, payload: Dict[str, Any]) -> "FlowEventBuilder":
        """设置事件负载"""
        self._payload = payload
        return self
    
    def with_metadata(self, metadata: Dict[str, Any]) -> "FlowEventBuilder":
        """设置事件元数据"""
        self._metadata = metadata
        return self
    
    def with_context(self, context: Dict[str, Any]) -> "FlowEventBuilder":
        """设置事件上下文"""
        self._context = context
        return self
    
    def build(self) -> FlowEvent:
        """构建 FlowEvent 实例"""
        if not self._event_type:
            raise ValueError("事件类型（event_type）必须设置")
        
        return FlowEvent(
            event_type=self._event_type,
            source=self._source,
            source_type=self._source_type,
            payload=self._payload,
            metadata=self._metadata,
            context=self._context
        )


# 常用事件类型常量
class EventTypes:
    """标准事件类型常量"""
    
    # 系统事件
    SYSTEM_STARTUP = "system.startup"
    SYSTEM_SHUTDOWN = "system.shutdown"
    SYSTEM_HEARTBEAT = "system.heartbeat"
    
    # Flow Engine 事件
    ENGINE_REGISTERED = "engine.registered"
    ENGINE_UNREGISTERED = "engine.unregistered"
    ENGINE_HEARTBEAT = "engine.heartbeat"
    
    # 任务事件
    TASK_CREATED = "task.created"
    TASK_ASSIGNED = "task.assigned"
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    
    # 知识管理事件
    KNOWLEDGE_CREATED = "knowledge.created"
    KNOWLEDGE_UPDATED = "knowledge.updated"
    KNOWLEDGE_QUERIED = "knowledge.queried"
    
    # 扩展事件
    EXTENSION_LOADED = "extension.loaded"
    EXTENSION_UNLOADED = "extension.unloaded"
    EXTENSION_ERROR = "extension.error"


def create_event(
    event_type: str,
    source: str = "system",
    payload: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None
) -> FlowEvent:
    """快速创建 FlowEvent 的辅助函数"""
    return FlowEvent(
        event_type=event_type,
        source=source,
        source_type="flow_engine" if source != "system" else "system",
        payload=payload or {},
        metadata=metadata or {},
        context=context or {}
    )