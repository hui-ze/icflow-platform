"""
扩展基类与注册表
"""

import abc
import asyncio
import logging
import uuid
from typing import Dict, Any, Optional, List, Set, Callable
from enum import Enum
from dataclasses import dataclass

from ..core.flow_event import FlowEvent


logger = logging.getLogger(__name__)


class ExtensionCapability(str, Enum):
    """扩展能力类型"""
    
    # 核心能力
    EVENT_PROCESSING = "event_processing"  # 事件处理
    EVENT_GENERATION = "event_generation"  # 事件生成
    
    # 数据能力
    DATA_STORAGE = "data_storage"  # 数据存储
    DATA_RETRIEVAL = "data_retrieval"  # 数据检索
    DATA_TRANSFORMATION = "data_transformation"  # 数据转换
    
    # 通信能力
    API_INTEGRATION = "api_integration"  # API集成
    MESSAGE_BROKER = "message_broker"  # 消息代理
    
    # 工具能力
    CODE_GENERATION = "code_generation"  # 代码生成
    DOCUMENT_PROCESSING = "document_processing"  # 文档处理
    IMAGE_PROCESSING = "image_processing"  # 图像处理
    
    # 领域特定能力
    EDA_TOOL_INTEGRATION = "eda_tool_integration"  # EDA工具集成
    DRC_LVS_AUTOMATION = "drc_lvs_automation"  # DRC/LVS自动化
    KNOWLEDGE_MANAGEMENT = "knowledge_management"  # 知识管理


@dataclass
class ExtensionInfo:
    """扩展信息"""
    extension_id: str
    name: str
    version: str
    description: str
    capabilities: List[ExtensionCapability]
    metadata: Dict[str, Any]
    dependencies: List[str]


class Extension(abc.ABC):
    """扩展基类"""
    
    # 扩展元数据（必须由子类设置）
    extension_id: str = None
    name: str = None
    version: str = "1.0.0"
    description: str = ""
    capabilities: List[ExtensionCapability] = []
    metadata: Dict[str, Any] = {}
    dependencies: List[str] = []
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化扩展
        
        Args:
            config: 扩展配置字典
        """
        if self.extension_id is None:
            raise ValueError("子类必须设置 extension_id 属性")
        
        self.config = config or {}
        self._message_bus = None
        self._running = False
        self.name = self.name or self.extension_id
        self._info = ExtensionInfo(
            extension_id=self.extension_id,
            name=self.name,
            version=self.version,
            description=self.description,
            capabilities=self.capabilities.copy(),
            metadata=self.metadata.copy(),
            dependencies=self.dependencies.copy()
        )
        
        # 统计数据
        self._stats = {
            "requests_processed": 0,
            "requests_failed": 0,
            "events_generated": 0,
            "start_time": None,
        }
    
    @property
    def message_bus(self):
        """获取消息总线实例"""
        return self._message_bus
    
    @message_bus.setter
    def message_bus(self, bus):
        """设置消息总线实例"""
        self._message_bus = bus
    
    @property
    def info(self) -> ExtensionInfo:
        """获取扩展信息"""
        return self._info
    
    async def start(self) -> None:
        """启动扩展"""
        if self._running:
            logger.warning(f"扩展 {self.extension_id} 已经在运行")
            return
        
        logger.info(f"启动扩展: {self.extension_id}")
        self._running = True
        self._stats["start_time"] = asyncio.get_event_loop().time()
        
        # 发送扩展加载事件
        if self._message_bus:
            await self._message_bus.publish(FlowEvent(
                event_type="extension.loaded",
                source=self.extension_id,
                source_type="extension",
                payload={
                    "extension_id": self.extension_id,
                    "name": self.name,
                    "version": self.version,
                    "capabilities": [c.value for c in self.capabilities],
                }
            ))
        
        # 调用子类的启动逻辑
        await self.on_start()
        
        logger.info(f"扩展 {self.extension_id} 启动完成")
    
    async def stop(self) -> None:
        """停止扩展"""
        if not self._running:
            logger.warning(f"扩展 {self.extension_id} 未在运行")
            return
        
        logger.info(f"停止扩展: {self.extension_id}")
        self._running = False
        
        # 调用子类的停止逻辑
        await self.on_stop()
        
        # 发送扩展卸载事件
        if self._message_bus:
            await self._message_bus.publish(FlowEvent(
                event_type="extension.unloaded",
                source=self.extension_id,
                source_type="extension",
                payload={
                    "extension_id": self.extension_id,
                    "uptime": asyncio.get_event_loop().time() - self._stats["start_time"],
                }
            ))
        
        logger.info(f"扩展 {self.extension_id} 停止完成")
    
    async def on_start(self) -> None:
        """扩展启动时的自定义逻辑（子类可重写）"""
        pass
    
    async def on_stop(self) -> None:
        """扩展停止时的自定义逻辑（子类可重写）"""
        pass
    
    async def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理扩展请求（主入口点）
        
        Args:
            request: 请求字典
            
        Returns:
            响应字典
        """
        if not self._running:
            raise RuntimeError(f"扩展 {self.extension_id} 未运行")
        
        try:
            logger.debug(f"扩展 {self.extension_id} 处理请求: {request.get('type', 'unknown')}")
            
            # 调用子类的处理逻辑
            result = await self.process_request(request)
            
            # 更新统计数据
            self._stats["requests_processed"] += 1
            
            logger.debug(f"扩展 {self.extension_id} 请求处理完成")
            return result
            
        except Exception as e:
            self._stats["requests_failed"] += 1
            logger.error(f"扩展 {self.extension_id} 处理请求失败: {e}", exc_info=True)
            raise
    
    @abc.abstractmethod
    async def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理请求的核心逻辑（必须由子类实现）
        
        Args:
            request: 请求字典
            
        Returns:
            响应字典
        """
        pass
    
    async def generate_event(self, event_type: str, payload: Dict[str, Any]) -> Optional[FlowEvent]:
        """生成事件并发布到消息总线"""
        if not self._message_bus:
            logger.warning(f"扩展 {self.extension_id} 未设置消息总线，无法生成事件")
            return None
        
        event = FlowEvent(
            event_type=event_type,
            source=self.extension_id,
            source_type="extension",
            payload=payload
        )
        
        await self._message_bus.publish(event)
        self._stats["events_generated"] += 1
        
        logger.debug(f"扩展 {self.extension_id} 生成事件: {event_type}")
        return event
    
    def has_capability(self, capability: ExtensionCapability) -> bool:
        """检查是否具有指定能力"""
        return capability in self.capabilities
    
    def get_stats(self) -> Dict[str, Any]:
        """获取扩展统计信息"""
        stats = self._stats.copy()
        stats.update({
            "extension_id": self.extension_id,
            "name": self.name,
            "version": self.version,
            "running": self._running,
            "capabilities": [c.value for c in self.capabilities],
        })
        
        if stats["start_time"]:
            stats["uptime"] = asyncio.get_event_loop().time() - stats["start_time"]
        
        return stats
    
    def is_running(self) -> bool:
        """检查扩展是否在运行"""
        return self._running


class ExtensionRegistry:
    """扩展注册表"""
    
    def __init__(self):
        self._extensions: Dict[str, Extension] = {}
        self._capability_index: Dict[ExtensionCapability, Set[str]] = {}
    
    def register(self, extension: Extension) -> None:
        """注册扩展"""
        if extension.extension_id in self._extensions:
            raise ValueError(f"扩展 ID 已存在: {extension.extension_id}")
        
        # 存储扩展
        self._extensions[extension.extension_id] = extension
        
        # 更新能力索引
        for capability in extension.capabilities:
            if capability not in self._capability_index:
                self._capability_index[capability] = set()
            self._capability_index[capability].add(extension.extension_id)
        
        logger.info(f"注册扩展: {extension.extension_id} ({len(extension.capabilities)} 个能力)")
    
    def unregister(self, extension_id: str) -> Optional[Extension]:
        """注销扩展"""
        extension = self._extensions.pop(extension_id, None)
        if not extension:
            return None
        
        # 从能力索引中移除
        for capability in extension.capabilities:
            if capability in self._capability_index:
                self._capability_index[capability].discard(extension_id)
                if not self._capability_index[capability]:
                    del self._capability_index[capability]
        
        logger.info(f"注销扩展: {extension_id}")
        return extension
    
    def get(self, extension_id: str) -> Optional[Extension]:
        """获取扩展"""
        return self._extensions.get(extension_id)
    
    def get_all(self) -> List[Extension]:
        """获取所有扩展"""
        return list(self._extensions.values())
    
    def get_by_capability(self, capability: ExtensionCapability) -> List[Extension]:
        """获取具有指定能力的扩展"""
        extension_ids = self._capability_index.get(capability, set())
        return [self._extensions[ext_id] for ext_id in extension_ids if ext_id in self._extensions]
    
    def get_capabilities(self) -> List[ExtensionCapability]:
        """获取所有注册的能力"""
        return list(self._capability_index.keys())
    
    async def start_all(self) -> None:
        """启动所有扩展"""
        tasks = [ext.start() for ext in self._extensions.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def stop_all(self) -> None:
        """停止所有扩展"""
        tasks = [ext.stop() for ext in self._extensions.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    def clear(self) -> None:
        """清空注册表"""
        self._extensions.clear()
        self._capability_index.clear()