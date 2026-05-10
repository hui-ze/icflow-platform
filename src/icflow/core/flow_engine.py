"""
Flow Engine 核心模块
定义事件驱动架构中的处理单元
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Set, Callable
from contextlib import asynccontextmanager

from .flow_event import FlowEvent, EventTypes


logger = logging.getLogger(__name__)


class FlowEngine(ABC):
    """Flow Engine 基类 - 事件处理单元"""
    
    # 引擎元数据
    engine_id: str = None  # 必须由子类设置
    engine_name: str = None
    engine_version: str = "1.0.0"
    engine_description: str = ""
    
    # 事件订阅
    subscribed_event_types: List[str] = []
    
    # 配置
    max_concurrent_tasks: int = 10
    retry_on_failure: bool = True
    max_retries: int = 3
    retry_delay: float = 1.0
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化 Flow Engine
        
        Args:
            config: 引擎配置字典
        """
        if self.engine_id is None:
            raise ValueError("子类必须设置 engine_id 属性")
        
        self.config = config or {}
        self._message_bus = None
        self._running = False
        self._tasks: Set[asyncio.Task] = set()
        self._concurrent_semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        
        # 统计数据
        self._stats = {
            "events_processed": 0,
            "events_failed": 0,
            "start_time": None,
            "last_heartbeat": None,
        }
        
        # 设置引擎名称（如果未设置）
        if self.engine_name is None:
            self.engine_name = self.engine_id.replace("_", " ").replace("-", " ").title()
    
    @property
    def message_bus(self):
        """获取消息总线实例"""
        return self._message_bus
    
    @message_bus.setter
    def message_bus(self, bus):
        """设置消息总线实例"""
        self._message_bus = bus
    
    async def start(self) -> None:
        """启动引擎"""
        if self._running:
            logger.warning(f"引擎 {self.engine_id} 已经在运行")
            return
        
        logger.info(f"启动引擎: {self.engine_id}")
        self._running = True
        self._stats["start_time"] = time.time()
        
        # 发送引擎注册事件
        await self._send_heartbeat()
        
        # 调用子类的启动逻辑
        await self.on_start()
        
        logger.info(f"引擎 {self.engine_id} 启动完成")
    
    async def stop(self) -> None:
        """停止引擎"""
        if not self._running:
            logger.warning(f"引擎 {self.engine_id} 未在运行")
            return
        
        logger.info(f"停止引擎: {self.engine_id}")
        self._running = False
        
        # 取消所有任务
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # 等待任务完成
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        
        # 调用子类的停止逻辑
        await self.on_stop()
        
        # 发送引擎注销事件
        if self._message_bus:
            await self._message_bus.publish(FlowEvent(
                event_type=EventTypes.ENGINE_UNREGISTERED,
                source=self.engine_id,
                source_type="flow_engine",
                payload={
                    "engine_id": self.engine_id,
                    "engine_name": self.engine_name,
                    "uptime": time.time() - self._stats["start_time"],
                }
            ))
        
        logger.info(f"引擎 {self.engine_id} 停止完成")
    
    async def on_start(self) -> None:
        """引擎启动时的自定义逻辑（子类可重写）"""
        pass
    
    async def on_stop(self) -> None:
        """引擎停止时的自定义逻辑（子类可重写）"""
        pass
    
    async def process_event(self, event: FlowEvent) -> Optional[FlowEvent]:
        """
        处理事件（主入口点）
        
        Args:
            event: 输入事件
            
        Returns:
            处理结果事件，如果无需返回则为 None
        """
        if not self._running:
            logger.warning(f"引擎 {self.engine_id} 未运行，忽略事件 {event.event_id}")
            return None
        
        # 检查事件类型是否匹配订阅
        if not self._should_handle_event(event):
            return None
        
        # 创建处理任务
        task = asyncio.create_task(self._process_event_with_retry(event))
        self._tasks.add(task)
        task.add_done_callback(lambda t: self._tasks.discard(t))
        
        # 等待任务完成并返回结果
        try:
            result = await task
            return result
        except Exception as e:
            logger.error(f"引擎 {self.engine_id} 处理事件失败: {e}", exc_info=True)
            return None
    
    async def _process_event_with_retry(self, event: FlowEvent) -> Optional[FlowEvent]:
        """带重试机制的事件处理"""
        retries = 0
        
        while retries <= self.max_retries:
            try:
                # 控制并发数
                async with self._concurrent_semaphore:
                    return await self._process_event_internal(event)
            except Exception as e:
                retries += 1
                
                if retries > self.max_retries or not self.retry_on_failure:
                    logger.error(
                        f"引擎 {self.engine_id} 处理事件失败（重试 {retries-1}/{self.max_retries}）: {e}",
                        exc_info=True
                    )
                    self._stats["events_failed"] += 1
                    
                    # 发送失败事件
                    if self._message_bus:
                        await self._message_bus.publish(FlowEvent(
                            event_type=EventTypes.TASK_FAILED,
                            source=self.engine_id,
                            source_type="flow_engine",
                            payload={
                                "original_event": event.to_dict(),
                                "error": str(e),
                                "retries": retries - 1,
                            }
                        ))
                    raise
                
                logger.warning(
                    f"引擎 {self.engine_id} 处理事件失败，{self.retry_delay}秒后重试 ({retries}/{self.max_retries}): {e}"
                )
                await asyncio.sleep(self.retry_delay)
    
    async def _process_event_internal(self, event: FlowEvent) -> Optional[FlowEvent]:
        """内部事件处理逻辑"""
        logger.debug(f"引擎 {self.engine_id} 开始处理事件: {event.event_type} [{event.event_id}]")
        
        # 调用子类的处理逻辑
        result = await self.process(event)
        
        # 如果结果是 dict，包装成 FlowEvent
        if result is not None and isinstance(result, dict):
            result = FlowEvent(
                event_type=event.event_type,
                source=self.engine_id,
                source_type="flow_engine",
                payload=result
            )
        
        # 更新统计数据
        self._stats["events_processed"] += 1
        
        # 发送处理完成事件
        if self._message_bus:
            await self._message_bus.publish(FlowEvent(
                event_type=EventTypes.TASK_COMPLETED,
                source=self.engine_id,
                source_type="flow_engine",
                payload={
                    "original_event": event.to_dict(),
                    "result_event": result.to_dict() if result else None,
                }
            ))
        
        logger.debug(f"引擎 {self.engine_id} 完成处理事件: {event.event_type} [{event.event_id}]")
        return result
    
    @abstractmethod
    async def process(self, event: FlowEvent) -> Optional[FlowEvent]:
        """
        处理事件的核心逻辑（必须由子类实现）
        
        Args:
            event: 输入事件
            
        Returns:
            处理结果事件，如果无需返回则为 None
        """
        pass
    
    def _should_handle_event(self, event: FlowEvent) -> bool:
        """检查是否应该处理此事件"""
        # 如果未指定订阅类型，则处理所有事件
        if not self.subscribed_event_types:
            return True
        
        # 检查事件类型是否匹配
        for pattern in self.subscribed_event_types:
            if pattern == event.event_type:
                return True
            if pattern.endswith(".*") and event.event_type.startswith(pattern[:-2]):
                return True
        
        return False
    
    async def _send_heartbeat(self) -> None:
        """发送心跳事件"""
        if not self._message_bus:
            return
        
        heartbeat_event = FlowEvent(
            event_type=EventTypes.ENGINE_HEARTBEAT,
            source=self.engine_id,
            source_type="flow_engine",
            payload={
                "engine_id": self.engine_id,
                "engine_name": self.engine_name,
                "engine_version": self.engine_version,
                "stats": self.get_stats(),
                "timestamp": time.time(),
            }
        )
        
        await self._message_bus.publish(heartbeat_event)
        self._stats["last_heartbeat"] = time.time()
    
    async def send_heartbeat(self) -> None:
        """发送心跳（外部调用）"""
        await self._send_heartbeat()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取引擎统计信息"""
        stats = self._stats.copy()
        stats.update({
            "engine_id": self.engine_id,
            "engine_name": self.engine_name,
            "engine_version": self.engine_version,
            "running": self._running,
            "active_tasks": len(self._tasks),
            "subscribed_event_types": self.subscribed_event_types,
        })
        
        if stats["start_time"]:
            stats["uptime"] = time.time() - stats["start_time"]
        
        return stats
    
    def is_running(self) -> bool:
        """检查引擎是否在运行"""
        return self._running
    
    @asynccontextmanager
    async def lifecycle(self):
        """提供引擎生命周期的上下文管理器"""
        try:
            await self.start()
            yield self
        finally:
            await self.stop()


class SimpleFlowEngine(FlowEngine):
    """简单的 Flow Engine 实现，支持回调函数"""
    
    def __init__(
        self,
        engine_id: str,
        process_callback: Callable[[FlowEvent], Optional[FlowEvent]],
        subscribed_event_types: Optional[List[str]] = None,
        **kwargs
    ):
        """
        初始化简单引擎
        
        Args:
            engine_id: 引擎ID
            process_callback: 事件处理回调函数
            subscribed_event_types: 订阅的事件类型列表
            **kwargs: 传递给父类的参数
        """
        self.engine_id = engine_id
        self._process_callback = process_callback
        
        if subscribed_event_types:
            self.subscribed_event_types = subscribed_event_types
        
        super().__init__(**kwargs)
    
    async def process(self, event: FlowEvent) -> Optional[FlowEvent]:
        """调用回调函数处理事件"""
        # 回调函数可能是同步或异步的
        if asyncio.iscoroutinefunction(self._process_callback):
            return await self._process_callback(event)
        else:
            return self._process_callback(event)


class FlowEngineRegistry:
    """Flow Engine 注册表"""
    
    def __init__(self):
        self._engines: Dict[str, FlowEngine] = {}
    
    def register(self, engine: FlowEngine) -> None:
        """注册引擎"""
        if engine.engine_id in self._engines:
            raise ValueError(f"引擎 ID 已存在: {engine.engine_id}")
        
        self._engines[engine.engine_id] = engine
        logger.info(f"注册引擎: {engine.engine_id}")
    
    def unregister(self, engine_id: str) -> Optional[FlowEngine]:
        """注销引擎"""
        engine = self._engines.pop(engine_id, None)
        if engine:
            logger.info(f"注销引擎: {engine_id}")
        return engine
    
    def get(self, engine_id: str) -> Optional[FlowEngine]:
        """获取引擎"""
        return self._engines.get(engine_id)
    
    def get_all(self) -> List[FlowEngine]:
        """获取所有引擎"""
        return list(self._engines.values())
    
    def get_by_event_type(self, event_type: str) -> List[FlowEngine]:
        """获取订阅指定事件类型的引擎"""
        matching_engines = []
        
        for engine in self._engines.values():
            if not engine.subscribed_event_types:  # 订阅所有事件
                matching_engines.append(engine)
                continue
            
            for pattern in engine.subscribed_event_types:
                if pattern == event_type:
                    matching_engines.append(engine)
                    break
                if pattern.endswith(".*") and event_type.startswith(pattern[:-2]):
                    matching_engines.append(engine)
                    break
        
        return matching_engines
    
    async def start_all(self) -> None:
        """启动所有引擎"""
        tasks = [engine.start() for engine in self._engines.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def stop_all(self) -> None:
        """停止所有引擎"""
        tasks = [engine.stop() for engine in self._engines.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    def clear(self) -> None:
        """清空注册表"""
        self._engines.clear()