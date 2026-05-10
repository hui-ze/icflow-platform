"""
消息总线抽象基类
"""

import abc
import asyncio
import logging
from typing import Callable, Optional, Any, Dict, List
from dataclasses import dataclass

from ..core.flow_event import FlowEvent


logger = logging.getLogger(__name__)


@dataclass
class Subscription:
    """消息订阅"""
    id: str
    event_type: str
    callback: Callable[[FlowEvent], Any]
    queue_name: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class MessageBus(abc.ABC):
    """消息总线抽象基类"""
    
    @abc.abstractmethod
    async def publish(self, event: FlowEvent) -> None:
        """
        发布事件
        
        Args:
            event: 要发布的事件
        """
        pass
    
    @abc.abstractmethod
    async def subscribe(
        self,
        event_type: str,
        callback: Callable[[FlowEvent], Any],
        queue_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Subscription:
        """
        订阅事件
        
        Args:
            event_type: 事件类型（支持通配符）
            callback: 事件处理回调函数
            queue_name: 队列名称（用于分组消费）
            metadata: 订阅元数据
            
        Returns:
            订阅对象
        """
        pass
    
    @abc.abstractmethod
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        取消订阅
        
        Args:
            subscription_id: 订阅ID
            
        Returns:
            是否成功取消订阅
        """
        pass
    
    @abc.abstractmethod
    async def start(self) -> None:
        """启动消息总线"""
        pass
    
    @abc.abstractmethod
    async def stop(self) -> None:
        """停止消息总线"""
        pass
    
    @abc.abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """获取消息总线统计信息"""
        pass