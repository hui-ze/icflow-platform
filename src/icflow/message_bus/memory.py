"""
内存消息总线实现
基于 asyncio.Queue 的简单实现，适用于单进程场景
"""

import asyncio
import logging
import uuid
import time
from typing import Callable, Optional, Dict, Any, List, Set
from collections import defaultdict

from .base import MessageBus, Subscription
from ..core.flow_event import FlowEvent


logger = logging.getLogger(__name__)


class MemoryMessageBus(MessageBus):
    """内存消息总线"""
    
    def __init__(self, max_queue_size: int = 1000):
        """
        初始化内存消息总线
        
        Args:
            max_queue_size: 每个队列的最大大小
        """
        self.max_queue_size = max_queue_size
        self._running = False
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self._subscription_by_id: Dict[str, Subscription] = {}
        self._queues: Dict[str, asyncio.Queue] = {}
        self._consumer_tasks: Set[asyncio.Task] = set()
        
        # 统计信息
        self._stats = {
            "events_published": 0,
            "events_delivered": 0,
            "events_dropped": 0,
            "subscriptions_active": 0,
            "start_time": None,
        }
    
    async def publish(self, event: FlowEvent) -> None:
        """发布事件"""
        if not self._running:
            logger.warning("消息总线未运行，事件将被丢弃")
            return
        
        self._stats["events_published"] += 1
        logger.debug(f"发布事件: {event.event_type} [{event.event_id}]")
        
        # 查找匹配的订阅
        delivered = False
        event_type = event.event_type
        
        # 支持通配符匹配
        matching_subscriptions = []
        
        # 直接匹配
        if event_type in self._subscriptions:
            matching_subscriptions.extend(self._subscriptions[event_type])
        
        # 通配符匹配（如 "engine.*"）
        for pattern, subscriptions in self._subscriptions.items():
            if pattern.endswith(".*") and event_type.startswith(pattern[:-2]):
                matching_subscriptions.extend(subscriptions)
        
        # 如果没有订阅者，记录并返回
        if not matching_subscriptions:
            logger.debug(f"事件 {event.event_type} 没有订阅者")
            return
        
        # 按队列分组订阅
        queue_subscriptions = defaultdict(list)
        for sub in matching_subscriptions:
            queue_name = sub.queue_name or "default"
            queue_subscriptions[queue_name].append(sub)
        
        # 将事件投递到每个队列
        for queue_name, subscriptions in queue_subscriptions.items():
            # 获取或创建队列
            if queue_name not in self._queues:
                self._queues[queue_name] = asyncio.Queue(maxsize=self.max_queue_size)
                # 启动该队列的消费者任务
                self._start_consumer_for_queue(queue_name)
            
            queue = self._queues[queue_name]
            
            # 尝试投递事件（非阻塞）
            try:
                queue.put_nowait({
                    "event": event,
                    "subscriptions": subscriptions
                })
                delivered = True
                self._stats["events_delivered"] += 1
            except asyncio.QueueFull:
                logger.warning(f"队列 {queue_name} 已满，事件被丢弃")
                self._stats["events_dropped"] += 1
        
        if delivered:
            logger.debug(f"事件 {event.event_type} 已投递到 {len(queue_subscriptions)} 个队列")
    
    async def subscribe(
        self,
        event_type: str,
        callback: Callable[[FlowEvent], Any],
        queue_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Subscription:
        """订阅事件"""
        subscription_id = str(uuid.uuid4())
        subscription = Subscription(
            id=subscription_id,
            event_type=event_type,
            callback=callback,
            queue_name=queue_name,
            metadata=metadata or {}
        )
        
        # 存储订阅
        self._subscriptions[event_type].append(subscription)
        self._subscription_by_id[subscription_id] = subscription
        self._stats["subscriptions_active"] += 1
        
        logger.info(f"创建订阅: {event_type} -> {subscription_id} (queue: {queue_name})")
        return subscription
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """取消订阅"""
        subscription = self._subscription_by_id.pop(subscription_id, None)
        if not subscription:
            return False
        
        # 从订阅列表中移除
        event_type = subscription.event_type
        if event_type in self._subscriptions:
            self._subscriptions[event_type] = [
                sub for sub in self._subscriptions[event_type]
                if sub.id != subscription_id
            ]
            
            # 如果该事件类型没有订阅者了，清理空列表
            if not self._subscriptions[event_type]:
                del self._subscriptions[event_type]
        
        self._stats["subscriptions_active"] -= 1
        logger.info(f"取消订阅: {subscription_id}")
        return True
    
    async def start(self) -> None:
        """启动消息总线"""
        if self._running:
            logger.warning("消息总线已经在运行")
            return
        
        logger.info("启动内存消息总线")
        self._running = True
        self._stats["start_time"] = time.time()
        
        # 为每个队列启动消费者任务
        for queue_name in list(self._queues.keys()):
            self._start_consumer_for_queue(queue_name)
        
        logger.info("内存消息总线启动完成")
    
    async def stop(self) -> None:
        """停止消息总线"""
        if not self._running:
            logger.warning("消息总线未在运行")
            return
        
        logger.info("停止内存消息总线")
        self._running = False
        
        # 取消所有消费者任务
        for task in self._consumer_tasks:
            if not task.done():
                task.cancel()
        
        # 等待任务完成
        if self._consumer_tasks:
            await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
        self._consumer_tasks.clear()
        
        # 清空队列
        for queue in self._queues.values():
            while not queue.empty():
                try:
                    queue.get_nowait()
                except:
                    pass
        
        logger.info("内存消息总线停止完成")
    
    def _start_consumer_for_queue(self, queue_name: str) -> None:
        """为指定队列启动消费者任务"""
        if queue_name not in self._queues:
            return
        
        async def consumer():
            queue = self._queues[queue_name]
            logger.debug(f"启动队列消费者: {queue_name}")
            
            while self._running:
                try:
                    # 从队列获取事件（支持超时，以便检查运行状态）
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    
                    event = item["event"]
                    subscriptions = item["subscriptions"]
                    
                    # 为每个订阅调用回调函数
                    tasks = []
                    for subscription in subscriptions:
                        task = asyncio.create_task(
                            self._call_subscription_callback(subscription, event)
                        )
                        tasks.append(task)
                    
                    # 等待所有回调完成
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # 标记任务完成
                    queue.task_done()
                    
                except asyncio.CancelledError:
                    logger.debug(f"队列消费者被取消: {queue_name}")
                    break
                except Exception as e:
                    logger.error(f"队列消费者处理异常: {e}", exc_info=True)
                    await asyncio.sleep(0.1)  # 避免快速循环
            
            logger.debug(f"队列消费者退出: {queue_name}")
        
        task = asyncio.create_task(consumer())
        self._consumer_tasks.add(task)
        task.add_done_callback(lambda t: self._consumer_tasks.discard(t))
    
    async def _call_subscription_callback(self, subscription: Subscription, event: FlowEvent) -> None:
        """调用订阅回调函数"""
        try:
            # 回调函数可能是同步或异步的
            if asyncio.iscoroutinefunction(subscription.callback):
                await subscription.callback(event)
            else:
                subscription.callback(event)
            
            logger.debug(f"回调执行成功: {subscription.id} -> {event.event_type}")
        except Exception as e:
            logger.error(f"订阅回调执行失败: {subscription.id} -> {event.event_type}: {e}", exc_info=True)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self._stats.copy()
        stats.update({
            "running": self._running,
            "queues_count": len(self._queues),
            "subscriptions_by_type": {
                event_type: len(subs)
                for event_type, subs in self._subscriptions.items()
            },
            "consumer_tasks": len(self._consumer_tasks),
        })
        
        if stats["start_time"]:
            stats["uptime"] = time.time() - stats["start_time"]
        
        return stats
    
    def create_queue(self, queue_name: str) -> None:
        """创建新队列"""
        if queue_name not in self._queues:
            self._queues[queue_name] = asyncio.Queue(maxsize=self.max_queue_size)
            
            # 如果总线正在运行，为该队列启动消费者
            if self._running:
                self._start_consumer_for_queue(queue_name)
    
    def remove_queue(self, queue_name: str) -> None:
        """移除队列"""
        if queue_name in self._queues:
            # 清空队列
            queue = self._queues[queue_name]
            while not queue.empty():
                try:
                    queue.get_nowait()
                except:
                    pass
            
            del self._queues[queue_name]
            logger.info(f"移除队列: {queue_name}")