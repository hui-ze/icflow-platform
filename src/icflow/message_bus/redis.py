"""
Redis 消息总线实现 (RedisMessageBus)

基于 Redis Pub/Sub + Stream 的跨进程消息总线。
支持事件发布-订阅、通配符模式匹配、队列分组。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Union

from redis.asyncio import Redis, ConnectionPool

from .base import MessageBus, Subscription
from ..core.flow_event import FlowEvent

logger = logging.getLogger(__name__)


class RedisMessageBus(MessageBus):
    """
    Redis 消息总线
    
    基于 Redis Pub/Sub 的事件发布-订阅实现。
    支持精确匹配和通配符模式（如 "test.*"）订阅。
    使用 Redis Stream 作为事件持久化存储（可选）。
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str = "",
        max_connections: int = 10,
        event_ttl: int = 86400,  # 事件保留时间（秒）
        stream_enabled: bool = False,  # 是否启用 Stream 持久化
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.max_connections = max_connections
        self.event_ttl = event_ttl
        self.stream_enabled = stream_enabled
        
        # Redis 连接
        self._pool: Optional[ConnectionPool] = None
        self._redis: Optional[Redis] = None
        self._pubsub: Optional = None  # PubSub 监听器
        
        # 订阅管理
        self._subscriptions: Dict[str, List[Subscription]] = {}
        self._subscription_by_id: Dict[str, Subscription] = {}
        
        # 通配符模式缓存
        self._wildcard_patterns: List[str] = []
        
        # 消费者任务
        self._consumer_tasks: Set[asyncio.Task] = set()
        
        # 状态
        self._running = False
        
        # 统计
        self._stats = {
            "events_published": 0,
            "events_delivered": 0,
            "events_dropped": 0,
            "subscriptions_active": 0,
            "start_time": None,
        }
        
        logger.info(f"RedisMessageBus 初始化: {host}:{port}/{db}")
    
    async def _connect(self) -> None:
        """连接 Redis"""
        self._pool = ConnectionPool(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password if self.password else None,
            max_connections=self.max_connections,
            decode_responses=True,
        )
        self._redis = Redis(connection_pool=self._pool)
        self._pubsub = self._redis.pubsub()
        
        # 测试连接
        await self._redis.ping()
        logger.info("Redis 连接成功")
    
    async def start(self) -> None:
        """启动消息总线"""
        if self._running:
            logger.warning("Redis消息总线已经在运行")
            return
        
        await self._connect()
        self._running = True
        self._stats["start_time"] = time.time()
        
        # 启动 PubSub 监听器
        task = asyncio.create_task(self._pubsub_listener())
        self._consumer_tasks.add(task)
        task.add_done_callback(self._consumer_tasks.discard)
        
        logger.info("Redis消息总线已启动")
    
    async def stop(self) -> None:
        """停止消息总线"""
        if not self._running:
            return
        
        self._running = False
        
        # 取消消费者任务
        for task in self._consumer_tasks:
            if not task.done():
                task.cancel()
        if self._consumer_tasks:
            await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
        self._consumer_tasks.clear()
        
        # 关闭连接
        if self._pubsub:
            await self._pubsub.close()
        if self._pool:
            await self._pool.disconnect()
        
        logger.info("Redis消息总线已停止")
    
    async def publish(self, event: FlowEvent) -> None:
        """发布事件到 Redis"""
        if not self._redis or not self._running:
            logger.warning("Redis消息总线未运行或未连接")
            return
        
        # 序列化事件
        event_data = json.dumps(event.to_dict())
        
        # 1. 通过 Pub/Sub 实时分发
        channel = f"event:{event.event_type}"
        await self._redis.publish(channel, event_data)
        
        # 2. 如果有通配符订阅，也发到通配符通道
        for pattern in self._wildcard_patterns:
            pattern_channel = f"event:{pattern.replace('*', '_wildcard_')}"
            await self._redis.publish(pattern_channel, event_data)
        
        # 3. 可选：持久化到 Stream
        if self.stream_enabled:
            stream_key = "icflow:events"
            await self._redis.xadd(
                stream_key,
                {"event_type": event.event_type, "data": event_data},
                maxlen=10000,  # 最多保留 10000 条
            )
            await self._redis.expire(stream_key, self.event_ttl)
        
        # 4. 本地统计
        self._stats["events_published"] += 1
        self._stats["events_delivered"] += await self._count_subscribers(event.event_type)
        
        logger.debug(f"事件已发布: {event.event_type} [{event.event_id}]")
    
    async def _count_subscribers(self, event_type: str) -> int:
        """计算匹配该事件类型的订阅者数量"""
        count = 0
        for et, subs in self._subscriptions.items():
            if et == event_type:
                count += len(subs)
            elif et.endswith(".*") and event_type.startswith(et[:-2]):
                count += len(subs)
        return count
    
    async def subscribe(
        self,
        event_type: str,
        callback: Callable,
        queue_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        订阅事件
        
        Args:
            event_type: 事件类型（支持通配符，如 "test.*"）
            callback: 事件处理回调
            queue_name: 队列分组名称
            metadata: 附加元数据
            
        Returns:
            订阅 ID
        """
        sub_id = str(uuid.uuid4())
        
        subscription = Subscription(
            id=sub_id,
            event_type=event_type,
            callback=callback,
            queue_name=queue_name or "default",
            metadata=metadata or {},
        )
        
        # 本地管理
        if event_type not in self._subscriptions:
            self._subscriptions[event_type] = []
        self._subscriptions[event_type].append(subscription)
        self._subscription_by_id[sub_id] = subscription
        
        # 记录通配符订阅
        if event_type.endswith(".*"):
            if event_type not in self._wildcard_patterns:
                self._wildcard_patterns.append(event_type)
        
        # 如果是运行中状态，订阅 Redis 频道
        if self._redis and self._pubsub and self._running:
            channel = f"event:{event_type}"
            await self._pubsub.subscribe(channel)
            
            # 通配符频道路由：使用专门的通道接收
            if event_type.endswith(".*"):
                pattern_channel = f"event:{event_type.replace('*', '_wildcard_')}"
                await self._pubsub.subscribe(pattern_channel)
        
        self._stats["subscriptions_active"] += 1
        
        logger.debug(f"订阅已创建: {event_type} [{sub_id[:8]}]")
        return sub_id
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """取消订阅"""
        sub = self._subscription_by_id.pop(subscription_id, None)
        if not sub:
            return False
        
        # 从列表中移除
        if sub.event_type in self._subscriptions:
            self._subscriptions[sub.event_type] = [
                s for s in self._subscriptions[sub.event_type]
                if s.id != subscription_id
            ]
            if not self._subscriptions[sub.event_type]:
                del self._subscriptions[sub.event_type]
                
                # 清理通配符缓存
                if sub.event_type in self._wildcard_patterns:
                    self._wildcard_patterns.remove(sub.event_type)
        
        self._stats["subscriptions_active"] -= 1
        
        logger.debug(f"订阅已取消: {sub.event_type} [{subscription_id[:8]}]")
        return True
    
    async def _pubsub_listener(self) -> None:
        """PubSub 消息监听循环"""
        if not self._pubsub:
            return
        
        try:
            async for message in self._pubsub.listen():
                if not self._running:
                    break
                
                if message["type"] != "message":
                    continue
                
                # 解析频道和事件数据
                channel = message["channel"]
                data = message["data"]
                
                try:
                    event_dict = json.loads(data)
                    event = FlowEvent.from_dict(event_dict)
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"事件反序列化失败: {e}")
                    continue
                
                # 从频道名提取事件类型
                channel_type = channel.replace("event:", "", 1)
                channel_type = channel_type.replace("_wildcard_", "*")
                
                # 查找匹配的本地订阅并调用回调
                await self._dispatch_to_subscribers(event, channel_type)
        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"PubSub 监听异常: {e}", exc_info=True)
    
    async def _dispatch_to_subscribers(self, event: FlowEvent, channel_type: str) -> None:
        """分发事件给匹配的订阅者"""
        tasks = []
        
        # 精确匹配
        for sub in self._subscriptions.get(channel_type, []):
            task = self._call_callback(sub, event)
            tasks.append(task)
        
        # 通配符匹配
        for et, subs in self._subscriptions.items():
            if et.endswith(".*") and channel_type.startswith(et[:-2]):
                for sub in subs:
                    tasks.append(self._call_callback(sub, event))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _call_callback(self, subscription: Subscription, event: FlowEvent) -> None:
        """调用订阅回调"""
        try:
            if asyncio.iscoroutinefunction(subscription.callback):
                await subscription.callback(event)
            else:
                subscription.callback(event)
        except Exception as e:
            logger.error(f"回调执行失败 [{subscription.id[:8]}]: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = dict(self._stats)
        stats.update({
            "running": self._running,
            "redis_host": self.host,
            "redis_port": self.port,
            "subscriptions_by_type": {
                et: len(subs) for et, subs in self._subscriptions.items()
            },
            "consumer_tasks": len(self._consumer_tasks),
        })
        
        if stats["start_time"]:
            stats["uptime"] = time.time() - stats["start_time"]
        
        return stats
