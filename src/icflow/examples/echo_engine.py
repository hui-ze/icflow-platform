"""
示例：Echo Flow Engine
接收事件并返回带前缀的响应
"""

import asyncio
import logging
from typing import Optional

from ..core.flow_engine import FlowEngine
from ..core.flow_event import FlowEvent


logger = logging.getLogger(__name__)


class EchoEngine(FlowEngine):
    """Echo Engine - 回声引擎示例"""
    
    engine_id = "echo-engine"
    engine_name = "Echo Engine"
    engine_description = "接收事件并返回带前缀的响应"
    
    # 订阅所有事件（示例）
    subscribed_event_types = ["echo.*", "test.*"]
    
    def __init__(self, prefix: str = "Echo: ", **kwargs):
        """
        初始化 Echo Engine
        
        Args:
            prefix: 回声前缀
            **kwargs: 传递给父类的参数
        """
        super().__init__(**kwargs)
        self.prefix = prefix
        self.echo_count = 0
    
    async def process(self, event: FlowEvent) -> Optional[FlowEvent]:
        """处理事件：添加前缀并返回"""
        self.echo_count += 1
        
        # 获取原始消息
        message = event.payload.get("message", "No message provided")
        
        # 创建回声响应
        echo_message = f"{self.prefix}{message} (count: {self.echo_count})"
        
        # 记录日志
        logger.info(f"Echo Engine 处理事件: {event.event_type} -> {echo_message}")
        
        # 创建响应事件
        response_event = event.copy(
            event_type=f"echo.response",
            source=self.engine_id,
            payload={
                "original_message": message,
                "echo_message": echo_message,
                "echo_count": self.echo_count,
                "original_event_id": event.event_id,
            },
            metadata={
                "processed_by": self.engine_id,
                "processing_time": event.age,
            }
        )
        
        return response_event
    
    async def on_start(self) -> None:
        """引擎启动时的自定义逻辑"""
        logger.info(f"Echo Engine 启动，前缀: '{self.prefix}'")
        
        # 发送启动完成事件
        if self.message_bus:
            await self.message_bus.publish(FlowEvent(
                event_type="engine.echo.ready",
                source=self.engine_id,
                payload={
                    "prefix": self.prefix,
                    "timestamp": asyncio.get_event_loop().time(),
                }
            ))
    
    async def on_stop(self) -> None:
        """引擎停止时的自定义逻辑"""
        logger.info(f"Echo Engine 停止，总计处理 {self.echo_count} 个回声")
    
    def get_stats(self) -> dict:
        """获取引擎统计信息（扩展父类）"""
        stats = super().get_stats()
        stats.update({
            "echo_count": self.echo_count,
            "prefix": self.prefix,
        })
        return stats


# 使用示例
async def example_usage():
    """使用示例"""
    from ..message_bus.memory import MemoryMessageBus
    
    # 创建消息总线和引擎
    message_bus = MemoryMessageBus()
    engine = EchoEngine(prefix="Received: ")
    engine.message_bus = message_bus
    
    # 启动消息总线和引擎
    await message_bus.start()
    await engine.start()
    
    # 创建测试事件
    test_event = FlowEvent(
        event_type="echo.test",
        source="test-client",
        payload={"message": "Hello, IC-Flow!"}
    )
    
    # 订阅响应事件
    responses = []
    
    async def response_handler(event: FlowEvent):
        responses.append(event)
        print(f"收到响应: {event.payload.get('echo_message')}")
    
    await message_bus.subscribe("echo.response", response_handler)
    
    # 发布事件
    await message_bus.publish(test_event)
    
    # 等待响应
    await asyncio.sleep(0.5)
    
    # 停止引擎和消息总线
    await engine.stop()
    await message_bus.stop()
    
    print(f"总计处理事件: {engine.echo_count}")
    print(f"收到响应数: {len(responses)}")
    
    return engine, responses


if __name__ == "__main__":
    # 运行示例
    asyncio.run(example_usage())