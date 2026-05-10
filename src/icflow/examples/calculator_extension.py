"""
示例：Calculator Extension
提供基本数学运算能力的扩展
"""

import logging
from typing import Dict, Any

from ..extensions.base import Extension, ExtensionCapability


logger = logging.getLogger(__name__)


class CalculatorExtension(Extension):
    """Calculator Extension - 计算器扩展示例"""
    
    extension_id = "calculator"
    name = "Calculator Extension"
    description = "提供基本数学运算能力"
    version = "1.0.0"
    
    # 扩展能力
    capabilities = [
        ExtensionCapability.DATA_TRANSFORMATION,
        ExtensionCapability.TOOL_CALL,
    ]
    
    # 元数据
    metadata = {
        "author": "IC-Flow Team",
        "category": "utility",
        "tags": ["math", "calculator", "utility"],
    }
    
    def __init__(self, **kwargs):
        """初始化计算器扩展"""
        super().__init__(**kwargs)
        self.operation_count = 0
    
    async def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """处理计算请求"""
        operation = request.get("operation")
        operands = request.get("operands", [])
        
        if not operation:
            raise ValueError("必须指定操作类型 (operation)")
        
        if not operands:
            raise ValueError("必须提供操作数 (operands)")
        
        # 执行计算
        result = self._calculate(operation, operands)
        self.operation_count += 1
        
        # 记录日志
        logger.info(f"Calculator Extension 执行操作: {operation}{operands} = {result}")
        
        # 发送计算完成事件
        if self.message_bus:
            await self.generate_event(
                "calculator.operation_completed",
                {
                    "operation": operation,
                    "operands": operands,
                    "result": result,
                    "operation_count": self.operation_count,
                }
            )
        
        return {
            "operation": operation,
            "operands": operands,
            "result": result,
            "operation_count": self.operation_count,
        }
    
    def _calculate(self, operation: str, operands: list) -> float:
        """执行具体计算"""
        if operation == "add":
            return sum(operands)
        elif operation == "subtract":
            if len(operands) != 2:
                raise ValueError("减法操作需要两个操作数")
            return operands[0] - operands[1]
        elif operation == "multiply":
            result = 1
            for num in operands:
                result *= num
            return result
        elif operation == "divide":
            if len(operands) != 2:
                raise ValueError("除法操作需要两个操作数")
            if operands[1] == 0:
                raise ValueError("除数不能为零")
            return operands[0] / operands[1]
        elif operation == "power":
            if len(operands) != 2:
                raise ValueError("幂运算需要两个操作数")
            return operands[0] ** operands[1]
        elif operation == "sqrt":
            if len(operands) != 1:
                raise ValueError("平方根运算需要一个操作数")
            if operands[0] < 0:
                raise ValueError("不能对负数求平方根")
            return operands[0] ** 0.5
        else:
            raise ValueError(f"不支持的操作类型: {operation}")
    
    async def on_start(self) -> None:
        """扩展启动时的自定义逻辑"""
        logger.info("Calculator Extension 启动")
        
        # 注册可用的操作
        self.available_operations = {
            "add": "加法 (a + b + ...)",
            "subtract": "减法 (a - b)",
            "multiply": "乘法 (a * b * ...)",
            "divide": "除法 (a / b)",
            "power": "幂运算 (a ^ b)",
            "sqrt": "平方根 (√a)",
        }
        
        # 发送扩展就绪事件
        if self.message_bus:
            await self.generate_event(
                "extension.calculator.ready",
                {
                    "operations": list(self.available_operations.keys()),
                    "version": self.version,
                }
            )
    
    async def on_stop(self) -> None:
        """扩展停止时的自定义逻辑"""
        logger.info(f"Calculator Extension 停止，总计执行 {self.operation_count} 次操作")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取扩展统计信息（扩展父类）"""
        stats = super().get_stats()
        stats.update({
            "operation_count": self.operation_count,
            "available_operations": list(self.available_operations.keys()) if hasattr(self, 'available_operations') else [],
        })
        return stats


# 使用示例
async def example_usage():
    """使用示例"""
    from ..message_bus.memory import MemoryMessageBus
    from ..extensions.base import ExtensionRegistry
    
    # 创建消息总线和扩展注册表
    message_bus = MemoryMessageBus()
    registry = ExtensionRegistry()
    
    # 创建并注册扩展
    calculator = CalculatorExtension()
    calculator.message_bus = message_bus
    registry.register(calculator)
    
    # 启动消息总线和扩展
    await message_bus.start()
    await calculator.start()
    
    # 执行计算请求
    requests = [
        {"operation": "add", "operands": [1, 2, 3, 4]},
        {"operation": "multiply", "operands": [2, 3, 4]},
        {"operation": "divide", "operands": [10, 2]},
        {"operation": "power", "operands": [2, 8]},
    ]
    
    results = []
    for req in requests:
        try:
            result = await calculator.handle_request(req)
            results.append(result)
            print(f"计算: {req['operation']}{req['operands']} = {result['result']}")
        except Exception as e:
            print(f"计算失败: {e}")
    
    # 显示统计信息
    print(f"\n扩展统计:")
    stats = calculator.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # 停止扩展和消息总线
    await calculator.stop()
    await message_bus.stop()
    
    return results


if __name__ == "__main__":
    # 运行示例
    import asyncio
    asyncio.run(example_usage())