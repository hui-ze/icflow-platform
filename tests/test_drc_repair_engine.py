"""
DRC修复主引擎测试
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.icflow.engines.drc_repair import DRCRepairMasterEngine
from src.icflow.core.flow_event import FlowEvent
from src.icflow.core.concrete_events import (
    DesignFlowEventTypes,
    ToolExecutionEventTypes,
    KnowledgeCaptureEventTypes,
)
from src.icflow.message_bus.memory import MemoryMessageBus


class TestDRCRepairMasterEngine:
    """DRC修复主引擎测试类"""
    
    @pytest.fixture
    def message_bus(self):
        """创建消息总线fixture"""
        return MemoryMessageBus()
    
    @pytest.fixture
    def engine(self, message_bus):
        """创建引擎fixture"""
        # 创建引擎
        engine = DRCRepairMasterEngine({"default_tool": "calibre"})
        # 设置消息总线
        engine.message_bus = message_bus
        return engine
    
    @pytest.mark.asyncio
    async def test_engine_initialization(self, engine):
        """测试引擎初始化"""
        assert engine.engine_id == "drc_repair_master_engine"
        assert engine.default_tool == "calibre"
        assert isinstance(engine.repair_strategies, dict)
        assert engine.active_repairs == {}
        assert engine.repair_history == []
        
        # 检查是否订阅了正确的事件类型
        assert DesignFlowEventTypes.DRC_VIOLATION_DETECTED in engine.subscribed_event_types
    
    @pytest.mark.asyncio
    async def test_start_stop(self, engine):
        """测试启动和停止"""
        await engine.start()
        assert engine.is_running()
        
        await engine.stop()
        assert not engine.is_running()
    
    @pytest.mark.asyncio
    @patch.object(DRCRepairMasterEngine, '_verify_repair_result', new_callable=AsyncMock, return_value=True)
    async def test_handle_drc_violation(self, mock_verify, engine, message_bus):
        """测试处理DRC违例事件"""
        # 启动消息总线和引擎
        await message_bus.start()
        await engine.start()
        
        # 创建DRC违例事件
        drc_event = FlowEvent(
            event_type=DesignFlowEventTypes.DRC_VIOLATION_DETECTED,
            source="test_source",
            payload={
                "task_id": "task_001",
                "violation_id": "viol_001",
                "violation_type": "min_width",
                "location": {"x": 100, "y": 200, "layer": "metal1"},
                "rule_description": "最小宽度违例"
            }
        )
        
        # 监听发布的事件
        published_events = []
        
        async def capture_event(event):
            published_events.append(event)
        
        # 订阅所有事件类型
        for event_type in [
            ToolExecutionEventTypes.TOOL_STARTED,
            ToolExecutionEventTypes.TOOL_COMPLETED,
            KnowledgeCaptureEventTypes.ENGINEER_DECISION
        ]:
            await message_bus.subscribe(event_type, capture_event)
        
        # 处理事件
        await engine.process(drc_event)
        
        # 等待异步操作完成
        await asyncio.sleep(0.1)
        
        # 验证事件发布
        assert len(published_events) >= 2  # 至少包括工具开始和完成事件
        
        # 验证工具执行事件
        tool_start_events = [e for e in published_events 
                           if e.event_type == ToolExecutionEventTypes.TOOL_STARTED]
        assert len(tool_start_events) == 1
        assert tool_start_events[0].payload["tool_name"] == "calibre"
        
        tool_complete_events = [e for e in published_events 
                              if e.event_type == ToolExecutionEventTypes.TOOL_COMPLETED]
        assert len(tool_complete_events) == 1
        
        # 验证知识捕获事件
        knowledge_events = [e for e in published_events 
                          if e.event_type == KnowledgeCaptureEventTypes.ENGINEER_DECISION]
        assert len(knowledge_events) >= 1
        
        # 验证修复状态
        assert "task_001" not in engine.active_repairs  # 修复已完成
        assert len(engine.repair_history) == 1
        
        # 停止引擎
        await engine.stop()
    
    @pytest.mark.asyncio
    async def test_select_repair_strategy(self, engine):
        """测试修复策略选择"""
        # 已知策略
        assert engine._select_repair_strategy("min_width") == "auto_widen"
        assert engine._select_repair_strategy("min_spacing") == "auto_move"
        assert engine._select_repair_strategy("min_area") == "auto_fill"
        assert engine._select_repair_strategy("notch") == "auto_adjust"
        
        # 未知策略 -> 手动审查
        assert engine._select_repair_strategy("unknown_type") == "manual_review"
    
    @pytest.mark.asyncio
    async def test_publish_knowledge_capture(self, engine, message_bus):
        """测试发布知识捕获事件"""
        await message_bus.start()
        await engine.start()
        
        # 创建源事件
        source_event = FlowEvent(event_type="test.source", source="test")
        
        # 监听事件
        captured_events = []
        await message_bus.subscribe(KnowledgeCaptureEventTypes.ENGINEER_DECISION, 
                                   lambda e: captured_events.append(e))
        
        # 发布知识捕获事件
        await engine._publish_knowledge_capture(
            rule_id="rule_001",
            context={"key": "value"},
            decision_reason="测试决策",
            related_files=["file1.txt"],
            source_event=source_event
        )
        
        # 等待事件传播
        await asyncio.sleep(0.05)
        
        assert len(captured_events) == 1
        event = captured_events[0]
        assert event.event_type == KnowledgeCaptureEventTypes.ENGINEER_DECISION
        assert event.payload["rule_id"] == "rule_001"
        assert event.payload["decision_reason"] == "测试决策"
        assert event.context["source_event_id"] == source_event.event_id
        
        await engine.stop()
    
    @pytest.mark.asyncio
    async def test_execute_repair_tool(self, engine, message_bus):
        """测试执行修复工具"""
        await message_bus.start()
        await engine.start()
        
        # 监听工具执行事件
        tool_events = []
        await message_bus.subscribe(ToolExecutionEventTypes.TOOL_STARTED,
                                   lambda e: tool_events.append(e))
        await message_bus.subscribe(ToolExecutionEventTypes.TOOL_COMPLETED,
                                   lambda e: tool_events.append(e))
        
        source_event = FlowEvent(event_type="test.source", source="test")
        
        await engine._execute_repair_tool(
            tool_name="calibre",
            violation_type="min_width",
            strategy="auto_widen",
            output_path="/tmp/test.log",
            source_event=source_event
        )
        
        # 等待异步操作
        await asyncio.sleep(0.1)
        
        assert len(tool_events) == 2
        assert tool_events[0].event_type == ToolExecutionEventTypes.TOOL_STARTED
        assert tool_events[1].event_type == ToolExecutionEventTypes.TOOL_COMPLETED
        
        # 验证命令参数
        assert "calibre" in tool_events[0].payload["command_line"]
        
        await engine.stop()
    
    @pytest.mark.asyncio
    async def test_verify_repair_result(self, engine):
        """测试验证修复结果"""
        # 这个方法使用了随机性，但我们至少可以调用它
        result = await engine._verify_repair_result(
            task_id="task_001",
            violation_id="viol_001",
            output_path="/tmp/test.log"
        )
        
        assert isinstance(result, bool)
    
    @pytest.mark.asyncio
    async def test_get_stats(self, engine):
        """测试获取统计信息"""
        stats = engine.get_stats()
        
        assert stats["engine_id"] == "drc_repair_master_engine"
        assert stats["active_repairs"] == 0
        assert stats["total_repaired"] == 0
        assert stats["total_failed"] == 0
        assert stats["repair_history_count"] == 0
    
    @pytest.mark.asyncio
    async def test_complete_and_fail_repair(self, engine, message_bus):
        """测试完成和失败修复流程"""
        await message_bus.start()
        await engine.start()
        
        # 创建源事件
        source_event = FlowEvent(
            event_type=DesignFlowEventTypes.DRC_VIOLATION_DETECTED,
            source="test",
            payload={"task_id": "task_001", "violation_id": "viol_001"}
        )
        
        # 监听知识捕获事件
        knowledge_events = []
        await message_bus.subscribe(KnowledgeCaptureEventTypes.ENGINEER_DECISION,
                                   lambda e: knowledge_events.append(e))
        
        # 测试完成修复
        await engine._complete_repair("task_001", "viol_001", source_event)
        
        await asyncio.sleep(0.05)
        
        assert len(knowledge_events) == 1
        assert "成功" in knowledge_events[0].payload["decision_reason"]
        
        # 重置
        knowledge_events.clear()
        
        # 测试失败修复
        await engine._fail_repair("task_002", "viol_002", source_event)
        
        await asyncio.sleep(0.05)
        
        assert len(knowledge_events) == 1
        assert "失败" in knowledge_events[0].payload["decision_reason"]
        
        await engine.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])