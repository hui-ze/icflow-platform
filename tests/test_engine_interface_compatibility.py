"""
引擎接口兼容性测试（简化集成测试）

绕过消息总线，直接验证三个核心引擎的接口兼容性：
1. DRC修复引擎处理DRC违例事件，发布TOOL_STARTED事件
2. EDA工具适配器引擎处理TOOL_STARTED事件，执行工具，发布TOOL_COMPLETED事件
3. 知识管理引擎处理知识捕获事件（KnowledgeCaptureEventTypes），存储知识

测试方法：
- 使用Mock模拟消息总线，捕获引擎发布的事件
- 直接调用引擎的process方法，验证事件处理逻辑
- 手动将事件从一个引擎传递到另一个引擎，验证接口兼容性
"""

import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
import json

from src.icflow.engines.drc_repair import DRCRepairMasterEngine
from src.icflow.engines.eda_tool_adapter import EDAToolAdapterEngine
from src.icflow.engines.knowledge_management import KnowledgeManagementEngine
from src.icflow.core.flow_event import FlowEvent
from src.icflow.core.concrete_events import (
    DesignFlowEventTypes,
    ToolExecutionEventTypes,
    KnowledgeCaptureEventTypes,
    create_drc_violation_event,
    create_tool_started_event,
    create_tool_execution_event,
    create_knowledge_capture_event,
    create_knowledge_rule_violation_event,
)


class MockMessageBus:
    """模拟消息总线，用于捕获引擎发布的事件"""
    
    def __init__(self):
        self.published_events = []
        self.subscriptions = {}
    
    async def publish(self, event: FlowEvent):
        """记录发布的事件"""
        self.published_events.append(event)
    
    async def subscribe(self, event_type, callback):
        """记录订阅，但不实际处理"""
        if event_type not in self.subscriptions:
            self.subscriptions[event_type] = []
        self.subscriptions[event_type].append(callback)
    
    async def start(self):
        pass
    
    async def stop(self):
        pass
    
    def get_events_by_type(self, event_type):
        """获取指定类型的事件"""
        return [e for e in self.published_events if e.event_type == event_type]
    
    def clear_events(self):
        """清空记录的事件"""
        self.published_events.clear()


class TestEngineInterfaceCompatibility:
    """引擎接口兼容性测试类"""
    
    @pytest_asyncio.fixture
    async def mock_message_bus(self):
        """创建模拟消息总线"""
        bus = MockMessageBus()
        await bus.start()
        yield bus
        await bus.stop()
    
    @pytest_asyncio.fixture
    async def drc_engine(self, mock_message_bus):
        """创建DRC修复引擎，使用模拟消息总线"""
        engine = DRCRepairMasterEngine({"default_tool": "calibre"})
        engine.message_bus = mock_message_bus
        await engine.start()
        yield engine
        await engine.stop()
    
    @pytest_asyncio.fixture
    async def eda_engine(self, mock_message_bus):
        """创建EDA工具适配器引擎，使用模拟消息总线"""
        engine = EDAToolAdapterEngine({
            "tool_paths": {
                "calibre": "/mock/path/calibre",
            },
            "default_timeout": 5,
        })
        engine.message_bus = mock_message_bus
        await engine.start()
        yield engine
        await engine.stop()
    
    @pytest_asyncio.fixture
    async def knowledge_engine(self, mock_message_bus):
        """创建知识管理引擎，使用模拟消息总线"""
        engine = KnowledgeManagementEngine()
        engine.message_bus = mock_message_bus
        await engine.start()
        yield engine
        await engine.stop()
    
    def create_mock_process(self, stdout: str = "", stderr: str = "", returncode: int = 0):
        """创建模拟的异步进程对象"""
        mock_process = AsyncMock()
        mock_process.communicate.return_value = (stdout.encode() if stdout else b"", 
                                                 stderr.encode() if stderr else b"")
        mock_process.returncode = returncode
        return mock_process
    
    @pytest.mark.asyncio
    async def test_drc_engine_publishes_tool_started(self, drc_engine, mock_message_bus):
        """
        测试DRC引擎处理DRC违例事件后发布TOOL_STARTED事件
        
        注意：process() 会同步阻塞直到整个修复流程完成（含内部sleep），
        因此修复完成后 active_repairs 已被清理，检查 repair_history。
        """
        # 创建DRC违例事件
        drc_event = create_drc_violation_event(
            task_id="test_task_001",
            violation_id="violation_001",
            violation_type="min_width",
            location={"layer": "M1", "x": 100, "y": 200, "width": 0.08, "height": 0.5},
            rule_description="Minimum width violation: 0.08um < 0.1um",
            source="test_interface"
        )
        
        # 清空消息总线记录
        mock_message_bus.clear_events()
        
        # 验证事件类型匹配
        assert drc_event.event_type == DesignFlowEventTypes.DRC_VIOLATION_DETECTED
        assert DesignFlowEventTypes.DRC_VIOLATION_DETECTED in drc_engine.subscribed_event_types
        
        # 直接调用DRC引擎的process方法（它会同步阻塞直到整个修复流程完成）
        await drc_engine.process(drc_event)
        
        # 验证DRC引擎发布了TOOL_STARTED事件
        tool_started_events = mock_message_bus.get_events_by_type(ToolExecutionEventTypes.TOOL_STARTED)
        assert len(tool_started_events) == 1
        
        tool_started_event = tool_started_events[0]
        payload = tool_started_event.payload
        assert payload["tool_name"] == "calibre"
        assert "calibre -repair -type min_width" in payload["command_line"]
        
        # 验证DRC引擎也发布了TOOL_COMPLETED事件
        tool_completed_events = mock_message_bus.get_events_by_type(ToolExecutionEventTypes.TOOL_COMPLETED)
        assert len(tool_completed_events) == 1
        
        # 验证DRC引擎记录了修复历史（process()同步完成，修复已移至repair_history）
        assert len(drc_engine.repair_history) > 0
        history_entry = drc_engine.repair_history[-1]
        assert history_entry["task_id"] == "test_task_001"
        assert history_entry["result"] in ("success", "failure")
    
    @pytest.mark.asyncio
    async def test_eda_engine_processes_tool_started(self, eda_engine, mock_message_bus):
        """
        测试EDA引擎处理TOOL_STARTED事件并执行工具
        """
        # 创建TOOL_STARTED事件（模拟DRC引擎的输出）
        tool_event = create_tool_started_event(
            tool_name="calibre",
            command_line="calibre -repair -type min_width -strategy widen -out /tmp/repair.log",
            source="drc_repair_master_engine"
        )
        
        # Mock工具执行，避免实际调用外部工具
        with patch('asyncio.create_subprocess_shell') as mock_create_subprocess:
            mock_process = self.create_mock_process(
                stdout="DRC repair completed successfully",
                stderr="",
                returncode=0
            )
            mock_create_subprocess.return_value = mock_process
            
            # 清空消息总线记录
            mock_message_bus.clear_events()
            
            # 直接调用EDA引擎的process方法（通过 _handle_tool_started 创建异步任务执行工具）
            await eda_engine.process(tool_event)
            
            # 等待EDA引擎的异步任务完成
            await asyncio.sleep(0.2)
            
            # 验证EDA引擎执行了工具
            assert mock_create_subprocess.called
            
            # 获取实际调用的命令
            call_args = mock_create_subprocess.call_args
            assert call_args is not None
            command = call_args[0][0]
            assert "calibre" in command
            assert "-repair" in command
            
            # 验证EDA引擎发布了TOOL_COMPLETED事件
            tool_completed_events = mock_message_bus.get_events_by_type(ToolExecutionEventTypes.TOOL_COMPLETED)
            assert len(tool_completed_events) == 1
            
            tool_completed_event = tool_completed_events[0]
            payload = tool_completed_event.payload
            assert payload["tool_name"] == "calibre"
            assert payload["execution_status"] == "completed"
            assert payload["exit_code"] == 0
    
    @pytest.mark.asyncio
    async def test_knowledge_engine_captures_events(self, knowledge_engine, mock_message_bus):
        """
        测试知识管理引擎捕获知识捕获事件（KnowledgeCaptureEventTypes）
        
        知识管理引擎只订阅 KnowledgeCaptureEventTypes 类型的事件，
        因此测试使用知识捕获事件而非设计流程/工具执行事件。
        """
        # 创建规则违例知识捕获事件
        rule_violation_event = create_knowledge_rule_violation_event(
            rule_id="violation_002",
            context={"layer": "M2", "x": 300, "y": 400},
            related_files=["/path/to/layout.gds"],
            source="drc_repair_master_engine"
        )
        
        # 创建工程师决策知识捕获事件
        engineer_decision_event = create_knowledge_capture_event(
            event_type=KnowledgeCaptureEventTypes.ENGINEER_DECISION,
            rule_id="violation_003",
            context={"layer": "M1", "violation_type": "min_width"},
            decision_reason="自动选择修复策略: auto_widen",
            related_files=["/path/to/repair.log"],
            source="drc_repair_master_engine"
        )
        
        # 创建工具输出解析知识捕获事件
        tool_output_event = create_knowledge_capture_event(
            event_type=KnowledgeCaptureEventTypes.TOOL_OUTPUT_PARSED,
            rule_id="violation_004",
            context={"tool_name": "calibre"},
            decision_reason="工具输出解析完成",
            related_files=["/path/to/output.log"],
            source="eda_tool_adapter_engine",
            tool_name="calibre",
            output_summary="DRC repair completed successfully"
        )
        
        # 记录初始知识数量
        initial_count = knowledge_engine.knowledge_count
        
        # 清空消息总线记录
        mock_message_bus.clear_events()
        
        # 将每个知识捕获事件传递给知识管理引擎
        await knowledge_engine.process(rule_violation_event)
        await knowledge_engine.process(engineer_decision_event)
        await knowledge_engine.process(tool_output_event)
        
        # 等待异步处理完成
        await asyncio.sleep(0.1)
        
        # 验证知识数量增加（3个事件应产生3条知识）
        assert knowledge_engine.knowledge_count == initial_count + 3
        
        # 验证知识管理引擎发布了知识存储完成事件
        knowledge_stored_events = mock_message_bus.get_events_by_type(KnowledgeCaptureEventTypes.RULE_VIOLATION)
        assert len(knowledge_stored_events) >= 1
        
        engineer_stored_events = mock_message_bus.get_events_by_type(KnowledgeCaptureEventTypes.ENGINEER_DECISION)
        assert len(engineer_stored_events) >= 1
        
        tool_output_stored_events = mock_message_bus.get_events_by_type(KnowledgeCaptureEventTypes.TOOL_OUTPUT_PARSED)
        assert len(tool_output_stored_events) >= 1
        
        # 验证知识库中包含了相关事件的知识
        assert len(knowledge_engine.knowledge_store) == 3
    
    @pytest.mark.asyncio
    async def test_engine_chain_compatibility(self, drc_engine, eda_engine, knowledge_engine, mock_message_bus):
        """
        测试三个引擎的链式兼容性：手动传递事件验证完整流程
        
        流程：
        1. DRC引擎处理DRC违例 → 发布TOOL_STARTED + 知识捕获事件
        2. EDA引擎处理TOOL_STARTED → 发布TOOL_COMPLETED
        3. 知识管理引擎处理DRC引擎发布的知识捕获事件
        """
        # 清空消息总线记录
        mock_message_bus.clear_events()
        
        # ====== 步骤1: DRC引擎处理违例事件 ======
        drc_event = create_drc_violation_event(
            task_id="test_task_003",
            violation_id="violation_003",
            violation_type="min_width",
            location={"layer": "M1", "x": 500, "y": 600, "width": 0.07, "height": 0.5},
            rule_description="Minimum width violation: 0.07um < 0.1um",
            source="test_chain"
        )
        
        # process()会同步完成整个DRC修复流程（内部含sleep）
        await drc_engine.process(drc_event)
        
        # 获取DRC引擎发布的TOOL_STARTED事件
        tool_started_events = mock_message_bus.get_events_by_type(ToolExecutionEventTypes.TOOL_STARTED)
        assert len(tool_started_events) == 1
        tool_started_event = tool_started_events[0]
        
        # 验证DRC引擎也发布了知识捕获事件
        knowledge_events = mock_message_bus.get_events_by_type(KnowledgeCaptureEventTypes.ENGINEER_DECISION)
        assert len(knowledge_events) >= 1  # 分析决策 + 修复结果
        
        # 记录DRC发布的知识捕获事件，用于后续知识引擎测试
        drc_knowledge_events = [e for e in mock_message_bus.published_events 
                                 if e.event_type in KnowledgeCaptureEventTypes.__dict__.values()
                                    or e.event_type.startswith("knowledge_capture.")]
        
        # ====== 步骤2: EDA引擎处理TOOL_STARTED事件 ======
        with patch('asyncio.create_subprocess_shell') as mock_create_subprocess:
            mock_process = self.create_mock_process(
                stdout="DRC repair completed",
                stderr="",
                returncode=0
            )
            mock_create_subprocess.return_value = mock_process
            
            # 清空消息总线记录，准备捕获EDA引擎发布的事件
            mock_message_bus.clear_events()
            
            # EDA引擎处理TOOL_STARTED事件
            await eda_engine.process(tool_started_event)
            
            # 等待EDA引擎异步任务完成
            await asyncio.sleep(0.2)
            
            # 验证EDA引擎执行了工具
            assert mock_create_subprocess.called
            
            # 获取EDA引擎发布的TOOL_COMPLETED事件
            tool_completed_events = mock_message_bus.get_events_by_type(ToolExecutionEventTypes.TOOL_COMPLETED)
            assert len(tool_completed_events) == 1
            tool_completed_event = tool_completed_events[0]
        
        # ====== 步骤3: 知识管理引擎处理DRC发布的知识捕获事件 ======
        # 记录初始知识数量
        initial_count = knowledge_engine.knowledge_count
        
        # 知识管理引擎处理DRC引擎发布的知识捕获事件
        for ke in drc_knowledge_events:
            await knowledge_engine.process(ke)
        
        # 等待知识处理完成
        await asyncio.sleep(0.1)
        
        # 验证知识数量增加
        assert knowledge_engine.knowledge_count > initial_count
        
        # 验证完整流程：三个引擎接口兼容
        # 1. DRC引擎能处理DRC违例并生成TOOL_STARTED事件 + 知识捕获事件 ✓
        # 2. EDA引擎能处理TOOL_STARTED事件并生成TOOL_COMPLETED事件 ✓
        # 3. 知识管理引擎能捕获DRC引擎发布的知识捕获事件 ✓
