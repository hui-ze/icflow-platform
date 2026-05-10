"""
DRC修复流程集成测试

使用真实 MemoryMessageBus 验证完整的 DRC 修复流程：
1. DRC违例事件发布 -> 消息总线路由到DRC引擎
2. DRC修复引擎处理 -> 发布TOOL_STARTED事件到消息总线
3. EDA工具适配器接收TOOL_STARTED -> 执行工具 -> 发布TOOL_COMPLETED
4. 知识管理引擎捕获知识捕获事件

使用mock模拟实际工具执行，验证各组件间的协作。
"""

import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
import json

from src.icflow.message_bus.memory import MemoryMessageBus
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
    create_knowledge_capture_event,
)


async def wait_for_condition(condition_func, timeout=5.0, interval=0.05):
    """轮询等待条件成立，超时抛出 TimeoutError"""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = condition_func()
        if result:
            return result
        await asyncio.sleep(interval)
    raise TimeoutError(f"等待条件超时 ({timeout}s)")


class TestDRCRepairIntegration:
    """DRC修复流程集成测试类"""
    
    @pytest_asyncio.fixture
    async def message_bus(self):
        """创建并启动消息总线"""
        bus = MemoryMessageBus()
        await bus.start()
        yield bus
        await bus.stop()
    
    @pytest_asyncio.fixture
    async def drc_engine(self, message_bus):
        """创建DRC修复引擎并订阅到消息总线"""
        engine = DRCRepairMasterEngine({"default_tool": "calibre"})
        engine.message_bus = message_bus
        
        # 订阅引擎的处理方法到消息总线
        for event_type in engine.subscribed_event_types:
            await message_bus.subscribe(
                event_type,
                lambda e: asyncio.create_task(engine.process(e))
            )
        
        await engine.start()
        yield engine
        await engine.stop()
    
    @pytest_asyncio.fixture
    async def eda_engine(self, message_bus):
        """创建EDA工具适配器引擎并订阅到消息总线"""
        engine = EDAToolAdapterEngine({
            "tool_paths": {
                "calibre": "/mock/path/calibre",
            },
            "default_timeout": 5,
        })
        engine.message_bus = message_bus
        
        for event_type in engine.subscribed_event_types:
            await message_bus.subscribe(
                event_type,
                lambda e: asyncio.create_task(engine.process(e))
            )
        
        await engine.start()
        yield engine
        await engine.stop()
    
    @pytest_asyncio.fixture
    async def knowledge_engine(self, message_bus):
        """创建知识管理引擎并订阅到消息总线"""
        engine = KnowledgeManagementEngine()
        engine.message_bus = message_bus
        
        for event_type in engine.subscribed_event_types:
            await message_bus.subscribe(
                event_type,
                lambda e: asyncio.create_task(engine.process(e))
            )
        
        await engine.start()
        yield engine
        await engine.stop()
    
    @pytest_asyncio.fixture
    async def all_engines(self, drc_engine, eda_engine, knowledge_engine):
        """返回所有引擎的元组"""
        return drc_engine, eda_engine, knowledge_engine
    
    def create_mock_process(self, stdout: str = "", stderr: str = "", returncode: int = 0):
        """创建模拟的异步进程对象"""
        mock_process = AsyncMock()
        mock_process.communicate.return_value = (stdout.encode() if stdout else b"", 
                                                 stderr.encode() if stderr else b"")
        mock_process.returncode = returncode
        return mock_process
    
    @pytest.mark.asyncio
    async def test_complete_drc_repair_flow(self, all_engines, message_bus):
        """
        测试完整的DRC修复流程（基于消息总线的异步协作）
        
        验证步骤：
        1. 发布DRC违例事件到消息总线
        2. DRC引擎通过消息总线接收并处理，发布TOOL_STARTED事件
        3. EDA适配器通过消息总线接收TOOL_STARTED，模拟工具执行，发布TOOL_COMPLETED
        4. 知识管理引擎通过消息总线接收知识捕获事件
        """
        drc_engine, eda_engine, knowledge_engine = all_engines
        
        # Mock EDA适配器的工具执行，避免实际调用外部工具
        with patch('asyncio.create_subprocess_shell') as mock_create_subprocess:
            mock_process = self.create_mock_process(
                stdout="DRC check completed successfully\n0 violations found",
                stderr="",
                returncode=0
            )
            mock_create_subprocess.return_value = mock_process
            
            # 创建并发布DRC违例事件
            drc_event = create_drc_violation_event(
                task_id="test_task_001",
                violation_id="violation_001",
                violation_type="min_width",
                location={"layer": "M1", "x": 100, "y": 200, "width": 0.08, "height": 0.5},
                rule_description="Minimum width violation: 0.08um < 0.1um",
                source="test_integration"
            )
            
            # 记录事件发布前的状态
            initial_knowledge_count = knowledge_engine.knowledge_count
            
            # 发布事件到消息总线
            await message_bus.publish(drc_event)
            
            # 等待DRC引擎处理违例事件（通过消息总线异步交付）
            # DRC引擎内部: _execute_repair_tool sleep 0.5s + _verify_repair_result sleep 0.2s + 其他开销
            # EDA引擎通过消息总线接收TOOL_STARTED后执行工具(~0.1s)
            await asyncio.sleep(1.5)
            
            # 验证EDA引擎执行了工具（DRC发布TOOL_STARTED -> EDA接收 -> 调用子进程）
            assert mock_create_subprocess.called
            
            # 验证DRC引擎记录了修复历史
            assert len(drc_engine.repair_history) > 0
            recent_repair = drc_engine.repair_history[-1]
            assert recent_repair["task_id"] == "test_task_001"
            assert recent_repair["violation_id"] == "violation_001"
            
            # 等待知识引擎处理完成（异步知识捕获）
            await asyncio.sleep(0.5)
            
            # 验证知识管理引擎捕获了知识
            # DRC引擎在修复过程中会发布知识捕获事件（ENGINEER_DECISION），
            # 这些事件通过消息总线传递给知识管理引擎
            assert knowledge_engine.knowledge_count > initial_knowledge_count
    
    @pytest.mark.asyncio
    async def test_event_routing(self, all_engines, message_bus):
        """
        测试事件在引擎间的正确路由
        
        验证步骤：
        1. 直接发布TOOL_STARTED事件（模拟DRC引擎的输出）
        2. 验证EDA适配器引擎通过消息总线接收并处理该事件
        """
        drc_engine, eda_engine, knowledge_engine = all_engines
        
        # Mock工具执行
        with patch('asyncio.create_subprocess_shell') as mock_create_subprocess:
            mock_process = self.create_mock_process()
            mock_create_subprocess.return_value = mock_process
            
            # 创建并发布TOOL_STARTED事件（模拟DRC引擎的输出）
            tool_event = create_tool_started_event(
                tool_name="calibre",
                command_line="-drc -hier -turbo -64",
                source="drc_repair_master_engine"
            )
            
            await message_bus.publish(tool_event)
            
            # 等待事件通过消息总线路由到EDA引擎并创建任务
            await asyncio.sleep(0.5)
            
            # 验证工具执行被调用
            assert mock_create_subprocess.called
            
            # 获取实际调用的命令
            call_args = mock_create_subprocess.call_args
            assert call_args is not None
            command = call_args[0][0]
            # EDA引擎组装命令：tool_path + command_line = "/mock/path/calibre -drc -hier -turbo -64"
            assert "calibre" in command
            assert "-drc" in command
