"""
EDA工具适配器引擎测试
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.icflow.engines.eda_tool_adapter import EDAToolAdapterEngine
from src.icflow.core.flow_event import FlowEvent
from src.icflow.core.concrete_events import (
    ToolExecutionEventTypes,
    DesignFlowEventTypes,
    create_tool_started_event,
)
from src.icflow.message_bus.memory import MemoryMessageBus


class TestEDAToolAdapterEngine:
    """EDA工具适配器引擎测试类"""
    
    @pytest.fixture
    def message_bus(self):
        """创建消息总线fixture"""
        return MemoryMessageBus()
    
    @pytest.fixture
    def engine(self, message_bus):
        """创建引擎fixture"""
        # 创建引擎，配置模拟工具路径
        config = {
            "tool_paths": {
                "calibre": "/mock/path/calibre",
                "icv": "/mock/path/icv",
            },
            "default_timeout": 10,
        }
        engine = EDAToolAdapterEngine(config)
        # 设置消息总线
        engine.message_bus = message_bus
        return engine
    
    @pytest.mark.asyncio
    async def test_engine_initialization(self, engine):
        """测试引擎初始化"""
        assert engine.engine_id == "eda_tool_adapter_engine"
        assert engine.engine_name == "EDA工具适配器引擎"
        assert engine.engine_description == "封装各类EDA工具调用细节，提供统一接口"
        
        # 检查是否订阅了正确的事件类型
        assert ToolExecutionEventTypes.TOOL_STARTED in engine.subscribed_event_types
        
        # 检查配置是否正确加载
        assert engine.tool_paths["calibre"] == "/mock/path/calibre"
        assert engine.default_timeout == 10
    
    @pytest.mark.asyncio
    async def test_start_stop(self, engine):
        """测试启动和停止"""
        # 启动引擎
        await engine.start()
        assert engine._running == True
        
        # 停止引擎
        await engine.stop()
        assert engine._running == False
    
    @pytest.mark.asyncio
    async def test_handle_tool_started_event_missing_tool_name(self, engine):
        """测试处理缺少tool_name的工具开始事件"""
        # 创建缺少tool_name的事件
        event = create_tool_started_event(
            tool_name="",
            command_line="-drc",
            source="test"
        )
        
        # 处理事件
        await engine.process(event)
        
        # 验证没有任务被创建
        assert len(engine.active_tasks) == 0
    
    @pytest.mark.asyncio
    async def test_handle_tool_started_event_missing_command_line(self, engine):
        """测试处理缺少command_line的工具开始事件"""
        # 创建缺少command_line的事件
        event = create_tool_started_event(
            tool_name="calibre",
            command_line="",
            source="test"
        )
        
        # 处理事件
        await engine.process(event)
        
        # 验证没有任务被创建
        assert len(engine.active_tasks) == 0
    
    @pytest.mark.asyncio
    async def test_assemble_command_valid(self, engine):
        """测试组装有效命令"""
        command = engine._assemble_command("calibre", "-drc -hier")
        assert command == "/mock/path/calibre -drc -hier"
    
    @pytest.mark.asyncio
    async def test_assemble_command_unknown_tool(self, engine):
        """测试组装未知工具命令"""
        command = engine._assemble_command("unknown_tool", "-arg")
        assert command is None
    
    @pytest.mark.asyncio
    async def test_get_tool_vendor(self, engine):
        """测试获取工具厂商"""
        assert engine._get_tool_vendor("calibre") == "siemens"
        assert engine._get_tool_vendor("icv") == "synopsys"
        assert engine._get_tool_vendor("innovus") == "cadence"
        assert engine._get_tool_vendor("unknown") == "unknown"
    
    @pytest.mark.asyncio
    async def test_parse_calibre_output_success(self, engine):
        """测试解析Calibre成功输出"""
        stdout = """
INFO: Starting Calibre DRC...
WARNING: Using default settings for layer mapping
Total DRC violations found: 0
INFO: Calibre DRC completed successfully
"""
        stderr = ""
        exit_code = 0
        
        result = engine._parse_calibre_output(stdout, stderr, exit_code)
        
        assert result["tool"] == "calibre"
        assert result["exit_code"] == 0
        assert len(result["errors"]) == 0
        assert len(result["warnings"]) == 1
        assert result["summary"]["drc_violations"] == 0
    
    @pytest.mark.asyncio
    async def test_parse_calibre_output_error(self, engine):
        """测试解析Calibre错误输出"""
        stdout = ""
        stderr = """
ERROR: License not available for Calibre
FATAL: Cannot proceed without license
"""
        exit_code = 1
        
        result = engine._parse_calibre_output(stdout, stderr, exit_code)
        
        assert result["tool"] == "calibre"
        assert result["exit_code"] == 1
        assert len(result["errors"]) >= 1
        assert "License not available for Calibre" in result["errors"][0]
    
    @pytest.mark.asyncio
    async def test_parse_generic_output(self, engine):
        """测试通用输出解析"""
        stdout = "Standard output"
        stderr = "Error output"
        exit_code = 1
        
        result = engine._parse_generic_output(stdout, stderr, exit_code)
        
        assert result["tool"] == "unknown"
        assert result["exit_code"] == 1
        assert len(result["errors"]) == 1
        assert result["errors"][0] == "Error output"
    
    @pytest.mark.asyncio
    async def test_check_license_unknown_vendor(self, engine):
        """测试检查未知厂商的许可证"""
        # 模拟未知工具
        with patch.object(engine, '_get_tool_vendor', return_value="unknown"):
            result = await engine._check_license("unknown_tool")
        
        # 未知厂商应该返回True（跳过检查）
        assert result == True
    
    @pytest.mark.asyncio
    @patch('asyncio.create_subprocess_shell')
    async def test_execute_tool_success(self, mock_subprocess, engine):
        """测试成功执行工具"""
        # 配置模拟子进程
        mock_process = AsyncMock()
        mock_process.communicate.return_value = (b"Tool completed", b"")
        mock_process.returncode = 0
        mock_subprocess.return_value = mock_process
        
        # 创建原始事件
        original_event = FlowEvent(
            event_type=ToolExecutionEventTypes.TOOL_STARTED,
            source="test",
            source_type="flow_engine",
            payload={"tool_name": "calibre", "command_line": "-drc"},
            metadata={"correlation_id": "test-correlation"}
        )
        
        # 执行工具
        await engine._execute_tool(
            "test-task",
            "calibre",
            "-drc",
            original_event
        )
        
        # 验证子进程被调用
        mock_subprocess.assert_called_once()
        
        # 验证消息总线发布了事件
        # 这里简化验证，实际测试中可能需要检查具体事件内容
    
    @pytest.mark.asyncio
    async def test_handle_event_unknown_type(self, engine):
        """测试处理未知类型事件"""
        # 创建未知类型事件
        event = FlowEvent(
            event_type="unknown.event.type",
            source="test",
            source_type="flow_engine",
            payload={},
            metadata={}
        )
        
        # 处理事件（应该记录警告但不报错）
        await engine.process(event)