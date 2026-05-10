"""
具体事件类型测试
"""

import pytest
from src.icflow.core.concrete_events import (
    DesignFlowEventTypes,
    ToolExecutionEventTypes,
    KnowledgeCaptureEventTypes,
    SystemHealthEventTypes,
    create_design_flow_event,
    create_tool_execution_event,
    create_knowledge_capture_event,
    create_system_health_event,
    create_drc_violation_event,
    create_tool_started_event,
    create_knowledge_rule_violation_event,
)
from src.icflow.core.flow_event import FlowEvent


class TestDesignFlowEventTypes:
    """设计流程事件类型常量测试"""
    
    def test_constants_exist(self):
        """测试常量存在且为字符串"""
        assert isinstance(DesignFlowEventTypes.TASK_STARTED, str)
        assert isinstance(DesignFlowEventTypes.PHASE_COMPLETED, str)
        assert isinstance(DesignFlowEventTypes.PHASE_FAILED, str)
        assert isinstance(DesignFlowEventTypes.TASK_COMPLETED, str)
        assert isinstance(DesignFlowEventTypes.TASK_FAILED, str)
        assert isinstance(DesignFlowEventTypes.DRC_VIOLATION_DETECTED, str)
        assert isinstance(DesignFlowEventTypes.LVS_VIOLATION_DETECTED, str)
    
    def test_prefix(self):
        """测试事件类型前缀正确"""
        assert DesignFlowEventTypes.TASK_STARTED.startswith("design_flow.")
        assert DesignFlowEventTypes.DRC_VIOLATION_DETECTED.startswith("design_flow.")


class TestToolExecutionEventTypes:
    """工具执行事件类型常量测试"""
    
    def test_constants_exist(self):
        """测试常量存在且为字符串"""
        assert isinstance(ToolExecutionEventTypes.TOOL_STARTED, str)
        assert isinstance(ToolExecutionEventTypes.TOOL_COMPLETED, str)
        assert isinstance(ToolExecutionEventTypes.TOOL_FAILED, str)
        assert isinstance(ToolExecutionEventTypes.OUTPUT_READY, str)
        assert isinstance(ToolExecutionEventTypes.LICENSE_CHECKED, str)
    
    def test_prefix(self):
        """测试事件类型前缀正确"""
        assert ToolExecutionEventTypes.TOOL_STARTED.startswith("tool_execution.")


class TestKnowledgeCaptureEventTypes:
    """知识捕获事件类型常量测试"""
    
    def test_constants_exist(self):
        """测试常量存在且为字符串"""
        assert isinstance(KnowledgeCaptureEventTypes.RULE_VIOLATION, str)
        assert isinstance(KnowledgeCaptureEventTypes.ENGINEER_DECISION, str)
        assert isinstance(KnowledgeCaptureEventTypes.TOOL_OUTPUT_PARSED, str)
        assert isinstance(KnowledgeCaptureEventTypes.DESIGN_PATTERN, str)
        assert isinstance(KnowledgeCaptureEventTypes.BEST_PRACTICE, str)
    
    def test_prefix(self):
        """测试事件类型前缀正确"""
        assert KnowledgeCaptureEventTypes.RULE_VIOLATION.startswith("knowledge_capture.")


class TestSystemHealthEventTypes:
    """系统健康事件类型常量测试"""
    
    def test_constants_exist(self):
        """测试常量存在且为字符串"""
        assert isinstance(SystemHealthEventTypes.RESOURCE_MONITOR, str)
        assert isinstance(SystemHealthEventTypes.ANOMALY_DETECTED, str)
        assert isinstance(SystemHealthEventTypes.THRESHOLD_EXCEEDED, str)
        assert isinstance(SystemHealthEventTypes.SERVICE_DEGRADED, str)
        assert isinstance(SystemHealthEventTypes.SERVICE_RECOVERED, str)
    
    def test_prefix(self):
        """测试事件类型前缀正确"""
        assert SystemHealthEventTypes.RESOURCE_MONITOR.startswith("system_health.")


class TestCreateDesignFlowEvent:
    """创建设计流程事件测试"""
    
    def test_basic_creation(self):
        """测试基本创建"""
        event = create_design_flow_event(
            event_type=DesignFlowEventTypes.TASK_STARTED,
            task_id="task_123",
            phase="drc_check",
            status="started"
        )
        
        assert isinstance(event, FlowEvent)
        assert event.event_type == DesignFlowEventTypes.TASK_STARTED
        assert event.payload["task_id"] == "task_123"
        assert event.payload["phase"] == "drc_check"
        assert event.payload["status"] == "started"
        assert "design_flow" in event.get_tags()
        assert event.get_priority() == 0
    
    def test_failed_status_priority(self):
        """测试失败状态时优先级升高"""
        event = create_design_flow_event(
            event_type=DesignFlowEventTypes.TASK_FAILED,
            task_id="task_456",
            status="failed"
        )
        
        assert event.get_priority() == 1
        assert event.payload["status"] == "failed"


class TestCreateToolExecutionEvent:
    """创建工具执行事件测试"""
    
    def test_basic_creation(self):
        """测试基本创建"""
        event = create_tool_execution_event(
            event_type=ToolExecutionEventTypes.TOOL_STARTED,
            tool_name="calibre",
            command_line="calibre -drc -hier"
        )
        
        assert isinstance(event, FlowEvent)
        assert event.event_type == ToolExecutionEventTypes.TOOL_STARTED
        assert event.payload["tool_name"] == "calibre"
        assert event.payload["command_line"] == "calibre -drc -hier"
        assert event.payload["execution_status"] == "started"
        assert "tool_execution" in event.get_tags()
        assert "calibre" in event.get_tags()
    
    def test_failed_status(self):
        """测试失败状态"""
        event = create_tool_execution_event(
            event_type=ToolExecutionEventTypes.TOOL_FAILED,
            tool_name="icv",
            execution_status="failed",
            exit_code=1
        )
        
        assert event.get_priority() == 2
        assert event.payload["exit_code"] == 1


class TestCreateKnowledgeCaptureEvent:
    """创建知识捕获事件测试"""
    
    def test_basic_creation(self):
        """测试基本创建"""
        event = create_knowledge_capture_event(
            event_type=KnowledgeCaptureEventTypes.RULE_VIOLATION,
            rule_id="rule_001",
            context={"layer": "metal1", "width": 0.1},
            decision_reason="最小宽度违例",
            related_files=["design.gds", "rule.deck"]
        )
        
        assert isinstance(event, FlowEvent)
        assert event.event_type == KnowledgeCaptureEventTypes.RULE_VIOLATION
        assert event.payload["rule_id"] == "rule_001"
        assert event.payload["context"]["layer"] == "metal1"
        assert event.payload["decision_reason"] == "最小宽度违例"
        assert "rule.deck" in event.payload["related_files"]
        assert "knowledge_capture" in event.get_tags()


class TestCreateSystemHealthEvent:
    """创建系统健康事件测试"""
    
    def test_basic_creation(self):
        """测试基本创建"""
        event = create_system_health_event(
            event_type=SystemHealthEventTypes.RESOURCE_MONITOR,
            resource_metrics={"cpu": 85.5, "memory": 70.2},
            severity="warning"
        )
        
        assert isinstance(event, FlowEvent)
        assert event.event_type == SystemHealthEventTypes.RESOURCE_MONITOR
        assert event.payload["resource_metrics"]["cpu"] == 85.5
        assert event.payload["severity"] == "warning"
        assert event.get_priority() == 1  # warning对应优先级1
    
    def test_critical_severity(self):
        """测试严重级别"""
        event = create_system_health_event(
            event_type=SystemHealthEventTypes.ANOMALY_DETECTED,
            severity="critical",
            suggested_action="立即重启服务"
        )
        
        assert event.get_priority() == 3


class TestShortcutFunctions:
    """快捷函数测试"""
    
    def test_create_drc_violation_event(self):
        """测试创建DRC违例事件"""
        event = create_drc_violation_event(
            task_id="task_789",
            violation_id="viol_001",
            violation_type="min_width",
            location={"x": 100, "y": 200, "layer": "metal1"},
            rule_description="金属1最小宽度0.1um"
        )
        
        assert event.event_type == DesignFlowEventTypes.DRC_VIOLATION_DETECTED
        assert event.payload["task_id"] == "task_789"
        assert event.payload["violation_id"] == "viol_001"
        assert event.payload["violation_type"] == "min_width"
        assert event.payload["location"]["x"] == 100
        assert event.payload["status"] == "violation_detected"
    
    def test_create_tool_started_event(self):
        """测试创建工具开始执行事件"""
        event = create_tool_started_event(
            tool_name="innovus",
            command_line="innovus -batch -script run.tcl"
        )
        
        assert event.event_type == ToolExecutionEventTypes.TOOL_STARTED
        assert event.payload["tool_name"] == "innovus"
        assert event.payload["execution_status"] == "started"
    
    def test_create_knowledge_rule_violation_event(self):
        """测试创建规则违例知识捕获事件"""
        event = create_knowledge_rule_violation_event(
            rule_id="rule_002",
            context={"temperature": 85, "voltage": 1.2},
            related_files=["design.v", "constraints.sdc"]
        )
        
        assert event.event_type == KnowledgeCaptureEventTypes.RULE_VIOLATION
        assert event.payload["knowledge_type"] == "rule_violation"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])