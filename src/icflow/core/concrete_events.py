"""
具体事件类型定义
根据 IC-Flow Platform 设计文档第1章定义的具体事件类型
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid
from .flow_event import FlowEvent, FlowEventBuilder


# ================ 事件类型常量 ================

class DesignFlowEventTypes:
    """设计流程事件类型"""
    TASK_STARTED = "design_flow.task_started"
    PHASE_COMPLETED = "design_flow.phase_completed"
    PHASE_FAILED = "design_flow.phase_failed"
    TASK_COMPLETED = "design_flow.task_completed"
    TASK_FAILED = "design_flow.task_failed"
    DRC_VIOLATION_DETECTED = "design_flow.drc_violation_detected"
    LVS_VIOLATION_DETECTED = "design_flow.lvs_violation_detected"


class ToolExecutionEventTypes:
    """工具执行事件类型"""
    TOOL_STARTED = "tool_execution.started"
    TOOL_COMPLETED = "tool_execution.completed"
    TOOL_FAILED = "tool_execution.failed"
    OUTPUT_READY = "tool_execution.output_ready"
    LICENSE_CHECKED = "tool_execution.license_checked"


class KnowledgeCaptureEventTypes:
    """知识捕获事件类型"""
    RULE_VIOLATION = "knowledge_capture.rule_violation"
    ENGINEER_DECISION = "knowledge_capture.engineer_decision"
    TOOL_OUTPUT_PARSED = "knowledge_capture.tool_output_parsed"
    DESIGN_PATTERN = "knowledge_capture.design_pattern"
    BEST_PRACTICE = "knowledge_capture.best_practice"


class SystemHealthEventTypes:
    """系统健康事件类型"""
    RESOURCE_MONITOR = "system_health.resource_monitor"
    ANOMALY_DETECTED = "system_health.anomaly_detected"
    THRESHOLD_EXCEEDED = "system_health.threshold_exceeded"
    SERVICE_DEGRADED = "system_health.service_degraded"
    SERVICE_RECOVERED = "system_health.service_recovered"


# ================ 事件构建辅助函数 ================

def create_design_flow_event(
    event_type: str,
    task_id: str,
    phase: Optional[str] = None,
    status: str = "started",
    metrics: Optional[Dict[str, Any]] = None,
    source: str = "system",
    **kwargs
) -> FlowEvent:
    """
    创建设计流程事件
    
    Args:
        event_type: 事件类型，来自 DesignFlowEventTypes
        task_id: 任务ID
        phase: 阶段标识（可选）
        status: 状态码（success, failed, in_progress等）
        metrics: 性能指标字典
        source: 事件来源
        **kwargs: 其他payload字段
        
    Returns:
        FlowEvent实例
    """
    payload = {
        "task_id": task_id,
        "phase": phase,
        "status": status,
        "metrics": metrics or {},
        **kwargs
    }
    
    # 自动添加标签
    metadata = {
        "tags": ["design_flow", "task"],
        "priority": 1 if status == "failed" else 0
    }
    
    return FlowEvent(
        event_type=event_type,
        source=source,
        source_type="flow_engine",
        payload=payload,
        metadata=metadata
    )


def create_tool_execution_event(
    event_type: str,
    tool_name: str,
    command_line: Optional[str] = None,
    output_path: Optional[str] = None,
    execution_status: str = "started",
    exit_code: Optional[int] = None,
    source: str = "system",
    **kwargs
) -> FlowEvent:
    """
    创建工具执行事件
    
    Args:
        event_type: 事件类型，来自 ToolExecutionEventTypes
        tool_name: 工具名称（如calibre, icv, innovus等）
        command_line: 命令行参数（可选）
        output_path: 输出文件路径（可选）
        execution_status: 执行状态（started, completed, failed等）
        exit_code: 退出码（可选）
        source: 事件来源
        **kwargs: 其他payload字段
        
    Returns:
        FlowEvent实例
    """
    payload = {
        "tool_name": tool_name,
        "command_line": command_line,
        "output_path": output_path,
        "execution_status": execution_status,
        "exit_code": exit_code,
        **kwargs
    }
    
    # 自动添加标签
    metadata = {
        "tags": ["tool_execution", tool_name],
        "priority": 2 if execution_status == "failed" else 0
    }
    
    return FlowEvent(
        event_type=event_type,
        source=source,
        source_type="flow_engine",
        payload=payload,
        metadata=metadata
    )


def create_knowledge_capture_event(
    event_type: str,
    rule_id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    decision_reason: Optional[str] = None,
    related_files: Optional[List[str]] = None,
    knowledge_type: str = "rule_violation",
    source: str = "system",
    **kwargs
) -> FlowEvent:
    """
    创建知识捕获事件
    
    Args:
        event_type: 事件类型，来自 KnowledgeCaptureEventTypes
        rule_id: 规则ID（可选）
        context: 上下文信息字典（可选）
        decision_reason: 决策理由（可选）
        related_files: 关联文件列表（可选）
        knowledge_type: 知识类型（rule_violation, engineer_decision等）
        source: 事件来源
        **kwargs: 其他payload字段
        
    Returns:
        FlowEvent实例
    """
    payload = {
        "rule_id": rule_id,
        "context": context or {},
        "decision_reason": decision_reason,
        "related_files": related_files or [],
        "knowledge_type": knowledge_type,
        **kwargs
    }
    
    # 自动添加标签
    metadata = {
        "tags": ["knowledge_capture", knowledge_type],
        "priority": 0
    }
    
    return FlowEvent(
        event_type=event_type,
        source=source,
        source_type="flow_engine",
        payload=payload,
        metadata=metadata
    )


def create_system_health_event(
    event_type: str,
    resource_metrics: Optional[Dict[str, Any]] = None,
    anomaly_stack: Optional[str] = None,
    suggested_action: Optional[str] = None,
    severity: str = "info",
    source: str = "system",
    **kwargs
) -> FlowEvent:
    """
    创建系统健康事件
    
    Args:
        event_type: 事件类型，来自 SystemHealthEventTypes
        resource_metrics: 资源指标字典（如cpu, memory, disk等）
        anomaly_stack: 异常堆栈信息（可选）
        suggested_action: 建议动作（可选）
        severity: 严重程度（info, warning, error, critical）
        source: 事件来源
        **kwargs: 其他payload字段
        
    Returns:
        FlowEvent实例
    """
    payload = {
        "resource_metrics": resource_metrics or {},
        "anomaly_stack": anomaly_stack,
        "suggested_action": suggested_action,
        "severity": severity,
        **kwargs
    }
    
    # 根据严重程度设置优先级
    priority_map = {
        "info": 0,
        "warning": 1,
        "error": 2,
        "critical": 3
    }
    
    metadata = {
        "tags": ["system_health", severity],
        "priority": priority_map.get(severity, 0)
    }
    
    return FlowEvent(
        event_type=event_type,
        source=source,
        source_type="system_monitor",
        payload=payload,
        metadata=metadata
    )


# ================ 快捷函数 ================

def create_drc_violation_event(
    task_id: str,
    violation_id: str,
    violation_type: str,
    location: Dict[str, Any],
    rule_description: str,
    source: str = "drc_engine"
) -> FlowEvent:
    """创建DRC违例事件（设计流程事件的特殊形式）"""
    return create_design_flow_event(
        event_type=DesignFlowEventTypes.DRC_VIOLATION_DETECTED,
        task_id=task_id,
        violation_id=violation_id,
        violation_type=violation_type,
        location=location,
        rule_description=rule_description,
        source=source,
        status="violation_detected"
    )


def create_tool_started_event(
    tool_name: str,
    command_line: str,
    source: str = "tool_adapter"
) -> FlowEvent:
    """创建工具开始执行事件"""
    return create_tool_execution_event(
        event_type=ToolExecutionEventTypes.TOOL_STARTED,
        tool_name=tool_name,
        command_line=command_line,
        execution_status="started",
        source=source
    )


def create_knowledge_rule_violation_event(
    rule_id: str,
    context: Dict[str, Any],
    related_files: List[str],
    source: str = "knowledge_engine"
) -> FlowEvent:
    """创建规则违例知识捕获事件"""
    return create_knowledge_capture_event(
        event_type=KnowledgeCaptureEventTypes.RULE_VIOLATION,
        rule_id=rule_id,
        context=context,
        knowledge_type="rule_violation",
        related_files=related_files,
        source=source
    )


# 导出所有事件类型常量
__all__ = [
    # 事件类型类
    "DesignFlowEventTypes",
    "ToolExecutionEventTypes",
    "KnowledgeCaptureEventTypes",
    "SystemHealthEventTypes",
    
    # 创建函数
    "create_design_flow_event",
    "create_tool_execution_event",
    "create_knowledge_capture_event",
    "create_system_health_event",
    
    # 快捷函数
    "create_drc_violation_event",
    "create_tool_started_event",
    "create_knowledge_rule_violation_event",
]