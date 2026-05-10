"""IC-Flow Platform REST API - 数据模型"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ==== 请求模型 ====

class WorkflowRunRequest(BaseModel):
    """工作流触发请求"""
    template_name: str = Field(default="drc_repair", description="工作流模板名称")
    task_id: Optional[str] = Field(default=None, description="任务ID（自动生成如果未提供）")
    violation_data: Optional[Dict[str, Any]] = Field(default=None, description="违例数据（可选）")


# ==== 响应模型 ====

class StepInfo(BaseModel):
    """步骤信息"""
    step_id: str
    name: str
    status: str
    error: Optional[str] = None


class WorkflowStatusResponse(BaseModel):
    """工作流状态响应"""
    workflow_id: str
    template_name: str
    status: str
    steps: List[StepInfo] = Field(default_factory=list)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    task_id: Optional[str] = None


class WorkflowRunResponse(BaseModel):
    """工作流触发响应"""
    workflow_id: str
    status: str = "started"
    message: str = "工作流已启动"
    task_id: Optional[str] = None


class EngineInfo(BaseModel):
    """引擎信息"""
    engine_id: str
    engine_name: str
    running: bool
    stats: Dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    """健康检查响应"""
    status: str = "healthy"
    version: str = "0.1.0"
    uptime: float = 0.0
    engines_online: int = 0
    engines: List[EngineInfo] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    """错误响应"""
    detail: str
    error_code: Optional[str] = None
