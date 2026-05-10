"""IC-Flow Platform REST API - 路由定义"""
from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException

from .schemas import (
    WorkflowRunRequest,
    WorkflowRunResponse,
    WorkflowStatusResponse,
    StepInfo,
    EngineInfo,
    HealthResponse,
    ErrorResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")


# ====== 依赖注入 ======

def get_app_state():
    """获取应用状态（由 main.py 设置）"""
    from fastapi import Request
    async def _get(request: Request):
        return request.app.state
    return _get


def get_orchestrator():
    """获取流程编排引擎实例"""
    from fastapi import Request
    async def _get(request: Request):
        state = request.app.state
        if not hasattr(state, "orchestrator") or state.orchestrator is None:
            raise HTTPException(status_code=503, detail="流程编排引擎不可用")
        return state.orchestrator
    return _get


def get_app_start_time():
    """获取应用启动时间"""
    from fastapi import Request
    async def _get(request: Request):
        state = request.app.state
        if not hasattr(state, "start_time"):
            state.start_time = 0.0
        return state.start_time
    return _get


# ====== 工作流端点 ======

@router.post(
    "/workflow/run",
    response_model=WorkflowRunResponse,
    summary="触发修复工作流",
    description="发布DRC违例事件或指定模板，触发自动修复工作流",
)
async def run_workflow(
    request: WorkflowRunRequest,
    orchestrator=Depends(get_orchestrator()),
):
    """触发一个新的修复工作流"""
    try:
        initial_data = {}
        if request.task_id:
            initial_data["task_id"] = request.task_id
        if request.violation_data:
            initial_data.update(request.violation_data)
        
        workflow_id = await orchestrator.start_workflow(
            template_name=request.template_name,
            initial_data=initial_data or None,
        )
        
        return WorkflowRunResponse(
            workflow_id=workflow_id,
            task_id=request.task_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"启动工作流失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"内部错误: {str(e)}")


@router.get(
    "/workflow/{workflow_id}",
    response_model=WorkflowStatusResponse,
    summary="查询工作流状态",
    description="根据工作流ID查询当前执行状态和步骤详情",
    responses={404: {"model": ErrorResponse}},
)
async def get_workflow_status(
    workflow_id: str,
    orchestrator=Depends(get_orchestrator()),
):
    """查询指定工作流的状态"""
    status = orchestrator.get_workflow_status(workflow_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"工作流不存在: {workflow_id}")
    
    return WorkflowStatusResponse(
        workflow_id=status.get("workflow_id", workflow_id),
        template_name=status.get("template_name", "unknown"),
        status=status.get("status", "unknown"),
        steps=[
            StepInfo(**s) for s in status.get("steps", [])
        ],
        started_at=status.get("started_at"),
        completed_at=status.get("completed_at"),
        task_id=status.get("context_summary", {}).get("task_id"),
    )


@router.get(
    "/workflows/active",
    response_model=List[WorkflowStatusResponse],
    summary="列出活跃工作流",
    description="获取当前正在运行的所有工作流",
)
async def list_active_workflows(
    orchestrator=Depends(get_orchestrator()),
):
    """列出所有活跃的工作流"""
    active = orchestrator.list_active_workflows()
    return [
        WorkflowStatusResponse(
            workflow_id=w["workflow_id"],
            template_name=w["template_name"],
            status=w["status"],
            steps=[StepInfo(**s) for s in w.get("steps", [])],
            started_at=w.get("started_at"),
        )
        for w in active
    ]


# ====== 引擎端点 ======

@router.get(
    "/engines",
    response_model=List[EngineInfo],
    summary="引擎状态概览",
    description="获取所有已注册引擎的运行状态和统计信息",
)
async def list_engines(
    orchestrator=Depends(get_orchestrator()),
):
    """列出所有引擎状态"""
    # 通过编排引擎获取其关联的引擎信息
    engines = []
    
    # 如果编排引擎有消息总线，可以从总线获取其他引擎的信息
    if hasattr(orchestrator, "_message_bus") and orchestrator._message_bus:
        # 这里简化处理，只返回编排引擎自身
        pass
    
    # 返回编排引擎信息
    stats = orchestrator.get_stats()
    engines.append(EngineInfo(
        engine_id=orchestrator.engine_id,
        engine_name=orchestrator.engine_name,
        running=orchestrator.is_running(),
        stats=stats,
    ))
    
    return engines


# ====== 健康检查端点 ======

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="健康检查",
    description="API服务健康检查端点，用于负载均衡器和K8s探针",
)
async def health_check(
    orchestrator=Depends(get_orchestrator()),
    start_time: float = Depends(get_app_start_time()),
):
    """健康检查"""
    import time
    uptime = time.time() - start_time if start_time else 0.0
    
    return HealthResponse(
        status="healthy",
        uptime=round(uptime, 2),
        engines_online=1 if orchestrator.is_running() else 0,
        engines=[
            EngineInfo(
                engine_id=orchestrator.engine_id,
                engine_name=orchestrator.engine_name,
                running=orchestrator.is_running(),
                stats=orchestrator.get_stats(),
            )
        ],
    )
