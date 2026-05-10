"""
流程编排引擎 (Flow Orchestrator)

职责：管理多步设计流程的编排与协调
触发事件：DesignFlowEvent（task_started, drc_violation_detected）
输出事件：DesignFlowEvent（流程状态变更）、SystemHealthEvent（工作流健康度）

工作流模板定义：
1. 违例分析 → 2. DRC修复策略选择 → 3. EDA工具执行 → 4. 结果验证 → 5. 知识入库

关键能力：
- 工作流模板管理（支持自定义步骤序列）
- 工作流状态追踪（pending/running/completed/failed/timeout）
- 超时控制与错误恢复
- 引擎间上下文传递
"""

import asyncio
import logging
import uuid
from typing import Dict, Any, Optional, List, Callable, Awaitable
from datetime import datetime, timezone
from enum import Enum

from src.icflow.core.flow_engine import FlowEngine, FlowEvent
from src.icflow.core.concrete_events import (
    DesignFlowEventTypes,
    ToolExecutionEventTypes,
    KnowledgeCaptureEventTypes,
    create_design_flow_event,
    create_tool_execution_event,
)


logger = logging.getLogger(__name__)


class WorkflowStatus(str, Enum):
    """工作流状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class WorkflowStep:
    """工作流步骤定义"""
    
    def __init__(
        self,
        step_id: str,
        name: str,
        handler: Callable[["WorkflowContext"], Awaitable[bool]],
        timeout: float = 300.0,
        retry_count: int = 0,
        depends_on: Optional[List[str]] = None,
    ):
        self.step_id = step_id
        self.name = name
        self.handler = handler
        self.timeout = timeout
        self.retry_count = retry_count
        self.depends_on = depends_on or []
        self.status: WorkflowStatus = WorkflowStatus.PENDING
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error: Optional[str] = None


class WorkflowContext:
    """工作流上下文——在步骤间传递的数据容器"""
    
    def __init__(self, workflow_id: str, initial_data: Optional[Dict[str, Any]] = None):
        self.workflow_id = workflow_id
        self.data: Dict[str, Any] = initial_data or {}
        self.results: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "steps_completed": 0,
            "steps_failed": 0,
        }
    
    def set(self, key: str, value: Any) -> None:
        """设置上下文数据"""
        self.data[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取上下文数据"""
        return self.data.get(key, default)
    
    def record_step_result(self, step_id: str, result: Any) -> None:
        """记录步骤执行结果"""
        self.results[step_id] = result
        self.metadata["steps_completed"] += 1


class FlowOrchestrator(FlowEngine):
    """
    流程编排引擎
    
    管理多步设计工作流的执行，协调各引擎间的调用顺序。
    支持自定义工作流模板、超时控制、错误重试。
    """
    
    engine_id = "flow_orchestrator"
    engine_name = "流程编排引擎"
    engine_description = "管理多步设计流程的编排与协调"
    
    # 订阅事件：设计流程事件
    subscribed_event_types = [
        DesignFlowEventTypes.TASK_STARTED,
        DesignFlowEventTypes.DRC_VIOLATION_DETECTED,
    ]
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化流程编排引擎
        
        Args:
            config: 引擎配置，支持：
                - default_timeout: 工作流默认超时（秒），默认600
                - max_concurrent_workflows: 最大并发工作流数，默认10
        """
        super().__init__(config)
        
        self.default_timeout = self.config.get("default_timeout", 600.0)
        self.max_concurrent = self.config.get("max_concurrent_workflows", 10)
        
        # 活跃的工作流
        self.active_workflows: Dict[str, "WorkflowRunner"] = {}
        
        # 工作流历史
        self.workflow_history: List[Dict[str, Any]] = []
        
        # 已注册的工作流模板
        self._workflow_templates: Dict[str, List[WorkflowStep]] = {}
        
        # 注册默认工作流模板
        self._register_default_templates()
        
        logger.info(f"流程编排引擎初始化完成: {self.engine_id}")
    
    def _register_default_templates(self) -> None:
        """注册默认工作流模板"""
        drc_repair_flow = [
            WorkflowStep(
                step_id="violation_analysis",
                name="违例分析",
                handler=self._step_violation_analysis,
                timeout=60.0,
            ),
            WorkflowStep(
                step_id="repair_strategy",
                name="修复策略选择",
                handler=self._step_repair_strategy,
                timeout=30.0,
                depends_on=["violation_analysis"],
            ),
            WorkflowStep(
                step_id="tool_execution",
                name="EDA工具执行",
                handler=self._step_tool_execution,
                timeout=300.0,
                retry_count=2,
                depends_on=["repair_strategy"],
            ),
            WorkflowStep(
                step_id="result_verification",
                name="结果验证",
                handler=self._step_result_verification,
                timeout=60.0,
                depends_on=["tool_execution"],
            ),
            WorkflowStep(
                step_id="knowledge_capture",
                name="知识入库",
                handler=self._step_knowledge_capture,
                timeout=30.0,
                depends_on=["result_verification"],
            ),
        ]
        
        self.register_workflow_template("drc_repair", drc_repair_flow)
        logger.info("已注册默认工作流模板: drc_repair (5个步骤)")
    
    def register_workflow_template(
        self, template_name: str, steps: List[WorkflowStep]
    ) -> None:
        """注册工作流模板"""
        self._workflow_templates[template_name] = steps
        logger.info(f"注册工作流模板: {template_name} ({len(steps)}个步骤)")
    
    def get_workflow_template(self, template_name: str) -> Optional[List[WorkflowStep]]:
        """获取工作流模板"""
        template = self._workflow_templates.get(template_name)
        if template:
            return [step for step in template]  # 返回副本
        return None
    
    def list_workflow_templates(self) -> List[str]:
        """列出所有已注册的工作流模板"""
        return list(self._workflow_templates.keys())
    
    async def start_workflow(
        self,
        template_name: str = "drc_repair",
        initial_data: Optional[Dict[str, Any]] = None,
        workflow_id: Optional[str] = None,
    ) -> str:
        """
        启动一个新工作流
        
        Args:
            template_name: 工作流模板名称
            initial_data: 初始数据
            workflow_id: 自定义工作流ID（可选）
            
        Returns:
            工作流ID
        
        Raises:
            ValueError: 模板不存在或并发数已达上限
        """
        if template_name not in self._workflow_templates:
            raise ValueError(f"工作流模板不存在: {template_name}")
        
        if len(self.active_workflows) >= self.max_concurrent:
            raise RuntimeError(f"并发工作流已达上限 ({self.max_concurrent})")
        
        wid = workflow_id or f"wf_{uuid.uuid4().hex[:12]}"
        context = WorkflowContext(wid, initial_data or {})
        steps = self.get_workflow_template(template_name)
        
        runner = WorkflowRunner(
            workflow_id=wid,
            template_name=template_name,
            steps=steps,
            context=context,
            orchestrator=self,
            default_timeout=self.default_timeout,
        )
        
        self.active_workflows[wid] = runner
        
        # 异步启动工作流执行
        asyncio.create_task(self._run_workflow(runner))
        
        logger.info(f"工作流已启动: {wid} (模板: {template_name})")
        return wid
    
    async def _run_workflow(self, runner: "WorkflowRunner") -> None:
        """异步执行工作流"""
        try:
            result = await runner.run()
            if result:
                logger.info(f"工作流完成: {runner.workflow_id}")
            else:
                logger.warning(f"工作流失败: {runner.workflow_id}")
        except Exception as e:
            logger.error(f"工作流异常: {runner.workflow_id}: {e}", exc_info=True)
        finally:
            # 从活跃列表移到历史
            if runner.workflow_id in self.active_workflows:
                del self.active_workflows[runner.workflow_id]
            
            history_entry = {
                "workflow_id": runner.workflow_id,
                "template_name": runner.template_name,
                "status": runner.status.value,
                "steps": [
                    {
                        "step_id": s.step_id,
                        "name": s.name,
                        "status": s.status.value,
                        "error": s.error,
                    }
                    for s in runner.steps
                ],
                "context_summary": {
                    k: v for k, v in runner.context.data.items()
                    if not isinstance(v, (bytes, bytearray))
                },
                "started_at": runner.started_at.isoformat() if runner.started_at else None,
                "completed_at": runner.completed_at.isoformat() if runner.completed_at else None,
            }
            self.workflow_history.append(history_entry)
    
    def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """获取工作流状态"""
        # 检查活跃工作流
        runner = self.active_workflows.get(workflow_id)
        if runner:
            return runner.get_status()
        
        # 检查历史
        for entry in self.workflow_history:
            if entry["workflow_id"] == workflow_id:
                return entry
        
        return None
    
    def list_active_workflows(self) -> List[Dict[str, Any]]:
        """列出所有活跃工作流"""
        return [r.get_status() for r in self.active_workflows.values()]
    
    async def process(self, event: FlowEvent) -> Optional[FlowEvent]:
        """
        处理事件——启动或管理工作流
        
        Args:
            event: 接收到的事件
            
        Returns:
            处理结果事件（如果适用）
        """
        if event.event_type == DesignFlowEventTypes.TASK_STARTED:
            # 从TASK_STARTED事件自动启动工作流
            payload = event.payload
            template = payload.get("workflow_template", "drc_repair")
            initial_data = {
                "task_id": payload.get("task_id", "unknown"),
                "phase": payload.get("phase"),
                "source_event": event.to_dict(),
            }
            try:
                wid = await self.start_workflow(
                    template_name=template,
                    initial_data=initial_data,
                )
                return create_design_flow_event(
                    event_type=DesignFlowEventTypes.TASK_STARTED,
                    task_id=payload.get("task_id", "unknown"),
                    phase=f"workflow_{template}_started",
                    status="in_progress",
                    metrics={"workflow_id": wid},
                    source=self.engine_id,
                )
            except (ValueError, RuntimeError) as e:
                logger.error(f"启动工作流失败: {e}")
                return None
        
        elif event.event_type == DesignFlowEventTypes.DRC_VIOLATION_DETECTED:
            # 从DRC违例事件自动启动修复工作流
            payload = event.payload
            initial_data = {
                "task_id": payload.get("task_id", "unknown"),
                "violation_id": payload.get("violation_id"),
                "violation_type": payload.get("violation_type"),
                "location": payload.get("location"),
                "rule_description": payload.get("rule_description"),
                "source_event": event.to_dict(),
                "auto_started": True,
            }
            try:
                wid = await self.start_workflow(
                    template_name="drc_repair",
                    initial_data=initial_data,
                )
                return create_design_flow_event(
                    event_type=DesignFlowEventTypes.TASK_STARTED,
                    task_id=payload.get("task_id", "unknown"),
                    phase="auto_workflow_started",
                    status="in_progress",
                    metrics={"workflow_id": wid},
                    source=self.engine_id,
                )
            except (ValueError, RuntimeError) as e:
                logger.error(f"自动启动DRC修复工作流失败: {e}")
                return None
        
        else:
            logger.warning(f"流程编排引擎收到未订阅的事件类型: {event.event_type}")
            return None
    
    async def publish_workflow_event(
        self,
        event_type: str,
        task_id: str,
        phase: str,
        status: str,
        metrics: Dict[str, Any],
    ) -> None:
        """发布工作流状态事件到消息总线"""
        event = create_design_flow_event(
            event_type=event_type,
            task_id=task_id,
            phase=phase,
            status=status,
            metrics=metrics,
            source=self.engine_id,
        )
        if self._message_bus:
            await self._message_bus.publish(event)
    
    # ============ 默认工作流步骤处理函数 ============
    
    async def _step_violation_analysis(self, ctx: WorkflowContext) -> bool:
        """步骤1：违例分析"""
        violation_type = ctx.get("violation_type", "unknown")
        rule_description = ctx.get("rule_description", "")
        logger.info(f"[{ctx.workflow_id}] 违例分析: type={violation_type}")
        
        # 发布分析事件
        if self._message_bus:
            await self.publish_workflow_event(
                event_type=DesignFlowEventTypes.PHASE_COMPLETED,
                task_id=ctx.get("task_id", "unknown"),
                phase="violation_analysis",
                status="in_progress",
                metrics={
                    "violation_type": violation_type,
                    "rule_description": rule_description,
                    "workflow_id": ctx.workflow_id,
                },
            )
        
        # 模拟分析耗时
        await asyncio.sleep(0.1)
        
        ctx.record_step_result("violation_analysis", {
            "violation_type": violation_type,
            "severity": "high" if violation_type in ("min_width", "min_spacing") else "medium",
            "analysis_duration": 0.1,
        })
        return True
    
    async def _step_repair_strategy(self, ctx: WorkflowContext) -> bool:
        """步骤2：修复策略选择"""
        violation_type = ctx.get("violation_type", "unknown")
        
        strategy_map = {
            "min_width": "auto_widen",
            "min_spacing": "auto_move",
            "min_area": "auto_fill",
            "notch": "auto_adjust",
            "short": "auto_cut",
            "open": "auto_connect",
            "mismatch": "auto_adjust",
        }
        strategy = strategy_map.get(violation_type, "manual_review")
        
        logger.info(f"[{ctx.workflow_id}] 修复策略: {violation_type} -> {strategy}")
        ctx.set("repair_strategy", strategy)
        ctx.set("tool_name", "calibre")
        
        # 发布策略事件
        if self._message_bus:
            await self.publish_workflow_event(
                event_type=DesignFlowEventTypes.PHASE_COMPLETED,
                task_id=ctx.get("task_id", "unknown"),
                phase="repair_strategy",
                status="completed",
                metrics={
                    "violation_type": violation_type,
                    "strategy": strategy,
                    "workflow_id": ctx.workflow_id,
                },
            )
        
        ctx.record_step_result("repair_strategy", {
            "strategy": strategy,
            "tool": "calibre",
        })
        return True
    
    async def _step_tool_execution(self, ctx: WorkflowContext) -> bool:
        """步骤3：EDA工具执行（通过消息总线发布事件）"""
        task_id = ctx.get("task_id", "unknown")
        violation_type = ctx.get("violation_type", "unknown")
        strategy = ctx.get("repair_strategy", "auto_widen")
        tool_name = ctx.get("tool_name", "calibre")
        
        logger.info(f"[{ctx.workflow_id}] 工具执行: {tool_name} ({strategy})")
        
        if self._message_bus:
            # 发布工具开始事件到消息总线，让EDA引擎处理
            cmd = f"{tool_name} -repair -type {violation_type} -strategy {strategy}"
            event = create_tool_execution_event(
                event_type=ToolExecutionEventTypes.TOOL_STARTED,
                tool_name=tool_name,
                command_line=cmd,
                output_path=f"/tmp/repair_{task_id}.log",
                execution_status="started",
                source=self.engine_id,
                workflow_id=ctx.workflow_id,
                task_id=task_id,
            )
            await self._message_bus.publish(event)
            logger.info(f"[{ctx.workflow_id}] 已发布TOOL_STARTED事件到消息总线")
        
        # 模拟等待工具执行完成
        await asyncio.sleep(0.5)
        
        ctx.record_step_result("tool_execution", {
            "tool": tool_name,
            "command": f"{tool_name} -repair -type {violation_type} -strategy {strategy}",
            "simulated": True,
        })
        return True
    
    async def _step_result_verification(self, ctx: WorkflowContext) -> bool:
        """步骤4：结果验证"""
        logger.info(f"[{ctx.workflow_id}] 结果验证")
        
        # 模拟验证
        import random
        verification_passed = random.random() < 0.95
        
        await asyncio.sleep(0.1)
        
        ctx.record_step_result("result_verification", {
            "passed": verification_passed,
            "confidence": 0.95 if verification_passed else 0.3,
        })
        
        if not verification_passed:
            logger.warning(f"[{ctx.workflow_id}] 结果验证未通过，需要人工干预")
            return False
        
        return True
    
    async def _step_knowledge_capture(self, ctx: WorkflowContext) -> bool:
        """步骤5：知识入库"""
        logger.info(f"[{ctx.workflow_id}] 知识入库")
        
        # 发布知识捕获事件
        if self._message_bus:
            from src.icflow.core.concrete_events import create_knowledge_capture_event
            event = create_knowledge_capture_event(
                event_type=KnowledgeCaptureEventTypes.ENGINEER_DECISION,
                rule_id=ctx.get("violation_id", "unknown"),
                context={
                    "violation_type": ctx.get("violation_type"),
                    "strategy": ctx.get("repair_strategy"),
                    "workflow_id": ctx.workflow_id,
                },
                decision_reason=f"工作流自动完成: {ctx.get('violation_type', 'unknown')} -> {ctx.get('repair_strategy', 'unknown')}",
                related_files=[],
                source=self.engine_id,
            )
            await self._message_bus.publish(event)
        
        ctx.record_step_result("knowledge_capture", {
            "knowledge_type": "engineer_decision",
            "records_stored": 1,
        })
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """获取引擎统计信息"""
        stats = super().get_stats()
        stats.update({
            "active_workflows": len(self.active_workflows),
            "completed_workflows": len(self.workflow_history),
            "workflow_templates": list(self._workflow_templates.keys()),
            "max_concurrent": self.max_concurrent,
        })
        return stats


class WorkflowRunner:
    """
    工作流运行器——负责执行单个工作流
    
    管理步骤的依赖关系、超时、重试逻辑。
    """
    
    def __init__(
        self,
        workflow_id: str,
        template_name: str,
        steps: List[WorkflowStep],
        context: WorkflowContext,
        orchestrator: FlowOrchestrator,
        default_timeout: float = 600.0,
    ):
        self.workflow_id = workflow_id
        self.template_name = template_name
        self.steps = steps
        self.context = context
        self.orchestrator = orchestrator
        self.default_timeout = default_timeout
        
        self.status: WorkflowStatus = WorkflowStatus.PENDING
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
    
    async def run(self) -> bool:
        """
        执行工作流（按步骤顺序执行，处理依赖）
        
        Returns:
            True 表示所有步骤成功完成，False 表示有步骤失败
        """
        self.status = WorkflowStatus.RUNNING
        self.started_at = datetime.now(timezone.utc)
        
        # 构建步骤依赖图
        step_map = {s.step_id: s for s in self.steps}
        completed = set()
        
        try:
            while len(completed) < len(self.steps):
                progress = False
                
                for step in self.steps:
                    if step.step_id in completed:
                        continue
                    
                    # 检查依赖是否全部完成且成功
                    deps_met = True
                    for dep_id in step.depends_on:
                        dep_step = step_map.get(dep_id)
                        if dep_step is None or dep_step.status != WorkflowStatus.COMPLETED:
                            deps_met = False
                            break
                    
                    if not deps_met:
                        continue
                    
                    # 执行步骤
                    progress = True
                    step.status = WorkflowStatus.RUNNING
                    step.started_at = datetime.now(timezone.utc)
                    
                    try:
                        success = await asyncio.wait_for(
                            step.handler(self.context),
                            timeout=step.timeout,
                        )
                    except asyncio.TimeoutError:
                        step.status = WorkflowStatus.TIMEOUT
                        step.error = f"步骤超时 ({step.timeout}s)"
                        logger.error(f"[{self.workflow_id}] 步骤超时: {step.step_id}")
                        completed.add(step.step_id)
                        continue
                    except Exception as e:
                        step.status = WorkflowStatus.FAILED
                        step.error = str(e)
                        logger.error(f"[{self.workflow_id}] 步骤异常: {step.step_id}: {e}")
                        completed.add(step.step_id)
                        continue
                    
                    if success:
                        step.status = WorkflowStatus.COMPLETED
                        step.completed_at = datetime.now(timezone.utc)
                        logger.info(f"[{self.workflow_id}] 步骤完成: {step.step_id}")
                    else:
                        step.status = WorkflowStatus.FAILED
                        step.error = "步骤返回失败"
                        step.completed_at = datetime.now(timezone.utc)
                        logger.warning(f"[{self.workflow_id}] 步骤失败: {step.step_id}")
                    
                    completed.add(step.step_id)
                
                if not progress and len(completed) < len(self.steps):
                    # 没有可执行的步骤了（依赖循环或依赖失败）
                    blocked = [s.step_id for s in self.steps if s.step_id not in completed]
                    logger.error(f"[{self.workflow_id}] 步骤阻塞: {blocked}")
                    self.status = WorkflowStatus.FAILED
                    self.completed_at = datetime.now(timezone.utc)
                    return False
            
            # 全部完成
            all_success = all(
                s.status == WorkflowStatus.COMPLETED for s in self.steps
            )
            self.status = WorkflowStatus.COMPLETED if all_success else WorkflowStatus.FAILED
            
        except Exception as e:
            logger.error(f"[{self.workflow_id}] 工作流异常: {e}", exc_info=True)
            self.status = WorkflowStatus.FAILED
        
        self.completed_at = datetime.now(timezone.utc)
        
        # 发布完成事件
        if self.orchestrator._message_bus:
            metrics = {
                "workflow_id": self.workflow_id,
                "template": self.template_name,
                "total_steps": len(self.steps),
                "completed_steps": sum(1 for s in self.steps if s.status == WorkflowStatus.COMPLETED),
                "status": self.status.value,
                "duration": (self.completed_at - self.started_at).total_seconds(),
            }
            await self.orchestrator.publish_workflow_event(
                event_type=DesignFlowEventTypes.TASK_COMPLETED if self.status == WorkflowStatus.COMPLETED
                          else DesignFlowEventTypes.TASK_FAILED,
                task_id=self.context.get("task_id", self.workflow_id),
                phase="workflow_completed",
                status=self.status.value,
                metrics=metrics,
            )
        
        return self.status == WorkflowStatus.COMPLETED
    
    def get_status(self) -> Dict[str, Any]:
        """获取工作流当前状态"""
        return {
            "workflow_id": self.workflow_id,
            "template_name": self.template_name,
            "status": self.status.value,
            "current_step": next(
                (s.step_id for s in self.steps if s.status == WorkflowStatus.RUNNING),
                None,
            ),
            "steps": [
                {
                    "step_id": s.step_id,
                    "name": s.name,
                    "status": s.status.value,
                    "error": s.error,
                }
                for s in self.steps
            ],
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


# 导出
__all__ = [
    "FlowOrchestrator",
    "WorkflowRunner",
    "WorkflowContext",
    "WorkflowStep",
    "WorkflowStatus",
]
