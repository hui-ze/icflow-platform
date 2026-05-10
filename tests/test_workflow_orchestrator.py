"""
FlowOrchestrator 流程编排引擎测试
"""

import asyncio
import pytest
import pytest_asyncio

from src.icflow.engines.workflow_orchestrator import (
    FlowOrchestrator,
    WorkflowRunner,
    WorkflowContext,
    WorkflowStep,
    WorkflowStatus,
)
from src.icflow.core.concrete_events import (
    DesignFlowEventTypes,
    create_drc_violation_event,
)
from src.icflow.core.flow_event import FlowEvent


class MockMessageBus:
    """模拟消息总线"""
    def __init__(self):
        self.published_events = []
    
    async def publish(self, event):
        self.published_events.append(event)
    
    async def subscribe(self, event_type, callback):
        pass
    
    async def start(self):
        pass
    
    async def stop(self):
        pass


class TestFlowOrchestratorInitialization:
    """流程编排引擎初始化测试"""
    
    @pytest.mark.asyncio
    async def test_engine_initialization(self):
        """验证引擎ID、名称、订阅事件类型"""
        engine = FlowOrchestrator()
        
        assert engine.engine_id == "flow_orchestrator"
        assert engine.engine_name == "流程编排引擎"
        assert DesignFlowEventTypes.TASK_STARTED in engine.subscribed_event_types
        assert DesignFlowEventTypes.DRC_VIOLATION_DETECTED in engine.subscribed_event_types
    
    @pytest.mark.asyncio
    async def test_default_templates(self):
        """验证默认工作流模板已注册"""
        engine = FlowOrchestrator()
        
        templates = engine.list_workflow_templates()
        assert "drc_repair" in templates
        
        template = engine.get_workflow_template("drc_repair")
        assert template is not None
        assert len(template) == 5
        assert template[0].step_id == "violation_analysis"
        assert template[-1].step_id == "knowledge_capture"
    
    @pytest.mark.asyncio
    async def test_register_custom_template(self):
        """验证自定义工作流模板注册"""
        engine = FlowOrchestrator()
        
        custom_steps = [
            WorkflowStep(step_id="step1", name="第一步", handler=lambda ctx: True),
            WorkflowStep(step_id="step2", name="第二步", handler=lambda ctx: True, depends_on=["step1"]),
        ]
        engine.register_workflow_template("custom_flow", custom_steps)
        
        assert "custom_flow" in engine.list_workflow_templates()
        template = engine.get_workflow_template("custom_flow")
        assert len(template) == 2


class TestFlowOrchestratorWorkflow:
    """工作流执行测试"""
    
    @pytest_asyncio.fixture
    async def engine(self):
        engine = FlowOrchestrator()
        engine.message_bus = MockMessageBus()
        await engine.start()
        yield engine
        await engine.stop()
    
    @pytest.mark.asyncio
    async def test_start_workflow(self, engine):
        """启动工作流并验证workflow_id格式"""
        wid = await engine.start_workflow(
            template_name="drc_repair",
            initial_data={"task_id": "test_001", "violation_type": "min_width"},
        )
        assert wid.startswith("wf_")
        assert len(wid) > 5
    
    @pytest.mark.asyncio
    async def test_get_workflow_status(self, engine):
        """查询工作流状态"""
        wid = await engine.start_workflow(
            template_name="drc_repair",
            initial_data={"task_id": "test_002"},
        )
        status = engine.get_workflow_status(wid)
        assert status is not None
        assert status["workflow_id"] == wid
        assert "steps" in status
        assert len(status["steps"]) == 5
    
    @pytest.mark.asyncio
    async def test_workflow_run_completion(self, engine):
        """等待工作流完成，验证最终状态"""
        wid = await engine.start_workflow(
            template_name="drc_repair",
            initial_data={"task_id": "test_003", "violation_type": "min_width"},
        )
        
        # 等待工作流完成（~0.8s执行时间）
        for _ in range(30):
            status = engine.get_workflow_status(wid)
            if status and status.get("status") in ("completed", "failed"):
                assert status["status"] == "completed"
                return
            await asyncio.sleep(0.1)
        
        pytest.fail("工作流未在预期时间内完成")
    
    @pytest.mark.asyncio
    async def test_list_active_workflows(self, engine):
        """列出活跃工作流"""
        wid = await engine.start_workflow(template_name="drc_repair")
        active = engine.list_active_workflows()
        assert any(w["workflow_id"] == wid for w in active)
    
    @pytest.mark.asyncio
    async def test_invalid_template(self, engine):
        """无效模板名应抛出ValueError"""
        with pytest.raises(ValueError, match="工作流模板不存在"):
            await engine.start_workflow(template_name="non_existent_template")
    
    @pytest.mark.asyncio
    async def test_process_task_started_event(self, engine):
        """处理TASK_STARTED事件应自动启动工作流"""
        event = FlowEvent(
            event_type=DesignFlowEventTypes.TASK_STARTED,
            source="test",
            source_type="flow_engine",
            payload={
                "task_id": "auto_001",
                "workflow_template": "drc_repair",
                "phase": "start",
            },
        )
        result = await engine.process(event)
        assert result is not None
        assert result.payload.get("metrics", {}).get("workflow_id", "").startswith("wf_")
    
    @pytest.mark.asyncio
    async def test_process_drc_violation_event(self, engine):
        """处理DRC违例事件应自动启动工作流"""
        drc_event = create_drc_violation_event(
            task_id="auto_002",
            violation_id="vio_auto",
            violation_type="min_spacing",
            location={"layer": "M1", "x": 0, "y": 0, "width": 0.1, "height": 0.1},
            rule_description="Test auto workflow",
            source="test",
        )
        result = await engine.process(drc_event)
        assert result is not None
        assert result.payload.get("metrics", {}).get("workflow_id", "").startswith("wf_")


class TestWorkflowRunner:
    """工作流运行器测试"""
    
    @pytest.mark.asyncio
    async def test_successful_workflow_steps(self):
        """所有步骤成功的场景"""
        results = []
        
        async def step_a(ctx):
            results.append("a")
            ctx.record_step_result("a", True)
            return True
        
        async def step_b(ctx):
            results.append("b")
            ctx.record_step_result("b", True)
            return True
        
        steps = [
            WorkflowStep(step_id="a", name="Step A", handler=step_a),
            WorkflowStep(step_id="b", name="Step B", handler=step_b, depends_on=["a"]),
        ]
        
        engine = FlowOrchestrator()
        ctx = WorkflowContext("test_wf_001")
        runner = WorkflowRunner("test_wf_001", "test", steps, ctx, engine)
        
        success = await runner.run()
        assert success
        assert runner.status == WorkflowStatus.COMPLETED
        assert results == ["a", "b"]
    
    @pytest.mark.asyncio
    async def test_failed_step_stops_workflow(self):
        """步骤失败应停止工作流"""
        async def failing_step(ctx):
            return False
        
        async def unreachable_step(ctx):
            pytest.fail("不应该被执行")
            return True
        
        steps = [
            WorkflowStep(step_id="fail", name="Fail", handler=failing_step),
            WorkflowStep(step_id="skip", name="Skip", handler=unreachable_step, depends_on=["fail"]),
        ]
        
        engine = FlowOrchestrator()
        ctx = WorkflowContext("test_wf_002")
        runner = WorkflowRunner("test_wf_002", "test", steps, ctx, engine)
        
        success = await runner.run()
        assert not success
        assert runner.status == WorkflowStatus.FAILED
        assert runner.steps[0].status == WorkflowStatus.FAILED
        assert runner.steps[1].status == WorkflowStatus.PENDING  # 因为依赖失败而未执行
