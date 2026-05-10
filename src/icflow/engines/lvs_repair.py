"""
LVS修复主引擎 (LVS Repair Master Engine)

职责：统筹LVS违例修复全流程，调用子引擎完成具体修复操作
触发事件：DesignFlowEvent（类型为 lvs_violation_detected）
输出事件：ToolExecutionEvent（调用Calibre、ICV等工具）、KnowledgeCaptureEvent（记录修复决策）
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from src.icflow.core.flow_engine import FlowEngine, FlowEvent
from src.icflow.core.concrete_events import (
    DesignFlowEventTypes,
    ToolExecutionEventTypes,
    KnowledgeCaptureEventTypes,
    create_tool_execution_event,
    create_knowledge_capture_event,
)


logger = logging.getLogger(__name__)


class LVSRepairMasterEngine(FlowEngine):
    """
    LVS修复主引擎
    
    关键能力：
    - 修复策略选择
    - 多工具协调
    - 迭代优化
    """
    
    engine_id = "lvs_repair_master_engine"
    engine_name = "LVS修复主引擎"
    engine_description = "统筹LVS违例修复全流程，调用子引擎完成具体修复操作"
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化LVS修复主引擎
        
        Args:
            config: 引擎配置字典
        """
        super().__init__(config)
        
        # 引擎配置
        self.default_tool = config.get("default_tool", "calibre") if config else "calibre"
        self.repair_strategies = {
            "short": "auto_cut",
            "open": "auto_connect",
            "mismatch": "auto_adjust",
            "floating": "auto_ground",
        }
        
        # 状态跟踪
        self.active_repairs: Dict[str, Dict[str, Any]] = {}  # task_id -> 修复信息
        self.repair_history: List[Dict[str, Any]] = []
        
        # 订阅LVS违例事件
        self.subscribed_event_types = [DesignFlowEventTypes.LVS_VIOLATION_DETECTED]
        
        logger.info(f"LVS修复主引擎初始化完成: {self.engine_id}")
    
    async def start(self):
        """启动引擎"""
        await super().start()
        logger.info(f"LVS修复主引擎启动: {self.engine_id}")
    
    async def stop(self):
        """停止引擎"""
        await super().stop()
        logger.info(f"LVS修复主引擎停止: {self.engine_id}")

    async def publish_event(self, event: FlowEvent) -> None:
        """
        发布事件到消息总线
        """
        if self._message_bus:
            logger.debug(f"LVS修复引擎发布事件: {event.event_type} [{event.event_id}]")
            await self._message_bus.publish(event)
        else:
            logger.warning(f"消息总线未设置，无法发布事件: {event.event_type}")

    async def process(self, event: FlowEvent) -> Optional[FlowEvent]:
        """
        处理事件（主入口）
        
        Args:
            event: 接收到的事件
            
        Returns:
            处理结果事件，如果无需返回则为 None
        """
        if event.event_type == DesignFlowEventTypes.LVS_VIOLATION_DETECTED:
            await self._handle_lvs_violation(event)
        else:
            logger.warning(f"收到未订阅的事件类型: {event.event_type}")
        
        # LVS修复引擎通常不返回事件，而是发布新事件到消息总线
        return None
    
    async def _handle_lvs_violation(self, event: FlowEvent) -> None:
        """
        处理LVS违例事件
        
        Args:
            event: LVS违例事件
        """
        payload = event.payload
        task_id = payload.get("task_id", "unknown_task")
        violation_id = payload.get("violation_id", "unknown_violation")
        violation_type = payload.get("violation_type", "unknown")
        
        logger.info(f"开始处理LVS违例: task={task_id}, violation={violation_id}, type={violation_type}")
        
        # 记录修复开始
        self.active_repairs[task_id] = {
            "task_id": task_id,
            "violation_id": violation_id,
            "violation_type": violation_type,
            "start_time": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress",
        }
        
        # 第1步：分析违例，选择修复策略
        strategy = self._select_repair_strategy(violation_type)
        logger.info(f"选定修复策略: {strategy}")
        
        # 发布知识捕获事件 - 记录分析决策
        await self._publish_knowledge_capture(
            rule_id=violation_id,
            context=payload,
            decision_reason=f"自动选择修复策略: {strategy}",
            related_files=payload.get("related_files", []),
            source_event=event
        )
        
        # 第2步：执行修复工具
        tool_output_path = f"/tmp/repair_{task_id}_{violation_id}.log"
        await self._execute_repair_tool(
            tool_name=self.default_tool,
            violation_type=violation_type,
            strategy=strategy,
            output_path=tool_output_path,
            source_event=event
        )
        
        # 第3步：验证修复结果
        verification_passed = await self._verify_repair_result(
            task_id=task_id,
            violation_id=violation_id,
            output_path=tool_output_path
        )
        
        # 第4步：更新修复状态
        if verification_passed:
            logger.info(f"LVS违例修复成功: {violation_id}")
            await self._complete_repair(task_id, violation_id, event)
        else:
            logger.warning(f"LVS违例修复失败或需要人工干预: {violation_id}")
            await self._fail_repair(task_id, violation_id, event)
    
    def _select_repair_strategy(self, violation_type: str) -> str:
        """
        根据违例类型选择修复策略
        
        Args:
            violation_type: 违例类型
            
        Returns:
            修复策略名称
        """
        return self.repair_strategies.get(violation_type, "manual_review")
    
    async def _publish_knowledge_capture(
        self,
        rule_id: str,
        context: Dict[str, Any],
        decision_reason: str,
        related_files: List[str],
        source_event: FlowEvent
    ) -> None:
        """
        发布知识捕获事件
        
        Args:
            rule_id: 规则ID
            context: 上下文信息
            decision_reason: 决策理由
            related_files: 关联文件
            source_event: 源事件（用于上下文链）
        """
        knowledge_event = create_knowledge_capture_event(
            event_type=KnowledgeCaptureEventTypes.ENGINEER_DECISION,
            rule_id=rule_id,
            context=context,
            decision_reason=decision_reason,
            related_files=related_files,
            source=self.engine_id
        )
        
        # 添加上下文链
        knowledge_event.context["source_event_id"] = source_event.event_id
        
        await self.publish_event(knowledge_event)
        logger.debug(f"发布知识捕获事件: {knowledge_event.event_id}")
    
    async def _execute_repair_tool(
        self,
        tool_name: str,
        violation_type: str,
        strategy: str,
        output_path: str,
        source_event: FlowEvent
    ) -> None:
        """
        执行修复工具
        
        Args:
            tool_name: 工具名称
            violation_type: 违例类型
            strategy: 修复策略
            output_path: 输出文件路径
            source_event: 源事件
        """
        # 构建命令行（模拟）
        cmd = f"{tool_name} -repair -type {violation_type} -strategy {strategy} -out {output_path}"
        
        logger.info(f"执行修复工具: {cmd}")
        
        # 发布工具开始事件
        tool_start_event = create_tool_execution_event(
            event_type=ToolExecutionEventTypes.TOOL_STARTED,
            tool_name=tool_name,
            command_line=cmd,
            output_path=output_path,
            execution_status="started",
            source=self.engine_id
        )
        tool_start_event.context["source_event_id"] = source_event.event_id
        
        await self.publish_event(tool_start_event)
        
        # 模拟工具执行延迟
        await asyncio.sleep(0.5)
        
        # 发布工具完成事件（模拟成功）
        tool_complete_event = create_tool_execution_event(
            event_type=ToolExecutionEventTypes.TOOL_COMPLETED,
            tool_name=tool_name,
            command_line=cmd,
            output_path=output_path,
            execution_status="completed",
            exit_code=0,
            source=self.engine_id
        )
        tool_complete_event.context["source_event_id"] = source_event.event_id
        
        await self.publish_event(tool_complete_event)
        
        logger.info(f"修复工具执行完成: {tool_name}")
    
    async def _verify_repair_result(
        self,
        task_id: str,
        violation_id: str,
        output_path: str
    ) -> bool:
        """
        验证修复结果
        
        Args:
            task_id: 任务ID
            violation_id: 违例ID
            output_path: 工具输出路径
            
        Returns:
            验证是否通过
        """
        # 模拟验证过程
        await asyncio.sleep(0.2)
        
        # 模拟95%的成功率
        import random
        return random.random() < 0.95
    
    async def _complete_repair(
        self,
        task_id: str,
        violation_id: str,
        source_event: FlowEvent
    ) -> None:
        """
        完成修复流程
        
        Args:
            task_id: 任务ID
            violation_id: 违例ID
            source_event: 源事件
        """
        # 更新状态
        repair_info = self.active_repairs.get(task_id, {})
        repair_info.update({
            "end_time": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "result": "success"
        })
        
        self.repair_history.append(repair_info.copy())
        
        # 发布知识捕获事件 - 记录成功修复
        await self._publish_knowledge_capture(
            rule_id=violation_id,
            context={"task_id": task_id, "repair_info": repair_info},
            decision_reason="LVS违例自动修复成功",
            related_files=[],  # 实际中可能包含修复后的文件
            source_event=source_event
        )
        
        # 清理活跃修复记录
        if task_id in self.active_repairs:
            del self.active_repairs[task_id]
        
        logger.info(f"修复完成: {violation_id}")
    
    async def _fail_repair(
        self,
        task_id: str,
        violation_id: str,
        source_event: FlowEvent
    ) -> None:
        """
        修复失败处理
        
        Args:
            task_id: 任务ID
            violation_id: 违例ID
            source_event: 源事件
        """
        # 更新状态
        repair_info = self.active_repairs.get(task_id, {})
        repair_info.update({
            "end_time": datetime.now(timezone.utc).isoformat(),
            "status": "failed",
            "result": "failure"
        })
        
        self.repair_history.append(repair_info.copy())
        
        # 发布知识捕获事件 - 记录失败
        await self._publish_knowledge_capture(
            rule_id=violation_id,
            context={"task_id": task_id, "repair_info": repair_info},
            decision_reason="LVS违例自动修复失败，需要人工干预",
            related_files=[],
            source_event=source_event
        )
        
        logger.warning(f"修复失败: {violation_id}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取引擎统计信息
        
        Returns:
            统计信息字典
        """
        return {
            "engine_id": self.engine_id,
            "active_repairs": len(self.active_repairs),
            "total_repaired": len([r for r in self.repair_history if r.get("result") == "success"]),
            "total_failed": len([r for r in self.repair_history if r.get("result") == "failure"]),
            "repair_history_count": len(self.repair_history),
        }


# 导出
__all__ = ["LVSRepairMasterEngine"]