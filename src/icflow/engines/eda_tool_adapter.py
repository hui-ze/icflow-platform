"""
EDA工具适配器引擎 (EDA Tool Adapter Engine)

职责：封装各类EDA工具（Cadence、Synopsys、Siemens）的调用细节，提供统一接口
触发事件：ToolExecutionEvent
输出事件：ToolExecutionEvent（结果）、DesignFlowEvent（工具执行状态）

关键能力：命令行组装、输出解析、错误处理、许可证管理
"""

import asyncio
import logging
import subprocess
import shlex
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import os
import re

from src.icflow.core.flow_engine import FlowEngine, FlowEvent
from src.icflow.core.concrete_events import (
    ToolExecutionEventTypes,
    DesignFlowEventTypes,
    create_tool_execution_event,
    create_design_flow_event,
    create_tool_started_event,
)


logger = logging.getLogger(__name__)


class EDAToolAdapterEngine(FlowEngine):
    """
    EDA工具适配器引擎
    
    关键能力：
    - 命令行组装：根据工具类型和参数生成可执行的命令行
    - 输出解析：解析工具输出，提取关键信息（错误、警告、结果）
    - 错误处理：捕获工具执行异常，转换为标准化错误事件
    - 许可证管理：检查和管理EDA工具许可证
    """
    
    engine_id = "eda_tool_adapter_engine"
    engine_name = "EDA工具适配器引擎"
    engine_description = "封装各类EDA工具调用细节，提供统一接口"
    
    # 订阅工具执行事件
    subscribed_event_types = [
        ToolExecutionEventTypes.TOOL_STARTED,
    ]
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化EDA工具适配器引擎
        
        Args:
            config: 引擎配置字典
        """
        super().__init__(config)
        
        # 工具路径配置
        self.tool_paths = self.config.get("tool_paths", {
            "calibre": "/tools/calibre/bin/calibre",
            "icv": "/tools/icv/bin/icv",
            "innovus": "/tools/innovus/bin/innovus",
            "genus": "/tools/genus/bin/genus",
            "xcelium": "/tools/xcelium/bin/xcelium",
        })
        
        # 许可证服务器配置
        self.license_servers = self.config.get("license_servers", {
            "cadence": "5280@lic-server",
            "synopsys": "27000@synopsys-lic",
            "siemens": "27000@siemens-lic",
        })
        
        # 默认超时时间（秒）
        self.default_timeout = self.config.get("default_timeout", 3600)
        
        # 输出解析器映射
        self.output_parsers = {
            "calibre": self._parse_calibre_output,
            "icv": self._parse_icv_output,
            "innovus": self._parse_innovus_output,
            "genus": self._parse_genus_output,
            "xcelium": self._parse_xcelium_output,
        }
        
        # 活动任务跟踪
        self.active_tasks: Dict[str, asyncio.Task] = {}
    
    async def process(self, event: FlowEvent) -> Optional[FlowEvent]:
        """
        处理事件（主入口）
        
        Args:
            event: 接收到的事件
            
        Returns:
            处理结果事件，如果无需返回则为 None
        """
        event_type = event.event_type
        
        if event_type == ToolExecutionEventTypes.TOOL_STARTED:
            await self._handle_tool_started(event)
        else:
            logger.warning(f"EDA工具适配器引擎收到未订阅的事件类型: {event_type}")
        
        # EDA工具适配器引擎通常不返回事件，而是发布新事件到消息总线
        return None
    
    async def _handle_tool_started(self, event: FlowEvent) -> None:
        """
        处理工具开始执行事件
        
        Args:
            event: ToolExecutionEvent (TOOL_STARTED)
        """
        payload = event.payload
        tool_name = payload.get("tool_name")
        command_line = payload.get("command_line")
        task_id = payload.get("task_id", event.metadata.get("correlation_id", str(event.event_id)))
        
        if not tool_name:
            logger.error("工具执行事件缺少tool_name字段")
            return
        
        if not command_line:
            logger.error(f"工具{tool_name}执行事件缺少command_line字段")
            return
        
        # 创建任务执行工具
        task = asyncio.create_task(
            self._execute_tool(task_id, tool_name, command_line, event)
        )
        self.active_tasks[task_id] = task
        task.add_done_callback(lambda t: self.active_tasks.pop(task_id, None))
        
        logger.info(f"已启动工具执行任务: {task_id}, 工具: {tool_name}")
    
    async def _execute_tool(
        self, 
        task_id: str,
        tool_name: str,
        command_line: str,
        original_event: FlowEvent
    ) -> None:
        """
        执行EDA工具
        
        Args:
            task_id: 任务ID
            tool_name: 工具名称
            command_line: 命令行参数
            original_event: 原始事件
        """
        # 1. 检查许可证
        license_available = await self._check_license(tool_name)
        if not license_available:
            await self._emit_tool_failed(
                task_id, tool_name, command_line,
                "许可证不可用",
                exit_code=-1,
                original_event=original_event
            )
            return
        
        # 2. 组装完整命令行
        full_command = self._assemble_command(tool_name, command_line)
        if not full_command:
            await self._emit_tool_failed(
                task_id, tool_name, command_line,
                f"工具 {tool_name} 未配置或路径不存在",
                exit_code=-1,
                original_event=original_event
            )
            return
        
        # 3. 发射工具执行中的设计流程事件
        await self._emit_design_flow_event(
            task_id, tool_name, "tool_execution_started", 
            {"command": full_command},
            original_event
        )
        
        # 4. 执行工具
        start_time = datetime.now(timezone.utc)
        exit_code, stdout, stderr = await self._run_command(full_command)
        end_time = datetime.now(timezone.utc)
        execution_time = (end_time - start_time).total_seconds()
        
        # 5. 解析输出
        parsed_output = self._parse_tool_output(tool_name, stdout, stderr, exit_code)
        
        # 6. 发射结果事件
        if exit_code == 0:
            await self._emit_tool_completed(
                task_id, tool_name, command_line,
                parsed_output, exit_code, execution_time,
                original_event
            )
            await self._emit_design_flow_event(
                task_id, tool_name, "tool_execution_completed",
                {
                    "execution_time": execution_time,
                    "exit_code": exit_code,
                    "parsed_output": parsed_output
                },
                original_event
            )
        else:
            await self._emit_tool_failed(
                task_id, tool_name, command_line,
                f"工具执行失败，退出码: {exit_code}",
                exit_code,
                original_event,
                parsed_output,
                execution_time
            )
            await self._emit_design_flow_event(
                task_id, tool_name, "tool_execution_failed",
                {
                    "execution_time": execution_time,
                    "exit_code": exit_code,
                    "error_message": parsed_output.get("error", "未知错误")
                },
                original_event
            )
    
    async def _check_license(self, tool_name: str) -> bool:
        """
        检查工具许可证是否可用
        
        Args:
            tool_name: 工具名称
            
        Returns:
            bool: 许可证是否可用
        """
        # 简化的许可证检查逻辑
        # 实际实现中，这里会调用lmstat或类似工具检查许可证服务器
        tool_vendor = self._get_tool_vendor(tool_name)
        license_server = self.license_servers.get(tool_vendor)
        
        if not license_server:
            logger.warning(f"工具 {tool_name} 未配置许可证服务器，跳过检查")
            return True
        
        # 模拟许可证检查（实际实现需调用lmstat）
        # 这里总是返回True，简化实现
        return True
    
    def _get_tool_vendor(self, tool_name: str) -> str:
        """
        根据工具名称获取厂商
        
        Args:
            tool_name: 工具名称
            
        Returns:
            str: 厂商名称
        """
        vendor_map = {
            "calibre": "siemens",
            "icv": "synopsys",
            "innovus": "cadence",
            "genus": "cadence",
            "xcelium": "cadence",
        }
        return vendor_map.get(tool_name, "unknown")
    
    def _assemble_command(self, tool_name: str, command_line: str) -> Optional[str]:
        """
        组装完整命令行
        
        Args:
            tool_name: 工具名称
            command_line: 命令行参数
            
        Returns:
            Optional[str]: 完整命令行，如果工具路径不存在则返回None
        """
        tool_path = self.tool_paths.get(tool_name)
        if not tool_path:
            logger.error(f"工具 {tool_name} 未配置路径")
            return None
        
        # 检查工具路径是否存在（简化检查）
        # 实际实现中可能需要检查文件是否存在或可执行
        return f"{tool_path} {command_line}"
    
    async def _run_command(self, command: str) -> tuple[int, str, str]:
        """
        运行命令行命令
        
        Args:
            command: 完整命令行
            
        Returns:
            tuple[int, str, str]: (退出码, stdout, stderr)
        """
        try:
            # 使用asyncio创建子进程
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                shell=True
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.default_timeout
            )
            
            exit_code = process.returncode
            return exit_code, stdout.decode('utf-8', errors='ignore'), stderr.decode('utf-8', errors='ignore')
            
        except asyncio.TimeoutError:
            logger.error(f"命令执行超时: {command}")
            return -2, "", "执行超时"
        except Exception as e:
            logger.error(f"命令执行异常: {e}")
            return -1, "", str(e)
    
    def _parse_tool_output(self, tool_name: str, stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
        """
        解析工具输出
        
        Args:
            tool_name: 工具名称
            stdout: 标准输出
            stderr: 标准错误
            exit_code: 退出码
            
        Returns:
            Dict[str, Any]: 解析后的输出
        """
        parser = self.output_parsers.get(tool_name, self._parse_generic_output)
        return parser(stdout, stderr, exit_code)
    
    def _parse_calibre_output(self, stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
        """解析Calibre输出"""
        result = {
            "tool": "calibre",
            "exit_code": exit_code,
            "errors": [],
            "warnings": [],
            "summary": {},
            "raw_stdout": stdout[:1000],  # 只保留前1000字符
            "raw_stderr": stderr[:1000],
        }
        
        # 提取错误和警告（简化实现）
        error_patterns = [
            r"ERROR:\s*(.+)",
            r"Error:\s*(.+)",
            r"FATAL:\s*(.+)",
        ]
        
        warning_patterns = [
            r"WARNING:\s*(.+)",
            r"Warning:\s*(.+)",
        ]
        
        for pattern in error_patterns:
            for match in re.finditer(pattern, stdout + stderr):
                result["errors"].append(match.group(1).strip())
        
        for pattern in warning_patterns:
            for match in re.finditer(pattern, stdout + stderr):
                result["warnings"].append(match.group(1).strip())
        
        # 尝试提取DRC结果
        drc_pattern = r"Total DRC violations found:\s*(\d+)"
        drc_match = re.search(drc_pattern, stdout)
        if drc_match:
            result["summary"]["drc_violations"] = int(drc_match.group(1))
        
        return result
    
    def _parse_icv_output(self, stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
        """解析ICV输出"""
        result = {
            "tool": "icv",
            "exit_code": exit_code,
            "errors": [],
            "warnings": [],
            "summary": {},
            "raw_stdout": stdout[:1000],
            "raw_stderr": stderr[:1000],
        }
        
        # 类似Calibre的解析逻辑
        return result
    
    def _parse_innovus_output(self, stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
        """解析Innovus输出"""
        result = {
            "tool": "innovus",
            "exit_code": exit_code,
            "errors": [],
            "warnings": [],
            "summary": {},
            "raw_stdout": stdout[:1000],
            "raw_stderr": stderr[:1000],
        }
        
        return result
    
    def _parse_genus_output(self, stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
        """解析Genus输出"""
        result = {
            "tool": "genus",
            "exit_code": exit_code,
            "errors": [],
            "warnings": [],
            "summary": {},
            "raw_stdout": stdout[:1000],
            "raw_stderr": stderr[:1000],
        }
        
        return result
    
    def _parse_xcelium_output(self, stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
        """解析Xcelium输出"""
        result = {
            "tool": "xcelium",
            "exit_code": exit_code,
            "errors": [],
            "warnings": [],
            "summary": {},
            "raw_stdout": stdout[:1000],
            "raw_stderr": stderr[:1000],
        }
        
        return result
    
    def _parse_generic_output(self, stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
        """通用输出解析"""
        return {
            "tool": "unknown",
            "exit_code": exit_code,
            "errors": [line for line in stderr.split('\n') if line.strip()] if stderr else [],
            "warnings": [],
            "summary": {},
            "raw_stdout": stdout[:1000],
            "raw_stderr": stderr[:1000],
        }
    
    async def _emit_tool_completed(
        self,
        task_id: str,
        tool_name: str,
        command_line: str,
        parsed_output: Dict[str, Any],
        exit_code: int,
        execution_time: float,
        original_event: FlowEvent
    ) -> None:
        """发射工具完成事件"""
        event = create_tool_execution_event(
            event_type=ToolExecutionEventTypes.TOOL_COMPLETED,
            tool_name=tool_name,
            command_line=command_line,
            output_path=parsed_output.get("output_file"),
            execution_status="completed",
            exit_code=exit_code,
            source=self.engine_id,
            task_id=task_id,
            parsed_output=parsed_output,
            execution_time=execution_time,
            correlation_id=original_event.metadata.get("correlation_id"),
        )
        
        if self._message_bus:
            await self._message_bus.publish(event)
        logger.info(f"工具执行完成: {tool_name}, 任务ID: {task_id}")
    
    async def _emit_tool_failed(
        self,
        task_id: str,
        tool_name: str,
        command_line: str,
        error_message: str,
        exit_code: int,
        original_event: FlowEvent,
        parsed_output: Optional[Dict[str, Any]] = None,
        execution_time: Optional[float] = None
    ) -> None:
        """发射工具失败事件"""
        event = create_tool_execution_event(
            event_type=ToolExecutionEventTypes.TOOL_FAILED,
            tool_name=tool_name,
            command_line=command_line,
            execution_status="failed",
            exit_code=exit_code,
            source=self.engine_id,
            task_id=task_id,
            error_message=error_message,
            parsed_output=parsed_output or {},
            execution_time=execution_time,
            correlation_id=original_event.metadata.get("correlation_id"),
        )
        
        if self._message_bus:
            await self._message_bus.publish(event)
        logger.error(f"工具执行失败: {tool_name}, 任务ID: {task_id}, 错误: {error_message}")
    
    async def _emit_design_flow_event(
        self,
        task_id: str,
        tool_name: str,
        phase: str,
        metrics: Dict[str, Any],
        original_event: FlowEvent
    ) -> None:
        """发射设计流程事件"""
        event = create_design_flow_event(
            event_type=DesignFlowEventTypes.PHASE_COMPLETED,
            task_id=task_id,
            phase=f"tool_{tool_name}_{phase}",
            status="completed" if "failed" not in phase else "failed",
            metrics=metrics,
            source=self.engine_id,
            correlation_id=original_event.metadata.get("correlation_id"),
        )
        
        if self._message_bus:
            await self._message_bus.publish(event)