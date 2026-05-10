"""
Extension Protocol 实现
定义扩展与平台间的通信协议
"""

import json
import logging
from typing import Dict, Any, Optional, List, Union
from enum import Enum
from pydantic import BaseModel, Field

from ..core.flow_event import FlowEvent


logger = logging.getLogger(__name__)


class ExtensionRequestType(str, Enum):
    """扩展请求类型"""
    
    # 核心请求
    PING = "ping"
    INFO = "info"
    HEALTH = "health"
    
    # 能力请求
    CAPABILITIES = "capabilities"
    EXECUTE = "execute"
    QUERY = "query"
    
    # 事件相关
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    PUBLISH = "publish"
    
    # 工具调用
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"


class ExtensionResponseStatus(str, Enum):
    """扩展响应状态"""
    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"
    UNAUTHORIZED = "unauthorized"
    NOT_FOUND = "not_found"
    INVALID_REQUEST = "invalid_request"


class ExtensionRequest(BaseModel):
    """扩展请求"""
    
    # 请求标识
    request_id: str = Field(..., description="请求唯一标识符")
    request_type: ExtensionRequestType = Field(..., description="请求类型")
    
    # 目标扩展
    extension_id: Optional[str] = Field(None, description="目标扩展ID")
    capability: Optional[str] = Field(None, description="目标能力类型")
    
    # 请求数据
    parameters: Dict[str, Any] = Field(default_factory=dict, description="请求参数")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="请求元数据")
    
    # 上下文
    context: Dict[str, Any] = Field(default_factory=dict, description="请求上下文")
    
    class Config:
        arbitrary_types_allowed = True
        use_enum_values = False


class ExtensionResponse(BaseModel):
    """扩展响应"""
    
    # 响应标识
    request_id: str = Field(..., description="对应请求ID")
    response_id: str = Field(..., description="响应唯一标识符")
    
    # 响应状态
    status: ExtensionResponseStatus = Field(..., description="响应状态")
    
    # 响应数据
    result: Optional[Dict[str, Any]] = Field(None, description="响应结果")
    error: Optional[Dict[str, Any]] = Field(None, description="错误信息")
    
    # 元数据
    metadata: Dict[str, Any] = Field(default_factory=dict, description="响应元数据")
    
    class Config:
        arbitrary_types_allowed = True
        use_enum_values = False


class ExtensionProtocol:
    """扩展协议处理器"""
    
    def __init__(self, extension_registry, message_bus):
        """
        初始化扩展协议
        
        Args:
            extension_registry: 扩展注册表实例
            message_bus: 消息总线实例
        """
        self.extension_registry = extension_registry
        self.message_bus = message_bus
        self._subscriptions: Dict[str, List[str]] = {}  # event_type -> [extension_id]
    
    async def handle_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理扩展协议请求
        
        Args:
            request_data: 请求数据（字典）
            
        Returns:
            响应数据（字典）
        """
        try:
            # 解析请求
            request = ExtensionRequest(**request_data)
            logger.debug(f"处理扩展协议请求: {request.request_type} [{request.request_id}]")
            
            # 根据请求类型路由处理
            handler_name = f"_handle_{request.request_type.value}"
            if hasattr(self, handler_name):
                handler = getattr(self, handler_name)
                response_data = await handler(request)
            else:
                response_data = self._create_error_response(
                    request.request_id,
                    ExtensionResponseStatus.INVALID_REQUEST,
                    f"未知的请求类型: {request.request_type}"
                )
            
            return response_data
            
        except Exception as e:
            logger.error(f"处理扩展协议请求失败: {e}", exc_info=True)
            request_id = request_data.get("request_id", "unknown")
            return self._create_error_response(
                request_id,
                ExtensionResponseStatus.ERROR,
                str(e)
            )
    
    async def _handle_ping(self, request: ExtensionRequest) -> Dict[str, Any]:
        """处理 ping 请求"""
        return self._create_success_response(
            request.request_id,
            {"message": "pong", "timestamp": self._current_timestamp()}
        )
    
    async def _handle_info(self, request: ExtensionRequest) -> Dict[str, Any]:
        """处理 info 请求"""
        if request.extension_id:
            # 获取指定扩展信息
            extension = self.extension_registry.get(request.extension_id)
            if not extension:
                return self._create_error_response(
                    request.request_id,
                    ExtensionResponseStatus.NOT_FOUND,
                    f"扩展不存在: {request.extension_id}"
                )
            
            result = {
                "extension": extension.info.__dict__,
                "stats": extension.get_stats(),
            }
        else:
            # 获取所有扩展信息
            extensions = self.extension_registry.get_all()
            result = {
                "extensions": [
                    {
                        "info": ext.info.__dict__,
                        "stats": ext.get_stats(),
                    }
                    for ext in extensions
                ],
                "total": len(extensions),
            }
        
        return self._create_success_response(request.request_id, result)
    
    async def _handle_health(self, request: ExtensionRequest) -> Dict[str, Any]:
        """处理 health 请求"""
        health_status = {
            "status": "healthy",
            "timestamp": self._current_timestamp(),
            "extensions": {},
        }
        
        # 检查所有扩展的健康状态
        extensions = self.extension_registry.get_all()
        for ext in extensions:
            ext_status = {
                "running": ext.is_running(),
                "stats": ext.get_stats(),
            }
            health_status["extensions"][ext.extension_id] = ext_status
            
            if not ext.is_running():
                health_status["status"] = "degraded"
        
        return self._create_success_response(request.request_id, health_status)
    
    async def _handle_capabilities(self, request: ExtensionRequest) -> Dict[str, Any]:
        """处理 capabilities 请求"""
        if request.capability:
            # 获取具有指定能力的扩展
            from .base import ExtensionCapability
            
            try:
                capability = ExtensionCapability(request.capability)
                extensions = self.extension_registry.get_by_capability(capability)
            except ValueError:
                return self._create_error_response(
                    request.request_id,
                    ExtensionResponseStatus.INVALID_REQUEST,
                    f"无效的能力类型: {request.capability}"
                )
            
            result = {
                "capability": request.capability,
                "extensions": [ext.info.__dict__ for ext in extensions],
                "count": len(extensions),
            }
        else:
            # 获取所有能力
            capabilities = self.extension_registry.get_capabilities()
            result = {
                "capabilities": [cap.value for cap in capabilities],
                "count": len(capabilities),
            }
        
        return self._create_success_response(request.request_id, result)
    
    async def _handle_execute(self, request: ExtensionRequest) -> Dict[str, Any]:
        """处理 execute 请求"""
        if not request.extension_id:
            return self._create_error_response(
                request.request_id,
                ExtensionResponseStatus.INVALID_REQUEST,
                "执行请求必须指定 extension_id"
            )
        
        # 获取扩展
        extension = self.extension_registry.get(request.extension_id)
        if not extension:
            return self._create_error_response(
                request.request_id,
                ExtensionResponseStatus.NOT_FOUND,
                f"扩展不存在: {request.extension_id}"
            )
        
        # 检查扩展是否在运行
        if not extension.is_running():
            return self._create_error_response(
                request.request_id,
                ExtensionResponseStatus.ERROR,
                f"扩展未运行: {request.extension_id}"
            )
        
        # 执行请求
        try:
            result = await extension.handle_request(request.parameters)
            return self._create_success_response(request.request_id, result)
        except Exception as e:
            return self._create_error_response(
                request.request_id,
                ExtensionResponseStatus.ERROR,
                f"执行失败: {str(e)}"
            )
    
    async def _handle_publish(self, request: ExtensionRequest) -> Dict[str, Any]:
        """处理 publish 请求"""
        event_type = request.parameters.get("event_type")
        payload = request.parameters.get("payload", {})
        
        if not event_type:
            return self._create_error_response(
                request.request_id,
                ExtensionResponseStatus.INVALID_REQUEST,
                "发布事件必须指定 event_type"
            )
        
        # 创建事件
        event = FlowEvent(
            event_type=event_type,
            source=request.parameters.get("source", "extension_protocol"),
            source_type="extension",
            payload=payload,
            metadata=request.parameters.get("metadata", {}),
        )
        
        # 发布事件
        await self.message_bus.publish(event)
        
        result = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "timestamp": event.timestamp,
        }
        
        return self._create_success_response(request.request_id, result)
    
    async def _handle_subscribe(self, request: ExtensionRequest) -> Dict[str, Any]:
        """处理 subscribe 请求"""
        event_type = request.parameters.get("event_type")
        extension_id = request.parameters.get("extension_id")
        
        if not event_type or not extension_id:
            return self._create_error_response(
                request.request_id,
                ExtensionResponseStatus.INVALID_REQUEST,
                "订阅必须指定 event_type 和 extension_id"
            )
        
        # 检查扩展是否存在
        extension = self.extension_registry.get(extension_id)
        if not extension:
            return self._create_error_response(
                request.request_id,
                ExtensionResponseStatus.NOT_FOUND,
                f"扩展不存在: {extension_id}"
            )
        
        # 注册订阅
        if event_type not in self._subscriptions:
            self._subscriptions[event_type] = []
        
        if extension_id not in self._subscriptions[event_type]:
            self._subscriptions[event_type].append(extension_id)
            
            # 实际的消息总线订阅（简化实现）
            async def event_handler(event: FlowEvent):
                # 将事件转发给扩展
                if extension.is_running():
                    try:
                        await extension.generate_event(
                            f"forwarded.{event.event_type}",
                            event.payload
                        )
                    except Exception as e:
                        logger.error(f"转发事件到扩展失败: {e}")
            
            await self.message_bus.subscribe(event_type, event_handler)
        
        result = {
            "event_type": event_type,
            "extension_id": extension_id,
            "subscription_id": f"{event_type}:{extension_id}",
        }
        
        return self._create_success_response(request.request_id, result)
    
    def _create_success_response(self, request_id: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """创建成功响应"""
        response = ExtensionResponse(
            request_id=request_id,
            response_id=self._generate_id(),
            status=ExtensionResponseStatus.SUCCESS,
            result=result,
            metadata={"timestamp": self._current_timestamp()},
        )
        return response.model_dump()
    
    def _create_error_response(self, request_id: str, status: ExtensionResponseStatus, message: str) -> Dict[str, Any]:
        """创建错误响应"""
        response = ExtensionResponse(
            request_id=request_id,
            response_id=self._generate_id(),
            status=status,
            error={"message": message},
            metadata={"timestamp": self._current_timestamp()},
        )
        return response.model_dump()
    
    def _generate_id(self) -> str:
        """生成唯一ID"""
        import uuid
        return str(uuid.uuid4())
    
    def _current_timestamp(self) -> float:
        """获取当前时间戳"""
        import time
        return time.time()