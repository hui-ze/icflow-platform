"""
扩展模块测试
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock

from src.icflow.extensions.base import (
    Extension,
    ExtensionRegistry,
    ExtensionCapability,
    ExtensionInfo
)
from src.icflow.extensions.protocol import (
    ExtensionProtocol,
    ExtensionRequest,
    ExtensionResponse,
    ExtensionRequestType,
    ExtensionResponseStatus
)


class TestExtension(Extension):
    """用于测试的Extension实现"""
    
    extension_id = "test-extension"
    
    async def process_request(self, request):
        return {"processed": True, "request": request}


@pytest.mark.asyncio
async def test_extension_creation():
    """测试Extension创建"""
    extension = TestExtension()
    
    assert extension.extension_id == "test-extension"
    assert extension.name == "test-extension"
    assert extension.version == "1.0.0"
    assert extension.description == ""
    assert extension.capabilities == []
    assert extension.metadata == {}
    assert extension.dependencies == []
    assert not extension.is_running()


@pytest.mark.asyncio
async def test_extension_with_config():
    """测试带配置的Extension"""
    config = {"key": "value"}
    extension = TestExtension(config=config)
    
    assert extension.config == config
    assert extension.info.extension_id == "test-extension"


@pytest.mark.asyncio
async def test_extension_start_stop():
    """测试扩展启动和停止"""
    extension = TestExtension()
    
    # 启动扩展
    await extension.start()
    assert extension.is_running()
    
    # 再次启动应该发出警告但不报错
    await extension.start()
    assert extension.is_running()
    
    # 停止扩展
    await extension.stop()
    assert not extension.is_running()
    
    # 再次停止应该发出警告但不报错
    await extension.stop()
    assert not extension.is_running()


@pytest.mark.asyncio
async def test_extension_with_message_bus():
    """测试带消息总线的扩展"""
    mock_bus = AsyncMock()
    extension = TestExtension()
    extension.message_bus = mock_bus
    
    await extension.start()
    
    # 检查是否发送了加载事件
    assert mock_bus.publish.called
    call_args = mock_bus.publish.call_args[0][0]
    assert call_args.event_type == "extension.loaded"
    
    await extension.stop()


@pytest.mark.asyncio
async def test_extension_handle_request():
    """测试扩展处理请求"""
    extension = TestExtension()
    await extension.start()
    
    request = {"action": "test"}
    result = await extension.handle_request(request)
    
    assert result["processed"] == True
    assert result["request"] == request
    
    await extension.stop()


@pytest.mark.asyncio
async def test_extension_generate_event():
    """测试扩展生成事件"""
    mock_bus = AsyncMock()
    extension = TestExtension()
    extension.message_bus = mock_bus
    
    await extension.start()
    
    event_type = "test.event"
    payload = {"data": "test"}
    
    event = await extension.generate_event(event_type, payload)
    
    assert event is not None
    assert event.event_type == event_type
    assert event.payload == payload
    assert event.source == extension.extension_id
    assert event.source_type == "extension"
    
    # 检查事件是否发布
    assert mock_bus.publish.called
    
    await extension.stop()


@pytest.mark.asyncio
async def test_extension_capabilities():
    """测试扩展能力"""
    extension = TestExtension()
    extension.capabilities = [
        ExtensionCapability.EVENT_PROCESSING,
        ExtensionCapability.DATA_STORAGE
    ]
    
    assert extension.has_capability(ExtensionCapability.EVENT_PROCESSING) == True
    assert extension.has_capability(ExtensionCapability.DATA_STORAGE) == True
    assert extension.has_capability(ExtensionCapability.API_INTEGRATION) == False


@pytest.mark.asyncio
async def test_extension_stats():
    """测试扩展统计信息"""
    extension = TestExtension()
    await extension.start()
    
    stats = extension.get_stats()
    
    assert "extension_id" in stats
    assert "name" in stats
    assert "version" in stats
    assert "running" in stats
    assert stats["running"] == True
    assert "requests_processed" in stats
    assert "start_time" in stats
    
    await extension.stop()
    
    stats = extension.get_stats()
    assert stats["running"] == False


@pytest.mark.asyncio
async def test_extension_registry():
    """测试ExtensionRegistry"""
    registry = ExtensionRegistry()
    
    # 创建并注册扩展
    extension1 = TestExtension()
    extension1.extension_id = "ext-1"
    extension1.capabilities = [ExtensionCapability.EVENT_PROCESSING]
    
    extension2 = TestExtension()
    extension2.extension_id = "ext-2"
    extension2.capabilities = [
        ExtensionCapability.DATA_STORAGE,
        ExtensionCapability.EVENT_PROCESSING
    ]
    
    registry.register(extension1)
    registry.register(extension2)
    
    # 获取扩展
    assert registry.get("ext-1") == extension1
    assert registry.get("ext-2") == extension2
    assert registry.get("non-existent") is None
    
    # 获取所有扩展
    all_extensions = registry.get_all()
    assert len(all_extensions) == 2
    assert extension1 in all_extensions
    assert extension2 in all_extensions
    
    # 按能力获取扩展
    event_extensions = registry.get_by_capability(ExtensionCapability.EVENT_PROCESSING)
    assert len(event_extensions) == 2
    assert extension1 in event_extensions
    assert extension2 in event_extensions
    
    data_extensions = registry.get_by_capability(ExtensionCapability.DATA_STORAGE)
    assert len(data_extensions) == 1
    assert extension2 in data_extensions
    
    # 获取所有能力
    capabilities = registry.get_capabilities()
    assert len(capabilities) == 2
    assert ExtensionCapability.EVENT_PROCESSING in capabilities
    assert ExtensionCapability.DATA_STORAGE in capabilities
    
    # 注销扩展
    unregistered = registry.unregister("ext-1")
    assert unregistered == extension1
    assert registry.get("ext-1") is None
    assert len(registry.get_all()) == 1
    
    # 清空注册表
    registry.clear()
    assert len(registry.get_all()) == 0


@pytest.mark.asyncio
async def test_extension_registry_start_stop():
    """测试注册表启动和停止所有扩展"""
    registry = ExtensionRegistry()
    
    # 创建并注册扩展
    extension1 = TestExtension()
    extension1.extension_id = "ext-1"
    
    extension2 = TestExtension()
    extension2.extension_id = "ext-2"
    
    registry.register(extension1)
    registry.register(extension2)
    
    # 启动所有扩展
    await registry.start_all()
    assert extension1.is_running()
    assert extension2.is_running()
    
    # 停止所有扩展
    await registry.stop_all()
    assert not extension1.is_running()
    assert not extension2.is_running()


def test_extension_request_response_models():
    """测试ExtensionRequest和ExtensionResponse模型"""
    # 测试请求模型
    request = ExtensionRequest(
        request_id="req-123",
        request_type=ExtensionRequestType.EXECUTE,
        extension_id="test-ext",
        parameters={"action": "test"},
        metadata={"priority": 1}
    )
    
    assert request.request_id == "req-123"
    assert request.request_type == ExtensionRequestType.EXECUTE
    assert request.extension_id == "test-ext"
    assert request.parameters["action"] == "test"
    assert request.metadata["priority"] == 1
    
    # 测试响应模型
    response = ExtensionResponse(
        request_id="req-123",
        response_id="resp-456",
        status=ExtensionResponseStatus.SUCCESS,
        result={"data": "success"},
        metadata={"timestamp": 1234567890}
    )
    
    assert response.request_id == "req-123"
    assert response.response_id == "resp-456"
    assert response.status == ExtensionResponseStatus.SUCCESS
    assert response.result["data"] == "success"
    assert response.metadata["timestamp"] == 1234567890


@pytest.mark.asyncio
async def test_extension_protocol():
    """测试ExtensionProtocol"""
    # 创建模拟组件
    mock_registry = Mock()
    mock_bus = AsyncMock()
    
    protocol = ExtensionProtocol(
        extension_registry=mock_registry,
        message_bus=mock_bus
    )
    
    # 测试ping请求
    request = {
        "request_id": "test-ping",
        "request_type": ExtensionRequestType.PING.value,
        "extension_id": None,
        "parameters": {},
        "metadata": {}
    }
    
    response = await protocol.handle_request(request)
    
    assert response["request_id"] == "test-ping"
    assert response["status"] == ExtensionResponseStatus.SUCCESS.value
    assert response["result"]["message"] == "pong"


@pytest.mark.asyncio
async def test_extension_protocol_info():
    """测试ExtensionProtocol info请求"""
    # 创建模拟扩展
    mock_extension = Mock()
    mock_extension.extension_id = "test-ext"
    mock_extension.name = "Test Extension"
    mock_extension.version = "1.0.0"
    mock_extension.description = "Test description"
    mock_extension.capabilities = []
    mock_extension.metadata = {}
    mock_extension.dependencies = []
    mock_extension.info = ExtensionInfo(
        extension_id="test-ext",
        name="Test Extension",
        version="1.0.0",
        description="Test description",
        capabilities=[],
        metadata={},
        dependencies=[]
    )
    mock_extension.get_stats.return_value = {"requests_processed": 0}
    
    # 创建注册表并注册扩展
    registry = ExtensionRegistry()
    registry.register(mock_extension)
    
    protocol = ExtensionProtocol(
        extension_registry=registry,
        message_bus=AsyncMock()
    )
    
    # 请求扩展信息
    request = {
        "request_id": "test-info",
        "request_type": ExtensionRequestType.INFO.value,
        "extension_id": "test-ext",
        "parameters": {},
        "metadata": {}
    }
    
    response = await protocol.handle_request(request)
    
    assert response["status"] == ExtensionResponseStatus.SUCCESS.value
    assert response["result"]["extension"]["extension_id"] == "test-ext"