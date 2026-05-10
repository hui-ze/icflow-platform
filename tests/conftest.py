"""
测试配置和共享fixture
"""

import pytest
import asyncio


@pytest.fixture
def event_loop():
    """创建事件循环fixture"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture
def anyio_backend():
    """anyio后端配置"""
    return "asyncio"