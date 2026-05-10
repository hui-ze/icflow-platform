"""
IC-Flow Platform 扩展模块
提供 Extension Protocol 实现
"""

from .base import Extension, ExtensionRegistry, ExtensionCapability
from .protocol import ExtensionProtocol, ExtensionRequest, ExtensionResponse

__all__ = [
    "Extension",
    "ExtensionRegistry",
    "ExtensionCapability",
    "ExtensionProtocol",
    "ExtensionRequest",
    "ExtensionResponse",
]