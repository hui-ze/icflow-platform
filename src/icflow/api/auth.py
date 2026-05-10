"""
IC-Flow Platform API 认证中间件

支持 API Key 认证方式。
从配置或环境变量读取密钥，空密钥则跳过认证。
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)


class APIKeyMiddleware(BaseHTTPMiddleware):
    """
    API Key 认证中间件
    
    从请求头读取 API Key，与配置中的密钥比对。
    如果配置未设置 API Key，则跳过认证（开发模式）。
    """
    
    def __init__(
        self,
        app,
        api_key: str = "",
        header_name: str = "X-API-Key",
        exclude_paths: Optional[list] = None,
    ):
        super().__init__(app)
        self.api_key = api_key
        self.header_name = header_name
        self.exclude_paths = set(exclude_paths or [
            "/",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/api/v1/health",
        ])
    
    async def dispatch(self, request: Request, call_next):
        # 跳过免认证路径
        if request.url.path in self.exclude_paths:
            return await call_next(request)
        
        # 如果未配置 API Key，跳过认证（开发模式）
        if not self.api_key:
            return await call_next(request)
        
        # 验证 API Key
        api_key = request.headers.get(self.header_name)
        if not api_key:
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "缺少 API Key",
                    "header": self.header_name,
                    "hint": f"请在请求头中设置 {self.header_name}",
                },
            )
        
        if api_key != self.api_key:
            return JSONResponse(
                status_code=403,
                content={"detail": "API Key 无效"},
            )
        
        return await call_next(request)
