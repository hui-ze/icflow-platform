"""
IC-Flow Platform REST API 入口

基于 FastAPI 的轻量级 REST API 服务。

启动方式：
    uvicorn icflow.api.main:app --host 0.0.0.0 --port 8000 --reload
    python -m icflow.api.main
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes import router
from .auth import APIKeyMiddleware
from src.icflow.config import get_settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # ==== 加载配置 ====
    settings = get_settings()
    app.state.settings = settings
    log_level = getattr(logging, settings.platform.log_level.upper(), logging.INFO)
    logging.getLogger("src.icflow").setLevel(log_level)
    
    logger.info("IC-Flow Platform API 启动中...")
    
    # ==== 初始化消息总线 ====
    bus_type = settings.message_bus.type
    if bus_type == "redis":
        logger.info("Redis 消息总线未实现，回退使用 MemoryMessageBus")
    
    from src.icflow.message_bus.memory import MemoryMessageBus
    bus = MemoryMessageBus()
    await bus.start()
    app.state.message_bus = bus
    logger.info("消息总线已启动")
    
    # ==== 初始化知识管理引擎（含持久化） ====
    from src.icflow.engines.knowledge_management import KnowledgeManagementEngine
    km_engine = KnowledgeManagementEngine()
    km_engine.message_bus = bus
    
    # 如果配置了数据库，初始化持久化仓储
    db_config = settings.knowledge.relational_db
    if db_config.type != "memory":
        try:
            from src.icflow.knowledge.repository import KnowledgeRepository
            repo = KnowledgeRepository(
                connection_string=db_config.connection_string,
                echo_sql=db_config.echo_sql,
            )
            await repo.initialize()
            km_engine.repository = repo
            app.state.knowledge_repo = repo
            logger.info(f"知识仓储已连接: {db_config.type}")
        except Exception as e:
            logger.warning(f"知识仓储初始化失败，使用内存存储: {e}")
    
    for et in km_engine.subscribed_event_types:
        await bus.subscribe(et, lambda e: asyncio.create_task(km_engine.process(e)))
    await km_engine.start()
    app.state.km_engine = km_engine
    logger.info("知识管理引擎已启动")
    
    # ==== 初始化流程编排引擎 ====
    from src.icflow.engines.workflow_orchestrator import FlowOrchestrator
    orchestrator = FlowOrchestrator({
        "default_timeout": settings.platform.debug and 60 or 600,
        "max_concurrent_workflows": 50,
    })
    orchestrator.message_bus = bus
    for et in orchestrator.subscribed_event_types:
        await bus.subscribe(et, lambda e: asyncio.create_task(orchestrator.process(e)))
    await orchestrator.start()
    app.state.orchestrator = orchestrator
    logger.info("流程编排引擎已启动")
    
    # ==== 初始化 DRC 修复引擎 ====
    from src.icflow.engines.drc_repair import DRCRepairMasterEngine
    drc_engine = DRCRepairMasterEngine({"default_tool": "calibre"})
    drc_engine.message_bus = bus
    for et in drc_engine.subscribed_event_types:
        await bus.subscribe(et, lambda e: asyncio.create_task(drc_engine.process(e)))
    await drc_engine.start()
    app.state.drc_engine = drc_engine
    logger.info("DRC修复引擎已启动")
    
    # ==== 初始化 EDA 工具适配器引擎 ====
    from src.icflow.engines.eda_tool_adapter import EDAToolAdapterEngine
    eda_engine = EDAToolAdapterEngine({
        "tool_paths": {"calibre": "/tools/calibre/bin/calibre"},
        "default_timeout": 300,
    })
    eda_engine.message_bus = bus
    for et in eda_engine.subscribed_event_types:
        await bus.subscribe(et, lambda e: asyncio.create_task(eda_engine.process(e)))
    await eda_engine.start()
    app.state.eda_engine = eda_engine
    logger.info("EDA工具适配器引擎已启动")
    
    # 记录启动时间
    app.state.start_time = time.time()
    
    logger.info(f"IC-Flow Platform API 启动完成 (v0.1.0 | 配置: {settings.platform.name})")
    
    yield
    
    # ==== 关闭阶段 ====
    logger.info("IC-Flow Platform API 关闭中...")
    
    await orchestrator.stop()
    await drc_engine.stop()
    await eda_engine.stop()
    await km_engine.stop()
    
    if hasattr(app.state, "knowledge_repo") and app.state.knowledge_repo:
        await app.state.knowledge_repo.close()
    
    await bus.stop()
    logger.info("IC-Flow Platform API 已关闭")


# 创建 FastAPI 应用
app = FastAPI(
    title="IC-Flow Platform API",
    description="基于事件的芯片设计流程自动化与知识管理平台 REST API",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Key 认证（从配置读取密钥，空则跳过）
settings = get_settings()
app.add_middleware(
    APIKeyMiddleware,
    api_key=settings.api.api_key,
    header_name=settings.api.api_key_header,
)

# 注册路由
app.include_router(router)


# 根路径
@app.get("/", tags=["Root"])
async def root():
    """API 根路径"""
    return {
        "name": "IC-Flow Platform API",
        "version": "0.1.0",
        "docs": "/docs",
        "health": "/api/v1/health",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "icflow.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
