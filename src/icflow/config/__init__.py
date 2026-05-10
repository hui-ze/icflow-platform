"""
IC-Flow Platform 配置管理

基于 pydantic-settings 的配置加载器。
支持：config.yaml 文件 + 环境变量覆盖
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ====== 各级配置模型 ======

class RedisConfig(BaseSettings):
    """Redis 消息总线配置"""
    host: str = Field(default="localhost")
    port: int = Field(default=6379)
    db: int = Field(default=0)
    password: str = Field(default="")
    max_connections: int = Field(default=10)


class MessageBusConfig(BaseSettings):
    """消息总线配置"""
    type: str = Field(default="memory")  # memory | redis
    redis: RedisConfig = Field(default_factory=RedisConfig)


class VectorDbConfig(BaseSettings):
    """向量数据库配置"""
    type: str = Field(default="memory")  # memory | qdrant
    url: str = Field(default="http://localhost:6333")
    collection: str = Field(default="icflow_knowledge")


class GraphDbConfig(BaseSettings):
    """图数据库配置"""
    type: str = Field(default="memory")  # memory | neo4j
    uri: str = Field(default="bolt://localhost:7687")
    username: str = Field(default="neo4j")
    password: str = Field(default="password")


class RelationalDbConfig(BaseSettings):
    """关系数据库配置"""
    type: str = Field(default="sqlite")  # sqlite | postgresql
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    database: str = Field(default="icflow")
    username: str = Field(default="icflow")
    password: str = Field(default="icflow_dev")
    echo_sql: bool = Field(default=False)

    @property
    def connection_string(self) -> str:
        """获取数据库连接字符串"""
        if self.type == "sqlite":
            return f"sqlite+aiosqlite:///./{self.database}.db"
        return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class KnowledgeConfig(BaseSettings):
    """知识管理配置"""
    vector_db: VectorDbConfig = Field(default_factory=VectorDbConfig)
    graph_db: GraphDbConfig = Field(default_factory=GraphDbConfig)
    relational_db: RelationalDbConfig = Field(default_factory=RelationalDbConfig)


class ApiConfig(BaseSettings):
    """API 服务配置"""
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)
    workers: int = Field(default=4)
    cors_origins: List[str] = Field(default=["*"])
    api_key: str = Field(default="")  # 为空则跳过认证
    api_key_header: str = Field(default="X-API-Key")


class MonitoringConfig(BaseSettings):
    """监控配置"""
    enabled: bool = Field(default=False)


class PlatformConfig(BaseSettings):
    """平台核心配置"""
    name: str = Field(default="IC-Flow Platform")
    version: str = Field(default="0.1.0")
    debug: bool = Field(default=False)
    log_level: str = Field(default="INFO")
    auto_register_engines: bool = Field(default=True)


class Settings(BaseSettings):
    """
    全局配置 - 合并 config.yaml + 环境变量
    
    环境变量优先级最高，配置文件名次之，默认值最低。
    环境变量前缀: ICFLOW_
    例如: ICFLOW_API_HOST=0.0.0.0 覆盖 api.host
    """
    
    model_config = SettingsConfigDict(
        env_prefix="ICFLOW_",
        env_nested_delimiter="__",
        yaml_file=None,  # 运行时从文件加载
        extra="ignore",
    )
    
    # 各模块配置
    message_bus: MessageBusConfig = Field(default_factory=MessageBusConfig)
    knowledge: KnowledgeConfig = Field(default_factory=KnowledgeConfig)
    platform: PlatformConfig = Field(default_factory=PlatformConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)


# ====== 配置加载器 ======

def _load_yaml_config(config_path: str) -> Dict[str, Any]:
    """从 YAML 文件加载配置"""
    path = Path(config_path)
    if not path.exists():
        return {}
    
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        # yaml 不是核心依赖，备选使用 json 解析
        pass
    except Exception:
        pass
    
    return {}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """深度合并两个字典（override 覆盖 base）"""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


@lru_cache()
def get_settings(config_path: Optional[str] = None) -> Settings:
    """
    获取配置单例
    
    Args:
        config_path: 配置文件路径，默认从环境变量 ICFLOW_CONFIG 或 ./config/config.yaml
    
    Returns:
        Settings 实例
    """
    # 确定配置文件路径
    if not config_path:
        config_path = os.environ.get("ICFLOW_CONFIG", "./config/config.yaml")
    
    # 加载 YAML 配置
    yaml_data = _load_yaml_config(config_path)
    
    # 从 YAML 创建 Settings（环境变量自动覆盖）
    settings = Settings(**yaml_data)
    
    return settings


# 导出
__all__ = [
    "Settings",
    "get_settings",
    "RedisConfig",
    "MessageBusConfig",
    "KnowledgeConfig",
    "ApiConfig",
    "PlatformConfig",
]
