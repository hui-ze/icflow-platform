"""
IC-Flow Platform 知识管理 - 仓储层

提供知识条目的 CRUD 操作，替换内存存储。
支持异步数据库操作，自动创建表结构。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
)

from .models import Base, KnowledgeEntryModel, KnowledgeRelationModel

logger = logging.getLogger(__name__)


class KnowledgeRepository:
    """
    知识仓储层
    
    提供数据库连接管理和知识 CRUD 操作。
    自动创建表结构（开发阶段适用，生产建议使用 Alembic 迁移）。
    """
    
    def __init__(self, connection_string: str, echo_sql: bool = False):
        self.connection_string = connection_string
        self._engine = None
        self._session_factory = None
        self._initialized = False
    
    async def initialize(self) -> None:
        """初始化数据库连接和表结构"""
        if self._initialized:
            return
        
        self._engine = create_async_engine(
            self.connection_string,
            echo=False,  # 不打印 SQL，减少日志
            pool_size=5,
            max_overflow=10,
        )
        
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        
        # 创建表（开发环境自动创建，生产建议用 Alembic）
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        self._initialized = True
        logger.info(f"知识仓储已初始化: {self.connection_string}")
    
    async def close(self) -> None:
        """关闭数据库连接"""
        if self._engine:
            await self._engine.dispose()
            self._initialized = False
            logger.info("知识仓储连接已关闭")
    
    @asynccontextmanager
    async def session(self):
        """获取数据库会话（上下文管理器）"""
        if not self._session_factory:
            raise RuntimeError("仓储未初始化，请先调用 initialize()")
        async with self._session_factory() as sess:
            yield sess
    
    # ====== 知识 CRUD ======
    
    async def save_knowledge(self, entry: KnowledgeEntryModel) -> str:
        """保存知识条目"""
        async with self.session() as sess:
            sess.add(entry)
            await sess.commit()
            logger.debug(f"知识已保存: {entry.id}")
            return entry.id
    
    async def get_knowledge(self, knowledge_id: str) -> Optional[KnowledgeEntryModel]:
        """根据 ID 获取知识条目"""
        async with self.session() as sess:
            result = await sess.execute(
                select(KnowledgeEntryModel).where(KnowledgeEntryModel.id == knowledge_id)
            )
            return result.scalar_one_or_none()
    
    async def search_knowledge(
        self,
        query: str,
        knowledge_type: Optional[str] = None,
        source: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
    ) -> Tuple[List[KnowledgeEntryModel], int]:
        """
        搜索知识条目
        
        Args:
            query: 搜索关键词
            knowledge_type: 过滤知识类型
            source: 过滤来源
            limit: 每页数量
            offset: 偏移量
            
        Returns:
            (条目列表, 总数)
        """
        async with self.session() as sess:
            conditions = []
            
            # 关键词搜索（决策理由或规则ID匹配）
            if query:
                conditions.append(
                    KnowledgeEntryModel.decision_reason.ilike(f"%{query}%")
                    | KnowledgeEntryModel.rule_id.ilike(f"%{query}%")
                )
            
            if knowledge_type:
                conditions.append(KnowledgeEntryModel.knowledge_type == knowledge_type)
            
            if source:
                conditions.append(KnowledgeEntryModel.source == source)
            
            # 构建查询
            stmt = select(KnowledgeEntryModel)
            count_stmt = select(func.count(KnowledgeEntryModel.id))
            
            if conditions:
                from sqlalchemy import and_
                stmt = stmt.where(and_(*conditions))
                count_stmt = count_stmt.where(and_(*conditions))
            
            # 获取总数
            total_result = await sess.execute(count_stmt)
            total = total_result.scalar() or 0
            
            # 获取分页数据
            stmt = stmt.order_by(KnowledgeEntryModel.stored_at.desc())
            stmt = stmt.offset(offset).limit(limit)
            result = await sess.execute(stmt)
            entries = list(result.scalars().all())
            
            return entries, total
    
    async def get_knowledge_count(self) -> int:
        """获取知识条目总数"""
        async with self.session() as sess:
            result = await sess.execute(func.count(KnowledgeEntryModel.id))
            return result.scalar() or 0
    
    async def get_knowledge_by_type(self, knowledge_type: str) -> List[KnowledgeEntryModel]:
        """按类型获取知识条目"""
        async with self.session() as sess:
            result = await sess.execute(
                select(KnowledgeEntryModel)
                .where(KnowledgeEntryModel.knowledge_type == knowledge_type)
                .order_by(KnowledgeEntryModel.stored_at.desc())
                .limit(100)
            )
            return list(result.scalars().all())
    
    async def delete_knowledge(self, knowledge_id: str) -> bool:
        """删除知识条目"""
        async with self.session() as sess:
            result = await sess.execute(
                delete(KnowledgeEntryModel).where(KnowledgeEntryModel.id == knowledge_id)
            )
            await sess.commit()
            return result.rowcount > 0
    
    # ====== 关联管理 ======
    
    async def save_relation(
        self,
        source_id: str,
        target_id: str,
        relation_type: str = "similar",
        similarity: float = 0.0,
    ) -> str:
        """保存知识关联"""
        from uuid import uuid4
        relation = KnowledgeRelationModel(
            id=f"rel_{uuid4().hex[:8]}",
            source_id=source_id,
            target_id=target_id,
            relation_type=relation_type,
            similarity_score=similarity,
        )
        async with self.session() as sess:
            sess.add(relation)
            await sess.commit()
            return relation.id
    
    async def get_relations(self, knowledge_id: str) -> List[KnowledgeRelationModel]:
        """获取知识条目的关联"""
        async with self.session() as sess:
            result = await sess.execute(
                select(KnowledgeRelationModel)
                .where(
                    (KnowledgeRelationModel.source_id == knowledge_id)
                    | (KnowledgeRelationModel.target_id == knowledge_id)
                )
            )
            return list(result.scalars().all())
    
    async def get_relation_count(self) -> int:
        """获取关联总数"""
        async with self.session() as sess:
            result = await sess.execute(func.count(KnowledgeRelationModel.id))
            return result.scalar() or 0
