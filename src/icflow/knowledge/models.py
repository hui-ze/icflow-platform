"""IC-Flow Platform 知识管理 - 数据模型"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, String, Float, Integer, DateTime, Text, JSON, Index
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class KnowledgeEntryModel(Base):
    """知识条目 ORM 模型"""
    
    __tablename__ = "knowledge_entries"
    
    id = Column(String(64), primary_key=True, comment="知识条目ID")
    event_type = Column(String(128), nullable=False, index=True, comment="事件类型")
    source = Column(String(128), nullable=False, index=True, comment="来源引擎")
    knowledge_type = Column(String(64), nullable=False, index=True, comment="知识类型")
    
    # 核心字段
    rule_id = Column(String(128), nullable=True, comment="规则ID")
    decision_reason = Column(Text, nullable=True, comment="决策理由")
    context = Column(JSON, nullable=True, comment="上下文信息")
    related_files = Column(JSON, nullable=True, comment="关联文件列表")
    payload = Column(JSON, nullable=True, comment="原始事件载荷")
    
    # 富化字段
    tags = Column(JSON, nullable=True, comment="分类标签")
    quality_score = Column(Float, default=0.0, comment="质量评分")
    
    # 时间戳
    captured_at = Column(DateTime(timezone=True), nullable=False, comment="捕获时间")
    stored_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), comment="存储时间")
    
    # 索引
    __table_args__ = (
        Index("idx_knowledge_type_captured", "knowledge_type", "captured_at"),
        Index("idx_source_type", "source", "knowledge_type"),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """转字典"""
        return {
            "id": self.id,
            "event_type": self.event_type,
            "source": self.source,
            "knowledge_type": self.knowledge_type,
            "rule_id": self.rule_id,
            "decision_reason": self.decision_reason,
            "context": self.context or {},
            "related_files": self.related_files or [],
            "tags": self.tags or [],
            "quality_score": self.quality_score,
            "captured_at": self.captured_at.isoformat() if self.captured_at else None,
            "stored_at": self.stored_at.isoformat() if self.stored_at else None,
        }


class KnowledgeRelationModel(Base):
    """知识关联 ORM 模型"""
    
    __tablename__ = "knowledge_relations"
    
    id = Column(String(64), primary_key=True, comment="关联ID")
    source_id = Column(String(64), nullable=False, index=True, comment="源知识条目ID")
    target_id = Column(String(64), nullable=False, index=True, comment="目标知识条目ID")
    relation_type = Column(String(32), default="similar", comment="关联类型")
    similarity_score = Column(Float, default=0.0, comment="相似度分数")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    __table_args__ = (
        Index("idx_source_target", "source_id", "target_id", unique=True),
    )
