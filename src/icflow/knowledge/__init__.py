"""
IC-Flow Platform 知识管理模块

提供知识的持久化存储、检索和关联管理。
"""

from .models import KnowledgeEntryModel, KnowledgeRelationModel
from .repository import KnowledgeRepository

__all__ = [
    "KnowledgeEntryModel",
    "KnowledgeRelationModel",
    "KnowledgeRepository",
]
