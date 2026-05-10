"""
知识管理引擎 (Knowledge Management Engine)

职责：捕获、索引、检索设计知识，构建组织记忆库
触发事件：各类 KnowledgeCaptureEvent
输出事件：KnowledgeCaptureEvent（标注、关联后）、SystemHealthEvent（知识库健康度）
关键能力：向量化检索、图谱关联、持续学习、质量评估
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, TYPE_CHECKING
from datetime import datetime, timezone
import uuid

from src.icflow.core.flow_engine import FlowEngine, FlowEvent
from src.icflow.core.concrete_events import (
    KnowledgeCaptureEventTypes,
    SystemHealthEventTypes,
    create_knowledge_capture_event,
    create_system_health_event,
)

if TYPE_CHECKING:
    from src.icflow.knowledge.repository import KnowledgeRepository


logger = logging.getLogger(__name__)


class KnowledgeManagementEngine(FlowEngine):
    """
    知识管理引擎
    
    关键能力：
    - 向量化检索
    - 图谱关联
    - 持续学习
    - 质量评估
    """
    
    engine_id = "knowledge_management_engine"
    engine_name = "知识管理引擎"
    engine_description = "捕获、索引、检索设计知识，构建组织记忆库"
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化知识管理引擎
        
        Args:
            config: 引擎配置字典
        """
        super().__init__(config)
        
        # 引擎配置
        self.vector_db_enabled = config.get("vector_db_enabled", True) if config else True
        self.graph_db_enabled = config.get("graph_db_enabled", True) if config else True
        self.cache_enabled = config.get("cache_enabled", True) if config else True
        
        # 知识存储（模拟，实际应连接数据库）
        self.knowledge_store: Dict[str, Dict[str, Any]] = {}  # knowledge_id -> 知识条目
        self.vector_index: Dict[str, List[float]] = {}  # knowledge_id -> 向量
        self.graph_relations: Dict[str, List[str]] = {}  # knowledge_id -> 相关ID列表
        
        # 缓存
        self.cache: Dict[str, Any] = {}
        
        # 持久化仓储（可选，用于生产环境）
        self._repository: Optional["KnowledgeRepository"] = None
        
        # 订阅知识捕获事件
        self.subscribed_event_types = [
            KnowledgeCaptureEventTypes.RULE_VIOLATION,
            KnowledgeCaptureEventTypes.ENGINEER_DECISION,
            KnowledgeCaptureEventTypes.TOOL_OUTPUT_PARSED,
            KnowledgeCaptureEventTypes.DESIGN_PATTERN,
            KnowledgeCaptureEventTypes.BEST_PRACTICE,
        ]
        
        # 统计数据
        self.knowledge_count = 0
        self.retrieval_count = 0
        self.cache_hits = 0
        
        logger.info(f"知识管理引擎初始化完成: {self.engine_id}")
    
    @property
    def repository(self) -> Optional["KnowledgeRepository"]:
        """获取知识仓储实例"""
        return self._repository
    
    @repository.setter
    def repository(self, repo: Optional["KnowledgeRepository"]) -> None:
        """设置知识仓储实例"""
        self._repository = repo
        if repo:
            logger.info(f"知识管理引擎已接入持久化仓储")
    
    async def start(self):
        """启动引擎"""
        await super().start()
        logger.info(f"知识管理引擎启动: {self.engine_id}")
        
        # 发送健康事件
        await self._send_health_event("服务启动", "info")
    
    async def stop(self):
        """停止引擎"""
        await super().stop()
        logger.info(f"知识管理引擎停止: {self.engine_id}")
        
        # 发送健康事件
        await self._send_health_event("服务停止", "info")
    
    async def publish_event(self, event: FlowEvent) -> None:
        """
        发布事件到消息总线
        """
        if self._message_bus:
            logger.debug(f"知识管理引擎发布事件: {event.event_type} [{event.event_id}]")
            await self._message_bus.publish(event)
        else:
            logger.warning(f"消息总线未设置，无法发布事件: {event.event_type}")
    
    async def process(self, event: FlowEvent) -> Optional[FlowEvent]:
        """
        处理事件（主入口）
        
        Args:
            event: 接收到的事件
            
        Returns:
            处理结果事件，如果无需返回则为 None
        """
        if event.event_type in self.subscribed_event_types:
            return await self._handle_knowledge_capture(event)
        else:
            logger.warning(f"知识管理引擎收到未知事件类型: {event.event_type}")
            return None
    
    async def _handle_knowledge_capture(self, event: FlowEvent) -> Optional[FlowEvent]:
        """
        处理知识捕获事件
        """
        logger.info(f"处理知识捕获事件: {event.event_type} [{event.event_id}]")
        
        # 提取知识数据
        knowledge_data = self._extract_knowledge_data(event)
        
        # 知识富化（添加上下文、分类标签等）
        enriched_knowledge = self._enrich_knowledge(knowledge_data, event)
        
        # 存储知识
        knowledge_id = await self._store_knowledge(enriched_knowledge)
        
        # 建立关联
        await self._build_relations(knowledge_id, enriched_knowledge)
        
        # 发送知识存储完成事件
        stored_event = create_knowledge_capture_event(
            event_type=event.event_type,  # 保持原始事件类型
            rule_id=enriched_knowledge.get("rule_id"),
            context=enriched_knowledge.get("context"),
            decision_reason=f"知识已存储并索引，类型: {enriched_knowledge.get('knowledge_type', 'unknown')}",
            related_files=enriched_knowledge.get("related_files", []),
            source=self.engine_id,
            knowledge_id=knowledge_id,
            storage_timestamp=datetime.now(timezone.utc).isoformat()
        )
        await self.publish_event(stored_event)
        
        # 更新统计数据
        self.knowledge_count += 1
        
        # 定期发送健康事件
        if self.knowledge_count % 10 == 0:
            await self._send_health_event(
                f"知识库容量: {self.knowledge_count} 条记录",
                "info"
            )
        
        return stored_event
    
    def _extract_knowledge_data(self, event: FlowEvent) -> Dict[str, Any]:
        """
        从事件中提取知识数据
        """
        payload = event.payload or {}
        
        # 基础字段
        knowledge_data = {
            "event_type": event.event_type,
            "source": event.source,
            "timestamp": event.timestamp,
            "payload": payload,
            "metadata": event.metadata or {},
        }
        
        # 根据事件类型提取特定字段
        if event.event_type == KnowledgeCaptureEventTypes.RULE_VIOLATION:
            knowledge_data["knowledge_type"] = "rule_violation"
            knowledge_data["rule_id"] = payload.get("rule_id")
            knowledge_data["context"] = payload.get("context", {})
        elif event.event_type == KnowledgeCaptureEventTypes.ENGINEER_DECISION:
            knowledge_data["knowledge_type"] = "engineer_decision"
            knowledge_data["decision_reason"] = payload.get("decision_reason")
            knowledge_data["context"] = payload.get("context", {})
        elif event.event_type == KnowledgeCaptureEventTypes.TOOL_OUTPUT_PARSED:
            knowledge_data["knowledge_type"] = "tool_output"
            knowledge_data["tool_name"] = payload.get("tool_name")
            knowledge_data["output_summary"] = payload.get("output_summary")
        elif event.event_type == KnowledgeCaptureEventTypes.DESIGN_PATTERN:
            knowledge_data["knowledge_type"] = "design_pattern"
            knowledge_data["pattern_name"] = payload.get("pattern_name")
            knowledge_data["pattern_description"] = payload.get("pattern_description")
        elif event.event_type == KnowledgeCaptureEventTypes.BEST_PRACTICE:
            knowledge_data["knowledge_type"] = "best_practice"
            knowledge_data["practice_name"] = payload.get("practice_name")
            knowledge_data["practice_description"] = payload.get("practice_description")
        
        # 复制通用字段到顶层（如果存在且未设置）
        for key in ["context", "related_files", "decision_reason", "rule_id", 
                    "tool_name", "output_summary", "pattern_name", "pattern_description",
                    "practice_name", "practice_description"]:
            if key in payload and key not in knowledge_data:
                knowledge_data[key] = payload[key]
        
        return knowledge_data
    
    def _enrich_knowledge(self, knowledge_data: Dict[str, Any], event: FlowEvent) -> Dict[str, Any]:
        """
        知识富化：添加上下文、分类标签等
        """
        enriched = knowledge_data.copy()
        
        # 添加分类标签
        tags = enriched.get("tags", [])
        tags.append(knowledge_data.get("knowledge_type", "unknown"))
        tags.append(f"source:{event.source}")
        enriched["tags"] = tags
        
        # 添加质量评分（模拟）
        enriched["quality_score"] = self._calculate_quality_score(knowledge_data)
        
        # 添加时间戳
        enriched["enrichment_timestamp"] = datetime.now(timezone.utc).isoformat()
        
        # 生成向量表示（模拟）
        if self.vector_db_enabled:
            enriched["vector"] = self._generate_vector(knowledge_data)
        
        return enriched
    
    async def _store_knowledge(self, knowledge: Dict[str, Any]) -> str:
        """
        存储知识到知识库（内存 + 可选持久化）
        """
        # 生成知识ID
        knowledge_id = f"knowledge_{uuid.uuid4().hex[:8]}"
        
        # 存储到内存（快速检索）
        self.knowledge_store[knowledge_id] = knowledge
        
        # 存储向量索引（模拟）
        if self.vector_db_enabled and "vector" in knowledge:
            self.vector_index[knowledge_id] = knowledge["vector"]
        
        # 如果配置了持久化仓储，同步写入数据库
        if self._repository:
            try:
                from datetime import timezone
                from src.icflow.knowledge.models import KnowledgeEntryModel
                
                entry = KnowledgeEntryModel(
                    id=knowledge_id,
                    event_type=knowledge.get("event_type", ""),
                    source=knowledge.get("source", ""),
                    knowledge_type=knowledge.get("knowledge_type", "unknown"),
                    rule_id=knowledge.get("rule_id"),
                    decision_reason=knowledge.get("decision_reason"),
                    context=knowledge.get("context"),
                    related_files=knowledge.get("related_files"),
                    payload=knowledge.get("payload"),
                    tags=knowledge.get("tags"),
                    quality_score=knowledge.get("quality_score", 0.0),
                    captured_at=datetime.now(timezone.utc),
                )
                await self._repository.save_knowledge(entry)
                logger.debug(f"知识已持久化: {knowledge_id}")
            except Exception as e:
                logger.error(f"知识持久化失败: {knowledge_id}: {e}", exc_info=True)
        
        logger.debug(f"知识存储完成: {knowledge_id} ({knowledge.get('knowledge_type', 'unknown')})")
        return knowledge_id
    
    async def _build_relations(self, knowledge_id: str, knowledge: Dict[str, Any]) -> None:
        """
        建立知识关联（图谱关系）
        """
        if not self.graph_db_enabled:
            return
        
        # 模拟关联建立：基于知识类型和标签建立关联
        relations = []
        
        # 查找相似知识（基于向量）
        if self.vector_db_enabled and "vector" in knowledge:
            similar_ids = self._find_similar_knowledge(knowledge_id, knowledge["vector"], limit=3)
            relations.extend(similar_ids)
        
        # 基于规则ID关联
        if "rule_id" in knowledge:
            rule_related = [
                kid for kid, kdata in self.knowledge_store.items()
                if kdata.get("rule_id") == knowledge["rule_id"] and kid != knowledge_id
            ]
            relations.extend(rule_related)
        
        # 去重
        relations = list(set(relations))
        self.graph_relations[knowledge_id] = relations
        
        if relations:
            logger.debug(f"为知识 {knowledge_id} 建立 {len(relations)} 个关联")
    
    async def retrieve_knowledge(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        检索知识（模拟实现）
        """
        self.retrieval_count += 1
        
        # 检查缓存
        cache_key = f"retrieve:{query}:{limit}"
        if self.cache_enabled and cache_key in self.cache:
            self.cache_hits += 1
            return self.cache[cache_key]
        
        # 模拟检索逻辑
        results = []
        for knowledge_id, knowledge in self.knowledge_store.items():
            # 简单关键字匹配（实际应使用向量检索）
            knowledge_text = str(knowledge).lower()
            if query.lower() in knowledge_text:
                results.append({
                    "knowledge_id": knowledge_id,
                    **knowledge,
                    "relevance_score": 0.8,  # 模拟相关性分数
                })
        
        # 按质量评分排序
        results.sort(key=lambda x: x.get("quality_score", 0), reverse=True)
        results = results[:limit]
        
        # 缓存结果
        if self.cache_enabled:
            self.cache[cache_key] = results
        
        return results
    
    def _calculate_quality_score(self, knowledge_data: Dict[str, Any]) -> float:
        """
        计算知识质量评分（模拟）
        """
        score = 0.5  # 基础分
        
        # 根据知识类型加分
        knowledge_type = knowledge_data.get("knowledge_type", "")
        if knowledge_type in ["best_practice", "design_pattern"]:
            score += 0.3
        elif knowledge_type == "engineer_decision":
            score += 0.2
        elif knowledge_type == "rule_violation":
            score += 0.1
        
        # 根据上下文完整性加分
        if knowledge_data.get("context"):
            score += 0.1
        if knowledge_data.get("related_files"):
            score += 0.1
        
        # 限制在0-1之间
        return min(max(score, 0.0), 1.0)
    
    def _generate_vector(self, knowledge_data: Dict[str, Any]) -> List[float]:
        """
        生成知识向量表示（模拟）
        """
        # 模拟向量生成：基于知识类型、标签等生成随机向量
        import random
        vector_dim = 128  # 模拟向量维度
        random.seed(str(knowledge_data.get("knowledge_type", "")))
        return [random.random() for _ in range(vector_dim)]
    
    def _find_similar_knowledge(self, source_id: str, vector: List[float], limit: int = 3) -> List[str]:
        """
        查找相似知识（基于向量）
        """
        if not self.vector_index:
            return []
        
        # 模拟相似度计算：计算向量余弦相似度
        similarities = []
        for knowledge_id, other_vector in self.vector_index.items():
            if knowledge_id == source_id:
                continue
            
            # 模拟相似度计算
            sim = self._cosine_similarity(vector, other_vector)
            similarities.append((knowledge_id, sim))
        
        # 按相似度排序
        similarities.sort(key=lambda x: x[1], reverse=True)
        return [kid for kid, _ in similarities[:limit]]
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        计算余弦相似度（模拟）
        """
        if len(vec1) != len(vec2) or len(vec1) == 0:
            return 0.0
        
        # 模拟计算
        import random
        random.seed(str(vec1[:5]) + str(vec2[:5]))
        return random.uniform(0.3, 0.9)  # 返回随机相似度
    
    async def _send_health_event(self, message: str, severity: str = "info") -> None:
        """
        发送系统健康事件
        """
        health_event = create_system_health_event(
            event_type=SystemHealthEventTypes.RESOURCE_MONITOR,
            resource_metrics={
                "knowledge_count": self.knowledge_count,
                "retrieval_count": self.retrieval_count,
                "cache_hits": self.cache_hits,
                "cache_hit_rate": self.cache_hits / max(self.retrieval_count, 1),
                "vector_index_size": len(self.vector_index),
                "graph_relations_count": sum(len(rels) for rels in self.graph_relations.values()),
            },
            suggested_action="无",
            severity=severity,
            source=self.engine_id,
            message=message
        )
        await self.publish_event(health_event)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取引擎统计信息（扩展父类方法）
        """
        stats = super().get_stats()
        stats.update({
            "knowledge_count": self.knowledge_count,
            "persisted": self._repository is not None,
            "retrieval_count": self.retrieval_count,
            "cache_hits": self.cache_hits,
            "cache_hit_rate": self.cache_hits / max(self.retrieval_count, 1),
            "vector_index_size": len(self.vector_index),
            "graph_relations_count": sum(len(rels) for rels in self.graph_relations.values()),
        })
        return stats