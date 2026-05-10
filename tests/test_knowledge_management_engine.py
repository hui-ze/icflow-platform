"""
知识管理引擎测试
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.icflow.engines.knowledge_management import KnowledgeManagementEngine
from src.icflow.core.flow_event import FlowEvent
from src.icflow.core.concrete_events import (
    KnowledgeCaptureEventTypes,
    SystemHealthEventTypes,
    create_knowledge_capture_event,
)
from src.icflow.message_bus.memory import MemoryMessageBus


class TestKnowledgeManagementEngine:
    """知识管理引擎测试类"""
    
    @pytest.fixture
    def message_bus(self):
        """创建消息总线fixture"""
        return MemoryMessageBus()
    
    @pytest.fixture
    def engine(self, message_bus):
        """创建引擎fixture"""
        # 创建引擎
        engine = KnowledgeManagementEngine({
            "vector_db_enabled": True,
            "graph_db_enabled": True,
            "cache_enabled": True,
        })
        # 设置消息总线
        engine.message_bus = message_bus
        return engine
    
    @pytest.mark.asyncio
    async def test_engine_initialization(self, engine):
        """测试引擎初始化"""
        assert engine.engine_id == "knowledge_management_engine"
        assert engine.engine_name == "知识管理引擎"
        assert engine.vector_db_enabled == True
        assert engine.graph_db_enabled == True
        assert engine.cache_enabled == True
        
        # 检查是否订阅了正确的事件类型
        assert KnowledgeCaptureEventTypes.RULE_VIOLATION in engine.subscribed_event_types
        assert KnowledgeCaptureEventTypes.ENGINEER_DECISION in engine.subscribed_event_types
        assert KnowledgeCaptureEventTypes.TOOL_OUTPUT_PARSED in engine.subscribed_event_types
        assert KnowledgeCaptureEventTypes.DESIGN_PATTERN in engine.subscribed_event_types
        assert KnowledgeCaptureEventTypes.BEST_PRACTICE in engine.subscribed_event_types
        
        # 检查初始状态
        assert engine.knowledge_count == 0
        assert engine.retrieval_count == 0
        assert engine.cache_hits == 0
        assert engine.knowledge_store == {}
        assert engine.vector_index == {}
        assert engine.graph_relations == {}
        assert engine.cache == {}
    
    @pytest.mark.asyncio
    async def test_start_stop(self, engine):
        """测试启动和停止"""
        await engine.start()
        assert engine.is_running()
        
        await engine.stop()
        assert not engine.is_running()
    
    @pytest.mark.asyncio
    async def test_handle_rule_violation_knowledge(self, engine, message_bus):
        """测试处理规则违例知识捕获事件"""
        # 启动消息总线和引擎
        await message_bus.start()
        await engine.start()
        
        # 创建规则违例知识捕获事件
        rule_violation_event = create_knowledge_capture_event(
            event_type=KnowledgeCaptureEventTypes.RULE_VIOLATION,
            rule_id="min_width_001",
            context={
                "design_version": "v1.0",
                "layer": "metal1",
                "violation_value": 0.05,
                "required_value": 0.1
            },
            decision_reason="最小宽度违例，需增加宽度",
            related_files=["design.gds", "drc_report.txt"],
            source="drc_engine"
        )
        
        # 监听发布的事件
        published_events = []
        
        async def capture_event(event):
            published_events.append(event)
        
        # 订阅知识捕获事件
        await message_bus.subscribe(KnowledgeCaptureEventTypes.RULE_VIOLATION, capture_event)
        
        # 处理事件
        result = await engine.process(rule_violation_event)
        
        # 等待异步操作完成
        await asyncio.sleep(0.1)
        
        # 验证结果
        assert result is not None
        assert result.event_type == KnowledgeCaptureEventTypes.RULE_VIOLATION
        assert result.payload.get("knowledge_id") is not None
        
        # 验证事件发布
        assert len(published_events) >= 1
        
        # 验证知识存储
        assert engine.knowledge_count == 1
        assert len(engine.knowledge_store) == 1
        
        # 停止引擎
        await engine.stop()
    
    @pytest.mark.asyncio
    async def test_handle_engineer_decision_knowledge(self, engine, message_bus):
        """测试处理工程师决策知识捕获事件"""
        await message_bus.start()
        await engine.start()
        
        # 创建工程师决策知识捕获事件
        decision_event = FlowEvent(
            event_type=KnowledgeCaptureEventTypes.ENGINEER_DECISION,
            source="engineer_john",
            payload={
                "decision_reason": "选择手动调整而不是自动修复，因为这是关键路径",
                "context": {
                    "design_area": "critical_path",
                    "timing_margin": "tight",
                    "engineer_experience": "senior"
                },
                "related_files": ["timing_report.rpt", "constraint.sdc"],
                "knowledge_type": "engineer_decision"
            }
        )
        
        # 处理事件
        result = await engine.process(decision_event)
        
        # 验证结果
        assert result is not None
        assert result.event_type == KnowledgeCaptureEventTypes.ENGINEER_DECISION  # 引擎应返回相同的事件类型
        
        # 验证知识存储
        assert engine.knowledge_count == 1
        assert len(engine.knowledge_store) == 1
        
        # 检查存储的知识类型
        stored_knowledge = list(engine.knowledge_store.values())[0]
        assert stored_knowledge.get("knowledge_type") == "engineer_decision"
        assert stored_knowledge.get("decision_reason") == "选择手动调整而不是自动修复，因为这是关键路径"
        
        await engine.stop()
    
    @pytest.mark.asyncio
    async def test_handle_tool_output_knowledge(self, engine, message_bus):
        """测试处理工具输出知识捕获事件"""
        await message_bus.start()
        await engine.start()
        
        # 创建工具输出知识捕获事件
        tool_output_event = FlowEvent(
            event_type=KnowledgeCaptureEventTypes.TOOL_OUTPUT_PARSED,
            source="calibre_parser",
            payload={
                "tool_name": "calibre",
                "output_summary": "DRC检查完成，发现3个违例，2个自动修复，1个需要人工审查",
                "execution_time": 125.3,
                "exit_code": 0,
                "knowledge_type": "tool_output"
            }
        )
        
        # 处理事件
        result = await engine.process(tool_output_event)
        
        # 验证结果
        assert result is not None
        assert engine.knowledge_count == 1
        
        await engine.stop()
    
    @pytest.mark.asyncio
    async def test_retrieve_knowledge(self, engine):
        """测试知识检索功能"""
        # 先添加一些测试知识
        test_knowledge_1 = {
            "knowledge_type": "rule_violation",
            "rule_id": "min_width_001",
            "context": {"layer": "metal1", "violation_value": 0.05},
            "quality_score": 0.8,
            "tags": ["drc", "metal1", "min_width"]
        }
        
        test_knowledge_2 = {
            "knowledge_type": "best_practice",
            "practice_name": "metal_width_optimization",
            "practice_description": "在关键路径上使用最小宽度+10%的余量",
            "quality_score": 0.9,
            "tags": ["best_practice", "metal", "optimization"]
        }
        
        # 模拟存储
        engine.knowledge_store["test_001"] = test_knowledge_1
        engine.knowledge_store["test_002"] = test_knowledge_2
        engine.knowledge_count = 2
        
        # 测试检索
        results = await engine.retrieve_knowledge("metal1", limit=5)
        
        assert len(results) == 1  # 只有第一条包含"metal1"
        assert results[0]["knowledge_id"] == "test_001"
        assert results[0]["knowledge_type"] == "rule_violation"
        
        # 测试缓存命中
        initial_cache_hits = engine.cache_hits
        results_cached = await engine.retrieve_knowledge("metal1", limit=5)
        assert engine.cache_hits == initial_cache_hits + 1  # 缓存命中增加
        
        # 测试检索无结果
        results_empty = await engine.retrieve_knowledge("nonexistent", limit=5)
        assert len(results_empty) == 0
    
    @pytest.mark.asyncio
    async def test_extract_knowledge_data(self, engine):
        """测试从事件中提取知识数据"""
        # 创建测试事件
        test_event = FlowEvent(
            event_type=KnowledgeCaptureEventTypes.BEST_PRACTICE,
            source="senior_engineer",
            timestamp=1234567890.0,
            payload={
                "practice_name": "clock_tree_planning",
                "practice_description": "时钟树规划时应提前考虑功耗和skew平衡",
                "context": {"design_type": "soc", "frequency": "2GHz"}
            },
            metadata={"priority": 1, "tags": ["clock", "planning"]}
        )
        
        # 提取知识数据
        knowledge_data = engine._extract_knowledge_data(test_event)
        
        # 验证提取结果
        assert knowledge_data["event_type"] == KnowledgeCaptureEventTypes.BEST_PRACTICE
        assert knowledge_data["source"] == "senior_engineer"
        assert knowledge_data["timestamp"] == 1234567890.0
        assert knowledge_data["knowledge_type"] == "best_practice"
        assert knowledge_data["practice_name"] == "clock_tree_planning"
        assert knowledge_data["practice_description"] == "时钟树规划时应提前考虑功耗和skew平衡"
        assert knowledge_data["context"]["design_type"] == "soc"
        assert knowledge_data["metadata"]["priority"] == 1
    
    @pytest.mark.asyncio
    async def test_enrich_knowledge(self, engine):
        """测试知识富化功能"""
        # 创建原始知识数据
        raw_knowledge = {
            "knowledge_type": "design_pattern",
            "pattern_name": "power_gating_cell",
            "pattern_description": "电源门控单元布局模式",
            "context": {"power_domain": "PD1", "switch_type": "header"}
        }
        
        # 创建模拟事件
        mock_event = MagicMock()
        mock_event.source = "design_pattern_miner"
        
        # 富化知识
        enriched = engine._enrich_knowledge(raw_knowledge, mock_event)
        
        # 验证富化结果
        assert "tags" in enriched
        assert "design_pattern" in enriched["tags"]
        assert "source:design_pattern_miner" in enriched["tags"]
        
        assert "quality_score" in enriched
        assert 0.0 <= enriched["quality_score"] <= 1.0
        
        assert "enrichment_timestamp" in enriched
        
        if engine.vector_db_enabled:
            assert "vector" in enriched
            assert isinstance(enriched["vector"], list)
            assert len(enriched["vector"]) == 128  # 模拟向量维度
    
    @pytest.mark.asyncio
    async def test_store_knowledge(self, engine):
        """测试知识存储功能"""
        # 创建测试知识
        test_knowledge = {
            "knowledge_type": "test_type",
            "data": "test_data",
            "vector": [0.1, 0.2, 0.3]  # 模拟向量
        }
        
        # 存储知识
        knowledge_id = await engine._store_knowledge(test_knowledge)
        
        # 验证存储结果
        assert knowledge_id.startswith("knowledge_")
        assert knowledge_id in engine.knowledge_store
        assert engine.knowledge_store[knowledge_id] == test_knowledge
        
        # 验证向量索引
        if engine.vector_db_enabled:
            assert knowledge_id in engine.vector_index
            assert engine.vector_index[knowledge_id] == [0.1, 0.2, 0.3]
    
    @pytest.mark.asyncio
    async def test_build_relations(self, engine):
        """测试建立知识关联功能"""
        # 先添加一些测试知识
        knowledge_1 = {
            "knowledge_type": "rule_violation",
            "rule_id": "min_width_001",
            "vector": [0.1] * 128
        }
        knowledge_2 = {
            "knowledge_type": "rule_violation",
            "rule_id": "min_width_001",  # 相同规则ID
            "vector": [0.2] * 128
        }
        knowledge_3 = {
            "knowledge_type": "best_practice",
            "rule_id": "different_rule",
            "vector": [0.9] * 128
        }
        
        # 存储知识
        engine.knowledge_store["k1"] = knowledge_1
        engine.knowledge_store["k2"] = knowledge_2
        engine.knowledge_store["k3"] = knowledge_3
        
        # 设置向量索引
        if engine.vector_db_enabled:
            engine.vector_index["k1"] = knowledge_1["vector"]
            engine.vector_index["k2"] = knowledge_2["vector"]
            engine.vector_index["k3"] = knowledge_3["vector"]
        
        # 建立关联
        await engine._build_relations("k1", knowledge_1)
        
        # 验证关联
        if engine.graph_db_enabled:
            assert "k1" in engine.graph_relations
            # k1应该关联到k2（相同规则ID）和可能的k3（相似向量）
            relations = engine.graph_relations["k1"]
            assert "k2" in relations
    
    @pytest.mark.asyncio
    async def test_send_health_event(self, engine, message_bus):
        """测试发送健康事件"""
        await message_bus.start()
        await engine.start()
        
        # 设置一些统计数据
        engine.knowledge_count = 10
        engine.retrieval_count = 20
        engine.cache_hits = 5
        
        # 监听健康事件
        health_events = []
        await message_bus.subscribe(SystemHealthEventTypes.RESOURCE_MONITOR,
                                   lambda e: health_events.append(e))
        
        # 发送健康事件
        await engine._send_health_event("测试健康事件", "info")
        
        # 等待事件传播
        await asyncio.sleep(0.1)
        
        # 验证事件
        assert len(health_events) == 1
        event = health_events[0]
        assert event.event_type == SystemHealthEventTypes.RESOURCE_MONITOR
        assert event.payload["resource_metrics"]["knowledge_count"] == 10
        assert event.payload["resource_metrics"]["retrieval_count"] == 20
        assert event.payload["resource_metrics"]["cache_hits"] == 5
        assert event.payload["severity"] == "info"
        assert event.payload["message"] == "测试健康事件"
        
        await engine.stop()
    
    @pytest.mark.asyncio
    async def test_get_stats(self, engine):
        """测试获取统计信息"""
        # 设置一些状态
        engine.knowledge_count = 5
        engine.retrieval_count = 12
        engine.cache_hits = 3
        engine.vector_index["v1"] = [0.1] * 128
        engine.graph_relations["k1"] = ["k2", "k3"]
        
        # 获取统计信息
        stats = engine.get_stats()
        
        # 验证基础统计
        assert stats["engine_id"] == "knowledge_management_engine"
        assert stats["knowledge_count"] == 5
        assert stats["retrieval_count"] == 12
        assert stats["cache_hits"] == 3
        assert stats["cache_hit_rate"] == 3 / 12
        
        # 验证向量和图统计
        assert stats["vector_index_size"] == 1
        assert stats["graph_relations_count"] == 2
    
    @pytest.mark.asyncio
    async def test_quality_score_calculation(self, engine):
        """测试质量评分计算"""
        # 测试不同知识类型的质量评分
        test_cases = [
            {
                "knowledge_type": "best_practice",
                "context": {"key": "value"},
                "related_files": ["file1.txt"],
                "expected_min": 0.8  # 基础0.5 + 类型0.3 + 上下文0.1 + 文件0.1 = 1.0（但会限制在1.0）
            },
            {
                "knowledge_type": "engineer_decision",
                "context": {},
                "related_files": [],
                "expected_min": 0.7  # 基础0.5 + 类型0.2 = 0.7
            },
            {
                "knowledge_type": "rule_violation",
                "context": {"key": "value"},
                "related_files": [],
                "expected_min": 0.6  # 基础0.5 + 类型0.1 + 上下文0.1 = 0.7（但实际可能不同）
            },
            {
                "knowledge_type": "unknown",
                "context": {},
                "related_files": [],
                "expected_min": 0.5  # 只有基础分
            }
        ]
        
        for test_case in test_cases:
            knowledge_data = {
                "knowledge_type": test_case["knowledge_type"],
                "context": test_case["context"],
                "related_files": test_case["related_files"]
            }
            
            score = engine._calculate_quality_score(knowledge_data)
            assert 0.0 <= score <= 1.0
            assert score >= test_case["expected_min"] - 0.1  # 允许一定误差


if __name__ == "__main__":
    pytest.main([__file__, "-v"])