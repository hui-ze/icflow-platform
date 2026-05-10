# IC-Flow Platform 设计文档
## 集成电路后端设计AI协同平台

**品牌声明**
- **产品品牌**：IC-Flow Platform
- **项目代号**：Project Synapse  
- **技术栈品牌**：FlowStack
- **设计理念继承自**：事件驱动与智能体架构框架
- **品牌独立性**：本产品拥有完全独立的品牌体系，所有术语均经过系统化设计：

| 原架构术语 | IC-Flow Platform 对应术语 |
|-------------------|---------------------------|
| lane              | Flow event                |
| Agent             | Flow Engine               |
| Event Bus         | Flow Message Bus          |
| Plugin            | Extension / Adapter (依上下文) |
| MCP               | Extension Protocol        |
| lane event system | Flow event system         |
| Agentic           | Flow-Driven               |
| 其他相关术语       | 依同类原则转换             |

---

## 第1章 事件模型设计

> 基于 Flow event system 构建。

### 1.1 核心事件类型
1. **设计流程事件** (`DesignFlowEvent`)
   - 触发时机：设计任务启动、阶段完成、错误发生
   - 携带数据：任务ID、阶段标识、状态码、时间戳
2. **工具执行事件** (`ToolExecutionEvent`)
   - 触发时机：EDA工具调用开始、结束、输出就绪
   - 携带数据：工具名称、命令行、输出路径、执行状态
3. **知识捕获事件** (`KnowledgeCaptureEvent`)
   - 触发时机：设计规则违反、工程师决策、工具输出解析完成
   - 携带数据：规则ID、上下文、决策理由、关联文件
4. **系统健康事件** (`SystemHealthEvent`)
   - 触发时机：资源监控、异常检测、性能阈值突破
   - 携带数据：资源指标、异常堆栈、建议动作

### 1.2 事件发布-订阅模型
- **发布者**：任何 Flow Engine 或 Extension
- **订阅者**：向 Flow Message Bus 注册关注的事件类型
- **路由规则**：基于事件类型、标签、优先级进行智能路由
- **持久化**：高优先级事件自动持久化，支持事后审计与重放

### 1.3 事件格式规范
```json
{
  "event_id": "uuid-v4",
  "event_type": "design_flow.phase_completed",
  "timestamp": "ISO8601",
  "source": "flow_engine:drc_repair:instance_id",
  "payload": {
    "task_id": "task_123",
    "phase": "drc_initial_check",
    "status": "success",
    "metrics": {...}
  },
  "tags": ["drc", "repair", "phase1"],
  "priority": "normal"
}
```

---

## 第2章 扩展接口规范设计

> 基于 Extension Protocol 的生命周期模型。

### 2.1 Extension 生命周期
1. **注册阶段**
   - Extension 向平台注册自身能力描述
   - 平台分配唯一 `extension_id` 并授权访问的事件范围
2. **初始化阶段**
   - Extension 加载配置、建立与 Flow Message Bus 的连接
   - 声明可处理的事件类型及提供的服务接口
3. **运行阶段**
   - 接收订阅的事件，执行业务逻辑
   - 可选择发布新事件或直接返回结果
4. **销毁阶段**
   - 优雅关闭连接，释放资源
   - 向平台注销，更新服务状态

### 2.2 通信协议
- **传输层**：gRPC over HTTP/2（默认），支持 WebSocket 用于实时流
- **消息格式**：Protocol Buffers（.proto 定义）
- **身份认证**：mTLS 双向证书认证
- **流量控制**：基于令牌桶的限流机制

### 2.3 错误恢复机制
- **重试策略**：指数退避 + 最大重试次数
- **死信队列**：无法处理的事件转入死信队列，供人工审查
- **健康检查**：定期心跳，失败时自动重启或标记为不可用
- **版本兼容**：向后兼容至少两个主要版本，支持平滑升级

---

## 第3章 核心引擎角色定义

> 基于 Flow event system 定义的核心 Flow Engine 角色。

### 3.1 DRC修复主引擎（DRC Repair Master Engine）
- **职责**：统筹DRC违例修复全流程，调用子引擎完成具体修复操作
- **触发事件**：`DesignFlowEvent`（类型为 `drc_violation_detected`）
- **输出事件**：`ToolExecutionEvent`（调用Calibre、ICV等工具）、`KnowledgeCaptureEvent`（记录修复决策）
- **关键能力**：修复策略选择、多工具协调、迭代优化

### 3.2 知识管理引擎（Knowledge Management Engine）
- **职责**：捕获、索引、检索设计知识，构建组织记忆库
- **触发事件**：各类 `KnowledgeCaptureEvent`
- **输出事件**：`KnowledgeCaptureEvent`（标注、关联后）、`SystemHealthEvent`（知识库健康度）
- **关键能力**：向量化检索、图谱关联、持续学习、质量评估

### 3.3 EDA工具适配器引擎（EDA Tool Adapter Engine）
- **职责**：封装各类EDA工具（Cadence、Synopsys、Siemens）的调用细节，提供统一接口
- **触发事件**：`ToolExecutionEvent`
- **输出事件**：`ToolExecutionEvent`（结果）、`DesignFlowEvent`（工具执行状态）
- **关键能力**：命令行组装、输出解析、错误处理、许可证管理

### 3.4 流程编排引擎（Process Orchestration Engine）
- **职责**：定义和执行多引擎协同的工作流，处理分支、循环、条件等逻辑
- **触发事件**：`DesignFlowEvent`（流程启动）
- **输出事件**：`DesignFlowEvent`（流程状态更新）
- **关键能力**：工作流定义（YAML/JSON）、状态持久化、异常处理、人工干预点

---

## 第4章 知识管理机制详细设计

### 4.1 事件驱动的知识捕获管道
1. **捕获点**：在关键设计决策、工具输出、规则违反处自动发射 `KnowledgeCaptureEvent`
2. **富化**：附加上下文（设计版本、工程师ID、时间、关联文件）
3. **分类**：根据预定义分类体系自动打标
4. **向量化**：使用领域适配的 embedding 模型生成向量表示
5. **存储**：同步写入向量数据库（Milvus/Qdrant）和图数据库（Neo4j）

### 4.2 混合存储架构
- **向量存储**：用于相似性检索，支持 ANN 索引
- **图存储**：用于关系推理（规则-违例-修复方案-工程师）
- **关系存储**：用于元数据、版本、审计日志
- **对象存储**：用于原始文件（日志、截图、报告）

### 4.3 多阶段检索算法
1. **召回阶段**：基于向量相似度召回 Top‑K 相关条目
2. **过滤阶段**：应用业务规则（版本兼容性、权限、时效性）过滤
3. **排序阶段**：综合相关性、置信度、热度进行排序
4. **解释阶段**：生成检索结果的解释说明，提高工程师信任度

### 4.4 持续学习闭环
- **反馈收集**：工程师对检索结果的评分、使用频率
- **模型更新**：定期用新数据重新训练 embedding 模型、分类器
- **评估指标**：检索准确率、工程师满意度、问题解决时间缩短率
- **自动优化**：基于评估结果调整检索策略、分类体系

### 4.5 降级容错机制
- **缓存层**：高频知识条目缓存，避免检索超时
- **默认策略**：当检索无结果时，返回预定义的通用建议
- **离线模式**：在网络或数据库故障时，使用本地缓存继续服务
- **健康监控**：实时监控知识库各组件状态，自动告警

---

## 第5章 系统集成与部署架构

> 已按 IC‑Flow 品牌术语完成修订，详见下文。

### 5.1 部署拓扑
- **开发环境**：单节点 Docker Compose，适合快速验证
- **测试环境**：Kubernetes 集群（3节点），模拟生产拓扑
- **生产环境**：多区域 Kubernetes 集群，具备高可用与灾备

### 5.2 容器化与编排
- **基础镜像**：`icflow/platform-base:1.0.0`（基于 Ubuntu LTS + 运行时）
- **服务镜像**：每个 Flow Engine 独立镜像，标签包含版本、组件
- **Kubernetes 部署**：使用 StatefulSet（有状态服务）与 Deployment（无状态服务）
- **服务发现**：Kubernetes Service + DNS，支持负载均衡

### 5.3 数据持久化
- **块存储**：高性能 SSD StorageClass (`icflow-fast-ssd`) 用于数据库
- **文件存储**：RWX 共享存储用于设计文件共享
- **备份策略**：每日全量备份 + 实时 WAL 归档，备份保留30天

### 5.4 监控与可观测性
- **指标收集**：Prometheus + Node Exporter + 自定义 Exporters
- **日志聚合**：EFK 栈（Elasticsearch, Fluentd, Kibana）
- **分布式追踪**：Jaeger，跟踪跨引擎调用链路
- **告警规则**：基于 Prometheus 规则，对接 PagerDuty/钉钉

### 5.5 性能优化
- **多级缓存**：L1（内存）、L2（Redis）、L3（CDN）
- **连接池**：数据库、消息总线、外部API 均使用连接池
- **异步处理**：高延迟操作异步化，避免阻塞主流程
- **资源配额**：基于命名空间的 CPU/内存限制，防止资源耗尽

### 5.6 安全与合规
- **网络策略**：Kubernetes NetworkPolicy 实现微服务间最小权限通信
- **密钥管理**：HashiCorp Vault 或 Kubernetes Secrets
- **审计日志**：所有关键操作记录不可篡改的审计日志
- **合规扫描**：定期进行漏洞扫描、许可证合规检查

### 5.7 灾备与多区域部署
- **主动-被动**：主区域承载流量，备区域定期同步数据
- **数据同步**：基于逻辑复制（数据库）与对象存储同步
- **故障切换**：DNS/GSLB 实现流量切换，RTO < 5分钟
- **演练计划**：每季度进行一次灾备演练，验证恢复流程

---

## 附录：术语对照完整表

| 原架构术语 | IC‑Flow Platform 术语 | 说明 |
|-------------------|------------------------|------|
| lane              | Flow event             | 事件的基本单位，携带业务数据 |
| Agent             | Flow Engine            | 执行特定任务的处理单元 |
| Event Bus         | Flow Message Bus       | 事件发布‑订阅的中枢 |
| Plugin            | Extension / Adapter    | 扩展平台能力的模块 |
| MCP               | Extension Protocol     | 扩展与平台间的通信协议 |
| lane event system | Flow event system      | 整体事件驱动架构 |
| Agentic           | Flow‑Driven            | 强调基于事件流的自主协同 |
| 原架构品牌         | IC‑Flow Platform       | 产品品牌名称 |
| –                 | Project Synapse        | 项目内部代号 |
| –                 | FlowStack              | 技术栈品牌名称 |

*文档版本：1.0.0（2026‑04‑20）*  
*所有设计均继承事件驱动与智能体架构理念，并已完全转换为 IC‑Flow Platform 独立品牌。*