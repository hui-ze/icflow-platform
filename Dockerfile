# ====== IC-Flow Platform Docker Build ======
# 多阶段构建: build → runtime

# ---- Stage 1: Build ----
FROM python:3.13-slim AS builder

WORKDIR /build

# 安装构建依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 复制并安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# ---- Stage 2: Runtime ----
FROM python:3.13-slim

LABEL maintainer="IC-Flow Team" \
      description="IC-Flow Platform - 半导体设计流程自动化与知识管理平台" \
      version="0.1.0"

# 创建非root用户
RUN addgroup --system --gid 1001 icflow \
    && adduser --system --uid 1001 --gid 1001 icflow

WORKDIR /app

# 从构建阶段复制已安装的依赖
COPY --from=builder /root/.local /home/icflow/.local

# 复制项目源码
COPY src/ ./src/
COPY config/config.yaml.example ./config/config.yaml
COPY pyproject.toml .
COPY README.md .

# 设置Python路径
ENV PATH=/home/icflow/.local/bin:$PATH \
    PYTHONPATH=/app/src:$PYTHONPATH \
    PYTHONUNBUFFERED=1 \
    ICFLOW_CONFIG=/app/config/config.yaml

# 切换到非root用户
USER icflow

# 健康检查
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/health')" || exit 1

# 默认启动API服务
EXPOSE 8000
CMD ["uvicorn", "icflow.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
