FROM python:3.13-slim

# 设置工作目录
WORKDIR /app

# 安装依赖（利用 Docker 层缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用源码和默认配置
COPY main.py .
COPY utils/ ./utils/
COPY config/ ./config/

# 创建运行时目录并设置权限
RUN mkdir -p /app/data /app/logs && \
    useradd --create-home appuser && \
    chown -R appuser:appuser /app

# 切换到非 root 用户
USER appuser

# 暴露端口
EXPOSE 8001

# 启动
CMD ["python", "main.py"]
