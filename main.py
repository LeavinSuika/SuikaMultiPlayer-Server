"""
-*-coding: utf-8-*-
主模块
"""
import logging
from logging.handlers import RotatingFileHandler
import yaml
from pathlib import Path
import uvicorn
from uvicorn.config import Config
from uvicorn.server import Server
import asyncio

# 读取配置
config_path = Path(__file__).parent / 'config' / 'config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

async def main():
    host = config["connection"]["host"]
    port = config["connection"]["port"]
    configs = Config(app="utils.api:app", host=host, port=port, loop="none", access_log=False)
    server = Server(configs)
    await server.serve()


if __name__ == "__main__":
    # 日志格式
    log_format = '[%(asctime)s %(levelname)s] [%(module)s] %(message)s'
    date_format = '%H:%M:%S'

    # 根日志器：控制台输出
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        force=True
    )

    # 文件日志：输出到 /logs
    logs_dir = Path(__file__).parent / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        logs_dir / 'server.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
    logging.getLogger().addHandler(file_handler)


    loop = asyncio.SelectorEventLoop()
    loop.run_until_complete(main())
