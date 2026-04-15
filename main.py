"""
-*-coding: utf-8-*-
主模块
"""
import logging
import yaml
from pathlib import Path
import uvicorn

# 读取配置
config_path = Path(__file__).parent / 'config' / 'config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


if __name__ == "__main__":
    host = config["connection"]["host"]
    port = config["connection"]["port"]
    uvicorn.run("utils.api:app", host=host, port=port, reload=True)

