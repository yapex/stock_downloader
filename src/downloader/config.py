# -*- coding: utf-8 -*-
"""
负责加载和解析 YAML 配置文件。
"""
from pathlib import Path
import yaml

def load_config(config_path: str = "config.yaml") -> dict:
    """加载 YAML 配置文件"""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"配置文件 {config_path} 不存在")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
