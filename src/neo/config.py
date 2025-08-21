"""Neo包配置管理模块

基于Box的配置管理，支持TOML格式配置文件。
"""

import tomllib
from pathlib import Path
from box import Box
from typing import Optional

# 全局配置对象缓存
_config_cache = {}
_default_config_path = Path.cwd() / "config.toml"


def load_config(config_path: Optional[Path] = None) -> Box:
    """从指定路径加载配置文件
    
    Args:
        config_path: 配置文件路径，如果为 None 则使用默认路径
        
    Returns:
        Box: 配置对象
    """
    if config_path is None:
        config_path = _default_config_path
    
    # 转换为绝对路径作为缓存键
    cache_key = str(config_path.resolve())
    
    # 检查缓存
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    # 读取配置文件
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with config_path.open("rb") as f:
        config_data = tomllib.load(f)
    
    # 创建 Box 对象并缓存
    config = Box(config_data)
    _config_cache[cache_key] = config
    
    return config


def get_config(config_path: Optional[Path] = None) -> Box:
    """获取配置对象
    
    Args:
        config_path: 配置文件路径，如果为 None 则使用默认路径
        
    Returns:
        Box: 配置对象
    """
    return load_config(config_path)


def clear_config_cache():
    """清空配置缓存"""
    global _config_cache
    _config_cache.clear()
