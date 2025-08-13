# config.py
import tomllib
from pathlib import Path
from typing import Any, Dict


class GlobalConfig:
    _instance = None
    _config_data: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # 直接定位项目根目录
            config_path = Path.cwd() / "config.toml"
            with config_path.open("rb") as f:
                cls._config_data = tomllib.load(f)
        return cls._instance

    def __getattr__(self, key: str) -> Any:
        """使用点号语法访问配置，如 config.database.host"""
        try:
            value = self._config_data[key]
            # 如果值是字典，返回一个支持点号访问的对象
            if isinstance(value, dict):
                return ConfigSection(value)
            return value
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' 对象没有属性 '{key}'")


class ConfigSection:
    """配置节对象，支持点号访问"""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def __getattr__(self, key: str) -> Any:
        try:
            value = self._data[key]
            if isinstance(value, dict):
                return ConfigSection(value)
            return value
        except KeyError:
            raise AttributeError(f"配置节中没有属性 '{key}'")


# 单例配置对象初始化
config = GlobalConfig()


if __name__ == "__main__":
    print(config.tushare.token)
