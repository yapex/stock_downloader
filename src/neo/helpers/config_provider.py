"""配置提供者模块

基于 dependency-injector 的配置提供者实现。
"""

from dependency_injector import providers
from typing import Optional, Any, Dict
from pathlib import Path
import tomllib
from box import Box


class TomlConfigProvider(providers.Provider):
    """基于 TOML 文件的配置提供者

    提供配置的单例访问，支持缓存和自动重载
    """

    def __init__(
        self,
        config_path: Optional[Path] = None,
        default_path: Path = Path.cwd() / "config.toml",
        **kwargs,
    ):
        """初始化 TOML 配置提供者

        Args:
            config_path: 显式指定的配置文件路径
            default_path: 默认配置文件路径
            **kwargs: 传递给父类的参数
        """
        super().__init__(**kwargs)
        self._config_path = config_path or default_path
        self._config_cache: Dict[str, Box] = {}
        self._last_modified_time: Optional[float] = None

    def _get_config_path(self) -> Path:
        """获取配置文件路径"""
        return self._config_path

    def _load_config(self) -> Box:
        """加载配置文件"""
        config_path = self._get_config_path()
        cache_key = str(config_path.resolve())

        # 检查文件是否已修改
        current_mtime = config_path.stat().st_mtime if config_path.exists() else None
        if (
            cache_key in self._config_cache
            and self._last_modified_time == current_mtime
        ):
            return self._config_cache[cache_key]

        # 读取配置文件
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        with config_path.open("rb") as f:
            config_data = tomllib.load(f)

        # 创建 Box 对象并缓存
        config = Box(config_data)
        self._config_cache[cache_key] = config
        self._last_modified_time = current_mtime

        return config

    def _provide(self, args: Any, kwargs: Any) -> Box:
        """提供配置对象 (Dependency Injector 内部调用)"""
        return self._load_config()

    def override(self, value: Any) -> None:
        """重写配置 (用于测试)"""
        if not isinstance(value, Box):
            value = Box(value)
        super().override(value)

    def reload(self) -> None:
        """强制重新加载配置"""
        cache_key = str(self._get_config_path().resolve())
        if cache_key in self._config_cache:
            del self._config_cache[cache_key]
        self._last_modified_time = None
