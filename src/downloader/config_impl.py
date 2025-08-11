"""配置接口实现

实现ConfigInterface及相关配置类，提供统一的配置访问方式。
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import os
from .interfaces.config import (
    IConfig,
    ITaskConfig,
    IGroupConfig,
    IDownloaderConfig,
    IConsumerConfig,
    IDatabaseConfig
)
from .config import load_config


@dataclass
class TaskConfigImpl(ITaskConfig):
    """任务配置实现"""
    _data: Dict[str, Any]
    
    @property
    def name(self) -> str:
        return self._data.get("name", "")
    
    @property
    def type(self) -> str:
        return self._data.get("type", "")
    
    @property
    def date_col(self) -> Optional[str]:
        return self._data.get("date_col")
    
    @property
    def statement_type(self) -> Optional[str]:
        return self._data.get("statement_type")
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取任务配置项"""
        return self._data.get(key, default)


@dataclass
class GroupConfigImpl(IGroupConfig):
    """任务组配置实现"""
    _data: Dict[str, Any]
    
    @property
    def description(self) -> str:
        return self._data.get("description", "")
    
    @property
    def symbols(self) -> List[str] | str:
        return self._data.get("symbols", [])
    
    @property
    def tasks(self) -> List[str]:
        return self._data.get("tasks", [])


@dataclass
class DownloaderConfigImpl(IDownloaderConfig):
    """下载器配置实现"""
    _data: Dict[str, Any]
    
    @property
    def max_producers(self) -> int:
        return self._data.get("max_producers", 1)
    
    @property
    def max_consumers(self) -> int:
        return self._data.get("max_consumers", 2)
    
    @property
    def producer_queue_size(self) -> int:
        return self._data.get("producer_queue_size", 1000)
    
    @property
    def data_queue_size(self) -> int:
        return self._data.get("data_queue_size", 5000)
    
    @property
    def symbols(self) -> Optional[List[str]]:
        return self._data.get("symbols")


@dataclass
class ConsumerConfigImpl(IConsumerConfig):
    """消费者配置实现"""
    _data: Dict[str, Any]
    
    @property
    def batch_size(self) -> int:
        return self._data.get("batch_size", 100)
    
    @property
    def flush_interval(self) -> int:
        return self._data.get("flush_interval", 30)
    
    @property
    def max_retries(self) -> int:
        return self._data.get("max_retries", 3)


@dataclass
class DatabaseConfigImpl(IDatabaseConfig):
    """数据库配置实现"""
    _data: Dict[str, Any]
    
    @property
    def path(self) -> str:
        return self._data.get("path", "data/stock.db")


class ConfigManager(IConfig):
    """配置管理器
    
    实现ConfigInterface，提供统一的配置访问方式。
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """初始化配置管理器
        
        Args:
            config_path: 配置文件路径
        """
        self._config_data = load_config(config_path)
        self._task_configs: Dict[str, ITaskConfig] = {}
        self._group_configs: Dict[str, IGroupConfig] = {}
        self._database_config: Optional[IDatabaseConfig] = None
        self._downloader_config: Optional[IDownloaderConfig] = None
        self._consumer_config: Optional[IConsumerConfig] = None
        
        self._initialize_configs()
    
    def _initialize_configs(self) -> None:
        """初始化各种配置对象"""
        # 初始化任务配置
        tasks_data = self._config_data.get("tasks", {})
        for task_name, task_data in tasks_data.items():
            self._task_configs[task_name] = TaskConfigImpl(task_data)
        
        # 初始化任务组配置
        groups_data = self._config_data.get("groups", {})
        for group_name, group_data in groups_data.items():
            self._group_configs[group_name] = GroupConfigImpl(group_data)
        
        # 初始化其他配置
        self._database_config = DatabaseConfigImpl(
            self._config_data.get("database", {})
        )
        self._downloader_config = DownloaderConfigImpl(
            self._config_data.get("downloader", {})
        )
        self._consumer_config = ConsumerConfigImpl(
            self._config_data.get("consumer", {})
        )
    
    def get_runtime_token(self) -> str:
        """获取运行时Tushare API Token（优先从环境变量）"""
        # 优先从环境变量获取
        token = os.getenv("TUSHARE_TOKEN")
        if token:
            return token
        
        # 从配置文件获取
        return self._config_data.get("tushare_token", "")
    
    @property
    def database(self) -> IDatabaseConfig:
        """数据库配置"""
        if self._database_config is None:
            raise ValueError("数据库配置未初始化")
        return self._database_config
    
    @property
    def downloader(self) -> IDownloaderConfig:
        """下载器配置"""
        if self._downloader_config is None:
            raise ValueError("下载器配置未初始化")
        return self._downloader_config
    
    @property
    def consumer(self) -> IConsumerConfig:
        """消费者配置"""
        if self._consumer_config is None:
            raise ValueError("消费者配置未初始化")
        return self._consumer_config
    
    def get_task_config(self, task_name: str) -> Optional[ITaskConfig]:
        """获取任务配置"""
        return self._task_configs.get(task_name)
    
    def get_group_config(self, group_name: str) -> Optional[IGroupConfig]:
        """获取任务组配置"""
        return self._group_configs.get(group_name)
    
    def get_all_tasks(self) -> Dict[str, ITaskConfig]:
        """获取所有任务配置"""
        return self._task_configs.copy()
    
    def get_all_groups(self) -> Dict[str, IGroupConfig]:
        """获取所有任务组配置"""
        return self._group_configs.copy()
    

    
    def validate(self) -> bool:
        """验证配置有效性"""
        try:
            # 验证必要的配置项
            if not self.get_runtime_token():
                return False
            
            if not self.database.path:
                return False
            
            # 验证任务配置
            for task in self._task_configs.values():
                if not task.name or not task.type:
                    return False
            
            # 验证任务组配置
            for group in self._group_configs.values():
                if not group.description or not group.tasks:
                    return False
                
                # 验证任务组中的任务是否存在
                for task_name in group.tasks:
                    if task_name not in self._task_configs:
                        return False
            
            return True
        except Exception:
            return False


def create_config_manager(config_path: str = "config.yaml") -> IConfig:
    """创建配置管理器实例
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        ConfigInterface: 配置接口实例
    """
    return ConfigManager(config_path)