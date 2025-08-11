"""配置接口定义

定义配置相关的Protocol接口，用于统一配置访问方式。
"""

from typing import Protocol, Dict, List, Any, Optional, runtime_checkable
from datetime import datetime


@runtime_checkable
class ITaskConfig(Protocol):
    """任务配置接口"""
    
    @property
    def name(self) -> str:
        """任务名称"""
        ...
    
    @property
    def type(self) -> str:
        """任务类型"""
        ...
    
    @property
    def date_col(self) -> Optional[str]:
        """日期列名"""
        ...
    
    @property
    def statement_type(self) -> Optional[str]:
        """财务报表类型"""
        ...
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取任务配置项"""
        ...


@runtime_checkable
class IGroupConfig(Protocol):
    """任务组配置接口"""
    
    @property
    def description(self) -> str:
        """组描述"""
        ...
    
    @property
    def symbols(self) -> List[str] | str:
        """股票代码列表或'all'"""
        ...
    
    @property
    def tasks(self) -> List[str]:
        """任务列表"""
        ...


@runtime_checkable
class IDownloaderConfig(Protocol):
    """下载器配置接口"""
    
    @property
    def max_producers(self) -> int:
        """最大生产者数量"""
        ...
    
    @property
    def max_consumers(self) -> int:
        """最大消费者数量"""
        ...
    
    @property
    def producer_queue_size(self) -> int:
        """生产者队列大小"""
        ...
    
    @property
    def data_queue_size(self) -> int:
        """数据队列大小"""
        ...
    
    @property
    def symbols(self) -> Optional[List[str]]:
        """股票代码列表"""
        ...


@runtime_checkable
class IConsumerConfig(Protocol):
    """消费者配置接口"""
    
    @property
    def batch_size(self) -> int:
        """批处理大小"""
        ...
    
    @property
    def flush_interval(self) -> int:
        """刷新间隔（秒）"""
        ...
    
    @property
    def max_retries(self) -> int:
        """最大重试次数"""
        ...


@runtime_checkable
class IDatabaseConfig(Protocol):
    """数据库配置接口"""
    
    @property
    def path(self) -> str:
        """数据库路径"""
        ...


@runtime_checkable
class IConfig(Protocol):
    """配置接口
    
    提供统一的配置访问方式，替代直接访问配置字典。
    """
    
    def get_runtime_token(self) -> str:
        """获取运行时Tushare API Token（优先从环境变量）"""
        ...
    
    @property
    def database(self) -> IDatabaseConfig:
        """数据库配置"""
        ...
    
    @property
    def downloader(self) -> IDownloaderConfig:
        """下载器配置"""
        ...
    
    @property
    def consumer(self) -> IConsumerConfig:
        """消费者配置"""
        ...
    
    def get_task_config(self, task_name: str) -> Optional[ITaskConfig]:
        """获取任务配置"""
        ...
    
    def get_group_config(self, group_name: str) -> Optional[IGroupConfig]:
        """获取任务组配置"""
        ...
    
    def get_all_tasks(self) -> Dict[str, ITaskConfig]:
        """获取所有任务配置"""
        ...
    
    def get_all_groups(self) -> Dict[str, IGroupConfig]:
        """获取所有任务组配置"""
        ...
    
    def validate(self) -> bool:
        """验证配置有效性"""
        ...