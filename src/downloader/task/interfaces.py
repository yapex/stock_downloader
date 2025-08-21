"""Task 包的接口定义"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Protocol

from downloader.task.types import DownloadTaskConfig, TaskPriority, TaskResult
from downloader.producer.fetcher_builder import TaskType
from downloader.database.schema_loader import SchemaLoader


class ITaskTypeConfig(ABC):
    """任务类型配置接口
    
    定义不同任务类型的优先级配置管理。
    """
    
    @abstractmethod
    def get_priority(self, task_type: TaskType) -> TaskPriority:
        """获取任务类型的优先级
        
        Args:
            task_type: 任务类型
            
        Returns:
            任务优先级
        """
        pass
    
    @abstractmethod
    def set_priority(self, task_type: TaskType, priority: TaskPriority) -> None:
        """设置任务类型的优先级
        
        Args:
            task_type: 任务类型
            priority: 任务优先级
        """
        pass


class ITaskScheduler(ABC):
    """任务调度器接口
    
    定义基于优先级的任务调度功能，支持动态添加任务和按优先级获取任务。
    """
    
    @abstractmethod
    def add_task(self, config: DownloadTaskConfig) -> None:
        """添加任务到调度器
        
        Args:
            config: 下载任务配置
        """
        pass
    
    @abstractmethod
    def add_tasks(self, configs: List[DownloadTaskConfig]) -> None:
        """批量添加任务
        
        Args:
            configs: 下载任务配置列表
        """
        pass
    
    @abstractmethod
    def get_next_task(self, timeout: Optional[float] = None) -> Optional[DownloadTaskConfig]:
        """获取下一个任务
        
        Args:
            timeout: 超时时间（秒），None 表示阻塞等待
            
        Returns:
            下一个任务配置，如果队列为空且超时则返回 None
        """
        pass
    
    @abstractmethod
    def get_all_tasks(self) -> List[DownloadTaskConfig]:
        """获取所有剩余任务（非阻塞）
        
        Returns:
            所有剩余任务的配置列表
        """
        pass
    
    @abstractmethod
    def is_empty(self) -> bool:
        """检查调度器是否为空
        
        Returns:
            如果调度器为空返回 True，否则返回 False
        """
        pass
    
    @abstractmethod
    def size(self) -> int:
        """获取队列中任务数量
        
        Returns:
            队列中的任务数量
        """
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """清空所有任务"""
        pass


class IDownloadTask(Protocol):
    """下载任务执行器接口"""
    
    def execute(self, config: DownloadTaskConfig) -> TaskResult:
        """执行下载任务
        
        Args:
            config: 任务配置
            
        Returns:
            任务执行结果
        """
        ...