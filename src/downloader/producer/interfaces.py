from typing import Protocol, Callable, Any, List, Optional, TYPE_CHECKING
from abc import ABC, abstractmethod
import pandas as pd
from .fetcher_builder import TaskType
from ..task.interfaces import IDownloadTask
from ..task.task_scheduler import TaskTypeConfig

if TYPE_CHECKING:
    from .downloader_manager import DownloadStats


class IApiManager(Protocol):
    """API 管理器接口"""
    
    def get_api_function(self, base_object: str, method_name: str) -> Callable:
        """获取 API 函数
        
        Args:
            base_object: API 对象名称（如 'pro', 'ts'）
            method_name: 方法名称
            
        Returns:
            API 函数对象
        """
        ...


class IDownloaderManager(ABC):
    """下载管理器接口
    
    定义下载管理器的核心功能，包括任务管理、执行控制和状态监控。
    """
    
    @abstractmethod
    def add_download_tasks(
        self, symbols: List[str], task_type: TaskType, **kwargs
    ) -> None:
        """添加下载任务
        
        Args:
            symbols: 股票代码列表
            task_type: 任务类型
            **kwargs: 传递给 create_task_configs 的额外参数
        """
        pass
    
    @abstractmethod
    def download_group(
        self,
        group: str,
        symbols: Optional[List[str]] = None,
        **kwargs
    ) -> "DownloadStats":
        """下载指定任务组的数据
        
        Args:
            group: 任务组名称
            symbols: 股票代码列表，如果为 None 则使用 ["all"]
            **kwargs: 传递给任务的额外参数
            
        Returns:
            下载统计信息
        """
        pass
    
    @abstractmethod
    def start(self) -> None:
        """启动下载管理器"""
        pass
    
    @abstractmethod
    def stop(self) -> None:
        """停止下载管理器"""
        pass
    
    @abstractmethod
    def run(self) -> "DownloadStats":
        """运行下载任务并返回统计信息
        
        Returns:
            下载统计信息
        """
        pass
    
    @abstractmethod
    def is_running(self) -> bool:
        """检查管理器是否正在运行
        
        Returns:
            如果正在运行返回 True，否则返回 False
        """
        pass
    
    @abstractmethod
    def is_shutdown_requested(self) -> bool:
        """检查是否请求关闭
        
        Returns:
            如果请求关闭返回 True，否则返回 False
        """
        pass
    
    @abstractmethod
    def get_stats(self) -> "DownloadStats":
        """获取当前统计信息
        
        Returns:
            下载统计信息
        """
        pass


class IFetcherBuilder(Protocol):
    """数据获取器构建器接口"""
    
    def build_by_task(
        self, 
        task_type: TaskType, 
        symbol: str = "", 
        **overrides: Any
    ) -> Callable[[], pd.DataFrame]:
        """构建指定股票代码的数据获取器
        
        Args:
            task_type: 任务类型
            symbol: 股票代码
            **overrides: 运行时参数覆盖
            
        Returns:
            可执行的数据获取函数
        """
        ...