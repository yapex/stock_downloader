from typing import Protocol, Callable, Any
import pandas as pd
from downloader.producer.fetcher_builder import TaskType


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