"""下载任务相关的类型定义"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd

from downloader.producer.fetcher_builder import TaskType


class TaskPriority(Enum):
    """任务优先级
    
    数值越小优先级越高。
    """
    HIGH = 1
    MEDIUM = 2
    LOW = 3


@dataclass(frozen=True)
class DownloadTaskConfig:
    """下载任务配置"""
    symbol: str
    task_type: TaskType
    priority: TaskPriority = TaskPriority.MEDIUM
    max_retries: int = 2
    
    def __post_init__(self):
        # 对于 STOCK_BASIC 任务，symbol 应该为空字符串
        if self.task_type == TaskType.STOCK_BASIC and self.symbol:
            object.__setattr__(self, 'symbol', '')


@dataclass
class TaskResult:
    """任务执行结果"""
    config: DownloadTaskConfig
    success: bool
    data: Optional[pd.DataFrame] = None
    error: Optional[Exception] = None
    retry_count: int = 0