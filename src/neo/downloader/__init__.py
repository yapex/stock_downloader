"""Neo 下载器子模块

提供数据下载相关的接口、类型定义和实现类。"""

from .interfaces import IDownloader
from .simple_downloader import SimpleDownloader
from .types import TaskType, TaskPriority, DownloadTaskConfig, TaskResult
from .fetcher_builder import FetcherBuilder, TushareApiManager
from neo.helpers.utils import normalize_stock_code, is_interval_greater_than_7_days, setup_logging

__all__ = [
    "IDownloader",
    "SimpleDownloader",
    "TaskType",
    "TaskPriority",
    "DownloadTaskConfig",
    "TaskResult",
    "FetcherBuilder",
    "TushareApiManager",
    "normalize_stock_code",
    "is_interval_greater_than_7_days",
    "setup_logging"
]