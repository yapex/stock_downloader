"""接口定义模块

本模块包含项目中所有的Protocol接口定义，用于实现面向接口编程。
"""

from .config import (
    IConfig,
    ITaskConfig,
    IGroupConfig,
    IDownloaderConfig,
    IConsumerConfig,
    IDatabaseConfig
)
from .logger import ILogger, StandardLogger, LoggerFactory
from .database import IDatabase, IDatabaseFactory
from .fetcher import IFetcher, FetcherProtocol
from .storage import IStorage, StorageProtocol

# 向后兼容的别名
ConfigInterface = IConfig
TaskConfig = ITaskConfig
GroupConfig = IGroupConfig
DownloaderConfig = IDownloaderConfig
ConsumerConfig = IConsumerConfig
DatabaseConfig = IDatabaseConfig
LoggerInterface = ILogger
DatabaseConnection = IDatabase
DatabaseConnectionFactory = IDatabaseFactory

__all__ = [
    # 新的接口名称
    "IConfig",
    "ITaskConfig",
    "IGroupConfig",
    "IDownloaderConfig",
    "IConsumerConfig",
    "IDatabaseConfig",
    "ILogger",
    "IDatabase",
    "IDatabaseFactory",
    "IFetcher",
    "IStorage",
    # 实现类
    "StandardLogger",
    "LoggerFactory",
    # 向后兼容的别名
    "ConfigInterface",
    "TaskConfig",
    "GroupConfig",
    "DownloaderConfig",
    "ConsumerConfig",
    "DatabaseConfig",
    "LoggerInterface",
    "DatabaseConnection",
    "FetcherProtocol",
    "StorageProtocol",
    "DatabaseConnectionFactory",
]