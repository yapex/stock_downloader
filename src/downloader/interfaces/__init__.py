"""接口定义模块

本模块包含项目中所有的Protocol接口定义，用于实现面向接口编程。
"""

from .config import (
    IConfig,
    ITaskConfig,
    IGroupConfig,
    IDownloaderConfig,
    IConsumerConfig,
    IDatabaseConfig,
)

# logger module removed
from .database import IDatabase, IDatabaseFactory
from .fetcher import IFetcher, FetcherProtocol
from .storage import IStorage, StorageProtocol
from .events import IEventListener, IEventBus
from .producer import IProducer, ProducerEvents
from .task import ITask

# 向后兼容的别名
ConfigInterface = IConfig
TaskConfig = ITaskConfig
GroupConfig = IGroupConfig
DownloaderConfig = IDownloaderConfig
ConsumerConfig = IConsumerConfig
DatabaseConfig = IDatabaseConfig
# LoggerInterface removed
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
    # "ILogger", # removed
    "IDatabase",
    "IDatabaseFactory",
    "IFetcher",
    "IStorage",
    "IEventListener",
    "IEventBus",
    "IProducer",
    "ProducerEvents",
    "ITaskHandler",
    "IIncrementalTaskHandler",
    "ITaskHandlerFactory",

    # "StandardLogger", # removed
    # "LoggerFactory", # removed
    # 向后兼容的别名
    "ConfigInterface",
    "TaskConfig",
    "GroupConfig",
    "DownloaderConfig",
    "ConsumerConfig",
    "DatabaseConfig",
    # "LoggerInterface", # removed
    "DatabaseConnection",
    "FetcherProtocol",
    "StorageProtocol",
    "DatabaseConnectionFactory",
    "TaskHandlerProtocol",
    "IncrementalTaskHandlerProtocol",
    "TaskHandlerFactoryProtocol",
]
