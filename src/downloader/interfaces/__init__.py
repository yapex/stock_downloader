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
from .fetcher import IFetcher
from .storage import IStorageSaver, IStorageSearcher
from .events import IEventListener, IEventBus
from .producer import IProducer, ProducerEvents

__all__ = [
    # 新的接口名称
    "IConfig",
    "ITaskConfig",
    "IGroupConfig",
    "IDownloaderConfig",
    "IConsumerConfig",
    "IDatabaseConfig",
    "IDatabase",
    "IDatabaseFactory",
    "IFetcher",
    "IStorageSaver",
    "IStorageSearcher",
    "IEventListener",
    "IEventBus",
    "IProducer",
    "ProducerEvents",
    "ITaskHandler",
    "IIncrementalTaskHandler",
    "ITaskHandlerFactory",
]
