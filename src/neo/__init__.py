"""Neo包：基于四层架构的股票数据下载系统

四层架构：
- downloader: 下载器层，专注网络IO和数据获取
- task_bus: 任务总线层，基于Huey的任务队列管理
- data_processor: 数据处理层，数据清洗、转换和验证
- database: 数据库层，数据持久化
"""

# 导入各层的主要接口和实现
from .downloader import IDownloader, SimpleDownloader
from .task_bus import ITaskBus, HueyTaskBus, get_huey
from .data_processor import IDataProcessor, SimpleDataProcessor
from .database import (
    IDBOperator,
    ISchemaLoader,
    ISchemaTableCreator,
    IBatchSaver,
    TableSchema,
    TableName,
    DatabaseConnectionManager,
    DBOperator,
    SchemaLoader,
    SchemaTableCreator,
    get_conn,
    get_memory_conn,
)

# 导入核心类型和配置
from .task_bus.types import TaskType, TaskPriority, DownloadTaskConfig, TaskResult
from .config import get_config

__version__ = "0.1.0"

__all__ = [
    # Downloader层
    "IDownloader",
    "SimpleDownloader",
    # Task Bus层
    "ITaskBus",
    "HueyTaskBus",
    "get_huey",
    # Data Processor层
    "IDataProcessor",
    "SimpleDataProcessor",
    # Database层
    "IDBOperator",
    "ISchemaLoader",
    "ISchemaTableCreator",
    "IBatchSaver",
    "TableSchema",
    "TableName",
    "DatabaseConnectionManager",
    "DBOperator",
    "SchemaLoader",
    "SchemaTableCreator",
    "get_conn",
    "get_memory_conn",
    # 核心类型和配置
    "TaskType",
    "TaskPriority",
    "DownloadTaskConfig",
    "TaskResult",
    "get_config",
]
