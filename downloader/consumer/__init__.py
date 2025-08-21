"""Consumer模块 - 负责处理和保存下载的数据"""

from .interfaces import IBatchSaver, IDataProcessor
from .batch_saver import BatchSaver
from .data_processor import DataProcessor

__all__ = [
    "IBatchSaver",
    "IDataProcessor", 
    "BatchSaver",
    "DataProcessor"
]