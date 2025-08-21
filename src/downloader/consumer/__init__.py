"""Consumer模块 - 负责从队列消费任务结果并进行批量处理"""

from .interfaces import IBatchSaver, IDataProcessor, IDataProcessorManager
from .batch_saver import BatchSaver
from .data_processor import DataProcessor
from .data_processor_manager import DataProcessorManager

__all__ = [
    # 接口
    'IBatchSaver',
    'IDataProcessor', 
    'IDataProcessorManager',
    # 实现类
    'BatchSaver',
    'DataProcessor',
    'DataProcessorManager',
]