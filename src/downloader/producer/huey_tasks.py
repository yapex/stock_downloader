# huey_tasks.py
"""
Huey任务定义模块

定义用于producer/consumer架构的huey任务。
Producer将TaskResult放入队列，Consumer从队列中获取并处理。
"""

from huey import SqliteHuey
from ..config import get_config
from ..task.types import TaskResult, DownloadTaskConfig
from ..consumer.data_processor import DataProcessor
from ..consumer.batch_saver import BatchSaver
from ..database.db_oprator import DBOperator
import logging
import pickle
from typing import Dict, Any

logger = logging.getLogger(__name__)

# 创建 Huey 实例
config = get_config()
huey = SqliteHuey(filename=config.huey.db_file, immediate=config.huey.immediate)

# 全局的数据处理器实例（懒加载）
_data_processor = None


def get_data_processor() -> DataProcessor:
    """获取数据处理器实例（单例模式）"""
    global _data_processor
    if _data_processor is None:
        db_operator = DBOperator()
        batch_saver = BatchSaver(db_operator)
        _data_processor = DataProcessor(batch_saver, batch_size=config.consumer.batch_size)
    return _data_processor


@huey.task()
def process_task_result(task_result_data: dict) -> None:
    """处理TaskResult的Huey任务
    
    Args:
        task_result_data: 序列化的TaskResult数据
    """
    try:
        # 反序列化TaskResult
        config_data = task_result_data['config']
        
        # 处理TaskType - 如果是字符串则转换为TaskType枚举
        task_type = config_data['task_type']
        if isinstance(task_type, str):
            from downloader.task.types import TaskType
            task_type = TaskType[task_type]
        
        config = DownloadTaskConfig(
            symbol=config_data['symbol'],
            task_type=task_type,
            priority=config_data['priority'],
            max_retries=config_data['max_retries']
        )
        
        # 处理数据 - 如果是字典则转换为DataFrame
        data = task_result_data['data']
        if data is not None and isinstance(data, dict):
            import pandas as pd
            data = pd.DataFrame(data)
        
        task_result = TaskResult(
            config=config,
            success=task_result_data['success'],
            data=data,
            error=task_result_data.get('error'),
            retry_count=task_result_data.get('retry_count', 0)
        )
        
        # 使用DataProcessor处理TaskResult
        data_processor = get_data_processor()
        data_processor.process_task_result(task_result)
        
        logger.info(f"TaskResult处理完成: {config.task_type.name}, symbol: {config.symbol}")
        
    except Exception as e:
        logger.error(f"处理TaskResult时出错: {e}")
        raise