"""Huey 任务定义模块

定义所有的 Huey 任务函数。
"""

import logging
from typing import Dict, Any, Optional

from .huey_config import huey
from ..data_processor.interfaces import IDataProcessor

logger = logging.getLogger(__name__)

# 全局数据处理器实例，由 HueyTaskBus 设置
_data_processor: Optional[IDataProcessor] = None


def set_data_processor(processor: IDataProcessor) -> None:
    """设置全局数据处理器实例

    Args:
        processor: 数据处理器实例
    """
    global _data_processor
    _data_processor = processor


def get_data_processor() -> IDataProcessor:
    """获取数据处理器实例

    Returns:
        IDataProcessor: 数据处理器实例
    """
    if _data_processor is not None:
        return _data_processor

    # 如果没有设置，则创建默认实例
    from ..data_processor.simple_data_processor import SimpleDataProcessor

    return SimpleDataProcessor()


@huey.task(retries=2, retry_delay=5)
def process_task_result(task_result_data: Dict[str, Any]) -> None:
    """处理任务结果的 Huey 任务

    Args:
        task_result_data: 序列化的TaskResult数据
    """
    try:
        # 延迟导入避免循环导入
        from .types import DownloadTaskConfig, TaskType, TaskPriority, TaskResult
        import pandas as pd

        # 获取数据处理器实例
        processor = get_data_processor()

        # 反序列化TaskResult
        config_data = task_result_data["config"]

        # 处理TaskType
        task_type = TaskType(config_data["task_type"])

        # 处理TaskPriority
        priority = TaskPriority(config_data["priority"])

        config = DownloadTaskConfig(
            symbol=config_data["symbol"],
            task_type=task_type,
            priority=priority,
            max_retries=config_data["max_retries"],
        )

        # 处理数据
        data = task_result_data["data"]
        if data is not None:
            # 数据已经是 'records' 格式，直接转换为 DataFrame
            data = pd.DataFrame.from_records(data)

        # 处理错误
        error = None
        if task_result_data["error"]:
            error = Exception(task_result_data["error"])

        task_result = TaskResult(
            config=config,
            success=task_result_data["success"],
            data=data,
            error=error,
            retry_count=task_result_data.get("retry_count", 0),
        )

        logger.info(f"开始处理任务结果: {task_result.config.symbol}")

        # 处理任务结果
        processor.process(task_result)

        logger.info(f"任务结果处理完成: {task_result.config.symbol}")

    except Exception as e:
        logger.error(f"处理TaskResult时出错: {e}")
        raise
