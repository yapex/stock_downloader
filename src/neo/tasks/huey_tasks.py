"""Huey 任务定义

定义带 @huey_task 装饰器的下载任务函数。
"""

import logging
import pandas as pd

from ..huey_config import huey
from ..task_bus.types import DownloadTaskConfig, TaskType, TaskPriority
# 延迟导入 SimpleDownloader 以避免循环导入
from ..data_processor.simple_data_processor import SimpleDataProcessor

logger = logging.getLogger(__name__)


@huey.task()
def download_task(task_type: TaskType, symbol: str) -> bool:
    """下载股票数据的 Huey 任务

    Args:
        task_type: 任务类型枚举
        symbol: 股票代码

    Returns:
        bool: 下载是否成功
    """
    try:
        # 延迟导入以避免循环导入
        from ..downloader.simple_downloader import SimpleDownloader
        
        logger.info(f"开始执行下载任务: {symbol}")

        # 创建下载器并执行下载
        downloader = SimpleDownloader()
        result = downloader.download(task_type, symbol)

        logger.info(f"下载任务完成: {symbol}, 成功: {result.success}")

        # 🔗 链式调用：下载完成后自动触发数据处理
        if result.success and result.data is not None:
            logger.info(f"🔄 触发数据处理任务: {symbol}")
            process_data_task(task_type.name, result.data)

        return result.success

    except Exception as e:
        logger.error(f"下载任务执行失败: {symbol}, 错误: {e}")
        return False


@huey.task()
def process_data_task(task_type: str, data: pd.DataFrame) -> bool:
    """数据处理任务

    直接接收任务类型和数据，调用 SimpleDataProcessor 进行处理。

    Args:
        task_type: 任务类型字符串
        data: 要处理的数据 DataFrame

    Returns:
        bool: 处理是否成功
    """
    try:
        logger.info(f"开始处理数据: {task_type}")

        # 直接调用数据处理器，传入简化的参数
        processor = SimpleDataProcessor()
        success = processor.process(task_type, data)

        if success:
            logger.info(f"数据处理成功: {task_type}")
        else:
            logger.warning(f"数据处理失败: {task_type}")

        return success

    except Exception as e:
        logger.error(f"数据处理任务执行失败: {e}")
        return False
