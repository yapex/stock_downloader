"""Huey 任务定义

定义带 @huey_task 装饰器的下载任务函数。
"""

import logging

from ..configs import huey
from ..task_bus.types import TaskType

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
        downloader = SimpleDownloader.create_default()
        result = downloader.download(task_type, symbol)

        logger.info(f"下载任务完成: {symbol}, 成功: {result.success}")

        # 🔗 链式调用：下载完成后自动触发数据处理
        if result.success and result.data is not None:
            logger.info(f"🔄 触发数据处理任务: {symbol}")
            # 直接处理数据，避免重复下载
            data_processor = SimpleDataProcessor.create_default()
            success = data_processor.process(task_type.name, result.data)
            logger.info(f"数据处理完成: {symbol}, 成功: {success}")
            return success

        return result.success

    except Exception as e:
        logger.error(f"下载任务执行失败: {symbol}, 错误: {e}")
        return False


@huey.task()
def process_data_task(task_type: TaskType, symbol: str) -> bool:
    """数据处理任务

    Args:
        task_type: 任务类型枚举
        symbol: 股票代码

    Returns:
        bool: 处理是否成功
    """
    try:
        logger.info(f"开始处理数据: {symbol}")

        # 创建数据处理器

        data_processor = SimpleDataProcessor.create_default()

        # 重新下载数据进行处理
        from ..downloader.simple_downloader import SimpleDownloader

        downloader = SimpleDownloader.create_default()
        result = downloader.download(task_type, symbol)

        if result.success and result.data is not None:
            success = data_processor.process(task_type.name, result.data)
            logger.info(f"数据处理完成: {symbol}, 成功: {success}")
            return success
        else:
            logger.warning(f"数据处理失败，无有效数据: {symbol}")
            return False

    except Exception as e:
        logger.error(f"数据处理任务执行失败: {symbol}, 错误: {e}")
        return False
