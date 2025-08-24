"""Huey 任务定义

定义带 @huey_task 装饰器的下载任务函数。
"""

import asyncio
import logging

import pandas as pd
from ..configs import huey
from ..task_bus.types import TaskType

# 延迟导入以避免循环导入
from ..data_processor.simple_data_processor import AsyncSimpleDataProcessor

logger = logging.getLogger(__name__)


async def _process_data_async(task_type: str, data: pd.DataFrame, symbol: str) -> bool:
    """异步处理数据的公共函数
    
    Args:
        task_type: 任务类型字符串
        data: 要处理的数据
        symbol: 股票代码
        
    Returns:
        bool: 处理是否成功
    """
    data_processor = AsyncSimpleDataProcessor.create_default()
    try:
        process_success = await data_processor.process(task_type, data)
        logger.info(f"数据处理完成: {symbol}, 成功: {process_success}")
        return process_success
    finally:
        # 确保数据处理器正确关闭，刷新所有缓冲区数据
        await data_processor.shutdown()


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
        from ..downloader.simple_downloader import get_downloader

        logger.info(f"开始执行下载任务: {symbol}")

        # 创建下载器并执行下载
        downloader = get_downloader(singleton=True)
        try:
            result = downloader.download(task_type, symbol)

            success = (
                result is not None and not result.empty if result is not None else False
            )
            logger.info(f"下载任务完成: {symbol}, 成功: {success}")

            # 🔗 链式调用：下载完成后自动触发数据处理
            if success and result is not None:
                logger.info(f"🔄 触发数据处理任务: {symbol}")
                # 触发独立的数据处理任务，利用 Huey 的任务调度
                process_result = process_data_task(task_type, symbol)
                return process_result

            return success
        finally:
            # 确保清理速率限制器资源
            downloader.cleanup()

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

        # 创建异步数据处理器并运行
        async def process_async():
            data_processor = AsyncSimpleDataProcessor.create_default()

            # 重新下载数据进行处理
            from ..downloader.simple_downloader import SimpleDownloader

            downloader = SimpleDownloader.create_default()
            try:
                result = downloader.download(task_type.name, symbol)

                success = (
                    result is not None and not result.empty
                    if result is not None
                    else False
                )
                if success and result is not None:
                    return await _process_data_async(task_type.name, result, symbol)
                else:
                    logger.warning(f"数据处理失败，无有效数据: {symbol}")
                    return False
            finally:
                # 确保清理速率限制器资源
                downloader.cleanup()

        return asyncio.run(process_async())

    except Exception as e:
        logger.error(f"数据处理任务执行失败: {symbol}, 错误: {e}")
        return False
