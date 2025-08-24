"""Huey 任务定义

定义带 @huey_task 装饰器的下载任务函数。
使用中心化的容器实例来获取依赖服务。
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
    from ..app import container

    data_processor = container.data_processor()
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
        logger.info(f"开始执行下载任务: {symbol}")

        # 从中心化的 app.py 获取共享的容器实例
        from ..app import container

        downloader = container.downloader()

        # 使用下载器执行下载
        try:
            result = downloader.download(task_type, symbol)

            success = (
                result is not None and not result.empty if result is not None else False
            )
            logger.info(f"下载任务完成: {symbol}, 成功: {success}")

            # 🔗 链式调用：下载完成后自动触发数据处理
            if success and result is not None:
                logger.info(f"🔄 触发数据处理任务: {symbol}")
                # 触发独立的数据处理任务，传递下载的数据
                process_data_task(task_type, symbol, result)  # 传递下载的数据
                # 返回下载的成功状态，而不是数据处理结果

            return success
        finally:
            # 确保清理速率限制器资源
            downloader.cleanup()

    except Exception as e:
        logger.error(f"下载任务执行失败: {symbol}, 错误: {e}")
        return False


@huey.task()
def process_data_task(task_type: TaskType, symbol: str, data: pd.DataFrame) -> bool:
    """数据处理任务

    Args:
        task_type: 任务类型枚举
        symbol: 股票代码
        data: 要处理的数据

    Returns:
        bool: 处理是否成功
    """
    try:
        logger.info(f"开始处理数据: {symbol}")

        # 创建异步数据处理器并运行
        async def process_async():
            try:
                # 直接使用传入的数据，不再重复下载
                success = (
                    data is not None and not data.empty if data is not None else False
                )
                if success and data is not None:
                    return await _process_data_async(task_type, data, symbol)
                else:
                    logger.warning(f"数据处理失败，无有效数据: {symbol}")
                    return False
            except Exception as e:
                logger.error(f"数据处理过程中发生错误: {symbol}, 错误: {e}")
                return False

        return asyncio.run(process_async())

    except Exception as e:
        logger.error(f"数据处理任务执行失败: {symbol}, 错误: {e}")
        return False
