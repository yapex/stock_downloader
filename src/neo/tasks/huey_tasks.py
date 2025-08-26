"""Huey 任务定义

定义带 @huey_task 装饰器的下载任务函数。
使用中心化的容器实例来获取依赖服务。
"""

import asyncio
import logging

import pandas as pd
from ..configs.huey_config import huey_fast, huey_slow
from ..task_bus.types import TaskType

logger = logging.getLogger(__name__)


def _process_data_sync(task_type: str, data: pd.DataFrame) -> bool:
    """异步处理数据的公共函数

    Args:
        task_type: 任务类型字符串
        data: 要处理的数据

    Returns:
        bool: 处理是否成功
    """
    from ..app import container

    data_processor = container.data_processor()
    try:
        process_success = data_processor.process(task_type, data)
        logger.debug(f"[HUEY] {task_type} 数据处理器返回结果: {process_success}")
        return process_success
    finally:
        # 确保数据处理器正确关闭，刷新所有缓冲区数据
        data_processor.shutdown()


@huey_fast.task()
def download_task(task_type: TaskType, symbol: str):
    """下载股票数据的 Huey 任务 (快速队列)

    下载完成后，直接调用慢速队列的数据处理任务。

    Args:
        task_type: 任务类型枚举
        symbol: 股票代码
    """
    try:
        logger.debug(f"🚀 [HUEY_FAST] 开始执行下载任务: {symbol} ({task_type})")

        # 从中心化的 app.py 获取共享的容器实例
        from ..app import container

        downloader = container.downloader()

        result = downloader.download(task_type, symbol)

        if result is not None and not result.empty:
            logger.debug(f"🚀 [HUEY_FAST] 下载完成: {symbol}, 准备提交到慢速队列...")
            # 手动调用慢速任务，并传递数据
            process_data_task(
                task_type=task_type,
                symbol=symbol,
                data_frame=result.to_dict("records"),
            )
        else:
            logger.warning(
                f"⚠️ [HUEY_FAST] 下载任务完成: {symbol}, 但返回空数据，不提交后续任务"
            )

    except Exception as e:
        logger.error(f"❌ [HUEY_FAST] 下载任务执行失败: {symbol}, 错误: {e}")
        raise e


@huey_slow.task()
def process_data_task(task_type: str, symbol: str, data_frame: list) -> bool:
    """数据处理任务 (慢速队列)

    Args:
        task_type: 任务类型字符串
        symbol: 股票代码
        data_frame: DataFrame 数据 (字典列表形式)

    Returns:
        bool: 处理是否成功
    """
    try:
        # 创建异步数据处理器并运行
        def process_sync():
            try:
                # 将字典列表转换为 DataFrame
                if data_frame and isinstance(data_frame, list) and len(data_frame) > 0:
                    df_data = pd.DataFrame(data_frame)
                    logger.debug(
                        f"🐌 [HUEY_SLOW] 开始异步保存数据: {symbol}_{task_type}, 数据行数: {len(df_data)}"
                    )
                    return _process_data_sync(task_type, df_data)
                else:
                    logger.warning(
                        f"⚠️ [HUEY_SLOW] 数据保存失败，无有效数据: {symbol}_{task_type}, 数据为空或None"
                    )
                    return False
            except Exception as e:
                raise e

        result = process_sync()
        logger.info(f"🏆 [HUEY_SLOW] 最终结果: {symbol}_{task_type}, 成功: {result}")
        return result

    except Exception as e:
        logger.error(f"❌ [HUEY_SLOW] 数据处理任务执行失败: {symbol}, 错误: {e}")
        raise e
