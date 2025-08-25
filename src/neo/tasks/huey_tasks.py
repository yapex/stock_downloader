"""Huey 任务定义

定义带 @huey_task 装饰器的下载任务函数。
使用中心化的容器实例来获取依赖服务。
"""

import asyncio
import logging

import pandas as pd
from ..configs import huey
from ..task_bus.types import TaskType

logger = logging.getLogger(__name__)


async def _process_data_async(task_type: str, data: pd.DataFrame) -> bool:
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
        process_success = await data_processor.process(task_type, data)
        logger.debug(f"[HUEY] {task_type} 数据处理器返回结果: {process_success}")
        return process_success
    finally:
        # 确保数据处理器正确关闭，刷新所有缓冲区数据
        await data_processor.shutdown()


@huey.task()
def download_task(task_type: TaskType, symbol: str) -> dict:
    """下载股票数据的 Huey 任务

    Args:
        task_type: 任务类型枚举
        symbol: 股票代码

    Returns:
        dict: 包含任务参数和下载数据的字典，键名与 process_data_task 参数匹配
    """
    try:
        logger.debug(f"🚀 [HUEY] 开始执行下载任务: {symbol} ({task_type})")

        # 从中心化的 app.py 获取共享的容器实例
        from ..app import container

        downloader = container.downloader()

        result = downloader.download(task_type, symbol)

        if result is not None and not result.empty:
            # 返回与 process_data_task 参数名匹配的字典
            return {
                "task_type": task_type,  # task_type 已经是字符串
                "symbol": symbol,
                "data_frame": result.to_dict(
                    "records"
                ),  # 将 DataFrame 转换为可序列化的字典列表
            }
        else:
            logger.warning(f"⚠️ [HUEY] 下载任务完成: {symbol}, 成功: False, 返回空数据")
            return {
                "task_type": task_type,
                "symbol": symbol,
                "data_frame": [],  # 空数据
            }
    except Exception as e:
        logger.error(f"❌ [HUEY] 下载任务执行失败: {symbol}, 错误: {e}")
        raise e


@huey.task()
def process_data_task(task_type: str, symbol: str, data_frame: list) -> bool:
    """数据处理任务

    参数名与 download_task 返回的字典键名完全匹配

    Args:
        task_type: 任务类型字符串（来自 download_task 返回字典的 'task_type' 键）
        symbol: 股票代码（来自 download_task 返回字典的 'symbol' 键）
        data_frame: DataFrame 数据（来自 download_task 返回字典的 'data_frame' 键）

    Returns:
        bool: 处理是否成功
    """
    try:
        # 创建异步数据处理器并运行
        async def process_async():
            try:
                # 将字典列表转换为 DataFrame
                if data_frame and isinstance(data_frame, list) and len(data_frame) > 0:
                    df_data = pd.DataFrame(data_frame)
                    logger.debug(
                        f"[HUEY] 开始异步保存数据: {symbol}_{task_type}, 数据行数: {len(df_data)}"
                    )
                    return await _process_data_async(task_type, df_data)
                else:
                    logger.warning(
                        f"⚠️ [HUEY] 数据保存失败，无有效数据: {symbol}_{task_type}, 数据为空或None"
                    )
                    return False
            except Exception as e:
                raise e

        result = asyncio.run(process_async())
        logger.info(f"🏁 [HUEY] 最终结果: {symbol}_{task_type}, 成功: {result}")
        return result

    except Exception as e:
        logger.error(f"❌ [HUEY] 数据处理任务执行失败: {symbol}, 错误: {e}")
        raise e
