# huey_tasks.py
from huey import SqliteHuey
from ..config import get_config
from box import Box
import logging

logger = logging.getLogger(__name__)

# 直接创建 Huey 实例
config = get_config()
huey = SqliteHuey(filename=config.huey.db_file, immediate=config.huey.immediate)


@huey.task()
def process_fetched_data(symbol: str, task_type: str, data_dict: dict):
    """
    处理从 Tushare 获取的数据

    Args:
        symbol: 股票代码
        task_type: 任务类型
        data_dict: 数据字典（从 DataFrame.to_dict() 转换而来）
    """
    try:
        # 创建任务对象，保持与原来 data_queue 中相同的结构
        task = Box({"symbol": symbol, "task_type": task_type, "data": data_dict})

        logger.info(f"Processing task: {task_type} for symbol: {symbol}")

        # TODO: 这里需要实现具体的数据处理逻辑
        # 原来的 consumer 逻辑应该移到这里
        # 例如：数据存储、数据转换等

        logger.info(f"Task completed: {task_type} for symbol: {symbol}")

    except Exception as e:
        logger.error(f"Error processing task {task_type} for symbol {symbol}: {e}")
        raise