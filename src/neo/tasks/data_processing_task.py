"""数据处理任务

从下载任务结果中获取数据，调用 SimpleDataProcessor 保存到 DuckDB。
"""

import logging
from typing import Dict, Any

from ..huey_config import huey
from ..data_processor.simple_data_processor import SimpleDataProcessor
from ..task_bus.types import TaskResult
from ..database.db_operator import DBOperator
from ..config import get_config

logger = logging.getLogger(__name__)


@huey.task(retries=2, retry_delay=5)
def process_data_task(task_result_dict: Dict[str, Any]) -> Dict[str, Any]:
    """处理下载任务结果的 Huey 任务
    
    Args:
        task_result_dict: 任务结果字典
    
    Returns:
        Dict[str, Any]: 处理结果字典
    """
    try:
        logger.info(f"开始处理数据: {task_result_dict.get('config', {}).get('symbol', 'unknown')}")
        
        # 重构 TaskResult 对象
        task_result = TaskResult.from_dict(task_result_dict)
        
        # 创建数据处理器
        config = get_config()
        db_operator = DBOperator(config.database.duckdb_path)
        data_processor = SimpleDataProcessor(db_operator)
        
        # 处理数据
        success = data_processor.process(task_result)
        
        logger.info(f"数据处理完成: {task_result_dict.get('config', {}).get('symbol', 'unknown')}, 成功: {success}")
        
        return {
            "symbol": task_result_dict.get('config', {}).get('symbol', 'unknown'),
            "task_type": task_result_dict.get('config', {}).get('task_type', 'unknown'),
            "success": success,
            "error": None,
        }
        
    except Exception as e:
        logger.error(f"数据处理失败: {task_result_dict.get('config', {}).get('symbol', 'unknown')}, 错误: {e}")
        return {
            "symbol": task_result_dict.get('config', {}).get('symbol', 'unknown'),
            "task_type": task_result_dict.get('config', {}).get('task_type', 'unknown'),
            "success": False,
            "error": str(e),
        }