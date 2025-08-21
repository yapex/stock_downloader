import logging
import pandas as pd
from typing import Optional

from downloader.database.db_oprator import DBOperator
from downloader.task.types import TaskType
from .interfaces import IBatchSaver


logger = logging.getLogger(__name__)


class BatchSaver:
    """批量保存器实现"""
    
    def __init__(self, db_operator: DBOperator):
        """初始化批量保存器
        
        Args:
            db_operator: 数据库操作器
        """
        self.db_operator = db_operator
    
    def save_batch(self, task_type: TaskType, data: pd.DataFrame) -> bool:
        """批量保存数据
        
        Args:
            task_type: 任务类型
            data: 要保存的数据
            
        Returns:
            bool: 保存是否成功
        """
        if data.empty:
            logger.warning(f"数据为空，跳过保存任务类型: {task_type.name}")
            return True
            
        try:
            # 将 TaskType 枚举名称转换为小写作为表键名
            table_key = task_type.name.lower()
            
            logger.info(f"开始保存 {len(data)} 条 {task_type.name} 数据")
            self.db_operator.upsert(table_key, data)
            logger.info(f"成功保存 {len(data)} 条 {task_type.name} 数据")
            return True
            
        except Exception as e:
            logger.error(f"保存 {task_type.name} 数据失败: {e}")
            return False