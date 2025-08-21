from typing import Protocol, List
import pandas as pd
from downloader.producer.fetcher_builder import TaskType


class IBatchSaver(Protocol):
    """批量保存器接口"""
    
    def save_batch(self, task_type: TaskType, data: pd.DataFrame) -> bool:
        """批量保存数据
        
        Args:
            task_type: 任务类型
            data: 要保存的数据
            
        Returns:
            bool: 保存是否成功
        """
        ...