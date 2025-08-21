"""数据处理器接口定义

定义数据处理相关的接口规范。
"""

from typing import Protocol
from .types import TaskResult


class IDataProcessor(Protocol):
    """数据处理器接口
    
    专注于数据清洗、转换和验证，不处理业务逻辑。
    """
    
    def process(self, task_result: TaskResult) -> bool:
        """处理任务结果
        
        Args:
            task_result: 任务执行结果
            
        Returns:
            bool: 处理是否成功
        """
        ...