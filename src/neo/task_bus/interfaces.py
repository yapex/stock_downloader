"""任务总线接口定义

定义任务总线相关的接口规范。
"""

from typing import Protocol
from .types import TaskResult


class ITaskBus(Protocol):
    """任务总线接口
    
    负责任务队列的管理和调度。
    """
    
    def submit_task(self, task_result: TaskResult) -> None:
        """提交任务到队列
        
        Args:
            task_result: 任务执行结果
        """
        ...
    
    def start_consumer(self) -> None:
        """启动消费者"""
        ...