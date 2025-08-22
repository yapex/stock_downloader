"""任务总线接口定义

定义任务总线相关的接口规范。
"""

from typing import Protocol
from .types import TaskResult


class ITaskBus(Protocol):
    """任务总线接口

    负责任务的提交和队列管理。
    消费者的启动应该由外部系统（如命令行工具）负责。
    """

    def submit_task(self, task_result: TaskResult) -> None:
        """提交任务到队列

        Args:
            task_result: 任务执行结果
        """
        ...
