"""任务总线接口定义

定义任务总线相关的接口规范。
"""

from typing import Protocol
from .types import TaskResult


class ITaskBus(Protocol):
    """任务总线接口

    负责任务的提交和队列管理。
    """

    def submit_task(self, task_result: TaskResult) -> None:
        """提交任务到队列

        Args:
            task_result: 任务执行结果
        """
        ...

    def start_consumer(self) -> None:
        """启动任务消费者

        启动消费者进程来处理队列中的任务。
        在测试环境中，可能会使用 immediate 模式立即执行任务。
        """
        ...
