"""Mock TaskBus 实现

用于测试的 TaskBus Mock 实现。
"""

from typing import List
from neo.task_bus.interfaces import ITaskBus
from neo.task_bus.types import TaskResult


class MockTaskBus(ITaskBus):
    """Mock TaskBus 实现

    用于测试，记录所有提交的任务。
    """

    def __init__(self):
        """初始化 Mock TaskBus"""
        self.submitted_tasks: List[TaskResult] = []
        self.submit_count = 0

    def submit_task(self, task_result: TaskResult) -> None:
        """提交任务到队列

        Args:
            task_result: 任务执行结果
        """
        self.submitted_tasks.append(task_result)
        self.submit_count += 1

    def start_consumer(self) -> None:
        """启动任务消费者

        在 Mock 实现中，这是一个空操作。
        """
        pass

    def get_submitted_count(self) -> int:
        """获取提交的任务数量"""
        return self.submit_count

    def get_submitted_tasks(self) -> List[TaskResult]:
        """获取所有提交的任务"""
        return self.submitted_tasks.copy()

    def clear(self) -> None:
        """清空记录的任务"""
        self.submitted_tasks.clear()
        self.submit_count = 0
