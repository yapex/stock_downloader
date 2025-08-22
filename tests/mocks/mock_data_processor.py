"""Mock 数据处理器用于测试

提供一个简单的数据处理器实现，用于测试任务处理流程。
"""

from typing import List
from neo.data_processor.interfaces import IDataProcessor
from neo.task_bus.types import TaskResult


class MockDataProcessor(IDataProcessor):
    """Mock 数据处理器

    记录所有处理的任务结果，用于测试验证。
    """

    def __init__(self):
        self.processed_tasks: List[TaskResult] = []
        self.process_success = True  # 控制处理是否成功
        self.process_call_count = 0

    def process(self, task_result: TaskResult) -> bool:
        """处理任务结果

        Args:
            task_result: 任务执行结果

        Returns:
            bool: 处理是否成功
        """
        self.process_call_count += 1
        self.processed_tasks.append(task_result)

        # 模拟处理逻辑
        if task_result.success and task_result.data is not None:
            return self.process_success
        else:
            return False

    def get_processed_count(self) -> int:
        """获取已处理的任务数量"""
        return len(self.processed_tasks)

    def get_processed_tasks(self) -> List[TaskResult]:
        """获取所有已处理的任务结果"""
        return self.processed_tasks.copy()

    def get_processed_symbols(self) -> List[str]:
        """获取所有已处理的股票代码"""
        return [
            task.config.symbol for task in self.processed_tasks if task.config.symbol
        ]

    def clear(self):
        """清空处理记录"""
        self.processed_tasks.clear()
        self.process_call_count = 0

    def set_process_success(self, success: bool):
        """设置处理是否成功"""
        self.process_success = success
