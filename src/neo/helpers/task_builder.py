"""任务构建器

负责根据参数构建下载任务列表。
"""

from typing import List, Protocol
from neo.task_bus.types import DownloadTaskConfig, TaskType, TaskPriority


class ITaskBuilder(Protocol):
    """任务构建器接口"""

    def build_tasks(
        self, symbols: List[str], task_types: List[TaskType], priority: TaskPriority
    ) -> List[DownloadTaskConfig]:
        """构建任务列表

        Args:
            symbols: 股票代码列表
            task_types: 任务类型列表
            priority: 任务优先级

        Returns:
            构建的任务列表
        """
        ...


class TaskBuilder:
    """任务构建器实现"""

    @classmethod
    def create_default(cls) -> "TaskBuilder":
        """创建默认的 TaskBuilder 实例

        Returns:
            TaskBuilder: 默认的任务构建器实例
        """
        return cls()

    def build_tasks(
        self, symbols: List[str], task_types: List[TaskType], priority: TaskPriority
    ) -> List[DownloadTaskConfig]:
        """构建任务列表

        Args:
            symbols: 股票代码列表，如果为空则表示不需要具体股票代码的任务（如stock_basic）
            task_types: 任务类型列表
            priority: 任务优先级

        Returns:
            构建的任务列表
        """
        tasks = []

        # 如果 task_types 为空，返回空列表
        if not task_types:
            return tasks

        if symbols:
            # 有股票代码的情况
            for symbol in symbols:
                for task_type in task_types:
                    tasks.append(
                        DownloadTaskConfig(
                            symbol=symbol, task_type=task_type, priority=priority
                        )
                    )
        else:
            # stock_basic组的情况，不需要指定股票代码
            for task_type in task_types:
                tasks.append(
                    DownloadTaskConfig(
                        symbol="",  # stock_basic任务不需要具体的股票代码
                        task_type=task_type,
                        priority=priority,
                    )
                )

        return tasks
