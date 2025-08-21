"""Neo 任务总线子模块

提供任务队列管理和调度相关的接口、类型定义和实现类。
"""

from .interfaces import ITaskBus
from .huey_task_bus import HueyTaskBus, get_huey
from .types import TaskResult, DownloadTaskConfig, TaskType, TaskPriority

__all__ = [
    "ITaskBus",
    "HueyTaskBus",
    "get_huey",
    "TaskResult",
    "DownloadTaskConfig",
    "TaskType",
    "TaskPriority",
]

# 导出Huey实例供命令行使用
huey = get_huey()