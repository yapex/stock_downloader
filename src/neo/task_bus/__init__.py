"""Neo 任务总线子模块

提供任务队列管理和调度相关的接口、类型定义和实现类。
"""

from .interfaces import ITaskBus
from .huey_task_bus import HueyTaskBus
from .types import TaskResult, DownloadTaskConfig, TaskType, TaskPriority

__all__ = [
    "ITaskBus",
    "HueyTaskBus",
    "TaskResult",
    "DownloadTaskConfig",
    "TaskType",
    "TaskPriority",
]

# 注意：不再导出全局 Huey 实例，请使用 HueyTaskBus 类
