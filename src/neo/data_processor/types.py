"""数据处理器相关类型定义

数据处理器模块主要处理 TaskResult，相关类型定义在 task_bus.types 中。
"""

# 数据处理器主要处理来自任务总线的 TaskResult
# 相关类型定义在 task_bus.types 模块中
from ..task_bus.types import TaskResult, DownloadTaskConfig, TaskType, TaskPriority

__all__ = [
    'TaskResult',
    'DownloadTaskConfig',
    'TaskType', 
    'TaskPriority'
]