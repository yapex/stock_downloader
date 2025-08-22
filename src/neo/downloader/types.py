"""下载器相关类型定义

下载器模块使用的核心类型定义已移动到 task_bus.types 中，
因为这些是 producer/consumer 共享的类型。
"""

# 导入共享的核心类型
from ..task_bus.types import TaskResult, DownloadTaskConfig, TaskType, TaskPriority

__all__ = ["TaskResult", "DownloadTaskConfig", "TaskType", "TaskPriority"]
