"""下载器模块类型定义

下载器模块主要使用来自任务总线的类型，这里重新导出以便使用。
"""

# 下载器主要使用来自任务总线的类型
from ..task_bus.types import DownloadTaskConfig, TaskType

__all__ = ["DownloadTaskConfig", "TaskType"]
