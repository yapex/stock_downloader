"""数据处理器模块类型定义

数据处理器模块主要使用来自任务构建器的类型，这里重新导出以便使用。
"""

# 数据处理器主要使用来自任务构建器的类型
from ..helpers.task_builder import DownloadTaskConfig

__all__ = ["DownloadTaskConfig"]
