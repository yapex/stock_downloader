"""Huey 任务定义 (兼容性模块)

此模块保持向后兼容性，所有任务已拆分到独立模块中：
- download_tasks.py: 下载相关任务
- data_processing_tasks.py: 数据处理相关任务
- metadata_sync_tasks.py: 元数据同步相关任务
"""

# 导入所有任务以保持向后兼容性
from .download_tasks import (
    build_and_enqueue_downloads_task,
    download_task,
)
from .data_processing_tasks import (
    process_data_task,
    _process_data_sync,
)
from .metadata_sync_tasks import (
    sync_metadata,
    get_sync_metadata_crontab,
)

# 重新导出所有任务函数，保持原有的导入路径可用
__all__ = [
    'build_and_enqueue_downloads_task',
    'download_task', 
    'process_data_task',
    '_process_data_sync',
    'sync_metadata',
    'get_sync_metadata_crontab',
]


# 所有任务函数已移动到独立模块中，此处仅保留导入以维持兼容性
