"""下载任务接口定义"""

from typing import Protocol
from downloader.task.types import DownloadTaskConfig, TaskResult


class IDownloadTask(Protocol):
    """下载任务执行器接口"""
    
    def execute(self, config: DownloadTaskConfig) -> TaskResult:
        """执行下载任务
        
        Args:
            config: 任务配置
            
        Returns:
            任务执行结果
        """
        ...