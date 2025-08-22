"""下载器接口定义

定义下载器相关的接口规范。"""

from typing import Protocol
from .types import DownloadTaskConfig, TaskResult


class IDownloader(Protocol):
    """下载器接口

    专注于网络I/O和数据获取，不处理业务逻辑。
    """

    def download(self, task_type: str, symbol: str) -> TaskResult:
        """执行下载任务

        Args:
            task_type: 任务类型字符串
            symbol: 股票代码

        Returns:
            TaskResult: 任务执行结果
        """
        ...
