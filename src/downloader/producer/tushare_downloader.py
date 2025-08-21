"""TushareDownloader 任务执行器

纯粹的任务执行器，负责执行单个下载任务并处理结果。
不再依赖DownloaderManager，而是被DownloaderManager管理和调度。
"""

import logging
from typing import Optional

from downloader.task.download_task import DownloadTask
from downloader.task.types import DownloadTaskConfig, TaskResult
from downloader.producer.huey_tasks import process_fetched_data
from downloader.task.interfaces import IDownloadTask

logger = logging.getLogger(__name__)


class TushareDownloader(IDownloadTask):
    """Tushare 任务执行器

    纯粹的任务执行器，负责执行单个下载任务并处理结果。
    包装 DownloadTask 并添加 Huey 任务处理逻辑。
    """

    def __init__(self):
        """初始化任务执行器"""
        self.download_task = DownloadTask()

    def execute(self, config: DownloadTaskConfig) -> TaskResult:
        """执行下载任务并处理结果
        
        Args:
            config: 下载任务配置
            
        Returns:
            TaskResult: 任务执行结果
        """
        result = self.download_task.execute(config)

        if result.success and result.data is not None and not result.data.empty:
            # 只有成功且数据非空时才触发 Huey 任务
            try:
                process_fetched_data(
                    config.symbol, config.task_type.name, result.data.to_dict()
                )
                logger.debug(f"已触发 Huey 任务处理: {config.symbol}")
            except Exception as e:
                logger.error(f"触发 Huey 任务失败: {config.symbol}, error: {e}")
                # 不影响主任务的成功状态

        return result
