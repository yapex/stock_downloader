"""
应用服务 - 外观模式实现
"""

from typing import List

from ..task_bus.types import DownloadTaskConfig
from ..services.consumer_runner import ConsumerRunner
from ..services.downloader_service import DownloaderService


class AppService:
    """
    应用服务外观 (Facade)

    作为应用功能的高级入口，将客户端代码与内部的多个子服务解耦。
    它本身不包含复杂的业务逻辑，而是将请求委托给相应的服务处理。
    """

    def __init__(
        self,
        consumer_runner: ConsumerRunner,
        downloader_service: DownloaderService,
    ):
        self.consumer_runner = consumer_runner
        self.downloader_service = downloader_service

    def run_data_processor(self, queue_name: str) -> None:
        """
        运行指定队列的数据处理器消费者。

        Args:
            queue_name: 要运行的队列名称 ('fast' or 'slow')
        """
        self.consumer_runner.run(queue_name)

    def run_downloader(
        self, tasks: List[DownloadTaskConfig], dry_run: bool = False
    ) -> None:
        """
        运行下载器，提交下载任务。

        Args:
            tasks: 下载任务列表
            dry_run: 是否为试运行模式
        """
        self.downloader_service.run(tasks, dry_run)
