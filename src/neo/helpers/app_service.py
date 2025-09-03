"""
应用服务 - 外观模式实现
"""

from typing import Dict, List

from .task_builder import DownloadTaskConfig
from ..services.consumer_runner import ConsumerRunner
from ..services.downloader_service import DownloaderService
from ..tasks.download_tasks import build_and_enqueue_downloads_task


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

    def build_and_submit_downloads(
        self, task_stock_mapping: Dict[str, List[str]]
    ) -> None:
        """
        构建任务类型和股票代码的映射关系，并提交增量下载任务。

        Args:
            task_stock_mapping: 任务类型到股票代码列表的映射
                               如 {'stock_basic': ['000001.SZ', '000002.SZ'], 'daily': ['000001.SZ']}
        """
        build_and_enqueue_downloads_task(task_stock_mapping)

    def build_task_stock_mapping_from_group(
        self, group_name: str, stock_codes: List[str] = None
    ) -> Dict[str, List[str]]:
        """
        从任务组名构建任务类型和股票代码的映射关系。

        这个方法将原来在 build_and_enqueue_downloads_task 中的构建逻辑提取出来，
        让调用方可以灵活地构建映射关系。

        Args:
            group_name: 在 config.toml 中定义的任务组名
            stock_codes: 可选的股票代码列表，如果提供则只处理这些股票

        Returns:
            任务类型到股票代码列表的映射
        """
        from ..app import container

        group_handler = container.group_handler()

        # 获取任务类型
        task_types = group_handler.get_task_types_for_group(group_name)
        if not task_types:
            return {}

        # 获取股票代码
        if stock_codes:
            symbols = stock_codes
        else:
            symbols = group_handler.get_symbols_for_group(group_name)

        if not symbols:
            return {}

        # 构建映射关系：每个任务类型对应所有股票代码
        return {task_type: symbols for task_type in task_types}
