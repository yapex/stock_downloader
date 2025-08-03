import logging
from abc import ABC, abstractmethod
from downloader.fetcher import TushareFetcher
from downloader.storage import ParquetStorage
import argparse


class BaseTaskHandler(ABC):
    """所有任务处理器的抽象基类。"""

    def __init__(
        self,
        task_config: dict,
        fetcher: TushareFetcher,
        storage: ParquetStorage,
        args: argparse.Namespace,
    ):
        self.task_config = task_config
        self.fetcher = fetcher
        self.storage = storage
        self.args = args
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def execute(self, **kwargs):
        """
        执行任务的主方法。
        使用 **kwargs 允许引擎传递额外的上下文信息，如 target_symbols。
        """
        raise NotImplementedError
