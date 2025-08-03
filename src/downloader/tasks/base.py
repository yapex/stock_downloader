from abc import ABC, abstractmethod
import logging

class BaseTaskHandler(ABC):
    """所有任务处理器的抽象基类。"""
    def __init__(self, task_config, fetcher, storage, args):
        self.task_config = task_config
        self.fetcher = fetcher
        self.storage = storage
        self.args = args
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def execute(self):
        """执行任务的主方法。"""
        raise NotImplementedError