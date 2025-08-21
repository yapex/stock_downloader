"""下载任务执行器

无状态的任务执行器，负责单个下载任务的执行，包含下载、重试、限流逻辑。
"""

from typing import Optional, Callable, Any
import logging
import pandas as pd
from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate

from downloader.producer.fetcher_builder import FetcherBuilder, TaskType
from downloader.producer.huey_tasks import process_fetched_data
from ..interfaces.download_task import IDownloadTask
from .types import DownloadTaskConfig, TaskResult, TaskPriority

logger = logging.getLogger(__name__)





class DownloadTask(IDownloadTask):
    """下载任务执行器
    
    无状态的任务执行器，负责单个下载任务的执行。
    """
    
    def __init__(self, rate_limiter: Optional[Limiter] = None):
        self.fetcher_builder = FetcherBuilder()
        self.rate_limiter = rate_limiter or self._create_default_rate_limiter()
    
    def _create_default_rate_limiter(self) -> Limiter:
        """创建默认的速率限制器"""
        return Limiter(
            InMemoryBucket([Rate(190, Duration.MINUTE)]),
            raise_when_fail=False,
            max_delay=Duration.MINUTE * 2,
        )
    
    def execute(self, config: DownloadTaskConfig) -> TaskResult:
        """执行下载任务
        
        Args:
            config: 任务配置
            
        Returns:
            任务执行结果
        """
        logger.debug(f"开始执行任务: {config.task_type.name}, symbol: {config.symbol}")
        
        try:
            # 应用速率限制
            self._apply_rate_limiting(config.task_type)
            
            # 获取数据
            data = self._fetch_data(config)
            
            # 处理成功的数据
            self._handle_successful_data(config, data)
            
            result = TaskResult(
                config=config,
                success=True,
                data=data
            )
            
            logger.debug(f"任务执行成功: {config.task_type.name}, symbol: {config.symbol}")
            return result
            
        except Exception as e:
            logger.warning(f"任务执行失败: {config.task_type.name}, symbol: {config.symbol}, error: {e}")
            return TaskResult(
                config=config,
                success=False,
                error=e
            )
    
    def _apply_rate_limiting(self, task_type: TaskType) -> None:
        """应用速率限制"""
        logger.debug(f"Rate limiting check for task: {task_type.name}")
        self.rate_limiter.try_acquire(task_type.name, 1)
    
    def _fetch_data(self, config: DownloadTaskConfig) -> pd.DataFrame:
        """获取数据"""
        if config.symbol == "" and config.task_type == TaskType.STOCK_BASIC:
            # STOCK_BASIC 特殊处理：不传入 symbol 参数
            fetcher = self.fetcher_builder.build_by_task(config.task_type)
            logger.debug(f"STOCK_BASIC任务类型特殊处理：不传入symbol参数")
        else:
            fetcher = self.fetcher_builder.build_by_task(config.task_type, config.symbol)
        
        return fetcher()
    
    def _handle_successful_data(self, config: DownloadTaskConfig, data: pd.DataFrame) -> None:
        """处理成功获取的数据"""
        if not data.empty:
            # 使用 Huey 任务处理数据
            process_fetched_data(config.symbol, config.task_type.name, data.to_dict())
            logger.debug(f"数据已提交处理: {config.task_type.name}, symbol: {config.symbol}, rows: {len(data)}")