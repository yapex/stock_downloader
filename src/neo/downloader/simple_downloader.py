"""简单下载器实现

专注网络IO和数据获取的下载器层实现。
"""

import logging
import pandas as pd
from typing import Dict, Optional
from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate

from ..config import get_config
from .interfaces import IDownloader
from .types import DownloadTaskConfig, TaskResult, TaskType
from .fetcher_builder import FetcherBuilder
from ..database.operator import DBOperator

logger = logging.getLogger(__name__)


class SimpleDownloader(IDownloader):
    """简化的下载器实现

    专注于网络IO和数据获取，不处理业务逻辑。
    """

    def __init__(
        self,
        rate_limiters: Optional[Dict[str, Limiter]] = None,
        db_operator: Optional[DBOperator] = None,
    ):
        """初始化下载器

        Args:
            rate_limiters: 按任务类型分组的速率限制器字典，如果为None则使用默认配置
            db_operator: 数据库操作器，用于智能日期参数处理
        """
        self.config = get_config()
        self.rate_limiters = rate_limiters or {}
        self.fetcher_builder = FetcherBuilder(db_operator=db_operator)

    def _get_rate_limiter(self, task_type: TaskType) -> Limiter:
        """获取指定任务类型的速率限制器

        Args:
            task_type: 任务类型

        Returns:
            Limiter: 对应的速率限制器
        """
        task_key = str(task_type.value)  # 转换为字符串以确保可哈希
        if task_key not in self.rate_limiters:
            # 为每个任务类型创建独立的速率限制器
            self.rate_limiters[task_key] = Limiter(
                InMemoryBucket([Rate(190, Duration.MINUTE)]),
                raise_when_fail=False,
                max_delay=Duration.MINUTE * 2,
            )
        return self.rate_limiters[task_key]

    def download(self, config: DownloadTaskConfig) -> TaskResult:
        """执行下载任务

        Args:
            config: 任务配置

        Returns:
            TaskResult: 任务执行结果
        """
        logger.debug(f"开始下载任务: {config.task_type.value}, symbol: {config.symbol}")

        try:
            # 应用速率限制
            self._apply_rate_limiting(config.task_type)

            # 获取数据
            data = self._fetch_data(config)

            result = TaskResult(config=config, success=True, data=data)

            # 将成功的任务结果提交到队列进行处理
            if data is not None and not data.empty:
                self._submit_to_queue(result)

            logger.debug(
                f"下载任务成功: {config.task_type.value}, symbol: {config.symbol}, rows: {len(data) if data is not None else 0}"
            )
            return result

        except Exception as e:
            logger.warning(
                f"下载任务失败: {config.task_type.value}, symbol: {config.symbol}, error: {e}"
            )

            return TaskResult(config=config, success=False, error=e)

    def _apply_rate_limiting(self, task_type: TaskType) -> None:
        """应用速率限制

        每个任务类型（表名）有独立的速率限制器
        """
        logger.debug(f"Rate limiting check for task: {task_type.value}")
        rate_limiter = self._get_rate_limiter(task_type)
        rate_limiter.try_acquire(task_type.value, 1)

    def _fetch_data(self, config: DownloadTaskConfig) -> Optional[pd.DataFrame]:
        """获取数据

        使用 FetcherBuilder 获取真实的 Tushare 数据。
        """
        try:
            # 使用 FetcherBuilder 构建数据获取器
            fetcher = self.fetcher_builder.build_by_task(
                task_type=config.task_type, symbol=config.symbol
            )

            # 执行数据获取
            data = fetcher()

            if data is not None and not data.empty:
                logger.info(f"✅ 真实数据获取完成: {len(data)} rows")
            else:
                logger.warning("⚠️ 数据获取结果为空")
            return data

        except Exception as e:
            logger.error(f"数据获取失败: {e}")
            raise

    def _submit_to_queue(self, result: TaskResult) -> None:
        """将 TaskResult 提交到 Huey 队列进行处理"""
        from neo.task_bus.huey_task_bus import HueyTaskBus

        try:
            # 创建 HueyTaskBus 实例并提交任务
            task_bus = HueyTaskBus()
            task_bus.submit_task(result)

            logger.info(
                f"✅ TaskResult 已提交到队列: {result.config.task_type.value}, symbol: {result.config.symbol}"
            )

        except Exception as e:
            logger.error(f"提交 TaskResult 到队列失败: {e}")
            # 不抛出异常，避免影响下载流程
