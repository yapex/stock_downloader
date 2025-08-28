"""简单下载器实现

提供基础的数据下载功能，支持速率限制和数据库操作。
"""

import logging
from typing import Optional
import pandas as pd

# DBOperator 不再使用，已移除导入
from neo.helpers.interfaces import IRateLimitManager
from neo.task_bus.types import TaskType
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.downloader.interfaces import IDownloader

logger = logging.getLogger(__name__)


class SimpleDownloader(IDownloader):
    """简化的下载器实现

    专注于网络IO和数据获取，不处理业务逻辑。
    """

    def __init__(
        self,
        fetcher_builder: FetcherBuilder,
        rate_limit_manager: IRateLimitManager,
    ):
        """初始化下载器

        Args:
            fetcher_builder: 数据获取器构建工具
            rate_limit_manager: 速率限制管理器
        """
        self.rate_limit_manager = rate_limit_manager
        self.fetcher_builder = fetcher_builder
        self.db_operator = None  # No longer used

    def download(
        self, task_type: TaskType, symbol: str, **kwargs
    ) -> Optional[pd.DataFrame]:
        """下载指定任务类型和股票代码的数据

        Args:
            task_type: 任务类型
            symbol: 股票代码
            **kwargs: 额外的下载参数，如 start_date

        Returns:
            下载的数据，或在失败时返回 None
        """
        try:
            # 应用速率限制
            self.rate_limit_manager.apply_rate_limiting(task_type)

            # 从构建器获取一个配置好的、可执行的 fetcher 函数
            fetcher = self.fetcher_builder.build_by_task(
                task_type, symbol=symbol, **kwargs
            )
            # 执行 fetcher 函数
            return fetcher()
        except Exception as e:
            logger.error(
                f"下载器执行失败 - 任务: {task_type}, 代码: {symbol}, 错误: {e}"
            )
            return None

    def _apply_rate_limiting(self, task_type: str) -> None:
        """应用速率限制

        每个任务类型（表名）有独立的速率限制器
        """
        # 获取任务类型常量并应用速率限制
        task_type_const = getattr(TaskType, task_type)
        self.rate_limit_manager.apply_rate_limiting(task_type_const)

    def _fetch_data(self, task_type: str, symbol: str) -> Optional[pd.DataFrame]:
        """获取数据

        使用 FetcherBuilder 获取真实的 Tushare 数据。
        下载任务不关心数据库状态，它会尝试下载所有可用的数据。
        日期过滤的逻辑移至数据处理任务中。

        Args:
            task_type: 任务类型
            symbol: 股票代码

        Returns:
            Optional[pd.DataFrame]: 下载的数据，失败时返回 None
        """
        try:
            # 下载任务不再检查数据库，总是尝试从一个较早的日期开始获取数据
            # 以确保能拉取到全量或最新的数据。
            # 具体的增量逻辑由处理任务负责。
            start_date = "19901218"
            logger.debug(
                f"开始为 {symbol}_{task_type} 获取数据，起始日期: {start_date}"
            )

            # 使用 FetcherBuilder 构建数据获取器
            fetcher = self.fetcher_builder.build_by_task(
                task_type=task_type, symbol=symbol, start_date=start_date
            )

            # 执行数据获取
            data = fetcher()

            if data is not None and not data.empty:
                logger.info(f"🚀 {symbol}_{task_type} 业务成功下载 {len(data)} 条数据")
            else:
                logger.debug(f"⚠️ {symbol}_{task_type} 数据获取结果为空")
            return data

        except Exception as e:
            logger.error(f"😱 {symbol}_{task_type} 数据获取失败: {e}")
            raise

    def cleanup(self):
        """清理下载器资源

        目前没有需要清理的资源，但提供接口以供AppService调用。
        """
        logger.debug("SimpleDownloader cleanup completed")
