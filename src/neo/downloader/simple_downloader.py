"""简单下载器实现

提供基础的数据下载功能，支持速率限制和数据库操作。
"""

import logging
from typing import Optional
import pandas as pd

from neo.database.operator import DBOperator
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
        db_operator: Optional[DBOperator] = None,
    ):
        """初始化下载器

        Args:
            rate_limit_manager: 速率限制管理器
            db_operator: 数据库操作器，用于智能日期参数处理
        """
        self.rate_limit_manager = rate_limit_manager
        self.fetcher_builder = fetcher_builder
        self.db_operator = db_operator

    def download(self, task_type: str, symbol: str) -> Optional[pd.DataFrame]:
        """执行下载任务

        Args:
            task_type: 任务类型字符串
            symbol: 股票代码

        Returns:
            Optional[pd.DataFrame]: 下载的数据，失败时返回 None
        """
        try:
            # 应用速率限制 - 直接使用字符串
            self._apply_rate_limiting(task_type)

            # 获取数据
            return self._fetch_data(task_type, symbol)

        except Exception as e:
            logger.error(f"❌ 下载任务失败: {symbol}_{task_type}, error: {e}")
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
        1. 先从数据库中获取最新日期
        2. 如果没有最新日期，就用默认的 19901218
        3. 使用 FetcherBuilder 构建数据获取器
        4. 执行数据获取

        Args:
            task_type: 任务类型
            symbol: 股票代码

        Returns:
            Optional[pd.DataFrame]: 下载的数据，失败时返回 None
        """
        try:
            # 获取数据库中的最新日期，如果没有就用默认的
            if self.db_operator is not None:
                last_date = self.db_operator.get_max_date(task_type)

                # 将 last_date 转成日期后加一天
                last_date = (pd.Timestamp(last_date) + pd.Timedelta(days=1)).strftime(
                    "%Y%m%d"
                )
            # 打印最新日期
            logger.debug(f"数据库 {symbol}_{task_type} 中最新日期: {last_date}")
            # 使用 FetcherBuilder 构建数据获取器
            fetcher = self.fetcher_builder.build_by_task(
                task_type=task_type, symbol=symbol, start_date=last_date
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
