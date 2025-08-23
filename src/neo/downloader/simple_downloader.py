"""简单下载器实现

提供基础的数据下载功能，支持速率限制和数据库操作。
"""

import logging
from typing import Optional
import pandas as pd

from neo.database.operator import DBOperator
from neo.helpers.interfaces import IRateLimitManager
from neo.task_bus.types import TaskType
from .fetcher_builder import FetcherBuilder
from .interfaces import IDownloader

logger = logging.getLogger(__name__)


class SimpleDownloader(IDownloader):
    """简化的下载器实现

    专注于网络IO和数据获取，不处理业务逻辑。
    """

    @classmethod
    def create_default(cls) -> "SimpleDownloader":
        """创建使用默认配置的下载器

        Returns:
            使用默认配置的 SimpleDownloader 实例
        """
        from neo.helpers.rate_limit_manager import get_rate_limit_manager

        return cls(
            rate_limit_manager=get_rate_limit_manager(),
            db_operator=DBOperator.create_default(),
        )

    def __init__(
        self,
        rate_limit_manager: IRateLimitManager,
        db_operator: Optional[DBOperator] = None,
    ):
        """初始化下载器

        Args:
            rate_limit_manager: 速率限制管理器
            db_operator: 数据库操作器，用于智能日期参数处理
        """
        self.rate_limit_manager = rate_limit_manager
        self.fetcher_builder = FetcherBuilder(db_operator=db_operator)

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
            data = self._fetch_data(task_type, symbol)

            logger.debug(
                f"下载任务成功: {task_type}, symbol: {symbol}, rows: {len(data) if data is not None else 0}"
            )
            return data

        except Exception as e:
            logger.warning(f"下载任务失败: {task_type}, symbol: {symbol}, error: {e}")
            return None

    def _apply_rate_limiting(self, task_type: str) -> None:
        """应用速率限制

        每个任务类型（表名）有独立的速率限制器
        """
        # 转换为枚举并应用速率限制
        task_type_enum = TaskType[task_type]
        self.rate_limit_manager.apply_rate_limiting(task_type_enum)

    def _fetch_data(self, task_type: str, symbol: str) -> Optional[pd.DataFrame]:
        """获取数据

        使用 FetcherBuilder 获取真实的 Tushare 数据。
        """
        try:
            # 转换为枚举以构建数据获取器
            task_type_enum = TaskType[task_type]
            # 使用 FetcherBuilder 构建数据获取器
            fetcher = self.fetcher_builder.build_by_task(
                task_type=task_type_enum, symbol=symbol
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
