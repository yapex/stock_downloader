"""下载任务模块

包含股票数据下载相关的 Huey 任务。
"""

import logging
from datetime import datetime, time, timedelta
from typing import Optional, Dict, Tuple, List

from ..configs.huey_config import huey_fast, huey_slow
from ..task_bus.types import TaskType
from ..helpers.utils import get_next_day_str
from ..configs.app_config import get_config

logger = logging.getLogger(__name__)


class DownloadTaskManager:
    """下载任务管理器，负责构建和管理下载任务"""

    def __init__(self):
        self.config = get_config()

    def _get_task_types_and_symbols(
        self, group_name: str, stock_codes: Optional[List[str]]
    ) -> Tuple[List[str], Optional[List[str]]]:
        """获取任务类型和股票代码列表

        Args:
            group_name: 任务组名
            stock_codes: 可选的股票代码列表

        Returns:
            Tuple[任务类型列表, 股票代码列表]
        """
        from ..app import container

        group_handler = container.group_handler()
        logger.debug(f"获取到 group_handler: {group_handler}")

        # 获取任务类型列表
        task_types = group_handler.get_task_types_for_group(group_name)
        logger.debug(f"获取到 task_types: {task_types}")
        if not task_types:
            logger.warning(f"任务组 '{group_name}' 中没有找到任何任务类型，任务结束。")
            return [], None

        # 获取股票代码列表
        if stock_codes:
            symbols = stock_codes
            logger.debug(f"使用命令行指定的股票代码: {symbols}")
        else:
            symbols = group_handler.get_symbols_for_group(group_name)
            logger.debug(
                f"使用任务组 '{group_name}' 配置的股票代码: {len(symbols) if symbols else 0} 个"
            )

        return task_types, symbols

    def _query_max_dates(
        self, task_types: List[str], symbols: Optional[List[str]]
    ) -> Dict[Tuple[str, str], Optional[str]]:
        """批量查询每个任务类型下所有股票的最新日期

        Args:
            task_types: 任务类型列表
            symbols: 股票代码列表

        Returns:
            Dict[Tuple[symbol, task_type], date]: 最新日期映射
        """
        from ..database.operator import ParquetDBQueryer
        from ..database.schema_loader import SchemaLoader

        schema_loader = SchemaLoader()
        parquet_op = ParquetDBQueryer(
            schema_loader=schema_loader,
            parquet_base_path=self.config.storage.parquet_base_path,
        )

        max_dates = {}
        for task_type in task_types:
            if symbols:  # 有具体股票代码的任务类型
                logger.debug(
                    f"正在为 {task_type} 查询 {len(symbols)} 个股票的最新日期..."
                )
                task_max_dates = parquet_op.get_max_date(task_type, symbols)
                for symbol, date in task_max_dates.items():
                    max_dates[(symbol, task_type)] = date
            else:  # stock_basic 等不需要具体股票代码的任务类型
                logger.debug(f"任务类型 {task_type} 不需要具体股票代码")
                max_dates[("", task_type)] = None

        return max_dates

    def _should_skip_task(self, latest_date: Optional[str]) -> bool:
        """检查是否应该跳过当前任务

        Args:
            latest_date: 最新数据日期 (YYYYMMDD格式)

        Returns:
            bool: 是否应该跳过
        """
        if not latest_date:
            return False

        today_str = datetime.now().strftime("%Y%m%d")
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        current_time = datetime.now().time()
        market_close_time = time(18, 0)  # 下午6点

        # 如果最大日期是今天且当前时间未到下午6点，跳过该任务
        if latest_date == today_str and current_time < market_close_time:
            return True

        # 如果最大日期是昨天且当前时间未到下午6点，也跳过该任务
        if latest_date == yesterday_str and current_time < market_close_time:
            return True

        return False

    def _enqueue_download_tasks(
        self,
        task_types: List[str],
        symbols: Optional[List[str]],
        max_dates: Dict[Tuple[str, str], Optional[str]],
    ) -> int:
        """派发下载任务到快速队列

        Args:
            task_types: 任务类型列表
            symbols: 股票代码列表
            max_dates: 最新日期映射

        Returns:
            int: 派发的任务数量
        """
        default_start_date = self.config.download_tasks.default_start_date
        enqueued_count = 0

        for task_type in task_types:
            if symbols:  # 有具体股票代码的任务类型
                for symbol in symbols:
                    latest_date = max_dates.get((symbol, task_type))

                    if self._should_skip_task(latest_date):
                        current_time = datetime.now().time()
                        logger.debug(
                            f"⏭️ 跳过 {symbol} 的 {task_type} 任务：数据尚未收盘，当前时间 {current_time.strftime('%H:%M')}"
                        )
                        continue

                    start_date = (
                        get_next_day_str(latest_date)
                        if latest_date
                        else default_start_date
                    )

                    # 派发任务到快速队列
                    download_task(
                        task_type=task_type, symbol=symbol, start_date=start_date
                    )
                    enqueued_count += 1
            else:  # stock_basic 等不需要具体股票代码的任务类型
                download_task(
                    task_type=task_type, symbol="", start_date=default_start_date
                )
                enqueued_count += 1

        return enqueued_count


@huey_slow.task()
def build_and_enqueue_downloads_task(
    group_name: str, stock_codes: Optional[List[str]] = None
):
    """构建并派发增量下载任务 (慢速队列)

    这是智能增量下载的第一步。
    它会查询数据湖中已有数据的最新日期，计算出需要下载的起始日期，
    然后将具体的下载任务派发到快速队列。

    Args:
        group_name: 在 config.toml 中定义的任务组名
        stock_codes: 可选的股票代码列表，如果提供则只处理这些股票
    """
    logger.debug(f"🛠️ [HUEY_SLOW] 开始构建增量下载任务, 任务组: {group_name}")

    try:
        task_manager = DownloadTaskManager()

        # 1. 获取任务类型和股票代码
        task_types, symbols = task_manager._get_task_types_and_symbols(
            group_name, stock_codes
        )
        if not task_types:
            return

        # 2. 批量查询最新日期
        max_dates = task_manager._query_max_dates(task_types, symbols)

        # 3. 派发下载任务
        enqueued_count = task_manager._enqueue_download_tasks(
            task_types, symbols, max_dates
        )

        logger.debug(f"✅ [HUEY_SLOW] 成功派发 {enqueued_count} 个增量下载任务。")

    except Exception as e:
        logger.error(f"❌ [HUEY_SLOW] 构建下载任务失败: {e}", exc_info=True)
        raise e


@huey_fast.task(retries=2, retry_delay=60)
def download_task(task_type: TaskType, symbol: str, **kwargs):
    """下载股票数据的 Huey 任务 (快速队列)

    下载完成后，直接调用慢速队列的数据处理任务。

    Args:
        task_type: 任务类型枚举
        symbol: 股票代码
        **kwargs: 额外的下载参数，如 start_date, end_date
    """
    try:
        logger.debug(f"🚀 [HUEY_FAST] 开始执行下载任务: {symbol} ({task_type})")

        # 从中心化的 app.py 获取共享的容器实例
        from ..app import container
        from .data_processing_tasks import process_data_task

        downloader = container.downloader()
        result = downloader.download(task_type, symbol, **kwargs)

        if result is not None and not result.empty:
            logger.info(f"🚀 [HUEY_FAST] 下载完成: {symbol}, 准备提交到慢速队列...")
            # 手动调用慢速任务，并传递数据
            process_data_task(
                task_type=task_type,
                symbol=symbol,
                data_frame=result.to_dict("records"),
            )
        else:
            logger.warning(
                f"⚠️ [HUEY_FAST] 下载任务完成: {symbol}, 但返回空数据，不提交后续任务"
            )

    except Exception as e:
        logger.error(f"❌ [HUEY_FAST] 下载任务执行失败: {symbol}, 错误: {e}")
        raise e
