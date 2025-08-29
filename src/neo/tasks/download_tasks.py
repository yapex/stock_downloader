"""下载任务模块

包含股票数据下载相关的 Huey 任务。
"""

import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from ..configs.app_config import get_config
from ..configs.huey_config import huey_fast, huey_slow
from ..helpers.utils import get_next_day_str
from ..task_bus.types import TaskType

if TYPE_CHECKING:
    from ..database.operator import ParquetDBQueryer

logger = logging.getLogger(__name__)


class DownloadTaskManager:
    """下载任务管理器，负责构建和管理下载任务 (无状态)"""

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
        self,
        db_queryer: "ParquetDBQueryer",
        task_types: List[str],
        symbols: Optional[List[str]],
        task_date_col_map: Dict[str, bool],
    ) -> Dict[Tuple[str, str], Optional[str]]:
        """批量查询每个任务类型下所有股票的最新日期

        Args:
            db_queryer: 数据库查询器实例
            task_types: 任务类型列表
            symbols: 股票代码列表
            task_date_col_map: 任务类型到是否有date_col的映射

        Returns:
            Dict[Tuple[symbol, task_type], date]: 最新日期映射
        """
        max_dates = {}
        for task_type in task_types:
            # 检查表是否定义了 date_col
            has_date_col = task_date_col_map.get(task_type, False)
            
            if has_date_col and symbols:  # 有日期列且有具体股票代码的任务类型
                logger.debug(
                    f"正在为 {task_type} 查询 {len(symbols)} 个股票的最新日期..."
                )
                task_max_dates = db_queryer.get_max_date(task_type, symbols)
                for symbol, date in task_max_dates.items():
                    max_dates[(symbol, task_type)] = date
            elif has_date_col:  # 有日期列但不需要具体股票代码的任务类型（如 trade_cal）
                logger.debug(f"正在为 {task_type} 查询最新日期...")
                # 对于不需要股票代码的任务，传入空字符串列表查询最新日期
                task_max_dates = db_queryer.get_max_date(task_type, [""]) 
                # 获取查询结果中的最新日期
                latest_date = task_max_dates.get("") if task_max_dates else None
                max_dates[("", task_type)] = latest_date
            else:  # 没有日期列的任务类型（如 stock_basic）
                logger.debug(f"任务类型 {task_type} 没有定义 date_col，跳过日期查询")
                # 对于没有日期列的表，不查询日期，直接设置为 None
                if symbols:
                    for symbol in symbols:
                        max_dates[(symbol, task_type)] = None
                else:
                    max_dates[("", task_type)] = None

        return max_dates

    def _should_skip_task(
        self, latest_date: Optional[str], latest_trading_day: Optional[str]
    ) -> bool:
        """检查是否应该跳过当前任务

        Args:
            latest_date: 最新数据日期 (YYYYMMDD格式)
            latest_trading_day: 最新交易日 (YYYYMMDD格式)

        Returns:
            bool: 是否应该跳过
        """
        if not latest_date:
            return False  # 没有历史数据，必须下载

        # 如果无法获取最新交易日，使用旧的、基于当前时间的简单逻辑作为备用
        if not latest_trading_day:
            logger.warning("无法获取最新交易日，使用旧逻辑进行判断。")
            today_str = datetime.now().strftime("%Y%m%d")
            yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            current_time = datetime.now().time()
            market_close_time = time(18, 0)

            if latest_date == today_str:
                return True
            if latest_date == yesterday_str and current_time < market_close_time:
                return True
            return False

        # --- 新的核心逻辑 ---
        # 1. 如果本地数据落后于最新交易日，立即下载（不管今天是否收盘）
        if latest_date < latest_trading_day:
            logger.info(
                f"📥 执行任务：本地数据落后 (本地: {latest_date}, 最新交易日: {latest_trading_day})"
            )
            return False
        
        # 2. 如果本地数据已经是最新交易日的数据，说明数据已经是最新的了
        if latest_date > latest_trading_day:
            logger.info(
                f"⏭️ 跳过任务：数据已是最新 (本地: {latest_date}, 最新交易日: {latest_trading_day})"
            )
            return True
        
        # 3. 如果本地数据等于最新交易日，需要判断今天是否收盘来决定是否下载今天的数据
        # latest_date == latest_trading_day 的情况
        today_str = datetime.now().strftime("%Y%m%d")
        if today_str == latest_trading_day:
            # 今天是交易日，需要判断是否收盘
            current_time = datetime.now().time()
            market_close_time = time(18, 0)  # 假设下午6点后数据稳定
            if current_time < market_close_time:
                logger.info(
                    f"⏭️ 跳过任务：等待 {latest_trading_day} 收盘数据 (本地: {latest_date}, 当前时间: {current_time.strftime('%H:%M')})"
                )
                return True
            else:
                logger.info(
                    f"📥 执行任务：{latest_trading_day} 已收盘，下载今日数据 (本地: {latest_date})"
                )
                return False
        else:
            # 今天不是交易日，本地数据已经是最新的
            logger.info(
                f"⏭️ 跳过任务：数据已是最新 (本地: {latest_date}, 最新交易日: {latest_trading_day})"
            )
            return True

    def _enqueue_download_tasks(
        self,
        latest_trading_day: Optional[str],
        task_types: List[str],
        symbols: Optional[List[str]],
        max_dates: Dict[Tuple[str, str], Optional[str]],
        task_date_col_map: Dict[str, bool],
    ) -> int:
        """派发下载任务到快速队列

        Args:
            latest_trading_day: 最新交易日
            task_types: 任务类型列表
            symbols: 股票代码列表
            max_dates: 最新日期映射
            task_date_col_map: 任务类型到是否有date_col的映射

        Returns:
            int: 派发的任务数量
        """
        default_start_date = self.config.download_tasks.default_start_date
        enqueued_count = 0

        for task_type in task_types:
            has_date_col = task_date_col_map.get(task_type, False)
            
            if has_date_col and symbols:  # 有日期列且有具体股票代码的任务类型
                for symbol in symbols:
                    latest_date = max_dates.get((symbol, task_type))

                    if self._should_skip_task(latest_date, latest_trading_day):
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
            elif has_date_col:  # 有日期列但不需要具体股票代码的任务类型（如 trade_cal）
                latest_date = max_dates.get(("", task_type))

                if self._should_skip_task(latest_date, latest_trading_day):
                    continue

                start_date = (
                    get_next_day_str(latest_date) if latest_date else default_start_date
                )
                
                # 派发任务到快速队列
                download_task(task_type=task_type, symbol="", start_date=start_date)
                enqueued_count += 1
            else:  # 没有日期列的任务类型（如 stock_basic），总是执行
                 if symbols:
                     for symbol in symbols:
                         # 派发任务到快速队列
                         download_task(
                             task_type=task_type, symbol=symbol, start_date=default_start_date
                         )
                         enqueued_count += 1
                 else:
                     # 派发任务到快速队列
                     download_task(task_type=task_type, symbol="", start_date=default_start_date)
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
        # 0. 初始化
        from ..app import container

        task_manager = DownloadTaskManager()

        db_queryer = container.db_queryer()
        schema_loader = container.schema_loader()
        

        # 1. 获取最新交易日 (只执行一次)
        latest_trading_day = db_queryer.get_latest_trading_day()
        if latest_trading_day:
            logger.debug(f"获取到最新交易日: {latest_trading_day}")
        else:
            logger.warning("未能获取到最新交易日，将使用备用逻辑。")

        # 2. 获取任务类型和股票代码
        task_types, symbols = task_manager._get_task_types_and_symbols(
            group_name, stock_codes
        )
        if not task_types:
            return
        
        # 2.1 检查各任务类型是否有date_col定义
        task_date_col_map = {}
        for task_type in task_types:
            try:
                table_config = schema_loader.get_table_config(task_type)
                has_date_col = hasattr(table_config, "date_col") and table_config.date_col is not None
                task_date_col_map[task_type] = has_date_col
            except (KeyError, AttributeError):
                task_date_col_map[task_type] = False

        # 3. 批量查询本地数据的最新日期
        max_dates = task_manager._query_max_dates(db_queryer, task_types, symbols, task_date_col_map)

        # 4. 派发下载任务
        enqueued_count = task_manager._enqueue_download_tasks(
            latest_trading_day, task_types, symbols, max_dates, task_date_col_map
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
