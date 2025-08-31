"""下载任务模块

包含股票数据下载相关的 Huey 任务。
V3版本采用交叉生成和随机优先级策略，以优化任务队列的均匀性、解决“车队效应”并减少内存占用。
"""

import logging
from datetime import datetime, time, timedelta
from typing import Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING

from ..configs.app_config import get_config
from ..configs.huey_config import huey_fast, huey_slow
from ..helpers.utils import get_next_day_str

if TYPE_CHECKING:
    from ..database.operator import ParquetDBQueryer
    from ..database.interfaces import ISchemaLoader

logger = logging.getLogger(__name__)


class DownloadTaskManager:
    """下载任务管理器，负责构建和管理下载任务 (无状态)"""

    def __init__(self, schema_loader: "ISchemaLoader"):
        self.config = get_config()
        self.schema_loader = schema_loader

    def _get_task_types_and_symbols(
        self, group_name: str, stock_codes: Optional[List[str]]
    ) -> Tuple[List[str], Optional[List[str]]]:
        """获取任务类型和股票代码列表"""
        from ..app import container

        group_handler = container.group_handler()
        logger.debug(f"获取到 group_handler: {group_handler}")

        task_types = group_handler.get_task_types_for_group(group_name)
        logger.debug(f"获取到 task_types: {task_types}")
        if not task_types:
            logger.warning(
                f"⏬ ⚠️ 任务组 '{group_name}' 中没有找到任何任务类型，任务结束。"
            )
            return [], None

        if stock_codes:
            symbols = stock_codes
            logger.debug(f"使用命令行指定的股票代码: {len(symbols)} 个")
        else:
            symbols = group_handler.get_symbols_for_group(group_name)
            logger.debug(
                f"使用任务组 '{group_name}' 配置的股票代码: {len(symbols) if symbols else 0} 个"
            )

        return task_types, symbols

    def _should_skip_task(
        self, latest_date: Optional[str], latest_trading_day: Optional[str]
    ) -> bool:
        """检查是否应该跳过当前任务"""
        if not latest_date:
            return False

        if not latest_trading_day:
            logger.warning("⏬ ⚠️ 无法获取最新交易日，使用旧逻辑进行判断。")
            today_str = datetime.now().strftime("%Y%m%d")
            yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            current_time = datetime.now().time()
            market_close_time = time(18, 0)

            if latest_date == today_str:
                return True
            if latest_date == yesterday_str and current_time < market_close_time:
                return True
            return False

        if latest_date < latest_trading_day:
            logger.info(
                f"⏬ 执行任务：本地数据落后 (本地: {latest_date}, 最新交易日: {latest_trading_day})"
            )
            return False

        if latest_date > latest_trading_day:
            logger.info(
                f"⏬ ⏭️ 跳过任务：数据已是最新 (本地: {latest_date}, 最新交易日: {latest_trading_day})"
            )
            return True

        today_str = datetime.now().strftime("%Y%m%d")
        if today_str == latest_trading_day:
            current_time = datetime.now().time()
            market_close_time = time(18, 0)
            if current_time < market_close_time:
                logger.info(
                    f"⏬ ⏭️ 跳过任务：等待 {latest_trading_day} 收盘数据 (本地: {latest_date}, 当前时间: {current_time.strftime('%H:%M')})"
                )
                return True
            else:
                logger.info(
                    f"⏬ 执行任务：{latest_trading_day} 已收盘，下载今日数据 (本地: {latest_date})"
                )
                return False
        else:
            logger.info(
                f"⏬ ⏭️ 跳过任务：数据已是最新 (本地: {latest_date}, 最新交易日: {latest_trading_day})"
            )
            return True

    def _generate_task_configs_for_type(
        self,
        task_type: str,
        symbols: Optional[List[str]],
        db_queryer: "ParquetDBQueryer",
        latest_trading_day: Optional[str],
    ) -> Iterator[Dict]:
        """为一个任务类型创建所有任务配置的生成器"""
        logger.debug(f"为任务类型 '{task_type}' 创建任务生成器...")
        default_start_date = self.config.download_tasks.default_start_date

        # --- 获取更新策略 (强制检查) ---
        download_tasks_config = self.config.download_tasks
        update_strategy = None
        if hasattr(download_tasks_config, task_type):
            task_config = getattr(download_tasks_config, task_type)
            if hasattr(task_config, "update_strategy"):
                update_strategy = task_config.update_strategy

        if not update_strategy:
            raise ValueError(
                f"⏬ ❌ 任务类型 '{task_type}' 在 config.toml 中缺少必须的 'update_strategy' 配置项。"
            )

        task_symbols = symbols if symbols else [""]

        # --- 根据策略派发任务 ---
        if update_strategy == "full_replace":
            logger.info(f"⏬ ⏩ 检测到全量替换策略 for {task_type}，将跳过增量检查。")
            for symbol in task_symbols:
                yield {
                    "task_type": task_type,
                    "symbol": symbol,
                    "start_date": default_start_date,
                }
            return

        elif update_strategy == "incremental":
            # --- 执行增量逻辑 ---
            try:
                table_config = self.schema_loader.get_table_config(task_type)
                has_date_col = (
                    hasattr(table_config, "date_col")
                    and table_config.date_col is not None
                )
            except (KeyError, AttributeError):
                has_date_col = False

            if has_date_col:
                max_dates = db_queryer.get_max_date(task_type, task_symbols)
                for symbol in task_symbols:
                    latest_date = max_dates.get(symbol)
                    if self._should_skip_task(latest_date, latest_trading_day):
                        continue
                    start_date = (
                        get_next_day_str(latest_date)
                        if latest_date
                        else default_start_date
                    )
                    yield {
                        "task_type": task_type,
                        "symbol": symbol,
                        "start_date": start_date,
                    }
            else:  # 没有日期列的任务，按全量处理
                logger.info(f"⏬ 任务 {task_type} 没有日期列，执行全量下载。")
                for symbol in task_symbols:
                    yield {
                        "task_type": task_type,
                        "symbol": symbol,
                        "start_date": default_start_date,
                    }
        else:
            raise ValueError(
                f"⏬ ❌ 任务类型 '{task_type}' 的更新策略 '{update_strategy}' 无效。有效值为 'full_replace' 或 'incremental'。"
            )


@huey_slow.task()
def build_and_enqueue_downloads_task(
    group_name: str, stock_codes: Optional[List[str]] = None
):
    """
    构建并派发增量下载任务 (慢速队列, V3 - 随机优先级)

    这是智能增量下载的第一步。它会为每个业务类型创建独立的任务生成器，
    然后通过轮询、交叉生成的方式，为每个任务附加一个随机优先级后再派发。
    这能确保队列任务的高度多样性，解决“车队效应”导致的 worker 阻塞。

    Args:
        group_name: 在 config.toml 中定义的任务组名
        stock_codes: 可选的股票代码列表，如果提供则只处理这些股票
    """
    logger.debug(f"[HUEY_SLOW] V3 开始构建增量下载任务, 任务组: {group_name}")
    try:
        from ..app import container

        db_queryer = container.db_queryer()
        schema_loader = container.schema_loader()
        task_manager = DownloadTaskManager(schema_loader)

        latest_trading_day = db_queryer.get_latest_trading_day()
        if latest_trading_day:
            logger.debug(f"获取到最新交易日: {latest_trading_day}")
        else:
            logger.warning("⏬ ⚠️ 未能获取到最新交易日，部分任务可能不会执行增量检查。")

        task_types, symbols = task_manager._get_task_types_and_symbols(
            group_name, stock_codes
        )
        if not task_types:
            return

        # 1. 为每个业务类型创建独立的“任务生成器”
        logger.debug(f"为 {len(task_types)} 个任务类型创建生成器: {task_types}")
        generators = [
            task_manager._generate_task_configs_for_type(
                tt, symbols, db_queryer, latest_trading_day
            )
            for tt in task_types
        ]

        # 2. 轮询、交叉生成并派发带有随机优先级的任务
        enqueued_count = 0
        active_generators = [iter(g) for g in generators]
        logger.debug(f"开始从 {len(active_generators)} 个生成器中轮询并派发任务...")

        while active_generators:
            # 倒序遍历，方便安全地移除耗尽的生成器
            for i in range(len(active_generators) - 1, -1, -1):
                gen_iter = active_generators[i]
                try:
                    task_params = next(gen_iter)
                    # 派发任务，不再附加随机优先级
                    download_task(**task_params)
                    logger.info(f"⏬ 已派发任务: {task_params}")
                    enqueued_count += 1
                except StopIteration:
                    # 这个生成器已经耗尽，将它从活跃列表中移除
                    active_generators.pop(i)

        logger.debug(f"[HUEY_SLOW] V3 成功派发 {enqueued_count} 个增量下载任务。")

    except Exception as e:
        logger.error(f"⏬ ❌ [HUEY_SLOW] V3 构建下载任务失败: {e}", exc_info=True)
        raise e


@huey_fast.task(retries=2, retry_delay=60)
def download_task(task_type: str, symbol: str, **kwargs):
    """
    下载股票数据的 Huey 任务 (快速队列)

    下载完成后，直接调用慢速队列的数据处理任务。

    Args:
        task_type: 任务类型字符串
        symbol: 股票代码
        **kwargs: 额外的下载参数，如 start_date, end_date
    """
    try:
        logger.debug(f"[HUEY_FAST] 开始执行下载任务: {symbol} ({task_type})")

        from ..app import container
        from .data_processing_tasks import process_data_task

        downloader = container.downloader()
        result = downloader.download(task_type, symbol, **kwargs)

        if result is not None and not result.empty:
            logger.info(
                f"⏬ [HUEY_FAST] 下载完成: {symbol}, 准备转换数据并提交到慢速队列..."
            )

            # --- 开始计时 ---
            start_dt = datetime.now()

            data_as_dict = result.to_dict("records")
            process_data_task(
                task_type=task_type,
                symbol=symbol,
                data_frame=data_as_dict,
            )

            end_dt = datetime.now()
            enqueue_duration = (end_dt - start_dt).total_seconds()
            logger.info(
                f"⏬ [HUEY_FAST] 成功提交到慢速队列: {symbol}, 转换及入队耗时: {enqueue_duration:.4f} 秒"
            )
            # --- 计时结束 ---
        else:
            logger.warning(
                f"⏬ ⚠️ [HUEY_FAST] 下载任务完成: {symbol}, 但返回空数据，不提交后续任务"
            )

    except Exception as e:
        logger.error(
            f"⏬ ❌ [HUEY_FAST] 下载任务执行失败，将在60秒后重试。任务: {task_type}, 代码: {symbol}, 错误: {e}",
            exc_info=True,
        )
        raise e


@huey_slow.task()
def cleanup_downloader_task():
    """清理下载器资源"""
    from ..app import container

    downloader = container.downloader()
    if hasattr(downloader, "cleanup"):
        downloader.cleanup()
    logger.info("Downloader resources cleaned up.")
