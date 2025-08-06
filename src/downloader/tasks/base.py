import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import sys
import time

# 导入核心组件以进行类型提示，增强代码可读性和健壮性
from downloader.fetcher import TushareFetcher
from downloader.storage import DuckDBStorage
from downloader.rate_limit import rate_limit

# 从新的 utils 模块导入工具函数
from ..utils import record_failed_task


class BaseTaskHandler(ABC):
    """所有任务处理器的抽象基类。"""

    def __init__(
        self,
        task_config: dict,
        fetcher: TushareFetcher,
        storage: DuckDBStorage,
        force_run: bool = False,
    ):
        self.task_config = task_config
        self.fetcher = fetcher
        self.storage = storage
        self.force_run = force_run
        self.logger = logging.getLogger(self.__class__.__name__)
        self._current_progress_bar = None

    def _safe_log(self, level: str, message: str, *args, **kwargs):
        """安全的日志输出方法，与进度条兼容"""
        if self._current_progress_bar is not None:
            # 如果当前有活跃的进度条，使用 tqdm.write 输出
            log_msg = f"{self.logger.name} - {level.upper()} - {message}"
            if args:
                log_msg = log_msg % args
            tqdm.write(log_msg)
        else:
            # 否则使用常规的 logger
            getattr(self.logger, level.lower())(message, *args, **kwargs)

    def _log_debug(self, message: str, *args, **kwargs):
        self._safe_log("DEBUG", message, *args, **kwargs)

    def _log_info(self, message: str, *args, **kwargs):
        """信息级别日志"""
        self._safe_log("INFO", message, *args, **kwargs)

    def _log_warning(self, message: str, *args, **kwargs):
        """警告级别日志"""
        self._safe_log("WARNING", message, *args, **kwargs)

    def _log_error(self, message: str, *args, **kwargs):
        """错误级别日志"""
        self._safe_log("ERROR", message, *args, **kwargs)

    @abstractmethod
    def execute(self, **kwargs):
        """执行任务的主方法。"""
        raise NotImplementedError


class IncrementalTaskHandler(BaseTaskHandler):
    """
    模板方法模式：为所有基于时间戳的增量任务提供一个固定的算法骨架。
    子类只需要实现几个特定的“可变点”即可。
    """

    def execute(self, **kwargs):
        target_symbols = kwargs.get("target_symbols")
        if not target_symbols:
            self._log_warning(
                f"任务 '{self.task_config['name']}' 未收到目标股票列表，跳过。"
            )
            return

        task_name = self.task_config["name"]
        self._log_debug(
            f"--- 开始为 {len(target_symbols)} 只股票执行增量任务: '{task_name}' ---"
        )

        try:
            progress_bar = tqdm(
                target_symbols,
                desc=f"执行: {task_name}",
                ncols=100,
                leave=True,
                file=sys.stdout,
            )
            self._current_progress_bar = progress_bar
        except (BrokenPipeError, OSError) as e:
            self._log_warning(f"进度条初始化失败，切换为静默模式: {e}")
            # 使用静默模式，禁用进度条
            self._current_progress_bar = None
            # 直接使用迭代器而不是进度条
            progress_bar = target_symbols

        network_error_symbols = []
        is_progress_bar_active = hasattr(progress_bar, 'close')  # 判断是否为真正的进度条
        try:
            for ts_code in progress_bar:
                is_success, is_network_error = self._process_single_symbol(
                    ts_code, is_retry=False
                )
                if not is_success and is_network_error:
                    network_error_symbols.append(ts_code)
        finally:
            self._current_progress_bar = None
            if is_progress_bar_active:
                progress_bar.close()

        if network_error_symbols:
            self._retry_network_errors(network_error_symbols)

    def _process_single_symbol(self, ts_code: str, is_retry: bool) -> tuple[bool, bool]:
        """
        处理单个股票的下载、保存和错误处理逻辑。

        Args:
            ts_code (str): 股票代码。
            is_retry (bool): 是否是重试操作。

        Returns:
            tuple[bool, bool]: (是否成功, 是否是网络错误)
        """
        task_name = self.task_config["name"]
        data_type = self.get_data_type()
        date_col = self.get_date_col()
        
        desc_prefix = f"重试: {data_type}_{ts_code}" if is_retry else f"处理: {data_type}_{ts_code}"
        if self._current_progress_bar:
            try:
                self._current_progress_bar.set_description(desc_prefix)
            except (BrokenPipeError, OSError):
                # 进度条更新失败，切换为静默模式
                self._log_debug(f"进度条更新失败，禁用进度条: {desc_prefix}")
                self._current_progress_bar = None

        try:
            if is_retry:
                time.sleep(1)

            latest_date = self.storage.get_latest_date(
                data_type, ts_code, date_col=date_col
            )
            start_date = "19901219"
            if latest_date:
                start_date = (
                    pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
                ).strftime("%Y%m%d")

            end_date = datetime.now().strftime("%Y%m%d")
            if start_date > end_date:
                return True, False

            rate_limit_config = self.task_config.get("rate_limit", {})
            calls_per_minute = rate_limit_config.get("calls_per_minute")

            task_key_suffix = "_retry" if is_retry else ""
            task_key = f"{task_name}_{ts_code}{task_key_suffix}"

            if calls_per_minute is not None:
                @rate_limit(calls_per_minute=calls_per_minute, task_key=task_key)
                def _fetch_data():
                    return self.fetch_data(ts_code, start_date, end_date)
                df = _fetch_data()
            else:
                df = self.fetch_data(ts_code, start_date, end_date)

            if df is not None:
                if not df.empty:
                    self.storage.save(df, data_type, ts_code, date_col=date_col)
                    if is_retry:
                        self._log_info(f"✅ 重试成功: {ts_code}")
                elif is_retry:
                    self._log_error(f"❌ 重试失败，数据为空: {ts_code}")
                    record_failed_task(task_name, f"{data_type}_{ts_code}", "retry_failed_fetch_empty")
            else:
                log_msg = f"❌ {'重试' if is_retry else '获取'}失败，返回None: {ts_code}"
                self._log_error(log_msg)
                fail_reason = "retry_failed_fetch_none" if is_retry else "fetch_failed"
                record_failed_task(task_name, f"{data_type}_{ts_code}", fail_reason)
            
            return True, False

        except Exception as e:
            error_msg = str(e).lower()
            is_network_error = any(
                keyword in error_msg
                for keyword in ["timeout", "connection", "network", "connect", "ssl", "name or service not known"]
            )

            if is_network_error:
                if not is_retry:
                    self._log_warning(f"网络错误，暂存股票 {ts_code} 以待重试: {e}")
                else:
                    self._log_error(f"❌ 重试处理股票 {ts_code} 时再次遇到网络错误: {e}")
                    record_failed_task(task_name, f"{data_type}_{ts_code}", f"retry_failed_network_{str(e)}")
                return False, True
            else:
                log_msg = f"❌ {'重试' if is_retry else '处理'}股票 {ts_code} 时发生未知错误: {e}"
                self._log_error(log_msg)
                fail_reason = f"retry_failed_{str(e)}" if is_retry else str(e)
                record_failed_task(task_name, f"{data_type}_{ts_code}", fail_reason)
                return False, False

    def _retry_network_errors(self, symbols: list):
        """重试因网络错误失败的股票"""
        if not symbols:
            return
            
        task_name = self.task_config["name"]
        self._log_info(f"开始重试 {len(symbols)} 只因网络错误失败的股票...")

        try:
            retry_progress_bar = tqdm(
                symbols,
                desc=f"重试: {task_name}",
                ncols=100,
                leave=True,
                file=sys.stdout,
            )
            self._current_progress_bar = retry_progress_bar
        except (BrokenPipeError, OSError) as e:
            self._log_warning(f"重试进度条初始化失败，切换为静默模式: {e}")
            # 使用静默模式，禁用进度条
            self._current_progress_bar = None
            retry_progress_bar = symbols

        is_retry_progress_bar_active = hasattr(retry_progress_bar, 'close')
        try:
            for ts_code in retry_progress_bar:
                self._process_single_symbol(ts_code, is_retry=True)
        finally:
            self._current_progress_bar = None
            if is_retry_progress_bar_active:
                retry_progress_bar.close()

    @abstractmethod
    def get_data_type(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_date_col(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def fetch_data(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        raise NotImplementedError

