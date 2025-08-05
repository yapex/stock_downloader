import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import sys
import time

# 导入核心组件以进行类型提示，增强代码可读性和健壮性
from downloader.fetcher import TushareFetcher
from downloader.storage import ParquetStorage
from downloader.rate_limit import rate_limit

# 从新的 utils 模块导入工具函数
from ..utils import record_failed_task


class BaseTaskHandler(ABC):
    """所有任务处理器的抽象基类。"""

    def __init__(
        self,
        task_config: dict,
        fetcher: TushareFetcher,
        storage: ParquetStorage,
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
        data_type = self.get_data_type()
        date_col = self.get_date_col()

        self._log_debug(
            f"--- 开始为 {len(target_symbols)} 只股票执行增量任务: '{task_name}' ---"
        )

        # 创建进度条并设置为当前活跃进度条
        progress_bar = tqdm(
            target_symbols,
            desc=f"执行: {task_name}",
            ncols=100,  # 固定进度条宽度
            leave=True,  # 完成后保留进度条
            file=sys.stdout,  # 确保输出到标准输出
        )
        self._current_progress_bar = progress_bar

        # 用于记录网络错误的股票代码
        network_error_symbols = []

        try:
            for ts_code in progress_bar:
                progress_bar.set_description(f"处理: {data_type}_{ts_code}")
                try:
                    latest_date = self.storage.get_latest_date(
                        data_type, ts_code, date_col=date_col
                    )
                    start_date = "19901219"
                    if latest_date:
                        start_date = (
                            pd.to_datetime(latest_date, format="%Y%m%d")
                            + timedelta(days=1)
                        ).strftime("%Y%m%d")

                    end_date = datetime.now().strftime("%Y%m%d")
                    if start_date > end_date:
                        continue

                    # 获取任务特定的速率限制配置
                    rate_limit_config = self.task_config.get("rate_limit", {})
                    calls_per_minute = rate_limit_config.get("calls_per_minute")

                    # 如果配置了速率限制，则应用它
                    if calls_per_minute is not None:
                        # 为每个任务创建一个带速率限制的包装函数
                        @rate_limit(
                            calls_per_minute=calls_per_minute,
                            task_key=f"{task_name}_{ts_code}",
                        )
                        def _fetch_data():
                            return self.fetch_data(ts_code, start_date, end_date)

                        df = _fetch_data()
                    else:
                        # 使用默认的无限制调用
                        df = self.fetch_data(ts_code, start_date, end_date)

                    if df is not None:
                        if not df.empty:
                            self.storage.save(df, data_type, ts_code, date_col=date_col)
                    else:
                        # 记录获取数据失败的情况
                        record_failed_task(
                            task_name, f"{data_type}_{ts_code}", "fetch_failed"
                        )

                except Exception as e:
                    # 检查是否是网络相关错误
                    error_msg = str(e).lower()
                    is_network_error = (
                        "timeout" in error_msg
                        or "connection" in error_msg
                        or "network" in error_msg
                        or "connect" in error_msg
                        or "ssl" in error_msg
                        or "name or service not known" in error_msg
                    )
                    
                    if is_network_error:
                        self._log_warning(f"网络错误，暂存股票 {ts_code} 以待重试: {e}")
                        network_error_symbols.append(ts_code)
                    else:
                        self._log_error(f"❌ 处理股票 {ts_code} 时发生未知错误: {e}")
                        record_failed_task(task_name, f"{data_type}_{ts_code}", str(e))
        finally:
            # 清理进度条引用
            self._current_progress_bar = None
            progress_bar.close()

        # 如果有网络错误的股票，尝试重新下载
        if network_error_symbols:
            self._retry_network_errors(network_error_symbols, data_type, date_col, task_name)

    def _retry_network_errors(self, symbols, data_type, date_col, task_name):
        """重试网络错误的股票"""
        self._log_info(f"开始重试 {len(symbols)} 只因网络错误失败的股票...")
        
        # 创建新的进度条用于重试
        retry_progress_bar = tqdm(
            symbols,
            desc=f"重试: {task_name}",
            ncols=100,
            leave=True,
            file=sys.stdout,
        )
        self._current_progress_bar = retry_progress_bar

        try:
            for ts_code in retry_progress_bar:
                retry_progress_bar.set_description(f"重试: {data_type}_{ts_code}")
                try:
                    # 等待一小段时间再重试，避免过于频繁的请求
                    time.sleep(1)
                    
                    latest_date = self.storage.get_latest_date(
                        data_type, ts_code, date_col=date_col
                    )
                    start_date = "19901219"
                    if latest_date:
                        start_date = (
                            pd.to_datetime(latest_date, format="%Y%m%d")
                            + timedelta(days=1)
                        ).strftime("%Y%m%d")

                    end_date = datetime.now().strftime("%Y%m%d")
                    if start_date > end_date:
                        continue

                    # 获取任务特定的速率限制配置
                    rate_limit_config = self.task_config.get("rate_limit", {})
                    calls_per_minute = rate_limit_config.get("calls_per_minute")

                    # 如果配置了速率限制，则应用它
                    if calls_per_minute is not None:
                        # 为每个任务创建一个带速率限制的包装函数
                        @rate_limit(
                            calls_per_minute=calls_per_minute,
                            task_key=f"{task_name}_{ts_code}_retry",
                        )
                        def _fetch_data():
                            return self.fetch_data(ts_code, start_date, end_date)

                        df = _fetch_data()
                    else:
                        # 使用默认的无限制调用
                        df = self.fetch_data(ts_code, start_date, end_date)

                    if df is not None:
                        if not df.empty:
                            self.storage.save(df, data_type, ts_code, date_col=date_col)
                            self._log_info(f"✅ 重试成功: {ts_code}")
                        else:
                            self._log_error(f"❌ 重试失败，数据为空: {ts_code}")
                            record_failed_task(
                                task_name, f"{data_type}_{ts_code}", "retry_failed_fetch_empty"
                            )
                    else:
                        self._log_error(f"❌ 重试失败，返回None: {ts_code}")
                        record_failed_task(
                            task_name, f"{data_type}_{ts_code}", "retry_failed_fetch_none"
                        )

                except Exception as e:
                    self._log_error(f"❌ 重试处理股票 {ts_code} 时发生未知错误: {e}")
                    record_failed_task(task_name, f"{data_type}_{ts_code}", f"retry_failed_{str(e)}")
        finally:
            # 清理进度条引用
            self._current_progress_bar = None
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
