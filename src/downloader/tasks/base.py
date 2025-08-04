import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import sys

# 导入核心组件以进行类型提示，增强代码可读性和健壮性
from downloader.fetcher import TushareFetcher
from downloader.storage import ParquetStorage

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

    def _log_info(self, message: str, *args, **kwargs):
        """信息级别日志"""
        self._safe_log('INFO', message, *args, **kwargs)

    def _log_warning(self, message: str, *args, **kwargs):
        """警告级别日志"""
        self._safe_log('WARNING', message, *args, **kwargs)

    def _log_error(self, message: str, *args, **kwargs):
        """错误级别日志"""
        self._safe_log('ERROR', message, *args, **kwargs)

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

        self._log_info(
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
                            pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
                        ).strftime("%Y%m%d")

                    end_date = datetime.now().strftime("%Y%m%d")
                    if start_date > end_date:
                        continue

                    df = self.fetch_data(ts_code, start_date, end_date)

                    if df is not None:
                        if not df.empty:
                            self.storage.save(df, data_type, ts_code, date_col=date_col)
                    else:
                        record_failed_task(
                            task_name, f"{data_type}_{ts_code}", "fetch_failed"
                        )

                except Exception as e:
                    self._log_error(f"❌ 处理股票 {ts_code} 时发生未知错误: {e}")
                    record_failed_task(task_name, f"{data_type}_{ts_code}", str(e))
        finally:
            # 清理进度条引用
            self._current_progress_bar = None
            progress_bar.close()

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
