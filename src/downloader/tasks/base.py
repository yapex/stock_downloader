import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import argparse

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
        args: argparse.Namespace,
    ):
        self.task_config = task_config
        self.fetcher = fetcher
        self.storage = storage
        self.args = args
        self.logger = logging.getLogger(self.__class__.__name__)

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
            self.logger.warning(
                f"任务 '{self.task_config['name']}' 未收到目标股票列表，跳过。"
            )
            return

        task_name = self.task_config["name"]
        data_type = self.get_data_type()
        date_col = self.get_date_col()

        self.logger.info(
            f"--- 开始为 {len(target_symbols)} 只股票执行增量任务: '{task_name}' ---"
        )

        progress_bar = tqdm(target_symbols, desc=f"执行: {task_name}")
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
                tqdm.write(f"❌ 处理股票 {ts_code} 时发生未知错误: {e}")
                record_failed_task(task_name, f"{data_type}_{ts_code}", str(e))

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
