import pandas as pd
from .base import IncrementalTaskHandler


class DailyBasicTaskHandler(IncrementalTaskHandler):
    """实现每日指标下载的具体逻辑。"""

    def get_data_type(self) -> str:
        return "daily_basic"

    def get_date_col(self) -> str:
        return self.task_config.get("date_col", "trade_date")

    def fetch_data(self, ts_code, start_date, end_date) -> pd.DataFrame | None:
        return self.fetcher.fetch_daily_basic(ts_code, start_date, end_date)
