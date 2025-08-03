import pandas as pd
from .base import IncrementalTaskHandler


class DailyTaskHandler(IncrementalTaskHandler):
    """实现日线数据下载的具体逻辑。"""

    def get_data_type(self) -> str:
        adjust = self.task_config.get("adjust", "none")
        return f"daily_{adjust or 'none'}"

    def get_date_col(self) -> str:
        return self.task_config.get("date_col", "trade_date")

    def fetch_data(self, ts_code, start_date, end_date) -> pd.DataFrame | None:
        adjust = self.task_config.get("adjust", "none")
        return self.fetcher.fetch_daily_history(ts_code, start_date, end_date, adjust)
