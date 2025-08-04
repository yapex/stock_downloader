import pandas as pd
from .base import IncrementalTaskHandler


class FinancialsTaskHandler(IncrementalTaskHandler):
    """实现财务报表下载的具体逻辑。"""

    def get_data_type(self) -> str:
        statement_type = self.task_config.get("statement_type")
        return f"financials_{statement_type}"

    def get_date_col(self) -> str:
        return self.task_config.get("date_col", "ann_date")

    def fetch_data(self, ts_code, start_date, end_date) -> pd.DataFrame | None:
        statement_type = self.task_config.get("statement_type")
        fetch_method_name = f"fetch_{statement_type}"
        try:
            fetch_method = getattr(self.fetcher, fetch_method_name)
            return fetch_method(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
        except AttributeError:
            self._log_error(
                f"Fetcher 中未找到方法 '{fetch_method_name}'，请检查 'statement_type' 配置。"
            )
            return None
