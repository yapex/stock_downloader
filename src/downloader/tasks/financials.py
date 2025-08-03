from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
from .base import BaseTaskHandler, logging


def record_failed_task(task_name: str, entity_id: str, reason: str):
    with open("failed_tasks.log", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()},{task_name},{entity_id},{reason}\n")


class FinancialsTaskHandler(BaseTaskHandler):
    """
    处理器插件：负责增量下载财务报表数据。
    """

    def execute(self, **kwargs):
        target_symbols = kwargs.get("target_symbols")
        if not target_symbols:
            self.logger.warning(
                f"任务 '{self.task_config['name']}' 未收到目标股票列表，跳过。"
            )
            return

        task_name = self.task_config["name"]
        statement_type = self.task_config.get("statement_type")
        if not statement_type:
            self.logger.error(f"任务 '{task_name}' 未配置 'statement_type'，无法执行。")
            return

        data_type = f"financials_{statement_type}"
        date_col = self.task_config.get("date_col", "ann_date")

        self.logger.info(
            f"--- 开始为 {len(target_symbols)} 只股票执行财务报表({statement_type})增量任务 ---"
        )

        progress_bar = tqdm(target_symbols, desc=f"执行: {task_name}")
        for ts_code in progress_bar:
            progress_bar.set_description(f"处理: {data_type}_{ts_code}")
            try:
                latest_date = self.storage.get_latest_date(
                    data_type, ts_code, date_col=date_col
                )
                start_date = "19900101"
                if latest_date:
                    start_date = (
                        pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
                    ).strftime("%Y%m%d")

                end_date = datetime.now().strftime("%Y%m%d")
                if start_date > end_date:
                    continue

                # ---> 核心修正：构造并调用正确的 Fetcher 方法 <---
                fetch_method_name = f"fetch_{statement_type}"
                fetch_method = getattr(self.fetcher, fetch_method_name)
                df = fetch_method(
                    ts_code=ts_code, start_date=start_date, end_date=end_date
                )

                if df is not None and not df.empty:
                    self.storage.save(df, data_type, ts_code, date_col=date_col)
                elif df is None:
                    record_failed_task(
                        task_name, f"{data_type}_{ts_code}", "fetch_failed"
                    )
            except AttributeError:
                self.logger.error(
                    f"Fetcher 中未找到方法 '{fetch_method_name}'，请检查 'statement_type' 配置。"
                )
                # 记录一次失败后，可以跳出循环，因为这很可能是配置错误
                break
            except Exception as e:
                tqdm.write(
                    f"❌ 处理股票 {ts_code} 的财务报表({statement_type})时发生未知错误: {e}"
                )
                record_failed_task(task_name, f"{data_type}_{ts_code}", str(e))
