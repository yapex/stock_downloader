from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
from .base import BaseTaskHandler, logging


class DailyTaskHandler(BaseTaskHandler):
    def execute(self):
        task_name = self.task_config["name"]
        adjust = self.task_config.get("adjust", "none")
        data_type = f"daily_{adjust or 'none'}"
        date_col = self.task_config.get("date_col", "trade_date")

        self.logger.info(f"--- 开始执行增量任务: '{task_name}' ---")

        # ---> 核心修正：直接从 task_config 获取已准备好的列表 <---
        target_symbols = self.task_config.get("target_symbols", [])
        if not target_symbols:
            self.logger.warning(f"任务 '{task_name}' 的目标股票列表为空，跳过。")
            return

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

                df = self.fetcher.fetch_daily_history(
                    ts_code, start_date, end_date, adjust
                )
                if df is not None:
                    if not df.empty:
                        self.storage.save(df, data_type, ts_code, date_col=date_col)
                    # (此处可以添加冷却期更新逻辑)
            except Exception as e:
                tqdm.write(f"❌ 处理股票 {ts_code} 时发生未知错误: {e}")
                # (此处可以添加失败记录逻辑)
