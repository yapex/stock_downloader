from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
from .base import BaseTaskHandler, logging


class DailyBasicTaskHandler(BaseTaskHandler):
    """
    处理器插件：负责增量下载每日指标数据 (daily_basic)。
    此版本采用正确的“以股票为中心”的数据模型。
    """

    def execute(self, **kwargs):
        target_symbols = kwargs.get("target_symbols")
        if not target_symbols:
            self.logger.warning(
                f"任务 '{self.task_config['name']}' 未收到目标股票列表，跳过。"
            )
            return

        task_name = self.task_config["name"]
        data_type = "daily_basic"
        date_col = self.task_config.get("date_col", "trade_date")

        self.logger.info(
            f"--- 开始为 {len(target_symbols)} 只股票执行每日指标增量任务: '{task_name}' ---"
        )

        progress_bar = tqdm(target_symbols, desc=f"执行: {task_name}")
        for ts_code in progress_bar:
            progress_bar.set_description(f"处理: {data_type}_{ts_code}")
            try:
                # 1. 检查单只股票的最新日期
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

                # 2. 调用新的 Fetcher 方法，一次性获取该股票的所有新数据
                df = self.fetcher.fetch_daily_basic(ts_code, start_date, end_date)

                # 3. 一次性增量保存
                if df is not None and not df.empty:
                    self.storage.save(df, data_type, ts_code, date_col=date_col)
                # (可以加入失败记录逻辑)

            except Exception as e:
                tqdm.write(f"❌ 处理股票 {ts_code} 的每日指标时发生未知错误: {e}")
                # (可以加入失败记录逻辑)
