from datetime import datetime, timedelta
from .base import BaseTaskHandler


class StockListTaskHandler(BaseTaskHandler):
    """
    处理器插件：负责'冷却期'全量更新股票列表的任务。
    """

    def execute(self, **kwargs):
        """
        执行股票列表的下载和全量覆盖保存。
        它会忽略传入的 kwargs (如 target_symbols)。
        """
        task_name = self.task_config["name"]
        task_type = self.task_config["type"]
        self._log_debug(f"--- 开始执行冷却期任务: '{task_name}' ---")

        interval_hours = self.task_config.get("update_interval_hours", 23)
        entity_id = task_type

        # 门禁检查：基于数据库中的最后更新时间
        if not self.force_run:
            last_updated = self.storage.get_table_last_updated("system", entity_id)
            if last_updated and datetime.now() - last_updated < timedelta(hours=interval_hours):
                self._log_debug(f"任务 '{task_name}' 处于冷却期，跳过。")
                return

        # 获取任务特定的速率限制配置
        # 限流现在由fetcher方法中的ratelimit库处理
        df = self.fetcher.fetch_stock_list()

        if df is not None:
            # 全量覆盖保存
            self.storage.overwrite(df, "system", entity_id)
        else:
            self._log_error(f"任务 '{task_name}' 获取股票列表失败。")
