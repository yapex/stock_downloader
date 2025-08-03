from datetime import datetime, timedelta
import logging
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
        self.logger.info(f"--- 开始执行冷却期任务: '{task_name}' ---")

        interval_hours = self.task_config.get("update_interval_hours", 23)
        entity_id = task_type
        target_file_path = self.storage._get_file_path("system", entity_id)

        # 门禁检查：基于文件最后修改时间
        if not self.args.force and target_file_path.exists():
            last_modified_time = datetime.fromtimestamp(
                target_file_path.stat().st_mtime
            )
            if datetime.now() - last_modified_time < timedelta(hours=interval_hours):
                self.logger.info(f"任务 '{task_name}' 处于冷却期，跳过。")
                return

        # 执行下载
        df = self.fetcher.fetch_stock_list()
        if df is not None:
            # 全量覆盖保存
            self.storage.overwrite(df, "system", entity_id)
        else:
            self.logger.error(f"任务 '{task_name}' 获取股票列表失败。")
