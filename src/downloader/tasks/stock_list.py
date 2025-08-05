from datetime import datetime, timedelta
from .base import BaseTaskHandler
from ..rate_limit import rate_limit


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
        target_file_path = self.storage._get_file_path("system", entity_id)

        # 门禁检查：基于文件最后修改时间
        if not self.force_run and target_file_path.exists():
            last_modified_time = datetime.fromtimestamp(
                target_file_path.stat().st_mtime
            )
            if datetime.now() - last_modified_time < timedelta(hours=interval_hours):
                self._log_debug(f"任务 '{task_name}' 处于冷却期，跳过。")
                return

        # 获取任务特定的速率限制配置
        rate_limit_config = self.task_config.get("rate_limit", {})
        calls_per_minute = rate_limit_config.get("calls_per_minute")

        # 执行下载，根据配置应用速率限制
        if calls_per_minute is not None:
            # 应用速率限制
            @rate_limit(calls_per_minute=calls_per_minute, task_key=task_name)
            def _fetch_stock_list():
                return self.fetcher.fetch_stock_list()

            df = _fetch_stock_list()
        else:
            # 使用默认的无限制调用
            df = self.fetcher.fetch_stock_list()

        if df is not None:
            # 全量覆盖保存
            self.storage.overwrite(df, "system", entity_id)
        else:
            self._log_error(f"任务 '{task_name}' 获取股票列表失败。")
