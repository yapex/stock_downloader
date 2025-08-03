import logging
from importlib.metadata import entry_points
from tqdm import tqdm
import argparse
import pandas as pd  # 需要导入pandas
from datetime import datetime

from .fetcher import TushareFetcher
from .storage import ParquetStorage

logger = logging.getLogger(__name__)

# record_failed_task 保持不变


class DownloadEngine:
    def __init__(
        self,
        config: dict,
        fetcher: TushareFetcher,
        storage: ParquetStorage,
        args: argparse.Namespace,
    ):
        self.config = config
        self.fetcher = fetcher
        self.storage = storage
        self.args = args
        self.task_registry = self._discover_task_handlers()

    def _discover_task_handlers(self) -> dict:
        # ... (此方法保持不变)
        registry = {}
        try:
            handlers_eps = entry_points(group="stock_downloader.task_handlers")
            logger.info(f"发现了 {len(handlers_eps)} 个任务处理器入口点。")
            for ep in handlers_eps:
                registry[ep.name] = ep.load()
                logger.info(f"  - 已注册处理器: '{ep.name}'")
        except Exception as e:
            logger.error(f"自动发现任务处理器时发生错误: {e}", exc_info=True)
        return registry

    def _get_target_symbols(self) -> list:
        """根据全局配置，确定最终需要下载的股票列表。"""
        symbols_config = self.config.get("downloader", {}).get("symbols", [])
        if symbols_config == "all":
            try:
                stock_list_file = self.storage._get_file_path("system", "stock_list")
                if stock_list_file.exists():
                    df_list = pd.read_parquet(stock_list_file)
                    logger.info(f"已从本地文件加载 {len(df_list)} 只股票作为目标。")
                    return df_list["ts_code"].tolist()
                else:
                    logger.warning("配置为'all'但未找到股票列表文件，将尝试实时获取。")
                    # 降级：实时获取
                    df_list = self.fetcher.fetch_stock_list()
                    if df_list is not None:
                        logger.info(f"已实时获取 {len(df_list)} 只股票作为目标。")
                        return df_list["ts_code"].tolist()
                    return []
            except Exception as e:
                logger.error(f"读取股票列表文件失败: {e}")
                return []
        return symbols_config

    def run(self):
        """
        执行整个下载流程的主方法。
        """
        logger.info("下载引擎启动...")
        tasks = self.config.get("tasks", [])
        defaults = self.config.get("defaults", {})

        if not tasks:
            return

        # --- 阶段一：执行非增量任务 (如 stock_list) ---
        cooldown_tasks = [
            t
            for t in tasks
            if t.get("update_strategy") == "cooldown" and t.get("enabled")
        ]
        for task_spec in cooldown_tasks:
            self._dispatch_task(task_spec, defaults)

        # --- 阶段二：执行增量任务 ---
        target_symbols = self._get_target_symbols()
        if not target_symbols:
            logger.warning("目标股票列表为空，增量任务将被跳过。")
            return

        incremental_tasks = [
            t
            for t in tasks
            if t.get("update_strategy") == "incremental" and t.get("enabled")
        ]
        for task_spec in incremental_tasks:
            # ---> 核心修正：将 symbols 列表注入到每个任务的配置中 <---
            task_spec["target_symbols"] = target_symbols
            self._dispatch_task(task_spec, defaults)

        logger.info("下载引擎所有任务执行完毕。")

    def _dispatch_task(self, task_spec: dict, defaults: dict):
        """分发单个任务到对应的处理器。"""
        task_type = task_spec.get("type")
        handler_class = self.task_registry.get(task_type)

        if handler_class:
            final_task_config = defaults.copy()
            final_task_config.update(task_spec)
            try:
                handler_instance = handler_class(
                    final_task_config, self.fetcher, self.storage, self.args
                )
                handler_instance.execute()
            except Exception as e:
                logger.error(
                    f"执行任务 '{task_spec.get('name')}' 时发生错误: {e}", exc_info=True
                )
        else:
            logger.warning(
                f"未找到类型为 '{task_type}' 的任务处理器，已跳过任务 '{task_spec.get('name')}'。"
            )
