import logging
from importlib.metadata import entry_points
import pandas as pd
from datetime import datetime

from .fetcher import TushareFetcher
from .storage import ParquetStorage
import argparse

logger = logging.getLogger(__name__)


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

    def run(self):
        logger.info("下载引擎启动...")
        tasks = self.config.get("tasks", [])
        defaults = self.config.get("defaults", {})
        downloader_config = self.config.get("downloader", {})

        if not tasks:
            logger.warning("配置文件中未找到任何任务。")
            return

        # ===================================================================
        #           核心修正：股票列表的确定逻辑
        # ===================================================================

        # 1. 执行 stock_list 任务（如果存在且需要更新），以确保 "all" 模式的数据源是最新的
        stock_list_tasks = [
            t for t in tasks if t.get("type") == "stock_list" and t.get("enabled")
        ]
        for task_spec in stock_list_tasks:
            self._dispatch_task(
                task_spec, defaults, target_symbols=None
            )  # stock_list 任务不需要 symbols

        # 2. 一次性、权威地确定本次运行的目标股票列表
        symbols_config = downloader_config.get("symbols", [])
        target_symbols = []
        if isinstance(symbols_config, list):
            target_symbols = symbols_config
            logger.info(
                f"将使用配置文件中指定的 {len(target_symbols)} 只股票作为目标。"
            )
        elif symbols_config == "all":
            try:
                stock_list_file = self.storage._get_file_path("system", "stock_list")
                if stock_list_file.exists():
                    df_list = pd.read_parquet(stock_list_file)
                    target_symbols = df_list["ts_code"].tolist()
                    logger.info(
                        f"已从本地文件加载 {len(target_symbols)} 只全市场股票作为目标。"
                    )
                else:
                    logger.warning(
                        "配置为'all'但股票列表文件不存在。请确保'更新A股列表'任务已成功运行。"
                    )
            except Exception as e:
                logger.error(f"准备'all'股票列表时出错: {e}")
        else:
            logger.error(f"downloader.symbols 配置格式不正确: {symbols_config}")

        if not target_symbols:
            logger.warning("最终目标股票列表为空，依赖股票列表的任务将被跳过。")

        # 3. 执行所有其他数据驱动的任务
        data_driven_tasks = [
            t for t in tasks if t.get("type") != "stock_list" and t.get("enabled")
        ]
        for task_spec in data_driven_tasks:
            # 将最终确定的股票列表作为上下文传递
            context = {"target_symbols": target_symbols}
            self._dispatch_task(task_spec, defaults, **context)

        logger.info("下载引擎所有任务执行完毕。")

    def _dispatch_task(self, task_spec: dict, defaults: dict, **kwargs):
        task_type = task_spec.get("type")
        handler_class = self.task_registry.get(task_type)

        if handler_class:
            final_task_config = defaults.copy()
            final_task_config.update(task_spec)
            try:
                handler_instance = handler_class(
                    final_task_config, self.fetcher, self.storage, self.args
                )
                handler_instance.execute(**kwargs)
            except Exception as e:
                logger.error(
                    f"执行任务 '{task_spec.get('name')}' 时发生错误: {e}", exc_info=True
                )
        else:
            logger.warning(
                f"未找到类型为 '{task_type}' 的任务处理器，已跳过任务 '{task_spec.get('name')}'。"
            )
