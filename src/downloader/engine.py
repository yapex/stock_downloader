import logging
from importlib.metadata import entry_points
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from .fetcher import TushareFetcher
from .storage import DuckDBStorage
from .buffer_pool import DataBufferPool
from .utils import record_failed_task

logger = logging.getLogger(__name__)


class DownloadEngine:
    def __init__(
        self, 
        config, 
        fetcher: TushareFetcher, 
        storage: DuckDBStorage, 
        force_run: bool = False,
        symbols_overridden: bool = False,
        group_name: str = "default"
    ):
        self.config = config
        self.fetcher = fetcher
        self.storage = storage
        self.force_run = force_run
        self.symbols_overridden = symbols_overridden  # 记录股票列表是否被命令行参数覆盖
        self.group_name = group_name  # 记录组名
        
        # 初始化缓冲池
        buffer_config = config.get("buffer_pool", {})
        self.buffer_pool = DataBufferPool(
            storage=self.storage,
            max_buffer_size=buffer_config.get("max_buffer_size", 500),
            max_buffer_memory_mb=buffer_config.get("max_buffer_memory_mb", 50),
            flush_interval_seconds=buffer_config.get("flush_interval_seconds", 15),
            auto_flush=buffer_config.get("auto_flush", True)
        )
        
        self.task_registry = self._discover_task_handlers()
        # 执行统计
        self.execution_stats = {
            'total_symbols': 0,
            'successful_symbols': [],
            'failed_symbols': [],
            'skipped_symbols': [],
            'failed_tasks': []
        }

    def _discover_task_handlers(self) -> dict:
        registry = {}
        try:
            handlers_eps = entry_points(group="stock_downloader.task_handlers")
            logger.debug(f"发现了 {len(handlers_eps)} 个任务处理器入口点。")
            for ep in handlers_eps:
                registry[ep.name] = ep.load()
                logger.debug(f"  - 已注册处理器: '{ep.name}'")
        except Exception as e:
            logger.error(f"自动发现任务处理器时发生错误: {e}", exc_info=True)
        return registry

    def run(self):
        logger.debug("下载引擎启动...")
        tasks = self.config.get("tasks", [])
        defaults = self.config.get("defaults", {})
        downloader_config = self.config.get("downloader", {})
        
        # 使用实例的组名
        group_name = self.group_name
        
        if not tasks:
            logger.warning("配置文件中未找到任何任务。")
            return
            
        # ===================================================================
        #   运行组前，计算 is_full_group_run 条件
        # ===================================================================
        
        # 1. 检查是否有命令行参数过滤（symbols_filter_applied）
        symbols_filter_applied = self.symbols_overridden
        
        # 2. 检查所有任务是否都被启用（all_tasks_enabled）
        all_tasks_enabled = all(task.get("enabled", False) for task in tasks)
        
        # 3. 计算是否为完整组运行
        is_full_group_run = not symbols_filter_applied and all_tasks_enabled
        
        logger.debug(f"完整组运行检查: symbols_filter_applied={symbols_filter_applied}, "
                    f"all_tasks_enabled={all_tasks_enabled}, is_full_group_run={is_full_group_run}")
        
        # 跟踪失败的任务
        failed_tasks = []

        # ===================================================================
        #           核心修正：股票列表的确定逻
        # ===================================================================

        # 1. 执行 stock_list 任务（如果存在且需要更新），
        # 以确保 "all" 模式的数据源是最新的
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
            logger.debug(
                f"将使用配置文件中指定的 {len(target_symbols)} 只股票作为目标。"
            )
        elif symbols_config == "all":
            try:
                if self.storage.table_exists("system", "stock_list"):
                    df_list = self.storage.query("system", "stock_list")
                    if not df_list.empty and "ts_code" in df_list.columns:
                        target_symbols = df_list["ts_code"].tolist()
                        logger.debug(
                            f"已从数据库加载 {len(target_symbols)} 只全市场股票作为目标。"
                        )
                    else:
                        logger.warning(
                            "股票列表数据为空或缺少 ts_code 列。请确保'更新A股列表'任务已成功运行。"
                        )
                else:
                    logger.warning(
                        "配置为'all'但股票列表表不存在。请确保'更新A股列表'任务已成功运行。"
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

        # Check for concurrency configuration
        max_workers = downloader_config.get("max_concurrent_tasks", 1)

        if max_workers > 1:
            logger.debug(
                f"使用 {max_workers} 个线程并发执行 {len(data_driven_tasks)} 个任务"
            )
            failed_tasks = self._run_tasks_concurrently(
                data_driven_tasks, defaults, target_symbols, max_workers
            )
        else:
            logger.debug(f"顺序执行 {len(data_driven_tasks)} 个任务")
            for task_spec in data_driven_tasks:
                try:
                    # 将最终确定的股票列表作为上下文传递
                    context = {"target_symbols": target_symbols}
                    self._dispatch_task(task_spec, defaults, **context)
                except Exception as e:
                    logger.error(f"任务 '{task_spec.get('name')}' 执行失败: {e}")
                    failed_tasks.append(task_spec.get('name'))

        logger.debug("下载引擎所有任务执行完毕。")
        
        # 确保缓冲池数据全部写入
        logger.info("正在刷新缓冲池数据...")
        self.buffer_pool.flush_all()
        self.buffer_pool.shutdown()
        logger.info("缓冲池数据刷新完成")
        
        # 更新执行统计
        self.execution_stats['total_symbols'] = len(target_symbols)
        self.execution_stats['failed_tasks'] = failed_tasks
        
        # 输出缓冲池统计信息
        stats = self.buffer_pool.get_stats()
        logger.info(f"缓冲池统计 - 总缓冲: {stats['total_buffered']}, 总刷新: {stats['total_flushed']}, 当前缓冲: {stats['current_buffer_size']}")
        logger.info(f"缓冲池统计 - 刷新次数: {stats['flush_count']}, 最后刷新: {stats['last_flush_time']}")
        logger.info(f"缓冲池统计 - 内存使用: {stats['current_buffer_memory_mb']:.2f} MB")
        
        # ===================================================================
        #   组内任务完成后，根据 is_full_group_run 和任务成功情况更新 last_run_ts
        # ===================================================================
        
        # 检查所有任务是否都成功（没有失败任务）
        all_success = len(failed_tasks) == 0
        
        if is_full_group_run and all_success:
            now = datetime.now()
            self.storage.set_last_run(group_name, now)
            logger.info(f"完整组运行完成且所有任务成功，已更新 {group_name} 的 last_run_ts: {now}")
        else:
            if not is_full_group_run:
                logger.debug(f"非完整组运行，不更新 {group_name} 的 last_run_ts")
            elif not all_success:
                logger.debug(f"有任务失败 {failed_tasks}，不更新 {group_name} 的 last_run_ts")

    def _run_tasks_concurrently(self, tasks, defaults, target_symbols, max_workers):
        """Execute tasks concurrently using a thread pool."""
        failed_tasks = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks to the executor
            future_to_task = {
                executor.submit(
                    self._dispatch_task,
                    task_spec,
                    defaults,
                    **{"target_symbols": target_symbols},
                ): task_spec
                for task_spec in tasks
            }

            # Wait for all tasks to complete and handle results
            for future in as_completed(future_to_task):
                task_spec = future_to_task[future]
                try:
                    # This will re-raise any exception caught in the thread
                    future.result()
                    logger.debug(f"任务 '{task_spec['name']}' 完成")
                except Exception as e:
                    logger.error(
                        f"任务 '{task_spec['name']}' 执行失败: {e}", exc_info=True
                    )
                    failed_tasks.append(task_spec.get('name'))
        
        return failed_tasks

    def _dispatch_task(self, task_spec: dict, defaults: dict, **kwargs):
        task_type = task_spec.get("type")
        handler_class = self.task_registry.get(task_type)

        if handler_class:
            final_task_config = defaults.copy()
            final_task_config.update(task_spec)
            try:
                handler_instance = handler_class(
                    final_task_config, self.fetcher, self.storage, self.buffer_pool, self.force_run
                )
                handler_instance.execute(**kwargs)
            except Exception as e:
                logger.error(
                    f"执行任务 '{task_spec.get('name')}' 时发生错误: {e}", exc_info=True
                )
                # Re-raise the exception so it can be caught by the concurrent executor
                raise
        else:
            logger.warning(
                f"未找到类型为 '{task_type}' 的任务处理器，"
                f"已跳过任务 '{task_spec.get('name')}'。"
            )

    def _is_full_group_run(self, configured_symbols, target_symbols, failed_tasks):
        """
        判定是否为完整组运行。
        
        完整组运行的判定条件：
        1. DownloadEngine 未指定 --symbols 或 --group 以外的过滤，执行的是该组配置里声明的 *全部* 目标股票。
        2. 所有组内任务都被调度且未中途失败。
        
        Args:
            configured_symbols: 配置中声明的完整股票池
            target_symbols: 实际执行的目标股票列表
            failed_tasks: 失败的任务列表
        
        Returns:
            bool: 是否为完整组运行
        """
        # 条件 2: 没有任务失败
        if failed_tasks:
            logger.debug(f"由于任务失败 {failed_tasks}，不是完整组运行")
            return False
        
        # 条件 1: 检查是否有命令行参数覆盖
        if self.symbols_overridden:
            logger.debug("股票列表被命令行参数覆盖，不是完整组运行")
            return False
        
        # 条件 1: 执行的股票列表应该是配置中声明的完整股票池
        if configured_symbols == "all":
            # 如果配置为 "all"，那么实际执行的股票列表应该是从数据库加载的全市场股票
            # 这里我们认为这是完整组运行（因为是从配置而不是命令行参数得来的）
            logger.debug("配置为 'all'，认为是完整组运行")
            return True
        elif isinstance(configured_symbols, list):
            # 如果配置为列表，那么实际执行的股票列表应该和配置一致
            # 如果 target_symbols 和 configured_symbols 一致，则认为是完整组运行
            configured_set = set(str(s) for s in configured_symbols)
            target_set = set(str(s) for s in target_symbols)
            
            if configured_set == target_set:
                logger.debug("目标股票列表与配置一致，认为是完整组运行")
                return True
            else:
                logger.debug(f"目标股票列表 {len(target_set)} 与配置股票列表 {len(configured_set)} 不一致，不是完整组运行")
                return False
        else:
            # 未知的配置格式
            logger.warning(f"未知的 symbols 配置格式: {configured_symbols}")
            return False
    
    def get_execution_stats(self):
        """获取执行统计信息"""
        return self.execution_stats.copy()
