import logging
from importlib.metadata import entry_points
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from queue import Queue
from typing import Dict, List, Any, Optional
from threading import Event
from time import sleep

from .fetcher import TushareFetcher
from .storage import DuckDBStorage
from .producer_pool import ProducerPool
from .consumer_pool import ConsumerPool
from .models import DownloadTask, TaskType, Priority
from .retry_policy import RetryPolicy, DEFAULT_RETRY_POLICY
from .progress_manager import progress_manager

logger = logging.getLogger(__name__)


class DownloadEngine:
    def __init__(
        self,
        config,
        fetcher: Optional[TushareFetcher] = None,
        storage: Optional[DuckDBStorage] = None,
        force_run: bool = False,
        symbols_overridden: bool = False,
        group_name: str = "default",
    ):
        self.config = config
        self.force_run = force_run
        self.symbols_overridden = symbols_overridden
        self.group_name = group_name

        # 在新架构中，不在初始化时创建数据库连接
        self._fetcher = fetcher  # 保留但不在初始化时使用
        self._storage = storage  # 保留但不在初始化时使用

        # 为了支持增量下载，需要在运行时创建存储实例
        self._runtime_storage: Optional[DuckDBStorage] = None


        # 解析配置
        downloader_config = self.config.get("downloader", {})

        # 队列管理
        self.task_queue: Optional[Queue] = None
        self.data_queue: Optional[Queue] = None
        self.producer_pool: Optional[ProducerPool] = None
        self.consumer_pool: Optional[ConsumerPool] = None

        # 从配置中获取线程池设置
        self.max_producers = downloader_config.get("max_producers", 2)
        self.max_consumers = downloader_config.get("max_consumers", 1)
        self.producer_queue_size = downloader_config.get("producer_queue_size", 1000)
        self.data_queue_size = downloader_config.get("data_queue_size", 500)

        # 重试策略
        retry_config = downloader_config.get("retry_policy", {})
        self.retry_policy = self._build_retry_policy(retry_config)

        # 任务发现（不依赖数据库连接）
        self.task_registry = self._discover_task_handlers()

        # 执行统计
        self.execution_stats = {
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "skipped_tasks": 0,
            "tasks_by_type": {},
            "success_by_symbol": {},
            "failed_symbols": [],
            "processing_start_time": None,
            "processing_end_time": None,
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

    def _build_retry_policy(self, retry_config: Dict[str, Any]) -> RetryPolicy:
        """构建重试策略"""
        if not retry_config:
            return DEFAULT_RETRY_POLICY

        return RetryPolicy(
            max_attempts=retry_config.get("max_attempts", 3),
            base_delay=retry_config.get("base_delay", 1.0),
            max_delay=retry_config.get("max_delay", 30.0),
            backoff_factor=retry_config.get("backoff_factor", 2.0),
        )

    def _setup_queues(self) -> None:
        """初始化队列和线程池"""
        logger.info("初始化队列和线程池...")

        # 创建队列
        self.task_queue = Queue(maxsize=self.producer_queue_size)
        self.data_queue = Queue(maxsize=self.data_queue_size)

        # 获取数据库路径
        db_path = self.config.get("database", {}).get("path", "data/stock.db")

        # 创建生产者池
        self.producer_pool = ProducerPool(
            max_producers=self.max_producers,
            task_queue=self.task_queue,
            data_queue=self.data_queue,
            retry_policy_config=self.retry_policy,
            dead_letter_path="logs/dead_letter.jsonl",
            fetcher=self.fetcher,
        )

        # 创建消费者池
        consumer_config = self.config.get("consumer", {})
        self.consumer_pool = ConsumerPool(
            max_consumers=self.max_consumers,
            data_queue=self.data_queue,
            batch_size=consumer_config.get("batch_size", 100),
            flush_interval=consumer_config.get("flush_interval", 30.0),
            db_path=db_path,
            max_retries=consumer_config.get("max_retries", 3),
        )

        logger.info(
            f"队列初始化完成 - 生产者: {self.max_producers}, 消费者: {self.max_consumers}"
        )

    def _build_download_tasks(
        self, target_symbols: List[str], enabled_tasks: List[Dict[str, Any]]
    ) -> List[DownloadTask]:
        """构建 DownloadTask 队列"""
        download_tasks = []

        for task_spec in enabled_tasks:
            task_type_str = task_spec.get("type")
            try:
                task_type = TaskType(task_type_str)
            except ValueError:
                logger.warning(f"未知的任务类型: {task_type_str}")
                continue

            # stock_list 类型任务不需要 symbols
            if task_type == TaskType.STOCK_LIST:
                task = DownloadTask(
                    symbol="system",
                    task_type=task_type,
                    params={"task_config": task_spec, "force_run": self.force_run},
                    priority=Priority.HIGH,  # stock_list 优先级最高
                )
                download_tasks.append(task)
            else:
                # 为每个股票创建任务
                for symbol in target_symbols:
                    # 确定日期范围
                    start_date, end_date = self._determine_date_range(task_spec, symbol)

                    if start_date > end_date:
                        continue  # 没有新数据需要下载

                    task = DownloadTask(
                        symbol=symbol,
                        task_type=task_type,
                        params={
                            "task_config": task_spec,
                            "start_date": start_date,
                            "end_date": end_date,
                            "force_run": self.force_run,
                        },
                        priority=Priority.NORMAL,
                    )
                    download_tasks.append(task)

        self.execution_stats["total_tasks"] = len(download_tasks)

        # 按类型统计任务
        for task in download_tasks:
            task_type_key = task.task_type.value
            if task_type_key not in self.execution_stats["tasks_by_type"]:
                self.execution_stats["tasks_by_type"][task_type_key] = 0
            self.execution_stats["tasks_by_type"][task_type_key] += 1

        logger.info(f"构建了 {len(download_tasks)} 个下载任务")
        for task_type, count in self.execution_stats["tasks_by_type"].items():
            logger.info(f"  - {task_type}: {count} 个任务")

        return download_tasks

    def _determine_date_range(
        self, task_spec: Dict[str, Any], symbol: str
    ) -> tuple[str, str]:
        """确定任务的日期范围"""
        # 获取任务类型
        task_type = task_spec.get("type", "")

        # 默认日期范围
        default_start = "19901219"
        end_date = datetime.now().strftime("%Y%m%d")

        # 如果强制运行，使用默认起始日期
        if self.force_run:
            return default_start, end_date

        # 尝试从存储获取最新日期以实现增量下载
        try:
            # 根据任务类型确定日期列
            date_column = self._get_date_column_for_task_type(task_type)
            
            # 获取正确的data_type（而不是task_type）
            data_type = self._get_data_type_for_task_spec(task_spec)

            # 尝试获取最新日期
            storage = self._get_runtime_storage()
            latest_date = storage.get_latest_date(data_type, symbol, date_column)
            if latest_date:
                # 从最新日期的下一天开始
                next_date = datetime.strptime(latest_date, "%Y%m%d") + timedelta(days=1)
                start_date = next_date.strftime("%Y%m%d")
                logger.debug(
                    f"增量下载 {symbol} {data_type}，从 {start_date} 开始（最新数据: {latest_date}）"
                )
                return start_date, end_date
        except Exception as e:
            logger.debug(
                f"获取 {symbol} {data_type} 最新日期失败，使用默认开始日期: {e}"
            )

        # 如果没有历史数据或获取失败，使用默认起始日期
        return default_start, end_date

    def _get_data_type_for_task_spec(self, task_spec: Dict[str, Any]) -> str:
        """根据任务配置获取正确的data_type"""
        task_type = task_spec.get("type", "")
        
        if task_type == "daily":
            # 对于daily任务，data_type需要包含adjust参数
            adjust = task_spec.get("adjust", "none")
            return f"daily_{adjust or 'none'}"
        elif task_type == "daily_basic":
            return "daily_basic"
        elif task_type == "financials":
            statement_type = task_spec.get("statement_type", "")
            return f"financials_{statement_type}" if statement_type else "financials"
        elif task_type == "stock_list":
            return "stock_list"
        else:
            # 对于其他类型，直接返回task_type
            return task_type

    def _get_date_column_for_task_type(self, task_type: str) -> str:
        """根据任务类型返回对应的日期列名"""
        # 如果是financials开头的任务类型，统一使用ann_date
        if task_type.startswith("financials"):
            return "ann_date"

        date_columns = {
            "daily": "trade_date",
            "daily_basic": "trade_date",
        }

        return date_columns.get(task_type, "trade_date")

    def _get_runtime_storage(self) -> DuckDBStorage:
        """获取运行时存储实例"""
        if not self._runtime_storage:
            db_path = self.config.get("database", {}).get("path", "data/stock.db")
            self._runtime_storage = DuckDBStorage(db_path)
        return self._runtime_storage

    def _submit_tasks_to_queue(self, tasks: List[DownloadTask]) -> None:
        """将任务提交到队列"""
        logger.info(f"开始提交 {len(tasks)} 个任务到队列...")

        submitted = 0
        for task in tasks:
            try:
                success = self.producer_pool.submit_task(task, timeout=5.0)
                if success:
                    submitted += 1
                else:
                    logger.warning(f"任务提交失败: {task.task_id}")
                    self.execution_stats["failed_tasks"] += 1
            except Exception as e:
                logger.error(f"提交任务异常 {task.task_id}: {e}")
                self.execution_stats["failed_tasks"] += 1

        logger.info(f"成功提交 {submitted}/{len(tasks)} 个任务")

    def _monitor_queues(self) -> None:
        """监控队列状态，检测任务完成"""
        logger.info("开始监控队列状态...")

        # 等待任务队列处理完成
        while True:
            task_queue_size = self.producer_pool.task_queue_size
            data_queue_size = self.consumer_pool.data_queue_size

            logger.debug(
                f"队列状态 - 任务队列: {task_queue_size}, 数据队列: {data_queue_size}"
            )

            # 如果任务队列为空且数据队列也为空，说明所有任务都完成了
            if task_queue_size == 0 and data_queue_size == 0:
                logger.info("所有队列为空，任务处理完成")
                break

            sleep(1.0)  # 每秒检查一次

        # 给消费者一些时间来处理最后的缓存数据
        logger.info("等待消费者处理完剩余数据...")
        self.consumer_pool.force_flush_all()
        sleep(2.0)

    def _monitor_queues_with_progress(self) -> None:
        """监控队列状态并更新进度条"""
        logger.info("开始监控队列状态...")

        last_completed = 0
        last_failed = 0

        # 等待任务队列处理完成
        while True:
            task_queue_size = self.producer_pool.task_queue_size
            data_queue_size = self.consumer_pool.data_queue_size

            # 获取统计信息并更新进度
            if self.producer_pool and self.consumer_pool:
                producer_stats = self.producer_pool.get_statistics()
                consumer_stats = self.consumer_pool.get_statistics()

                # 估算完成和失败的任务数
                completed_tasks = consumer_stats.get("total_batches_processed", 0)
                failed_tasks = consumer_stats.get("total_failed_operations", 0)

                # 只在数量发生变化时更新进度条
                if completed_tasks != last_completed or failed_tasks != last_failed:
                    progress_manager.update_progress(
                        completed=completed_tasks,
                        failed=failed_tasks,
                        current_task=f"队列: {task_queue_size} | 数据: {data_queue_size}",
                    )
                    last_completed = completed_tasks
                    last_failed = failed_tasks

            logger.debug(
                f"队列状态 - 任务队列: {task_queue_size}, 数据队列: {data_queue_size}"
            )

            # 如果任务队列为空且数据队列也为空，说明所有任务都完成了
            if task_queue_size == 0 and data_queue_size == 0:
                logger.info("所有队列为空，任务处理完成")
                break

            sleep(1.0)  # 每秒检查一次

        # 给消费者一些时间来处理最后的缓存数据
        logger.info("等待消费者处理完剩余数据...")
        self.consumer_pool.force_flush_all()
        sleep(2.0)

        # 最终更新进度
        if self.consumer_pool:
            consumer_stats = self.consumer_pool.get_statistics()
            progress_manager.update_progress(
                completed=consumer_stats.get("total_batches_processed", 0),
                failed=consumer_stats.get("total_failed_operations", 0),
                current_task="处理完成",
            )

    def _shutdown_pools(self) -> None:
        """关闭线程池"""
        logger.info("关闭线程池...")

        if self.producer_pool:
            self.producer_pool.stop()

        if self.consumer_pool:
            self.consumer_pool.stop()

        logger.info("线程池已关闭")

    def run(self):
        """执行基于队列的下载任务，使用进度管理器实时显示进度"""
        logger.info("启动基于队列的下载引擎...")
        self.execution_stats["processing_start_time"] = datetime.now()

        try:
            # 1. 解析配置与构建队列
            tasks = self.config.get("tasks", [])
            if not tasks:
                progress_manager.print_warning("配置文件中未找到任何任务")
                logger.warning("配置文件中未找到任何任务。")
                return

            # 获取启用的任务
            enabled_tasks = [task for task in tasks if task.get("enabled", False)]
            if not enabled_tasks:
                progress_manager.print_warning("没有启用的任务")
                logger.warning("没有启用的任务。")
                return

            # 2. 准备目标股票列表（仅针对需要股票列表的任务）
            target_symbols = self._prepare_target_symbols(enabled_tasks)

            # 3. 初始化队列和线程池（不创建数据库连接）
            self._setup_queues()

            # 4. 构建 DownloadTask 队列
            download_tasks = self._build_download_tasks(target_symbols, enabled_tasks)

            if not download_tasks:
                progress_manager.print_warning("没有任务需要执行")
                logger.warning("没有任务需要执行。")
                return

            # 初始化进度条
            progress_manager.initialize(
                total_tasks=len(download_tasks),
                description=f"处理 {self.group_name} 组任务",
            )

            # 5. 启动生产者和消费者池
            logger.info("启动生产者和消费者线程池...")
            self.producer_pool.start()
            self.consumer_pool.start()

            # 6. 按组装填任务队列
            self._submit_tasks_to_queue(download_tasks)

            # 7. 监控两队列，检测全部完成后关闭线程池
            self._monitor_queues_with_progress()

            # 8. 统计成功/失败/跳过数量供总结
            self._collect_final_statistics()

            # 完成进度条
            progress_manager.finish()

            # 显示最终统计
            final_stats = progress_manager.get_stats()
            if final_stats["failed_tasks"] > 0:
                progress_manager.print_warning(
                    f"完成处理，成功 {final_stats['completed_tasks']} 个，失败 {final_stats['failed_tasks']} 个任务 "
                    f"(详见 logs/downloader.log)"
                )
            else:
                progress_manager.print_info(
                    f"所有 {final_stats['completed_tasks']} 个任务处理完成"
                )

            logger.info("所有任务处理完成")

        except KeyboardInterrupt:
            progress_manager.print_warning("用户中断下载")
            logger.info("用户中断下载")
            raise
        except Exception as e:
            progress_manager.print_error(f"下载引擎异常: {str(e)}")
            logger.error(f"下载引擎执行异常: {e}", exc_info=True)
            raise
        finally:
            # 确保资源清理
            self._shutdown_pools()
            self.execution_stats["processing_end_time"] = datetime.now()

            # 输出最终统计到日志
            self._print_final_summary()

    def _prepare_target_symbols(self, enabled_tasks: List[Dict[str, Any]]) -> List[str]:
        """准备目标股票列表"""
        from .utils import normalize_stock_code
        
        # 检查是否有需要股票列表的任务
        needs_symbols = any(task.get("type") != "stock_list" for task in enabled_tasks)

        if not needs_symbols:
            logger.info("所有任务都是系统级任务，无需股票列表")
            return []

        downloader_config = self.config.get("downloader", {})
        symbols_config = downloader_config.get("symbols", [])
        target_symbols = []

        if isinstance(symbols_config, list):
            # 标准化股票代码格式
            target_symbols = []
            for symbol in symbols_config:
                try:
                    normalized_symbol = normalize_stock_code(symbol)
                    target_symbols.append(normalized_symbol)
                except (ValueError, TypeError) as e:
                    logger.warning(f"跳过无效的股票代码 '{symbol}': {e}")
                    continue
            logger.info(f"使用配置指定的 {len(target_symbols)} 只股票")
        elif symbols_config == "all":
            # 这里需要数据库连接来获取股票列表
            # 由于新架构延迟创建连接，这里先返回空列表
            # TODO: 在实际运行时从数据库获取
            logger.warning("配置为 'all' 模式，但新架构中延迟获取股票列表")
            target_symbols = []
        else:
            logger.error(f"未知的 symbols 配置: {symbols_config}")

        return target_symbols

    def _collect_final_statistics(self) -> None:
        """收集最终统计信息"""
        if self.producer_pool:
            producer_stats = self.producer_pool.get_statistics()
            logger.info(
                f"生产者统计: 处理任务 {producer_stats.get('tasks_processed', 0)} 个"
            )

        if self.consumer_pool:
            consumer_stats = self.consumer_pool.get_statistics()
            logger.info(
                f"消费者统计: 处理批次 {consumer_stats.get('total_batches_processed', 0)} 个"
            )
            logger.info(
                f"消费者统计: 刷新操作 {consumer_stats.get('total_flush_operations', 0)} 次"
            )

            # 更新执行统计
            self.execution_stats["successful_tasks"] = consumer_stats.get(
                "total_batches_processed", 0
            )
            self.execution_stats["failed_tasks"] = consumer_stats.get(
                "total_failed_operations", 0
            )

    def _print_final_summary(self) -> None:
        """输出最终汇总信息"""
        stats = self.execution_stats

        processing_time = None
        if stats["processing_start_time"] and stats["processing_end_time"]:
            processing_time = (
                stats["processing_end_time"] - stats["processing_start_time"]
            )

        logger.info("=" * 60)
        logger.info("下载引擎执行汇总:")
        logger.info(f"  总任务数: {stats['total_tasks']}")
        logger.info(f"  成功任务数: {stats['successful_tasks']}")
        logger.info(f"  失败任务数: {stats['failed_tasks']}")
        logger.info(f"  跳过任务数: {stats['skipped_tasks']}")

        if stats["tasks_by_type"]:
            logger.info("  按类型统计:")
            for task_type, count in stats["tasks_by_type"].items():
                logger.info(f"    {task_type}: {count}")

        if processing_time:
            logger.info(f"  处理耗时: {processing_time}")

        if stats["failed_symbols"]:
            logger.info(f"  失败股票: {len(stats['failed_symbols'])} 只")
            for symbol in stats["failed_symbols"][:10]:  # 只显示前10个
                logger.info(f"    {symbol}")
            if len(stats["failed_symbols"]) > 10:
                logger.info(f"    ... 还有 {len(stats['failed_symbols']) - 10} 只")

        logger.info("=" * 60)

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
                    failed_tasks.append(task_spec.get("name"))

        return failed_tasks

    def _dispatch_task(self, task_spec: dict, defaults: dict, **kwargs):
        task_type = task_spec.get("type")
        handler_class = self.task_registry.get(task_type)

        if handler_class:
            final_task_config = defaults.copy()
            final_task_config.update(task_spec)
            try:
                handler_instance = handler_class(
                    final_task_config,
                    self.fetcher,
                    self.storage,
                    force_run=self.force_run,
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
                logger.debug(
                    f"目标股票列表 {len(target_set)} 与配置股票列表 {len(configured_set)} 不一致，不是完整组运行"
                )
                return False
        else:
            # 未知的配置格式
            logger.warning(f"未知的 symbols 配置格式: {configured_symbols}")
            return False

    def get_execution_stats(self):
        """获取执行统计信息"""
        return self.execution_stats.copy()

    # 属性访问器，保持向后兼容性
    @property
    def fetcher(self) -> Optional[TushareFetcher]:
        """获取 TushareFetcher 实例（向后兼容）"""
        return self._fetcher

    @property
    def storage(self) -> Optional[DuckDBStorage]:
        """获取 DuckDBStorage 实例（向后兼容）"""
        return self._storage
