import logging
import time
from importlib.metadata import entry_points
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from queue import Queue, Full
from typing import Dict, List, Any, Optional
from time import sleep

from .fetcher import TushareFetcher
from .fetcher_factory import get_fetcher, get_fetcher_instance_info
from .storage import DuckDBStorage
from .storage_factory import get_storage
from .producer import Producer
from .consumer_pool import ConsumerPool
from .models import DownloadTask, TaskType, Priority
from .retry_policy import RetryPolicy, DEFAULT_RETRY_POLICY
from .progress_events import (
    progress_event_manager, start_phase, end_phase, update_total, ProgressPhase
)

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
        
        # 使用单例模式的fetcher，确保所有Producer共享同一个实例
        self._singleton_fetcher: Optional[TushareFetcher] = None


        # 解析配置
        downloader_config = self.config.get("downloader", {})

        # 队列管理
        self.task_queue: Optional[Queue] = None
        self.data_queue: Optional[Queue] = None
        self.consumer_pool: Optional[ConsumerPool] = None

        # 从配置中获取线程池设置
        self.max_producers = downloader_config.get("max_producers", 1)
        self.max_consumers = downloader_config.get("max_consumers", 1)
        
        # 智能队列大小配置：基于生产者数量和API限流设置合理的队列大小
        # 考虑到API限流是50次/分钟，设置较小的队列避免大量任务堆积
        self.producer_queue_size = downloader_config.get("producer_queue_size", min(100, self.max_producers * 20))
        self.data_queue_size = downloader_config.get("data_queue_size", 200)
        
        # 批量提交配置
        self.batch_submit_size = downloader_config.get("batch_submit_size", 10)  # 每批提交的任务数
        self.submit_delay = downloader_config.get("submit_delay", 1.0)  # 批次间延迟（秒）
        self.queue_high_watermark = downloader_config.get("queue_high_watermark", 0.8)  # 队列高水位线

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
        logger.info(
            f"[引擎初始化] 开始初始化队列和线程池 - "
            f"生产者队列大小: {self.producer_queue_size}, 数据队列大小: {self.data_queue_size}"
        )

        # 创建共享队列
        self.task_queue = Queue(maxsize=self.producer_queue_size)
        self.data_queue = Queue(maxsize=self.data_queue_size)
        
        logger.info(
            f"[引擎初始化] 队列创建完成 - "
            f"任务队列: {self.task_queue.qsize()}/{self.producer_queue_size}, "
            f"数据队列: {self.data_queue.qsize()}/{self.data_queue_size}"
        )

        # 获取数据库路径
        db_path = self.config.get("database", {}).get("path", "data/stock.db")
        logger.info(f"[引擎初始化] 数据库路径: {db_path}")

        # 初始化生产者列表（动态创建）
        self.producers = []
        logger.info("[引擎初始化] 生产者列表已初始化")

        # 创建消费者池
        consumer_config = self.config.get("consumer", {})
        batch_size = consumer_config.get("batch_size", 100)
        flush_interval = consumer_config.get("flush_interval", 30.0)
        max_retries = consumer_config.get("max_retries", 3)
        
        logger.info(
            f"[引擎初始化] 创建消费者池 - "
            f"最大消费者: {self.max_consumers}, 批次大小: {batch_size}, "
            f"刷新间隔: {flush_interval}s, 最大重试: {max_retries}"
        )
        
        self.consumer_pool = ConsumerPool(
            max_consumers=self.max_consumers,
            data_queue=self.data_queue,
            batch_size=batch_size,
            flush_interval=flush_interval,
            db_path=db_path,
            max_retries=max_retries,
        )

        logger.info(
            f"[引擎初始化] 队列初始化完成 - "
            f"最大生产者: {self.max_producers}, 最大消费者: {self.max_consumers}, "
            f"Fetcher ID: {id(self.fetcher)}"
        )

    def _build_download_tasks(
        self, target_symbols: List[str], enabled_tasks: List[Dict[str, Any]]
    ) -> List[DownloadTask]:
        """构建 DownloadTask 队列"""
        download_tasks = []

        # 分离系统任务和业务任务
        system_tasks = []
        business_tasks = []
        
        for task_spec in enabled_tasks:
            task_type_str = task_spec.get("type")
            try:
                task_type = TaskType(task_type_str)
            except ValueError:
                logger.warning(f"未知的任务类型: {task_type_str}")
                continue
                
            if task_type == TaskType.STOCK_LIST:
                system_tasks.append(task_spec)
            else:
                business_tasks.append(task_spec)
        
        # 处理系统任务
        for task_spec in system_tasks:
            task = DownloadTask(
                symbol="system",
                task_type=TaskType.STOCK_LIST,
                params={"task_config": task_spec, "force_run": self.force_run},
                priority=Priority.HIGH,  # stock_list 优先级最高
            )
            download_tasks.append(task)
        
        # 批量处理业务任务
        if business_tasks and target_symbols:
            logger.info(f"开始批量构建 {len(business_tasks)} 种任务类型，{len(target_symbols)} 只股票的任务")
            start_time = time.time()
            
            # 按任务类型批量获取最新日期
            latest_dates_cache = {}
            for task_spec in business_tasks:
                data_type = self._get_data_type_for_task_spec(task_spec)
                date_col = self._get_date_column_for_task_type(task_spec.get("type", ""))
                
                if not self.force_run:
                    try:
                        storage = self._get_runtime_storage()
                        # 将data_type映射到内部数据类型
                        if data_type == "daily_basic" or data_type.startswith("fundamental"):
                            internal_type = "fundamental"
                        elif data_type.startswith("daily"):
                            internal_type = "daily"
                        elif data_type.startswith("financials"):
                            internal_type = "financial"
                        else:
                            internal_type = "daily"  # 默认
                        
                        latest_dates = storage.batch_get_latest_dates(target_symbols, internal_type)
                        latest_dates_cache[(data_type, date_col)] = latest_dates
                        logger.debug(f"批量获取 {data_type} 类型最新日期: {len(latest_dates)} 个有效记录")
                    except Exception as e:
                        logger.warning(f"批量获取 {data_type} 最新日期失败，将使用默认日期: {e}")
                        latest_dates_cache[(data_type, date_col)] = {}
                else:
                    latest_dates_cache[(data_type, date_col)] = {}
            
            # 为每个股票和任务类型创建任务
            for task_spec in business_tasks:
                task_type_str = task_spec.get("type")
                task_type = TaskType(task_type_str)
                data_type = self._get_data_type_for_task_spec(task_spec)
                date_col = self._get_date_column_for_task_type(task_type_str)
                latest_dates = latest_dates_cache.get((data_type, date_col), {})
                
                for symbol in target_symbols:
                    # 使用缓存的最新日期确定日期范围
                    start_date, end_date = self._determine_date_range_with_cache(
                        task_spec, symbol, latest_dates.get(symbol)
                    )

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
            
            elapsed = time.time() - start_time
            logger.info(f"批量任务构建完成，耗时 {elapsed:.2f}s")

        self.execution_stats["total_tasks"] = len(download_tasks)

        # 按类型统计任务
        for task in download_tasks:
            # 对于财务任务，使用更具体的data_type进行统计
            if task.task_type == TaskType.FINANCIALS:
                task_config = task.params.get("task_config", {})
                data_type = self._get_data_type_for_task_spec(task_config)
                task_type_key = data_type
            else:
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
            self._get_date_column_for_task_type(task_type)
            
            # 获取正确的data_type（而不是task_type）
            data_type = self._get_data_type_for_task_spec(task_spec)

            # 尝试获取最新日期
            storage = self._get_runtime_storage()
            # 将data_type映射到内部数据类型
            if data_type == "daily_basic" or data_type.startswith("fundamental"):
                internal_type = "fundamental"
            elif data_type.startswith("daily"):
                internal_type = "daily"
            elif data_type.startswith("financials"):
                internal_type = "financial"
            else:
                internal_type = "daily"  # 默认
            
            latest_date = storage.get_latest_date_by_stock(symbol, internal_type)
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

    def _determine_date_range_with_cache(
        self, task_spec: Dict[str, Any], symbol: str, cached_latest_date: str = None
    ) -> tuple[str, str]:
        """使用缓存的最新日期确定任务的日期范围，提升性能"""
        # 默认日期范围
        default_start = "19901219"
        end_date = datetime.now().strftime("%Y%m%d")

        # 如果强制运行，使用默认起始日期
        if self.force_run:
            return default_start, end_date

        # 使用缓存的最新日期
        if cached_latest_date:
            try:
                # 从最新日期的下一天开始
                next_date = datetime.strptime(cached_latest_date, "%Y%m%d") + timedelta(days=1)
                start_date = next_date.strftime("%Y%m%d")
                data_type = self._get_data_type_for_task_spec(task_spec)
                logger.debug(
                    f"增量下载 {symbol} {data_type}，从 {start_date} 开始（缓存最新数据: {cached_latest_date}）"
                )
                return start_date, end_date
            except Exception as e:
                logger.debug(
                    f"解析缓存日期 {cached_latest_date} 失败，使用默认开始日期: {e}"
                )

        # 如果没有缓存数据，使用默认起始日期
        return default_start, end_date

    def _get_data_type_for_task_spec(self, task_spec: Dict[str, Any]) -> str:
        """根据任务配置获取正确的data_type，适应新的分区表结构"""
        task_type = task_spec.get("type", "")
        
        if task_type == "daily":
            # 日线数据统一存储在 daily_data 表中
            return "daily"
        elif task_type == "daily_basic":
            # 每日指标数据存储在 daily_data 表中
            return "daily_basic"
        elif task_type == "financials":
            # 财务数据根据报表类型存储在不同表中
            statement_type = task_spec.get("statement_type", "")
            if statement_type in ["income", "balancesheet", "cashflow"]:
                return statement_type
            else:
                return "financials"
        elif task_type == "stock_list":
            return "stock_list"
        else:
            # 对于其他类型，直接返回task_type
            return task_type

    def _get_date_column_for_task_type(self, task_type: str) -> str:
        """根据任务类型返回对应的日期列名"""
        # 财务报表数据使用 ann_date
        if task_type in ["income", "balancesheet", "cashflow", "financials"]:
            return "ann_date"

        date_columns = {
            "daily": "trade_date",
            "daily_basic": "trade_date",
            "stock_list": "list_date",
        }

        return date_columns.get(task_type, "trade_date")

    def _get_runtime_storage(self) -> DuckDBStorage:
        """获取运行时存储实例"""
        if not self._runtime_storage:
            db_path = self.config.get("database", {}).get("path", "data/stock.db")
            self._runtime_storage = get_storage(db_path)
        return self._runtime_storage

    def _submit_tasks_to_queue(self, tasks: List[DownloadTask]) -> None:
        """使用多个生产者并行提交任务到共享队列"""
        logger.info(
            f"[引擎任务提交] 开始并行提交 {len(tasks)} 个任务到队列 - "
            f"队列当前大小: {self.task_queue.qsize()}, 最大容量: {self.producer_queue_size}"
        )
        
        if not tasks:
            logger.info("[引擎任务提交] 没有任务需要提交")
            return
        
        # 清空之前的生产者列表
        self.producers.clear()
        
        # 检查任务类型，决定生产者数量
        has_system_tasks = any(task.task_type == TaskType.STOCK_LIST for task in tasks)
        
        if has_system_tasks:
            # 系统表任务使用单个生产者
            num_producers = 1
            logger.info(
                f"[引擎任务提交] 检测到系统表任务，使用单线程处理 - "
                f"Fetcher ID: {id(self.fetcher)}"
            )
        else:
            # 业务表任务使用多个生产者
            num_producers = min(self.max_producers, max(1, len(tasks) // 50))
            logger.info(
                f"[引擎任务提交] 业务表任务，创建 {num_producers} 个生产者并行处理 - "
                f"最大生产者数: {self.max_producers}, 任务数: {len(tasks)}, Fetcher ID: {id(self.fetcher)}"
            )
        
        # 创建多个生产者实例，共享同一个队列
        for i in range(num_producers):
            producer = Producer(
                task_queue=self.task_queue,
                data_queue=self.data_queue,
                fetcher=self.fetcher
            )
            self.producers.append(producer)
            logger.info(
                f"[引擎任务提交] 生产者 {i+1} 已创建 - "
                f"Producer Fetcher ID: {id(producer.fetcher)}"
            )
        
        try:
            # 启动所有生产者
            for i, producer in enumerate(self.producers):
                producer.start()
                logger.info(
                    f"[引擎任务提交] 生产者 {i+1} 已启动 - "
                    f"线程ID: {producer.ident if hasattr(producer, 'ident') else 'N/A'}"
                )
            
            # 将所有任务提交到共享队列
            submitted = 0
            for task in tasks:
                # 任务会被任意一个空闲的生产者处理
                success = self._submit_task_to_shared_queue(task)
                if success:
                    submitted += 1
                else:
                    logger.warning(
                        f"[引擎任务提交] 任务提交失败 - ID: {task.task_id}, "
                        f"股票: {task.symbol}, 类型: {task.task_type.value}"
                    )
                    self.execution_stats["failed_tasks"] += 1
            
            logger.info(
                f"[引擎任务提交] 成功提交 {submitted}/{len(tasks)} 个任务到共享队列 - "
                f"队列大小: {self.task_queue.qsize()}"
            )
            
            # 等待所有任务完成
            self._wait_for_all_tasks_completion()
            
        finally:
             # 停止所有生产者
             self._stop_all_producers()
    
    def _submit_task_to_shared_queue(self, task: DownloadTask, timeout: float = 5.0) -> bool:
        """提交任务到共享队列"""
        try:
            queue_size_before = self.task_queue.qsize()
            self.task_queue.put(task, timeout=timeout)
            logger.debug(
                f"[引擎队列操作] 任务已提交到共享队列 - "
                f"ID: {task.task_id}, 股票: {task.symbol}, 类型: {task.task_type.value}, "
                f"队列大小: {queue_size_before} -> {self.task_queue.qsize()}"
            )
            return True
        except Full:
            logger.error(
                f"[引擎队列操作] 提交任务到共享队列失败 - 队列已满 - "
                f"ID: {task.task_id}, 股票: {task.symbol}, 类型: {task.task_type.value}, "
                f"超时时间: {timeout}s, 队列大小: {self.task_queue.qsize()}"
            )
            return False
        except Exception as e:
            logger.error(
                f"[引擎队列操作] 提交任务到共享队列失败 - 未知错误 - "
                f"ID: {task.task_id}, 股票: {task.symbol}, 类型: {task.task_type.value}, "
                f"错误: {e}, 队列大小: {self.task_queue.qsize()}"
            )
            return False
    
    def _wait_for_all_tasks_completion(self) -> None:
        """等待所有任务完成"""
        logger.info(
            f"[引擎任务等待] 开始等待所有任务完成 - "
            f"当前任务队列: {self.task_queue.qsize()}, 生产者数量: {len(self.producers)}"
        )
        
        last_queue_size = -1
        wait_cycles = 0
        
        # 等待任务队列为空
        while not self.task_queue.empty():
            current_size = self.task_queue.qsize()
            if current_size != last_queue_size:
                logger.info(
                    f"[引擎任务等待] 任务队列处理中 - 剩余任务: {current_size}"
                )
                last_queue_size = current_size
            time.sleep(0.1)
            wait_cycles += 1
            
            # 每10秒输出一次详细状态
            if wait_cycles % 100 == 0:
                active_producers = sum(1 for p in self.producers if hasattr(p, 'is_running') and p.is_running)
                logger.info(
                    f"[引擎任务等待] 等待状态更新 - "
                    f"剩余任务: {current_size}, 活跃生产者: {active_producers}/{len(self.producers)}"
                )
        
        # 等待所有生产者完成处理
        all_idle = False
        idle_check_cycles = 0
        while not all_idle:
            all_idle = True
            active_count = 0
            for i, producer in enumerate(self.producers):
                if hasattr(producer, 'is_running') and producer.is_running:
                    if hasattr(producer, 'task_queue_size') and producer.task_queue_size > 0:
                        all_idle = False
                        active_count += 1
            
            if not all_idle:
                if idle_check_cycles % 50 == 0:  # 每5秒输出一次
                    logger.info(
                        f"[引擎任务等待] 等待生产者完成处理 - "
                        f"活跃生产者: {active_count}/{len(self.producers)}"
                    )
                time.sleep(0.1)
                idle_check_cycles += 1
        
        logger.info(
            f"[引擎任务等待] 所有任务已完成 - "
            f"总等待周期: {wait_cycles}, 最终队列大小: {self.task_queue.qsize()}"
        )
    
    def _stop_all_producers(self) -> None:
        """停止所有生产者"""
        logger.info(
            f"[引擎生产者停止] 开始停止所有生产者 - "
            f"当前生产者数量: {len(self.producers)}, 剩余任务: {self.task_queue.qsize()}"
        )
        
        for i, producer in enumerate(self.producers):
            try:
                # 获取生产者状态信息
                is_alive = producer.is_alive() if hasattr(producer, 'is_alive') else False
                is_running = getattr(producer, 'is_running', False)
                
                logger.info(
                    f"[引擎生产者停止] 正在停止生产者 {i+1} - "
                    f"状态: alive={is_alive}, running={is_running}"
                )
                
                producer.stop()
                
                # 等待生产者停止
                if hasattr(producer, 'join'):
                    producer.join(timeout=3.0)
                    final_alive = producer.is_alive() if hasattr(producer, 'is_alive') else False
                    if final_alive:
                        logger.warning(
                            f"[引擎生产者停止] 生产者 {i+1} 未能在3秒内停止"
                        )
                    else:
                        logger.info(
                            f"[引擎生产者停止] 生产者 {i+1} 已成功停止"
                        )
                else:
                    logger.info(
                        f"[引擎生产者停止] 生产者 {i+1} 停止信号已发送"
                    )
                    
            except Exception as e:
                logger.error(
                    f"[引擎生产者停止] 停止生产者 {i+1} 时出错: {e}", exc_info=True
                )
        
        # 清空生产者列表
        self.producers.clear()
        logger.info(
            f"[引擎生产者停止] 所有生产者已停止 - "
            f"最终任务队列: {self.task_queue.qsize()}, 数据队列: {self.data_queue.qsize()}"
        )

    def _monitor_queues(self) -> None:
        """监控队列状态，检测任务完成"""
        logger.info("开始监控队列状态...")

        # 等待任务队列处理完成
        while True:
            task_queue_size = self.task_queue.qsize() if self.task_queue else 0
            data_queue_size = self.consumer_pool.data_queue_size

            logger.debug(
                f"队列状态 - 任务队列: {task_queue_size}, 数据队列: {data_queue_size}"
            )

            # 如果任务队列为空且数据队列也为空，说明所有任务都完成了
            if task_queue_size == 0 and data_queue_size == 0:
                logger.info("所有队列为空，任务处理完成")
                break

            sleep(0.1)  # 每0.1秒检查一次，提供更流畅的进度更新

        # 给消费者一些时间来处理最后的缓存数据
        logger.info("等待消费者处理完剩余数据...")
        self.consumer_pool.force_flush_all()
        sleep(2.0)

    def _monitor_queues_with_progress(self) -> None:
        """监控队列状态并更新进度条"""
        logger.info("开始监控队列状态...")

        last_completed = 0
        last_failed = 0
        update_count = 0

        # 进度条由 progress_event_manager 管理

        # 等待任务队列处理完成
        while True:
            task_queue_size = self.task_queue.qsize() if self.task_queue else 0
            data_queue_size = self.consumer_pool.data_queue_size

            # 获取统计信息并更新进度
            if self.producers and self.consumer_pool:
                # 汇总所有生产者的统计信息
                total_processed = 0
                total_failed = 0
                for producer in self.producers:
                    producer_stats = producer.get_statistics()
                    total_processed += producer_stats.get("tasks_processed", 0)
                    total_failed += producer_stats.get("tasks_failed", 0)
                
                self.consumer_pool.get_statistics()

                # 使用生产者统计获取更实时的任务进度
                completed_tasks = total_processed
                failed_tasks = total_failed

                # 进度条由 progress_event_manager 管理，这里只记录变化
                if (completed_tasks != last_completed or failed_tasks != last_failed or 
                    update_count % 5 == 0):
                    # 进度更新由事件管理器处理
                    pass
                    last_completed = completed_tasks
                    last_failed = failed_tasks
                
                update_count += 1

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
        if self.producers:
            # 汇总所有生产者的统计信息
            total_processed = 0
            total_failed = 0
            for producer in self.producers:
                producer_stats = producer.get_statistics()
                total_processed += producer_stats.get("tasks_processed", 0)
                total_failed += producer_stats.get("tasks_failed", 0)
            
            # 进度更新由事件管理器处理
            pass

    def _shutdown_pools(self) -> None:
        """关闭线程池"""
        logger.info(
            f"[引擎关闭] 开始关闭线程池 - "
            f"生产者数量: {len(self.producers)}, 消费者池状态: {self.consumer_pool.running if self.consumer_pool else 'None'}"
        )

        # 停止所有生产者
        if self.producers:
            logger.info(f"[引擎关闭] 停止 {len(self.producers)} 个生产者")
            for i, producer in enumerate(self.producers):
                try:
                    producer.stop()
                    logger.info(f"[引擎关闭] 生产者 {i+1} 停止信号已发送")
                except Exception as e:
                    logger.error(f"[引擎关闭] 停止生产者 {i+1} 时出错: {e}")
        else:
            logger.info("[引擎关闭] 没有生产者需要停止")

        if self.consumer_pool:
            logger.info("[引擎关闭] 停止消费者池")
            try:
                self.consumer_pool.stop()
                logger.info("[引擎关闭] 消费者池已停止")
            except Exception as e:
                logger.error(f"[引擎关闭] 停止消费者池时出错: {e}")
        else:
            logger.info("[引擎关闭] 没有消费者池需要停止")

        logger.info(
            f"[引擎关闭] 线程池已关闭 - "
            f"最终任务队列: {self.task_queue.qsize() if self.task_queue else 'N/A'}, "
            f"最终数据队列: {self.data_queue.qsize() if self.data_queue else 'N/A'}"
        )

    def run(self):
        """执行基于队列的下载任务，分阶段处理：先系统表，后业务表并行"""
        logger.info("启动基于队列的下载引擎...")
        self.execution_stats["processing_start_time"] = datetime.now()

        try:
            # 启动进度事件管理器
            progress_event_manager.start()
            
            # 开始初始化阶段
            start_phase(ProgressPhase.INITIALIZATION)
            
            # 1. 解析配置与构建队列
            tasks = self.config.get("tasks", [])
            if not tasks:
                logger.warning("配置文件中未找到任何任务")
                logger.warning("配置文件中未找到任何任务。")
                end_phase(ProgressPhase.COMPLETED)
                return

            # 获取启用的任务
            enabled_tasks = [task for task in tasks if task.get("enabled", False)]
            if not enabled_tasks:
                logger.warning("没有启用的任务")
                logger.warning("没有启用的任务。")
                end_phase(ProgressPhase.COMPLETED)
                return

            # 2. 分离系统表任务和业务表任务
            system_tasks = [task for task in enabled_tasks if task.get("type") == "stock_list"]
            business_tasks = [task for task in enabled_tasks if task.get("type") != "stock_list"]
            
            logger.info(f"发现 {len(system_tasks)} 个系统表任务，{len(business_tasks)} 个业务表任务")

            # 3. 初始化队列和线程池
            logger.info("初始化队列和线程池...")
            self._setup_queues()

            # 阶段1：处理系统表任务（单线程，优先级高）
            if system_tasks:
                logger.info("=== 阶段1：处理系统表任务 ===")
                self._process_system_tasks(system_tasks)
                logger.info("系统表任务处理完成")

            # 阶段2：处理业务表任务（多线程并行）
            if business_tasks:
                logger.info("=== 阶段2：处理业务表任务 ===")
                # 准备目标股票列表
                target_symbols = self._prepare_target_symbols(business_tasks)
                logger.info(f"目标股票列表准备完成，共 {len(target_symbols)} 只股票")
                
                self._process_business_tasks(business_tasks, target_symbols)
                logger.info("业务表任务处理完成")

            # 统计成功/失败/跳过数量供总结
            self._collect_final_statistics()
            
            # 完成所有阶段
            end_phase(ProgressPhase.COMPLETED)
            
            # 显示最终统计
            logger.info("所有任务处理完成")

        except KeyboardInterrupt:
            logger.warning("用户中断下载")
            logger.info("用户中断下载")
            raise
        except Exception as e:
            logger.error(f"下载引擎异常: {str(e)}")
            logger.error(f"下载引擎执行异常: {e}", exc_info=True)
            raise
        finally:
            # 确保资源清理
            self._shutdown_pools()
            self.execution_stats["processing_end_time"] = datetime.now()

            # 停止进度事件管理器
            progress_event_manager.stop()

            # 输出最终统计到日志
            self._print_final_summary()

    def _process_system_tasks(self, system_tasks: List[Dict[str, Any]]) -> None:
        """处理系统表任务（优先级高）"""
        # 构建系统表任务
        download_tasks = self._build_download_tasks([], system_tasks)
        
        if not download_tasks:
            logger.info("没有系统表任务需要执行")
            return
            
        logger.info(f"开始处理 {len(download_tasks)} 个系统表任务")
        
        # 更新总任务数并开始下载阶段
        update_total(len(download_tasks), ProgressPhase.DOWNLOADING)
        start_phase(ProgressPhase.DOWNLOADING, len(download_tasks))
        
        # 启动消费者池（按配置数量创建所有消费者）
        if not self.consumer_pool.running:
            self.consumer_pool.start()
        
        # 提交系统表任务到队列
        self._submit_tasks_to_queue(download_tasks)
        
        # 监控队列直到完成
        self._monitor_queues_with_progress()
        
        # 下载完成后，开始保存阶段
        # 保存阶段的总数等于下载阶段的任务数
        update_total(len(download_tasks), ProgressPhase.SAVING)
        start_phase(ProgressPhase.SAVING, len(download_tasks))
        
        logger.info("系统表任务处理完成")

    def _process_business_tasks(self, business_tasks: List[Dict[str, Any]], target_symbols: List[str]) -> None:
        """处理业务表任务（并行处理）"""
        # 构建业务表任务
        download_tasks = self._build_download_tasks(target_symbols, business_tasks)
        
        if not download_tasks:
            logger.info("没有业务表任务需要执行")
            return
            
        logger.info(f"开始处理 {len(download_tasks)} 个业务表任务")
        
        # 更新总任务数并开始下载阶段
        update_total(len(download_tasks), ProgressPhase.DOWNLOADING)
        start_phase(ProgressPhase.DOWNLOADING, len(download_tasks))
        
        # 如果消费者池还没有启动，先启动它
        if not self.consumer_pool.running:
            logger.info("消费者池未启动，先启动消费者池")
            self.consumer_pool.start()
        
        logger.info(f"使用配置的 {self.consumer_pool.max_consumers} 个消费者处理业务表任务")
        logger.info(f"使用配置的 {self.max_producers} 个生产者处理 {len(target_symbols)} 只股票")
        
        # 提交业务表任务到队列
        self._submit_tasks_to_queue(download_tasks)
        
        # 监控队列直到完成
        self._monitor_queues_with_progress()
        
        # 下载完成后，开始保存阶段
        # 保存阶段的总数等于下载阶段的任务数
        update_total(len(download_tasks), ProgressPhase.SAVING)
        start_phase(ProgressPhase.SAVING, len(download_tasks))
        
        logger.info("业务表任务处理完成")

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
            # 从数据库获取所有股票列表
            try:
                logger.info("正在从数据库获取股票列表...")
                start_time = time.time()
                storage = self._get_runtime_storage()
                target_symbols = storage.get_all_stock_codes()
                elapsed = time.time() - start_time
                if target_symbols:
                    logger.info(f"从数据库获取到 {len(target_symbols)} 只股票 (耗时 {elapsed:.2f}s)")
                    if elapsed > 3.0:  # 如果查询耗时超过3秒，记录warning
                        logger.warning(f"股票列表查询耗时较长: {elapsed:.2f}s")
                else:
                    logger.warning("数据库中没有股票列表，请先运行 stock_list 任务")
                    target_symbols = []
            except Exception as e:
                logger.error(f"从数据库获取股票列表失败: {e}")
                target_symbols = []
        else:
            logger.error(f"未知的 symbols 配置: {symbols_config}")

        return target_symbols

    def _collect_final_statistics(self) -> None:
        """收集最终统计信息"""
        if self.producers:
            total_processed = 0
            total_failed = 0
            
            # 汇总所有生产者的统计信息
            for producer in self.producers:
                producer_stats = producer.get_statistics()
                total_processed += producer_stats.get('tasks_processed', 0)
                total_failed += producer_stats.get('tasks_failed', 0)
            
            logger.info(
                f"生产者统计: 处理任务 {total_processed} 个"
            )
            
            # 更新执行统计
            self.execution_stats["successful_tasks"] = total_processed
            self.execution_stats["failed_tasks"] = total_failed

        if self.consumer_pool:
            consumer_stats = self.consumer_pool.get_statistics()
            logger.info(
                f"消费者统计: 处理批次 {consumer_stats.get('total_batches_processed', 0)} 个"
            )
            logger.info(
                f"消费者统计: 刷新操作 {consumer_stats.get('total_flush_operations', 0)} 次"
            )
            logger.info(
                f"消费者统计: 失败操作 {consumer_stats.get('total_failed_operations', 0)} 次"
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
    def fetcher(self) -> TushareFetcher:
        """获取fetcher单例实例"""
        if self._singleton_fetcher is None:
            logger.info("初始化TushareFetcher单例实例")
            self._singleton_fetcher = get_fetcher(use_singleton=True)
            
            # 记录fetcher实例信息
            instance_info = get_fetcher_instance_info()
            logger.info(f"TushareFetcher实例信息: {instance_info}")
            
        return self._singleton_fetcher

    @property
    def storage(self) -> Optional[DuckDBStorage]:
        """获取 DuckDBStorage 实例（向后兼容）"""
        return self._storage
