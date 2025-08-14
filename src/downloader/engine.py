"""下载引擎模块

实现基于生产者-消费者模式的股票数据下载引擎。
核心特性：
- 分阶段处理：系统表任务优先，业务表任务并行
- 批量优化：任务构建、日期查询、数据写入均采用批量处理
- 并发控制：多生产者并行获取数据，多消费者并行写入
- 可靠性保障：失败重试、优雅关闭、完善的异常处理
"""

import time
from datetime import datetime
from queue import Queue
from typing import Dict, List, Any, Optional

from .fetcher import TushareFetcher
from .fetcher_factory import get_singleton
from .storage import PartitionedStorage
from .storage_factory import get_storage
from .producer import Producer
from .consumer_pool import ConsumerPool
from .models import DownloadTask, TaskType, Priority

from .utils import get_logger

logger = get_logger(__name__)


class DownloadEngine:
    """下载引擎
    
    负责协调整个数据下载流程，包括：
    1. 任务发现与构建
    2. 分阶段执行（系统表 -> 业务表）
    3. 生产者-消费者协调
    4. 进度监控与统计
    """
    

    
    def __init__(
        self,
        config: Dict[str, Any],
        fetcher: Optional[TushareFetcher] = None,
        storage: Optional[PartitionedStorage] = None,
        force_run: bool = False,
        symbols_overridden: bool = False,
        group_name: str = "default",
    ):
        """初始化下载引擎
        
        Args:
            config: 配置字典
            fetcher: 可选的fetcher实例，用于测试
            storage: 可选的storage实例，用于测试
            force_run: 是否强制运行（忽略增量逻辑）
            symbols_overridden: 股票列表是否被覆盖
            group_name: 任务组名称
        """
        self.config = config
        self.force_run = force_run
        self.symbols_overridden = symbols_overridden
        self.group_name = group_name
        
        # 依赖注入的实例（主要用于测试）
        self._injected_fetcher = fetcher
        self._injected_storage = storage
        
        # 运行时实例
        self._singleton_fetcher: Optional[TushareFetcher] = None
        self._runtime_storage: Optional[PartitionedStorage] = None
        
        # 队列和线程池组件
        self.task_queue: Optional[Queue] = None
        self.data_queue: Optional[Queue] = None
        self.consumer_pool: Optional[ConsumerPool] = None
        self.producers: List[Producer] = []

        
        # 配置参数
        self._load_configuration()
        
        # 执行统计
        self.execution_stats = {
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "skipped_tasks": 0,
            "tasks_by_type": {},
            "processing_start_time": None,
            "processing_end_time": None,
        }
    
    def _load_configuration(self) -> None:
        """加载配置参数"""
        downloader_config = self.config.get("downloader", {})
        
        # 线程配置
        self.max_producers = downloader_config.get("max_producers", 4)
        self.max_consumers = downloader_config.get("max_consumers", 2)
        
        # 队列配置
        self.producer_queue_size = downloader_config.get("producer_queue_size", 1000)
        self.data_queue_size = downloader_config.get("data_queue_size", 500)
        
        logger.info(
            f"引擎配置加载完成 - 生产者: {self.max_producers}, "
            f"消费者: {self.max_consumers}, 任务队列: {self.producer_queue_size}, "
            f"数据队列: {self.data_queue_size}"
        )
    
    @property
    def fetcher(self) -> TushareFetcher:
        """获取fetcher实例"""
        if self._injected_fetcher is not None:
            return self._injected_fetcher
        
        if self._singleton_fetcher is None:
            self._singleton_fetcher = get_singleton()
        
        return self._singleton_fetcher
    
    @property
    def storage(self) -> Optional[PartitionedStorage]:
        """获取storage实例"""
        return self._injected_storage
    
    def _get_runtime_storage(self) -> PartitionedStorage:
        """获取运行时storage实例"""
        if self._injected_storage is not None:
            return self._injected_storage
        
        if self._runtime_storage is None:
            db_path = self.config.get("database", {}).get("path", "data/stock.db")
            self._runtime_storage = get_storage(db_path)
        
        return self._runtime_storage
    
    def run(self) -> None:
        """执行下载任务
        
        分阶段处理：
        1. 初始化阶段：解析配置、构建队列
        2. 系统表阶段：处理stock_list等基础数据
        3. 业务表阶段：并行处理股票数据
        """
        logger.info("启动下载引擎...")
        self.execution_stats["processing_start_time"] = datetime.now()
        
        try:

            
            # 1. 解析和验证配置
            enabled_tasks = self._parse_and_validate_config()
            if not enabled_tasks:
                logger.warning("没有启用的任务")
    
                return
            
            # 2. 分离任务类型
            system_tasks, business_tasks = self._separate_task_types(enabled_tasks)
            logger.info(f"发现 {len(system_tasks)} 个系统表任务，{len(business_tasks)} 个业务表任务")
            
            # 3. 初始化基础设施
            self._initialize_infrastructure()
            
            # 4. 阶段1：处理系统表任务
            if system_tasks:
                self._execute_system_tasks(system_tasks)
            
            # 5. 阶段2：处理业务表任务
            if business_tasks:
                self._execute_business_tasks(business_tasks)
            
            # 6. 收集最终统计
            self._collect_final_statistics()
            
            
            logger.info("所有任务处理完成")
            
        except KeyboardInterrupt:
            logger.warning("用户中断下载")
            raise
        except Exception as e:
            logger.error(f"下载引擎执行异常: {e}", exc_info=True)
            raise
        finally:
            self._cleanup_resources()
            self.execution_stats["processing_end_time"] = datetime.now()
            self._print_final_summary()
    
    def _parse_and_validate_config(self) -> List[Dict[str, Any]]:
        """解析和验证配置"""
        tasks = self.config.get("tasks", [])
        if not tasks:
            logger.warning("配置文件中未找到任何任务")
            return []
        
        enabled_tasks = [task for task in tasks if task.get("enabled", False)]
        if not enabled_tasks:
            logger.warning("没有启用的任务")
            return []
        
        # 验证任务类型
        valid_tasks = []
        for task in enabled_tasks:
            task_type = task.get("type")
            try:
                TaskType(task_type)
                valid_tasks.append(task)
            except ValueError:
                logger.warning(f"跳过未知的任务类型: {task_type}")
        
        return valid_tasks
    
    def _separate_task_types(self, enabled_tasks: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """分离系统表任务和业务表任务"""
        system_tasks = []
        business_tasks = []
        
        for task in enabled_tasks:
            if task.get("type") == "stock_list":
                system_tasks.append(task)
            else:
                business_tasks.append(task)
        
        return system_tasks, business_tasks
    
    def _initialize_infrastructure(self) -> None:
        """初始化基础设施"""
        logger.info("初始化队列和线程池...")
        
        # 创建数据队列
        self.data_queue = Queue(maxsize=self.data_queue_size)
        

        
        # 创建消费者池
        consumer_config = self.config.get("consumer", {})
        db_path = self.config.get("database", {}).get("path", "data/stock.db")
        
        self.consumer_pool = ConsumerPool(
            max_consumers=self.max_consumers,
            data_queue=self.data_queue,
            batch_size=consumer_config.get("batch_size", 100),
            flush_interval=consumer_config.get("flush_interval", 30.0),
            db_path=db_path,
            max_retries=consumer_config.get("max_retries", 3),
        )
        
        logger.info("基础设施初始化完成")
    
    def _execute_system_tasks(self, system_tasks: List[Dict[str, Any]]) -> None:
        """执行系统表任务"""
        logger.info("=== 阶段1：处理系统表任务 ===")
        
        # 构建系统表任务
        download_tasks = self._build_download_tasks([], system_tasks)
        if not download_tasks:
            logger.info("没有系统表任务需要执行")
            return
        
        logger.info(f"开始处理 {len(download_tasks)} 个系统表任务")
        

        
        # 启动消费者池
        if not self.consumer_pool.running:
            self.consumer_pool.start()
        
        # 使用单个生产者处理系统任务
        self._process_tasks_with_producers(download_tasks, num_producers=1)
        
        logger.info("系统表任务处理完成")
    
    def _execute_business_tasks(self, business_tasks: List[Dict[str, Any]]) -> None:
        """执行业务表任务"""
        logger.info("=== 阶段2：处理业务表任务 ===")
        
        # 准备目标股票列表
        target_symbols = self._prepare_target_symbols(business_tasks)
        if not target_symbols:
            logger.warning("没有目标股票，跳过业务表任务")
            return
        
        logger.info(f"目标股票列表准备完成，共 {len(target_symbols)} 只股票")
        
        # 构建业务表任务
        download_tasks = self._build_download_tasks(target_symbols, business_tasks)
        if not download_tasks:
            logger.info("没有业务表任务需要执行")
            return
        
        logger.info(f"开始处理 {len(download_tasks)} 个业务表任务")
        

        
        # 确保消费者池已启动
        if not self.consumer_pool.running:
            self.consumer_pool.start()
        
        # 使用多个生产者并行处理业务任务
        num_producers = min(self.max_producers, max(1, len(download_tasks) // 50))
        self._process_tasks_with_producers(download_tasks, num_producers)
        
        logger.info("业务表任务处理完成")
    
    def _prepare_target_symbols(self, enabled_tasks: List[Dict[str, Any]]) -> List[str]:
        """准备目标股票列表"""
        from .utils import normalize_stock_code
        
        # 检查是否需要股票列表
        needs_symbols = any(task.get("type") != "stock_list" for task in enabled_tasks)
        if not needs_symbols:
            logger.info("所有任务都是系统级任务，无需股票列表")
            return []
        
        downloader_config = self.config.get("downloader", {})
        symbols_config = downloader_config.get("symbols", [])
        
        if isinstance(symbols_config, list):
            # 使用配置指定的股票列表
            target_symbols = []
            for symbol in symbols_config:
                try:
                    normalized_symbol = normalize_stock_code(symbol)
                    target_symbols.append(normalized_symbol)
                except (ValueError, TypeError) as e:
                    logger.warning(f"跳过无效的股票代码 '{symbol}': {e}")
            
            logger.info(f"使用配置指定的 {len(target_symbols)} 只股票")
            return target_symbols
        
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
                    if elapsed > 3.0:
                        logger.warning(f"股票列表查询耗时较长: {elapsed:.2f}s")
                else:
                    logger.warning("数据库中没有股票列表，请先运行 stock_list 任务")
                
                return target_symbols or []
            
            except Exception as e:
                logger.error(f"从数据库获取股票列表失败: {e}")
                return []
        
        else:
            logger.error(f"未知的 symbols 配置: {symbols_config}")
            return []
    
    def _build_download_tasks(self, target_symbols: List[str], enabled_tasks: List[Dict[str, Any]]) -> List[DownloadTask]:
        """构建下载任务列表"""
        download_tasks = []
        
        # 分离系统任务和业务任务
        system_tasks = []
        business_tasks = []
        
        for task_spec in enabled_tasks:
            task_type_str = task_spec.get("type")
            try:
                task_type = TaskType(task_type_str)
                if task_type == TaskType.STOCK_LIST:
                    system_tasks.append(task_spec)
                else:
                    business_tasks.append(task_spec)
            except ValueError:
                logger.warning(f"未知的任务类型: {task_type_str}")
                continue
        
        # 处理系统任务
        for task_spec in system_tasks:
            task = DownloadTask(
                symbol="system",
                task_type=TaskType.STOCK_LIST,
                params={"task_config": task_spec, "force_run": self.force_run},
                priority=Priority.HIGH,
            )
            download_tasks.append(task)
        
        # 批量处理业务任务
        if business_tasks and target_symbols:
            logger.info(f"开始批量构建 {len(business_tasks)} 种任务类型，{len(target_symbols)} 只股票的任务")
            start_time = time.time()
            
            # 批量获取最新日期缓存
            latest_dates_cache = self._build_latest_dates_cache(business_tasks, target_symbols)
            
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
        
        # 更新统计信息
        self.execution_stats["total_tasks"] = len(download_tasks)
        
        # 按类型统计任务
        for task in download_tasks:
            if task.task_type == TaskType.FINANCIALS:
                task_config = task.params.get("task_config", {})
                data_type = self._get_data_type_for_task_spec(task_config)
                task_type_key = data_type
            else:
                task_type_key = task.task_type.value
            
            self.execution_stats["tasks_by_type"][task_type_key] = (
                self.execution_stats["tasks_by_type"].get(task_type_key, 0) + 1
            )
        
        logger.info(f"构建了 {len(download_tasks)} 个下载任务")
        for task_type, count in self.execution_stats["tasks_by_type"].items():
            logger.info(f"  - {task_type}: {count} 个任务")
        
        return download_tasks
    
    def _build_latest_dates_cache(self, business_tasks: List[Dict[str, Any]], target_symbols: List[str]) -> Dict[tuple, Dict[str, str]]:
        """批量构建最新日期缓存"""
        latest_dates_cache = {}
        
        if self.force_run:
            # 强制运行时不需要查询最新日期
            for task_spec in business_tasks:
                data_type = self._get_data_type_for_task_spec(task_spec)
                date_col = self._get_date_column_for_task_type(task_spec.get("type", ""))
                latest_dates_cache[(data_type, date_col)] = {}
            return latest_dates_cache
        
        # 按任务类型批量获取最新日期
        for task_spec in business_tasks:
            data_type = self._get_data_type_for_task_spec(task_spec)
            date_col = self._get_date_column_for_task_type(task_spec.get("type", ""))
            
            try:
                storage = self._get_runtime_storage()
                # 映射到内部数据类型
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
        
        return latest_dates_cache
    
    def _process_tasks_with_producers(self, tasks: List[DownloadTask], num_producers: int) -> None:
        """使用指定数量的生产者处理任务"""
        if not tasks:
            logger.info("没有任务需要处理")
            return
        
        logger.info(f"使用 {num_producers} 个生产者处理 {len(tasks)} 个任务")
        
        try:
            # 创建生产者
            self.producers = []
            from concurrent.futures import ThreadPoolExecutor
            thread_pool_executor = ThreadPoolExecutor(max_workers=num_producers)
            
            for i in range(num_producers):
                producer = Producer(
                    fetcher=self.fetcher,
                    thread_pool_executor=thread_pool_executor
                )
                self.producers.append(producer)
            
            # 启动生产者
            for i, producer in enumerate(self.producers):
                producer.start()
                logger.info(f"生产者 {i+1} 已启动")
            
            # 提交任务到队列
            self._submit_tasks_to_queue(tasks)
            
            # 等待任务完成
            self._wait_for_tasks_completion()
        
        finally:
            # 停止生产者
            self._stop_producers()
    
    def _submit_tasks_to_queue(self, tasks: List[DownloadTask]) -> None:
        """提交任务到生产者"""
        logger.info(f"开始提交 {len(tasks)} 个任务到生产者")
        
        submitted = 0
        producer_index = 0
        
        for task in tasks:
            try:
                # 轮询分配任务给生产者
                producer = self.producers[producer_index]
                if producer.submit_task(task, timeout=5.0):
                    submitted += 1
                    producer_index = (producer_index + 1) % len(self.producers)
                else:
                    logger.error(f"提交任务失败: {task.task_id}")
                    self.execution_stats["failed_tasks"] += 1
            except Exception as e:
                logger.error(f"提交任务失败: {task.task_id}, 错误: {e}")
                self.execution_stats["failed_tasks"] += 1
        
        logger.info(f"成功提交 {submitted}/{len(tasks)} 个任务")
    
    def _wait_for_tasks_completion(self) -> None:
        """等待任务完成"""
        logger.info("等待所有任务完成...")
        
        while True:
            # 检查所有生产者的任务队列是否为空且生产者都在运行
            all_queues_empty = all(producer.task_queue.empty() for producer in self.producers)
            all_producers_running = all(getattr(producer, 'running', False) for producer in self.producers)
            
            if all_queues_empty and not all_producers_running:
                break
            
            time.sleep(1.0)
        
        logger.info("所有任务已完成")
    
    def _stop_producers(self) -> None:
        """停止所有生产者"""
        logger.info("停止所有生产者...")
        
        for producer in self.producers:
            if hasattr(producer, 'stop'):
                producer.stop()
        
        # 等待生产者线程结束
        for producer in self.producers:
            if producer.is_alive():
                producer.join(timeout=10.0)
        
        self.producers.clear()
        logger.info("所有生产者已停止")
    
    def _cleanup_resources(self) -> None:
        """清理资源"""
        logger.info("清理资源...")
        
        # 停止生产者
        self._stop_producers()
        
        # 停止消费者池
        if self.consumer_pool and self.consumer_pool.running:
            self.consumer_pool.stop()
        
        logger.info("资源清理完成")
    
    def _collect_final_statistics(self) -> None:
        """收集最终统计信息"""
        if self.producers:
            total_processed = 0
            total_failed = 0
            
            for producer in self.producers:
                if hasattr(producer, 'stats'):
                    stats = producer.stats
                    total_processed += getattr(stats, 'tasks_processed', 0)
                    total_failed += getattr(stats, 'tasks_failed', 0)
            
            self.execution_stats["successful_tasks"] = total_processed - total_failed
            self.execution_stats["failed_tasks"] = total_failed
    
    def _print_final_summary(self) -> None:
        """打印最终统计摘要"""
        stats = self.execution_stats
        start_time = stats.get("processing_start_time")
        end_time = stats.get("processing_end_time")
        
        if start_time and end_time:
            duration = end_time - start_time
            logger.info(f"处理耗时: {duration}")
        
        logger.info(f"任务统计: 总计 {stats['total_tasks']}, "
                   f"成功 {stats['successful_tasks']}, "
                   f"失败 {stats['failed_tasks']}, "
                   f"跳过 {stats['skipped_tasks']}")
        
        if stats['tasks_by_type']:
            logger.info("按类型统计:")
            for task_type, count in stats['tasks_by_type'].items():
                logger.info(f"  - {task_type}: {count}")
    
    def get_execution_stats(self) -> Dict[str, Any]:
        """获取执行统计信息"""
        return self.execution_stats.copy()
    
    # 辅助方法
    
    def _determine_date_range_with_cache(
        self, task_spec: Dict[str, Any], symbol: str, cached_latest_date: str = None
    ) -> tuple[str, str]:
        """使用缓存确定日期范围"""
        from datetime import datetime, timedelta
        
        # 获取配置的日期范围
        start_date_str = task_spec.get("start_date", "20100101")
        end_date_str = task_spec.get("end_date", datetime.now().strftime("%Y%m%d"))
        
        if self.force_run or not cached_latest_date:
            return start_date_str, end_date_str
        
        # 使用缓存的最新日期作为起始日期
        try:
            latest_date = datetime.strptime(cached_latest_date, "%Y%m%d")
            next_date = latest_date + timedelta(days=1)
            return next_date.strftime("%Y%m%d"), end_date_str
        except (ValueError, TypeError):
            return start_date_str, end_date_str
    
    def _get_data_type_for_task_spec(self, task_spec: Dict[str, Any]) -> str:
        """获取任务规格对应的数据类型"""
        task_type = task_spec.get("type", "")
        
        # 基础映射
        type_mapping = {
            "daily": "daily",
            "daily_basic": "daily_basic",
            "stock_list": "stock_list",
        }
        
        if task_type in type_mapping:
            return type_mapping[task_type]
        
        # 财务数据的特殊处理
        if task_type == "financials":
            return task_spec.get("data_type", "financials_income")
        
        return task_type
    
    def _get_date_column_for_task_type(self, task_type: str) -> str:
        """获取任务类型对应的日期列名"""
        date_column_mapping = {
            "daily": "trade_date",
            "daily_basic": "trade_date",
            "financials": "end_date",
            "stock_list": "list_date",
        }
        
        return date_column_mapping.get(task_type, "trade_date")
