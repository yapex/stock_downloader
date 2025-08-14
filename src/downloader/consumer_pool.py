"""
消费者管理器（ConsumerPool）

基于 ThreadPoolExecutor 管理多个消费者线程，每个线程从数据队列获取数据批次，
累积到本地缓存，然后批量写入 DuckDB。实现延迟初始化和失败处理机制。
"""

import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Dict, List, Any
from queue import Queue, Empty, Full
import pandas as pd

from .models import DataBatch
from .storage import PartitionedStorage
from .storage_factory import get_storage
from .simple_retry import simple_retry
from .utils import record_failed_task, get_logger


logger = get_logger(__name__)


class ConsumerWorker:
    """单个消费者工作线程"""
    
    def __init__(self, 
                 worker_id: int, 
                 data_queue: Queue,
                 batch_size: int = 100,
                 flush_interval: float = 30.0,
                 db_path: str = "data/stock.db",
                 max_retries: int = 3):
        """
        初始化消费者工作线程
        
        Args:
            worker_id: 工作线程ID
            data_queue: 数据队列
            batch_size: 批量大小，达到此数量时立即刷新
            flush_interval: 刷新间隔（秒），超过此时间自动刷新
            db_path: DuckDB数据库路径
            max_retries: 最大重试次数
        """
        self.worker_id = worker_id
        self.data_queue = data_queue
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.db_path = db_path
        self.max_retries = max_retries
        self.running = False
        
        # 延迟初始化DuckDB连接
        self._storage: Optional[PartitionedStorage] = None
        self._last_flush_time = time.time()
        
        # 本地缓存：按数据类型分组累积DataBatch
        self._cache: Dict[str, List[DataBatch]] = defaultdict(list)
        self._cache_lock = threading.Lock()
        
        # 统计信息
        self.batches_processed = 0
        self.batches_cached = 0
        self.flush_operations = 0
        self.failed_operations = 0
        
        self.logger = get_logger(f"{__name__}.worker_{worker_id}")
    
    @property
    def storage(self) -> PartitionedStorage:
        """延迟初始化DuckDB连接"""
        if self._storage is None:
            self.logger.info(f"Worker {self.worker_id}: 初始化DuckDB连接 -> {self.db_path}")
            self._storage = get_storage(self.db_path)
        return self._storage
    
    def start(self) -> None:
        """启动消费者工作线程"""
        self.running = True
        self.logger.info(f"Consumer worker {self.worker_id} started")
        
        while self.running:
            try:
                # 从数据队列获取数据批次
                batch = self._get_data_batch()
                
                if batch is not None:
                    self._process_batch(batch)
                
                # 检查是否需要刷新缓存
                self._check_flush_conditions()
                
            except Exception as e:
                self.logger.error(f"Worker {self.worker_id} unexpected error: {e}")
                self.failed_operations += 1
                time.sleep(1)  # 防止快速循环
        
        # 停止时刷新所有缓存数据
        self._flush_all_cache()
        self.logger.info(f"Consumer worker {self.worker_id} stopped")
    
    def stop(self) -> None:
        """停止消费者工作线程"""
        self.running = False
        self.logger.info(f"Consumer worker {self.worker_id} stopping...")
    
    def _get_data_batch(self) -> Optional[DataBatch]:
        """从数据队列获取数据批次"""
        try:
            # 使用较短的超时以便能响应停止信号
            return self.data_queue.get(timeout=1.0)
        except Empty:
            return None
    
    def _process_batch(self, batch: DataBatch) -> None:
        """处理单个数据批次"""
        try:
            if batch.is_empty:
                self.logger.debug(f"Worker {self.worker_id}: 跳过空数据批次 {batch.batch_id}")
                self.batches_processed += 1
                return
            
            # 获取数据类型，用于分组
            data_type = batch.meta.get('task_type', 'unknown')
            cache_key = f"{data_type}_{batch.symbol}" if batch.symbol else data_type
            
            # 将批次添加到缓存
            with self._cache_lock:
                self._cache[cache_key].append(batch)
                self.batches_cached += 1
            
            self.batches_processed += 1
            self.logger.debug(f"Worker {self.worker_id}: 缓存数据批次 {batch.batch_id}, "
                             f"类型: {cache_key}, 大小: {batch.size}")
            
        except Exception as e:
            self.logger.error(f"Worker {self.worker_id}: 处理批次失败 {batch.batch_id}: {e}")
            self._handle_batch_error(batch, e)
    
    def _check_flush_conditions(self) -> None:
        """检查是否需要刷新缓存"""
        current_time = time.time()
        should_flush = False
        
        with self._cache_lock:
            # 条件1: 某个缓存组达到批量大小
            for cache_key, batches in self._cache.items():
                total_records = sum(batch.size for batch in batches)
                if total_records >= self.batch_size:
                    should_flush = True
                    break
            
            # 条件2: 超过刷新间隔
            if current_time - self._last_flush_time >= self.flush_interval:
                should_flush = bool(self._cache)  # 只有当有缓存数据时才刷新
        
        if should_flush:
            self._flush_all_cache()
    
    def _flush_all_cache(self) -> None:
        """刷新所有缓存数据到数据库"""
        if not self._cache:
            return
        
        with self._cache_lock:
            cache_snapshot = dict(self._cache)
            self._cache.clear()
            self._last_flush_time = time.time()
        
        if not cache_snapshot:
            return
        
        self.logger.info(f"Worker {self.worker_id}: 开始刷新缓存，共 {len(cache_snapshot)} 组数据")
        
        for cache_key, batches in cache_snapshot.items():
            if not batches:
                continue
            
            try:
                self._bulk_insert_batches(cache_key, batches)
                self.flush_operations += 1
            except Exception as e:
                self.logger.error(f"Worker {self.worker_id}: 批量插入失败 {cache_key}: {e}")
                self._handle_flush_error(cache_key, batches, e)
    
    @simple_retry(max_retries=2, task_name="bulk_insert")
    def _bulk_insert_batches(self, cache_key: str, batches: List[DataBatch]) -> None:
        """批量插入数据批次到数据库"""
        if not batches:
            return
        
        # 合并所有批次的DataFrame
        dfs = []
        total_records = 0
        
        for batch in batches:
            if not batch.is_empty:
                dfs.append(batch.df)
                total_records += batch.size
        
        if not dfs:
            self.logger.debug(f"Worker {self.worker_id}: 没有有效数据需要插入 {cache_key}")
            return
        
        # 合并DataFrame
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # 去重（如果有重复的日期数据）
        date_columns = ['trade_date', 'cal_date', 'ann_date', 'f_ann_date']
        existing_date_col = None
        for col in date_columns:
            if col in combined_df.columns:
                existing_date_col = col
                break
        
        if existing_date_col:
            # 按日期去重，保留最新的数据
            combined_df = combined_df.drop_duplicates(subset=['ts_code', existing_date_col], keep='last')
        
        # 从第一个批次的元数据中获取正确的数据类型和实体ID
        if batches:
            first_batch = batches[0]
            data_type = first_batch.meta.get('task_type', 'unknown')
        else:
            # 备用方案：从cache_key解析（但这种情况不应该发生）
            parts = cache_key.split('_', 1)
            data_type = parts[0]
            parts[1] if len(parts) > 1 else 'system'
        
        # 根据数据类型调用相应的存储方法
        success = False
        if data_type in ['daily', 'daily_basic']:
            success = self.storage.save_daily(combined_df)
        elif data_type in ['income', 'balancesheet', 'cashflow', 'financials']:
            success = self.storage.save_financial_data(combined_df)
        elif data_type == 'stock_list':
            success = self.storage.save_stock_list(combined_df)
        else:
            # 对于其他类型，尝试使用通用方法
            self.logger.warning(f"未知数据类型 {data_type}，使用通用保存方法")
            success = self.storage.save_daily(combined_df)
        
        if not success:
            raise Exception(f"保存数据失败: {data_type}")
        

        
        self.logger.info(f"Worker {self.worker_id}: 成功插入 {len(combined_df)} 条记录到 {cache_key}")
    
    def _handle_batch_error(self, batch: DataBatch, error: Exception) -> None:
        """处理批次处理错误"""
        # 记录失败的批次
        record_failed_task(
            task_name=f"process_batch_worker_{self.worker_id}",
            entity_id=batch.symbol or batch.batch_id,
            reason=str(error),
            error_category="unknown"
        )
        
        self.failed_operations += 1
        self.logger.error(f"Worker {self.worker_id}: 批次处理失败 {batch.batch_id}")
    
    def _handle_flush_error(self, cache_key: str, batches: List[DataBatch], error: Exception) -> None:
        """处理刷新错误，将失败的批次写入死信"""
        for batch in batches:
            record_failed_task(
                task_name=f"bulk_insert_worker_{self.worker_id}",
                entity_id=batch.symbol or cache_key,
                reason=f"flush_failed_{error}",
                error_category="unknown"
            )
        
        self.failed_operations += 1
        total_records = sum(batch.size for batch in batches if not batch.is_empty)
        self.logger.error(f"Worker {self.worker_id}: 刷新失败 {cache_key}, "
                         f"丢失 {len(batches)} 个批次 ({total_records} 条记录)")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取工作线程统计信息"""
        with self._cache_lock:
            cache_stats = {}
            total_cached_records = 0
            
            for cache_key, batches in self._cache.items():
                batch_count = len(batches)
                record_count = sum(batch.size for batch in batches if not batch.is_empty)
                cache_stats[cache_key] = {
                    'batches': batch_count,
                    'records': record_count
                }
                total_cached_records += record_count
        
        return {
            'worker_id': self.worker_id,
            'running': self.running,
            'batches_processed': self.batches_processed,
            'batches_cached': self.batches_cached,
            'flush_operations': self.flush_operations,
            'failed_operations': self.failed_operations,
            'cache_groups': len(cache_stats),
            'cached_records': total_cached_records,
            'cache_details': cache_stats,
            'has_storage_connection': self._storage is not None
        }


class ConsumerPool:
    """消费者池管理器"""
    
    def __init__(self,
                 max_consumers: int = 2,
                 data_queue: Optional[Queue] = None,
                 batch_size: int = 100,
                 flush_interval: float = 30.0,
                 db_path: str = "data/stock.db",
                 max_retries: int = 3):
        """
        初始化消费者池
        
        Args:
            max_consumers: 最大消费者线程数
            data_queue: 数据队列，如果为None则创建新队列
            batch_size: 批量大小，达到此数量时立即刷新
            flush_interval: 刷新间隔（秒），超过此时间自动刷新
            db_path: DuckDB数据库路径
            max_retries: 最大重试次数
        """
        self.max_consumers = max_consumers
        self.data_queue = data_queue or Queue()
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.db_path = db_path
        self.max_retries = max_retries
        
        # 线程池和工作线程
        self.executor: Optional[ThreadPoolExecutor] = None
        self.workers: Dict[int, ConsumerWorker] = {}
        self.running = False
        self.current_consumers = 0  # 当前活跃的消费者数量
        
        # 统计信息
        self.start_time: Optional[datetime] = None
        self._stats_lock = threading.Lock()
        
        self.logger = get_logger(__name__)
    
    def start(self) -> None:
        """
        启动消费者池，按配置的最大消费者数量创建所有消费者
        """
        if self.running:
            self.logger.warning("Consumer pool is already running")
            return
        
        self.running = True
        self.start_time = datetime.now()
        
        # 创建线程池
        self.executor = ThreadPoolExecutor(max_workers=self.max_consumers)
        
        # 启动所有配置的消费者
        self._start_consumers(self.max_consumers)
        
        self.logger.info(f"Consumer pool started with {self.max_consumers} workers, "
                        f"batch_size={self.batch_size}, flush_interval={self.flush_interval}s")
    
    def _start_consumers(self, num_consumers: int) -> None:
        """启动指定数量的消费者"""
        num_consumers = min(num_consumers, self.max_consumers - self.current_consumers)
        
        for i in range(self.current_consumers, self.current_consumers + num_consumers):
            worker = ConsumerWorker(
                worker_id=i,
                data_queue=self.data_queue,
                batch_size=self.batch_size,
                flush_interval=self.flush_interval,
                db_path=self.db_path,
                max_retries=self.max_retries
            )
            
            self.workers[i] = worker
            # 提交工作线程到线程池
            self.executor.submit(worker.start)
            
        self.current_consumers += num_consumers
        self.logger.info(f"Started {num_consumers} new consumers, total active: {self.current_consumers}")
    
    def scale_consumers(self, target_consumers: int) -> None:
        """
        简化的消费者调整方法（实际上不做任何调整，保持配置的消费者数量）
        
        Args:
            target_consumers: 目标消费者数量（忽略此参数）
        """
        if not self.running:
            self.logger.warning("Consumer pool is not running")
            return
            
        self.logger.info(f"Consumer pool running with {self.current_consumers} consumers (fixed configuration)")
    
    def stop(self, timeout: float = 60.0) -> None:
        """停止消费者池"""
        if not self.running:
            return
        
        self.logger.info("Stopping consumer pool...")
        self.running = False
        
        # 停止所有工作线程
        for worker in self.workers.values():
            worker.stop()
        
        # 等待线程池关闭（给足够时间让缓存数据刷新完成）
        if self.executor:
            self.executor.shutdown(wait=True)
        
        # 清理资源
        self.workers.clear()
        self.executor = None
        
        self.logger.info("Consumer pool stopped")
    
    def submit_data(self, data_batch: DataBatch, timeout: float = 1.0) -> bool:
        """提交数据批次到数据队列"""
        if not self.running:
            raise RuntimeError("Consumer pool is not running")
        
        try:
            self.data_queue.put(data_batch, timeout=timeout)
            self.logger.debug(f"Data batch submitted: {data_batch.batch_id}")
            return True
        except Full:
            self.logger.warning(f"Data queue full, failed to submit batch: {data_batch.batch_id}")
            return False
    
    def force_flush_all(self) -> None:
        """强制刷新所有消费者的缓存"""
        if not self.running:
            return
        
        self.logger.info("Force flushing all consumer caches...")
        
        for worker in self.workers.values():
            if worker.running:
                worker._flush_all_cache()
        
        self.logger.info("Force flush completed")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._stats_lock:
            uptime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            
            # 汇总工作线程统计
            worker_stats = []
            total_processed = 0
            total_cached = 0
            total_flush_ops = 0
            total_failed_ops = 0
            total_cache_records = 0
            
            for worker in self.workers.values():
                stats = worker.get_statistics()
                worker_stats.append(stats)
                total_processed += stats['batches_processed']
                total_cached += stats['batches_cached']
                total_flush_ops += stats['flush_operations']
                total_failed_ops += stats['failed_operations']
                total_cache_records += stats['cached_records']
            
            return {
                'running': self.running,
                'max_consumers': self.max_consumers,
                'active_workers': len([w for w in self.workers.values() if w.running]),
                'data_queue_size': self.data_queue.qsize(),
                'batch_size': self.batch_size,
                'flush_interval': self.flush_interval,
                'uptime_seconds': uptime,
                'total_batches_processed': total_processed,
                'total_batches_cached': total_cached,
                'total_flush_operations': total_flush_ops,
                'total_failed_operations': total_failed_ops,
                'total_cached_records': total_cache_records,
                'worker_statistics': worker_stats
            }
    
    def wait_for_empty_queue(self, timeout: Optional[float] = None) -> bool:
        """等待数据队列为空"""
        if not self.running:
            return True
        
        start_time = time.time()
        
        while not self.data_queue.empty():
            if timeout and (time.time() - start_time) > timeout:
                self.logger.warning("Timeout waiting for data queue to empty")
                return False
            time.sleep(0.1)
        
        # 队列空后，给工作线程一些时间处理剩余的缓存
        time.sleep(1.0)
        
        return True
    
    @property
    def is_running(self) -> bool:
        """检查消费者池是否正在运行"""
        return self.running
    
    @property
    def data_queue_size(self) -> int:
        """获取数据队列大小"""
        return self.data_queue.qsize()
