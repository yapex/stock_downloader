"""
生产者管理器（ProducerPool）

基于 ThreadPoolExecutor 管理多个生产者线程，每个线程从任务队列获取任务，
使用 TushareFetcher 获取数据，然后将数据放入数据队列。
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Callable, Dict, Any
from queue import Queue, Empty, Full
from functools import wraps
from datetime import datetime

from .fetcher import TushareFetcher
from .models import DownloadTask, DataBatch, TaskType, Priority
from .error_handler import (
    enhanced_retry, 
    NETWORK_RETRY_STRATEGY, 
    API_LIMIT_RETRY_STRATEGY,
    classify_error,
    ErrorCategory
)
from .retry_policy import (
    RetryPolicy, 
    DEFAULT_RETRY_POLICY, 
    NETWORK_RETRY_POLICY,
    DeadLetterLogger
)
from .utils import record_failed_task

logger = logging.getLogger(__name__)


def rate_limit(calls: int, period: int = 60):
    """
    速率限制装饰器
    
    Args:
        calls: 每个周期内最大调用次数
        period: 时间周期（秒）
    """
    def decorator(func: Callable) -> Callable:
        last_calls = []
        lock = threading.Lock()
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                now = time.time()
                # 移除超过时间窗口的调用记录
                last_calls[:] = [call_time for call_time in last_calls if now - call_time < period]
                
                # 检查是否达到限制
                if len(last_calls) >= calls:
                    sleep_time = period - (now - last_calls[0])
                    if sleep_time > 0:
                        logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                        time.sleep(sleep_time)
                        now = time.time()
                
                # 记录本次调用时间
                last_calls.append(now)
                
            return func(*args, **kwargs)
        return wrapper
    return decorator


class ProducerWorker:
    """单个生产者工作线程"""
    
    def __init__(self, worker_id: int, fetcher: TushareFetcher, 
                 task_queue: Queue, data_queue: Queue, 
                 retry_policy_config: Optional[RetryPolicy] = None,
                 dead_letter_path: str = "logs/dead_letter.jsonl"):
        """
        初始化生产者工作线程
        
        Args:
            worker_id: 工作线程ID
            fetcher: TushareFetcher实例
            task_queue: 任务队列
            data_queue: 数据队列
            retry_policy_config: 重试策略配置
            dead_letter_path: 死信日志路径
        """
        self.worker_id = worker_id
        self.fetcher = fetcher
        self.task_queue = task_queue
        self.data_queue = data_queue
        self.retry_policy = retry_policy_config or DEFAULT_RETRY_POLICY
        self.dead_letter_logger = DeadLetterLogger(dead_letter_path)
        self.running = False
        self.logger = logging.getLogger(f"{__name__}.worker_{worker_id}")
        
        # 添加速率限制
        self._rate_limited_fetch = self._add_rate_limits()
    
    def _add_rate_limits(self) -> Dict[str, Callable]:
        """为不同的获取方法添加速率限制"""
        return {
            TaskType.STOCK_LIST: rate_limit(calls=150, period=60)(self.fetcher.fetch_stock_list),
            TaskType.DAILY: rate_limit(calls=400, period=60)(self.fetcher.fetch_daily_history),
            TaskType.DAILY_BASIC: rate_limit(calls=150, period=60)(self.fetcher.fetch_daily_basic),
            TaskType.FINANCIALS: self._get_financials_fetcher()
        }
    
    def _get_financials_fetcher(self) -> Callable:
        """获取财务数据的速率限制包装器"""
        @rate_limit(calls=150, period=60)
        def fetch_financials(task: DownloadTask):
            """统一的财务数据获取接口"""
            params = task.params
            ts_code = task.symbol
            start_date = params.get('start_date', '')
            end_date = params.get('end_date', '')
            
            financial_type = params.get('financial_type', 'income')
            
            if financial_type == 'income':
                return self.fetcher.fetch_income(ts_code, start_date, end_date)
            elif financial_type == 'balancesheet':
                return self.fetcher.fetch_balancesheet(ts_code, start_date, end_date)
            elif financial_type == 'cashflow':
                return self.fetcher.fetch_cashflow(ts_code, start_date, end_date)
            else:
                raise ValueError(f"Unknown financial_type: {financial_type}")
        
        return fetch_financials
    
    def start(self) -> None:
        """启动生产者工作线程"""
        self.running = True
        self.logger.info(f"Producer worker {self.worker_id} started")
        
        while self.running:
            try:
                # 从任务队列获取任务
                task = self._get_task()
                if task is None:
                    continue
                
                # 处理任务
                self._process_task(task)
                
            except Exception as e:
                self.logger.error(f"Worker {self.worker_id} unexpected error: {e}")
                time.sleep(1)  # 防止快速循环
    
    def stop(self) -> None:
        """停止生产者工作线程"""
        self.running = False
        self.logger.info(f"Producer worker {self.worker_id} stopping...")
    
    def _get_task(self) -> Optional[DownloadTask]:
        """从任务队列获取任务"""
        try:
            # 使用较短的超时以便能响应停止信号
            task = self.task_queue.get(timeout=1.0)
            return task
        except Empty:
            return None
    
    def _process_task(self, task: DownloadTask) -> None:
        """处理单个任务"""
        try:
            self.logger.debug(f"Processing task {task.task_id}: {task.symbol} - {task.task_type.value}")
            
            # 获取数据
            data = self._fetch_data(task)
            
            # 创建数据批次
            if data is not None:
                data_batch = DataBatch(
                    df=data,
                    meta={
                        'task_type': task.task_type.value,
                        'symbol': task.symbol,
                        'params': task.params,
                        'worker_id': self.worker_id,
                        'processed_at': datetime.now().isoformat()
                    },
                    task_id=task.task_id,
                    symbol=task.symbol
                )
                
                # 将数据放入数据队列
                self._put_data(data_batch)
                
                self.logger.debug(f"Task {task.task_id} completed successfully, "
                                f"got {len(data)} records")
            else:
                # 数据为空，创建空的数据批次
                empty_batch = DataBatch.empty(
                    task_id=task.task_id,
                    symbol=task.symbol,
                    meta={
                        'task_type': task.task_type.value,
                        'reason': 'no_data',
                        'worker_id': self.worker_id,
                        'processed_at': datetime.now().isoformat()
                    }
                )
                self._put_data(empty_batch)
                
                self.logger.debug(f"Task {task.task_id} completed with no data")
            
            # 只有从队列中获取的任务才需要标记完成
            # self.task_queue.task_done()
                
        except Exception as e:
            self.logger.error(f"Error processing task {task.task_id}: {e}")
            self._handle_task_error(task, e)
    
    def _fetch_data(self, task: DownloadTask):
        """获取数据"""
        if task.task_type == TaskType.STOCK_LIST:
            return self._rate_limited_fetch[TaskType.STOCK_LIST]()
        
        elif task.task_type == TaskType.DAILY:
            params = task.params
            return self._rate_limited_fetch[TaskType.DAILY](
                ts_code=task.symbol,
                start_date=params.get('start_date', ''),
                end_date=params.get('end_date', ''),
                adjust=params.get('adjust', 'hfq')
            )
        
        elif task.task_type == TaskType.DAILY_BASIC:
            params = task.params
            return self._rate_limited_fetch[TaskType.DAILY_BASIC](
                ts_code=task.symbol,
                start_date=params.get('start_date', ''),
                end_date=params.get('end_date', '')
            )
        
        elif task.task_type == TaskType.FINANCIALS:
            return self._rate_limited_fetch[TaskType.FINANCIALS](task)
        
        else:
            raise ValueError(f"Unknown task type: {task.task_type}")
    
    def _put_data(self, data_batch: DataBatch) -> None:
        """将数据放入数据队列"""
        try:
            # 使用较短的超时避免阻塞
            self.data_queue.put(data_batch, timeout=5.0)
        except Full:
            self.logger.warning(f"Data queue full, dropping data batch {data_batch.batch_id}")
            # 记录丢失的数据批次
            record_failed_task(
                task_name=f"data_queue_full",
                entity_id=data_batch.symbol,
                reason="data_queue_full",
                error_category="system"
            )
    
    def _handle_task_error(self, task: DownloadTask, error: Exception) -> None:
        """处理任务错误"""
        # 使用智能重试策略判断是否应该重试
        should_retry = self.retry_policy.should_retry(error, task.retry_count + 1)
        
        if should_retry and task.can_retry():
            # 计算延迟时间
            delay = self.retry_policy.get_delay(task.retry_count + 1)
            
            # 增加重试次数
            retry_task = task.increment_retry()
            
            # 延迟后重新入队
            if delay > 0:
                self.logger.info(f"Task {task.task_id} will retry after {delay:.2f}s delay "
                               f"({retry_task.retry_count}/{task.max_retries})")
                time.sleep(delay)
            
            try:
                self.task_queue.put(retry_task, timeout=1.0)
                self.logger.info(f"Task {task.task_id} requeued for retry "
                               f"({retry_task.retry_count}/{task.max_retries})")
            except Full:
                self.logger.error(f"Failed to requeue task {task.task_id}, "
                                f"task queue full")
                self._write_to_deadletter(task, error)
        else:
            # 写入死信日志
            self._write_to_deadletter(task, error)
        
        # 只有从队列中获取的任务才需要标记完成
        # self.task_queue.task_done()
    
    def _write_to_deadletter(self, task: DownloadTask, error: Exception) -> None:
        """写入死信日志"""
        # 使用新的DeadLetterLogger写入死信记录
        self.dead_letter_logger.write_dead_letter(task, error)
        
        # 同时保持原有的记录方式以兼容性
        error_category = classify_error(error)
        reason = f"max_retries_exceeded" if task.retry_count >= task.max_retries else str(error)
        
        record_failed_task(
            task_name=f"{task.task_type.value}_task",
            entity_id=task.symbol,
            reason=reason,
            error_category=error_category.value
        )
        
        self.logger.error(f"Task {task.task_id} sent to dead letter: {reason}")


class ProducerPool:
    """生产者池管理器"""
    
    def __init__(self, 
                 max_producers: int = 4,
                 task_queue: Optional[Queue] = None,
                 data_queue: Optional[Queue] = None,
                 retry_policy_config: Optional[RetryPolicy] = None,
                 dead_letter_path: str = "logs/dead_letter.jsonl",
                 fetcher_rate_limit: int = 150):
        """
        初始化生产者池
        
        Args:
            max_producers: 最大生产者线程数
            task_queue: 任务队列，如果为None则创建新队列
            data_queue: 数据队列，如果为None则创建新队列
            retry_policy_config: 重试策略配置
            dead_letter_path: 死信日志路径
            fetcher_rate_limit: TushareFetcher的默认速率限制
        """
        self.max_producers = max_producers
        self.task_queue = task_queue or Queue()
        self.data_queue = data_queue or Queue()
        self.retry_policy_config = retry_policy_config or DEFAULT_RETRY_POLICY
        self.dead_letter_path = dead_letter_path
        
        # 创建TushareFetcher实例
        self.fetcher = TushareFetcher(default_rate_limit=fetcher_rate_limit)
        
        # 线程池
        self.executor: Optional[ThreadPoolExecutor] = None
        self.workers: Dict[int, ProducerWorker] = {}
        self.running = False
        
        # 统计信息
        self.start_time: Optional[datetime] = None
        self.tasks_processed = 0
        self.tasks_failed = 0
        self._stats_lock = threading.Lock()
        
        self.logger = logging.getLogger(__name__)
    
    def start(self) -> None:
        """启动生产者池"""
        if self.running:
            self.logger.warning("Producer pool is already running")
            return
        
        self.running = True
        self.start_time = datetime.now()
        
        # 创建线程池
        self.executor = ThreadPoolExecutor(max_workers=self.max_producers)
        
        # 创建并启动工作线程
        for i in range(self.max_producers):
            worker = ProducerWorker(
                worker_id=i,
                fetcher=self.fetcher,
                task_queue=self.task_queue,
                data_queue=self.data_queue,
                retry_policy_config=self.retry_policy_config,
                dead_letter_path=self.dead_letter_path
            )
            
            self.workers[i] = worker
            # 提交工作线程到线程池
            self.executor.submit(worker.start)
        
        self.logger.info(f"Producer pool started with {self.max_producers} workers")
    
    def stop(self, timeout: float = 30.0) -> None:
        """停止生产者池"""
        if not self.running:
            return
        
        self.logger.info("Stopping producer pool...")
        self.running = False
        
        # 停止所有工作线程
        for worker in self.workers.values():
            worker.stop()
        
        # 关闭线程池
        if self.executor:
            self.executor.shutdown(wait=True)
        
        # 清理资源
        self.workers.clear()
        self.executor = None
        
        self.logger.info("Producer pool stopped")
    
    def submit_task(self, task: DownloadTask, timeout: float = 1.0) -> bool:
        """提交任务到任务队列"""
        if not self.running:
            raise RuntimeError("Producer pool is not running")
        
        try:
            self.task_queue.put(task, timeout=timeout)
            self.logger.debug(f"Task submitted: {task.task_id}")
            return True
        except Full:
            self.logger.warning(f"Task queue full, failed to submit task: {task.task_id}")
            return False
    
    def get_data(self, timeout: float = 1.0) -> Optional[DataBatch]:
        """从数据队列获取数据"""
        try:
            return self.data_queue.get(timeout=timeout)
        except Empty:
            return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._stats_lock:
            uptime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            
            return {
                'running': self.running,
                'max_producers': self.max_producers,
                'active_workers': len([w for w in self.workers.values() if w.running]),
                'task_queue_size': self.task_queue.qsize(),
                'data_queue_size': self.data_queue.qsize(),
                'tasks_processed': self.tasks_processed,
                'tasks_failed': self.tasks_failed,
                'uptime_seconds': uptime,
                'retry_policy_config': self.retry_policy_config.to_dict(),
                'dead_letter_path': str(self.dead_letter_path)
            }
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> None:
        """等待所有任务完成"""
        if not self.running:
            return
        
        try:
            # 等待任务队列中的所有任务完成
            if timeout:
                start_time = time.time()
                while not self.task_queue.empty():
                    if time.time() - start_time > timeout:
                        self.logger.warning("Timeout waiting for task completion")
                        break
                    time.sleep(0.1)
            else:
                self.task_queue.join()  # 等待所有任务完成
            
            self.logger.info("All tasks completed")
        except KeyboardInterrupt:
            self.logger.info("Task completion wait interrupted")
    
    @property
    def is_running(self) -> bool:
        """检查生产者池是否正在运行"""
        return self.running
    
    @property
    def task_queue_size(self) -> int:
        """获取任务队列大小"""
        return self.task_queue.qsize()
    
    @property
    def data_queue_size(self) -> int:
        """获取数据队列大小"""
        return self.data_queue.qsize()
