"""生产者池管理器 - 重新设计版本

简化的单线程生产者实现，专注于任务处理的核心逻辑。
移除了不必要的复杂性，提供清晰的接口和可靠的错误处理。
"""

import logging
import time
import threading
from typing import Optional, Dict, Any
from queue import Queue, Empty, Full
from datetime import datetime

from .fetcher import TushareFetcher
from .fetcher_factory import get_fetcher
from .models import DownloadTask, DataBatch, TaskType
from .error_handler import classify_error, ErrorCategory
from .retry_policy import RetryPolicy, DEFAULT_RETRY_POLICY, RetryLogger
from .utils import record_failed_task
from .progress_events import task_started, task_completed, task_failed

logger = logging.getLogger(__name__)


class TaskProcessor:
    """任务处理器 - 负责单个任务的执行"""
    
    def __init__(self, fetcher: TushareFetcher):
        self.fetcher = fetcher
        self.logger = logging.getLogger(f"{__name__}.processor")
        self.logger.debug(f"TaskProcessor初始化，使用fetcher实例ID: {id(fetcher)}")
    
    def process(self, task: DownloadTask) -> Optional[DataBatch]:
        """处理单个任务并返回数据批次"""
        try:
            self.logger.debug(f"Processing task {task.task_id}: {task.symbol} - {task.task_type.value}")
            
            # 获取数据
            data = self._fetch_data(task)
            
            # 创建数据批次
            if data is not None and not data.empty:
                return DataBatch(
                    df=data,
                    meta={
                        'task_type': task.task_type.value,
                        'symbol': task.symbol,
                        'params': task.params,
                        'processed_at': datetime.now().isoformat()
                    },
                    task_id=task.task_id,
                    symbol=task.symbol
                )
            else:
                # 返回空数据批次
                return DataBatch.empty(
                    task_id=task.task_id,
                    symbol=task.symbol,
                    meta={
                        'task_type': task.task_type.value,
                        'reason': 'no_data',
                        'processed_at': datetime.now().isoformat()
                    }
                )
                
        except Exception as e:
            self.logger.error(f"Error processing task {task.task_id}: {e}")
            raise
    
    def _fetch_data(self, task: DownloadTask):
        """根据任务类型获取数据"""
        if task.task_type == TaskType.STOCK_LIST:
            return self.fetcher.fetch_stock_list()
        
        elif task.task_type == TaskType.DAILY:
            params = task.params
            return self.fetcher.fetch_daily_history(
                ts_code=task.symbol,
                start_date=params.get('start_date', ''),
                end_date=params.get('end_date', ''),
                adjust=params.get('adjust', 'hfq')
            )
        
        elif task.task_type == TaskType.DAILY_BASIC:
            params = task.params
            return self.fetcher.fetch_daily_basic(
                ts_code=task.symbol,
                start_date=params.get('start_date', ''),
                end_date=params.get('end_date', '')
            )
        
        elif task.task_type == TaskType.FINANCIALS:
            return self._fetch_financials_data(task)
        
        else:
            raise ValueError(f"Unknown task type: {task.task_type}")
    
    def _fetch_financials_data(self, task: DownloadTask):
        """获取财务数据"""
        params = task.params
        ts_code = task.symbol
        start_date = params.get('start_date', '')
        end_date = params.get('end_date', '')
        
        # 从任务配置中获取财务报表类型
        task_config = params.get('task_config', {})
        statement_type = task_config.get('statement_type', 'income')
        
        if statement_type == 'income':
            return self.fetcher.fetch_income(ts_code, start_date, end_date)
        elif statement_type == 'balancesheet':
            return self.fetcher.fetch_balancesheet(ts_code, start_date, end_date)
        elif statement_type == 'cashflow':
            return self.fetcher.fetch_cashflow(ts_code, start_date, end_date)
        else:
            raise ValueError(f"Unknown statement_type: {statement_type}")


class RetryManager:
    """重试管理器 - 负责任务重试逻辑"""
    
    def __init__(self, retry_policy: RetryPolicy, dead_letter_path: str):
        self.retry_policy = retry_policy
        self.retry_logger = RetryLogger(dead_letter_path)
        self.logger = logging.getLogger(f"{__name__}.retry")
    
    def handle_task_failure(self, task: DownloadTask, error: Exception, task_queue: Queue) -> bool:
        """处理任务失败，返回是否成功重新入队"""
        should_retry = self.retry_policy.should_retry(error, task.retry_count + 1)
        
        self.logger.info(
            f"[重试管理] 评估任务重试 - ID: {task.task_id}, "
            f"股票: {task.symbol}, 当前重试: {task.retry_count}, "
            f"最大重试: {task.max_retries}, 是否可重试: {should_retry and task.can_retry()}"
        )
        
        if should_retry and task.can_retry():
            return self._retry_task(task, task_queue)
        else:
            self.logger.warning(
                f"[重试管理] 任务不可重试 - ID: {task.task_id}, "
                f"股票: {task.symbol}, 原因: {'超过最大重试次数' if not task.can_retry() else '重试策略拒绝'}"
            )
            self._send_to_dead_letter(task, error)
            return False
    
    def _retry_task(self, task: DownloadTask, task_queue: Queue) -> bool:
        """重试任务"""
        delay = self.retry_policy.get_delay(task.retry_count + 1)
        retry_task = task.increment_retry()
        
        self.logger.info(
            f"[重试管理] 准备重试任务 - ID: {task.task_id}, "
            f"股票: {task.symbol}, 新重试次数: {retry_task.retry_count}/{task.max_retries}, "
            f"延迟: {delay:.2f}s"
        )
        
        if delay > 0:
            time.sleep(delay)
        
        try:
            task_queue.put(retry_task, timeout=1.0)
            self.logger.info(
                f"[重试管理] 任务重新入队成功 - ID: {task.task_id}, "
                f"股票: {task.symbol}, 当前队列大小: {task_queue.qsize()}"
            )
            return True
        except Full:
            self.logger.error(
                f"[重试管理] 任务重新入队失败 - ID: {task.task_id}, "
                f"股票: {task.symbol}, 原因: 队列已满"
            )
            self._send_to_dead_letter(task, Exception("Queue full during retry"))
            return False
    
    def _send_to_dead_letter(self, task: DownloadTask, error: Exception):
        """发送任务到死信队列"""
        self.retry_logger.log_failed_symbol(task.symbol)
        
        error_category = classify_error(error)
        reason = f"max_retries_exceeded" if task.retry_count >= task.max_retries else str(error)
        
        record_failed_task(
            task_name=f"{task.task_type.value}_task",
            entity_id=task.symbol,
            reason=reason,
            error_category=error_category.value
        )
        
        self.logger.error(
            f"[重试管理] 任务发送到死信队列 - ID: {task.task_id}, "
            f"股票: {task.symbol}, 最终重试次数: {task.retry_count}, "
            f"失败原因: {reason}, 错误类型: {error_category.value}"
        )


class ProducerStats:
    """生产者统计信息管理器"""
    
    def __init__(self):
        self.start_time: Optional[datetime] = None
        self.tasks_processed = 0
        self.tasks_failed = 0
        self._lock = threading.Lock()
    
    def start(self):
        """开始统计"""
        with self._lock:
            self.start_time = datetime.now()
    
    def increment_processed(self):
        """增加已处理任务数"""
        with self._lock:
            self.tasks_processed += 1
    
    def increment_failed(self):
        """增加失败任务数"""
        with self._lock:
            self.tasks_failed += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            uptime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            return {
                'tasks_processed': self.tasks_processed,
                'tasks_failed': self.tasks_failed,
                'uptime_seconds': uptime
            }


class Producer:
    """生产者 - 简化的单线程实现"""
    
    def __init__(self, 
                 task_queue: Optional[Queue] = None,
                 data_queue: Optional[Queue] = None,
                 retry_policy: Optional[RetryPolicy] = None,
                 dead_letter_path: str = "logs/dead_letter.jsonl",
                 fetcher: Optional[TushareFetcher] = None):
        """初始化生产者"""
        self.task_queue = task_queue or Queue()
        self.data_queue = data_queue or Queue()
        
        # 使用单例模式的fetcher，确保所有Producer共享同一个实例
        if fetcher is not None:
            self.fetcher = fetcher
            logger.info(f"Producer使用传入的fetcher实例，ID: {id(fetcher)}")
        else:
            self.fetcher = get_fetcher(use_singleton=True)
            logger.info(f"Producer使用单例fetcher实例，ID: {id(self.fetcher)}")
        
        # 组件初始化
        self.processor = TaskProcessor(self.fetcher)
        self.retry_manager = RetryManager(
            retry_policy or DEFAULT_RETRY_POLICY, 
            dead_letter_path
        )
        self.stats = ProducerStats()
        
        # 运行状态
        self.running = False
        self.worker_thread: Optional[threading.Thread] = None
        
        self.logger = logging.getLogger(__name__)
    
    def start(self) -> None:
        """启动生产者"""
        if self.running:
            self.logger.warning("Producer is already running")
            return
        
        self.running = True
        self.stats.start()
        
        # 启动工作线程
        self.worker_thread = threading.Thread(
            target=self._worker_loop,
            name="ProducerWorker",
            daemon=True
        )
        self.worker_thread.start()
        
        self.logger.info("Producer started")
    
    def stop(self, timeout: float = 30.0) -> None:
        """停止生产者"""
        if not self.running:
            return
        
        self.logger.info("Stopping producer...")
        self.running = False
        
        # 等待工作线程结束
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=timeout)
            if self.worker_thread.is_alive():
                self.logger.warning("Worker thread did not stop within timeout")
        
        self.worker_thread = None
        self.logger.info("Producer stopped")
    
    def submit_task(self, task: DownloadTask, timeout: float = 1.0) -> bool:
        """提交任务"""
        if not self.running:
            raise RuntimeError("Producer is not running")
        
        try:
            self.task_queue.put(task, timeout=timeout)
            self.logger.debug(f"Task submitted: {task.task_id}")
            return True
        except Full:
            self.logger.warning(f"Task queue full, failed to submit task: {task.task_id}")
            return False
    
    def get_data(self, timeout: float = 1.0) -> Optional[DataBatch]:
        """获取处理结果"""
        try:
            return self.data_queue.get(timeout=timeout)
        except Empty:
            return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.get_stats()
        stats.update({
            'running': self.running,
            'active_workers': 1 if self.running else 0,
            'task_queue_size': self.task_queue.qsize(),
            'data_queue_size': self.data_queue.qsize()
        })
        return stats
    
    def _worker_loop(self) -> None:
        """工作线程主循环"""
        self.logger.info("Producer worker started")
        
        while self.running:
            try:
                # 获取任务
                task = self._get_task()
                if task is None:
                    continue
                
                # 处理任务
                self._process_single_task(task)
                
            except Exception as e:
                self.logger.error(f"Unexpected error in worker loop: {e}")
                time.sleep(1)  # 防止快速循环
        
        self.logger.info("Producer worker stopped")
    
    def _get_task(self) -> Optional[DownloadTask]:
        """获取任务"""
        try:
            return self.task_queue.get(timeout=1.0)
        except Empty:
            return None
    
    def _process_single_task(self, task: DownloadTask) -> None:
        """处理单个任务"""
        # 任务开始日志
        self.logger.info(
            f"[任务处理] 开始处理任务 - ID: {task.task_id}, "
            f"类型: {task.task_type.value}, 股票: {task.symbol}, "
            f"重试次数: {task.retry_count}/{task.max_retries}, "
            f"Producer实例: {id(self)}, Fetcher实例: {id(self.fetcher)}"
        )
        
        # 发送任务开始事件
        task_started(
            task_id=task.task_id,
            symbol=task.symbol,
            message=f"{task.task_type.value}"
        )
        
        start_time = time.time()
        
        try:
            # 使用处理器处理任务
            data_batch = self.processor.process(task)
            
            # 计算处理时间
            processing_time = time.time() - start_time
            
            # 将结果放入数据队列
            self.data_queue.put(data_batch, block=True)
            
            # 更新统计
            self.stats.increment_processed()
            
            # 任务成功日志
            data_count = len(data_batch.df) if data_batch and data_batch.df is not None else 0
            self.logger.info(
                f"[任务处理] 任务完成 - ID: {task.task_id}, "
                f"股票: {task.symbol}, 数据量: {data_count}条, "
                f"处理时间: {processing_time:.2f}s"
            )
            
            # 发送任务完成事件
            task_completed(
                task_id=task.task_id,
                symbol=task.symbol
            )
            
        except Exception as e:
            # 计算处理时间
            processing_time = time.time() - start_time
            
            # 任务失败日志
            self.logger.warning(
                f"[任务处理] 任务失败 - ID: {task.task_id}, "
                f"股票: {task.symbol}, 错误: {e}, "
                f"处理时间: {processing_time:.2f}s, 重试次数: {task.retry_count}"
            )
            
            # 使用重试管理器处理失败
            retry_success = self.retry_manager.handle_task_failure(task, e, self.task_queue)
            
            if not retry_success:
                self.stats.increment_failed()
                self.logger.error(
                    f"[任务处理] 任务最终失败 - ID: {task.task_id}, "
                    f"股票: {task.symbol}, 已达最大重试次数"
                )
                
                # 发送任务失败事件
                task_failed(
                    task_id=task.task_id,
                    symbol=task.symbol,
                    message=str(e)
                )
    
    @property
    def is_running(self) -> bool:
        """检查是否正在运行"""
        return self.running
    
    @property
    def task_queue_size(self) -> int:
        """获取任务队列大小"""
        return self.task_queue.qsize()
    
    @property
    def data_queue_size(self) -> int:
        """获取数据队列大小"""
        return self.data_queue.qsize()