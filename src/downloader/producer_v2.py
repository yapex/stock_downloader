"""ProducerV2 实现

基于 IProducer 接口的简化实现，专注于任务处理和事件通知。
线程管理由外部 ThreadPoolExecutor 负责。
"""

import threading
from concurrent.futures import ThreadPoolExecutor, Future
from queue import Queue, Empty, Full
from typing import Optional
import pandas as pd

from .interfaces.producer import IProducer, ProducerEvents
from .interfaces.fetcher import IFetcher
from .interfaces.events import IEventBus
from .models import DownloadTask, DataBatch, TaskType
from .utils import get_logger

logger = get_logger(__name__)


class ProducerV2(IProducer):
    """生产者V2实现
    
    特点：
    - 依赖注入：IFetcher、ThreadPoolExecutor、IEventBus
    - 自管理队列：内部维护任务队列
    - 事件驱动：通过事件通知处理结果
    - 无线程管理：使用外部提供的线程池
    """
    
    def __init__(
        self,
        fetcher: IFetcher,
        executor: ThreadPoolExecutor,
        event_bus: IEventBus,
        queue_size: int = 100
    ):
        """初始化生产者
        
        Args:
            fetcher: 数据获取器
            executor: 线程池执行器
            event_bus: 事件总线
            queue_size: 任务队列大小
        """
        self._fetcher = fetcher
        self._executor = executor
        self._event_bus = event_bus
        self._task_queue: Queue[DownloadTask] = Queue(maxsize=queue_size)
        self._running = False
        self._worker_future: Optional[Future] = None
        self._lock = threading.Lock()
        
    def start(self) -> None:
        """启动生产者"""
        with self._lock:
            if self._running:
                logger.warning("Producer already running")
                return
                
            self._running = True
            self._worker_future = self._executor.submit(self._worker_loop)
            logger.info("Producer started")
    
    def stop(self, timeout: float = 30.0) -> None:
        """停止生产者
        
        Args:
            timeout: 等待停止的超时时间（秒）
        """
        with self._lock:
            if not self._running:
                return
                
            self._running = False
            
        # 等待工作线程结束
        if self._worker_future:
            try:
                self._worker_future.result(timeout=timeout)
            except Exception as e:
                logger.error(f"Error stopping producer: {e}")
            finally:
                self._worker_future = None
                
        logger.info("Producer stopped")
    
    def submit_task(self, task: DownloadTask, timeout: float = 1.0) -> bool:
        """提交任务到队列
        
        Args:
            task: 要提交的任务
            timeout: 提交超时时间（秒）
            
        Returns:
            是否成功提交
            
        Raises:
            RuntimeError: 生产者未运行时提交任务
        """
        if not self._running:
            raise RuntimeError("Producer is not running")
            
        try:
            self._task_queue.put(task, timeout=timeout)
            return True
        except Full:
            logger.warning(f"Failed to submit task {task.symbol}: queue full")
            return False
    
    @property
    def is_running(self) -> bool:
        """检查是否正在运行"""
        return self._running
    
    def _worker_loop(self) -> None:
        """工作线程主循环"""
        logger.info("Producer worker loop started")
        
        while self._running:
            try:
                # 获取任务（非阻塞，避免无法及时响应停止信号）
                task = self._task_queue.get(timeout=0.1)
                self._process_task(task)
                self._task_queue.task_done()
                
            except Empty:
                # 队列为空，继续循环
                continue
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                
        logger.info("Producer worker loop ended")
    
    def _process_task(self, task: DownloadTask) -> None:
        """处理单个任务
        
        Args:
            task: 要处理的任务
        """
        try:
            logger.debug(f"Processing task: {task.symbol}, type: {task.task_type.value}")
            
            # 根据任务类型调用相应的 fetcher 方法
            data = self._fetch_data_by_type(task)
            
            if data is not None and not data.empty:
                # 创建数据批次
                batch = DataBatch(
                    df=data,
                    meta={
                        'task_type': task.task_type.value,
                        'params': task.params,
                        'priority': task.priority.value
                    },
                    task_id=task.task_id,
                    symbol=task.symbol
                )
                
                # 通过事件通知数据准备完成
                self._event_bus.publish(ProducerEvents.DATA_READY, batch)
                
                # 通知任务完成
                self._event_bus.publish(ProducerEvents.TASK_COMPLETED, task)
                
                logger.debug(f"Task completed: {task.symbol}")
            else:
                logger.warning(f"No data fetched for task: {task.symbol}")
                self._event_bus.publish(ProducerEvents.TASK_FAILED, task)
                
        except Exception as e:
            logger.error(f"Failed to process task {task.symbol}: {e}")
            self._event_bus.publish(ProducerEvents.TASK_FAILED, task)
    
    def _fetch_data_by_type(self, task: DownloadTask) -> pd.DataFrame | None:
        """根据任务类型获取数据
        
        Args:
            task: 下载任务
            
        Returns:
            获取到的数据或None
        """
        params = task.params
        
        if task.task_type == TaskType.STOCK_LIST:
            return self._fetcher.fetch_stock_list()
        
        elif task.task_type == TaskType.DAILY:
            return self._fetcher.fetch_daily_history(
                ts_code=task.symbol,
                start_date=params.get('start_date', ''),
                end_date=params.get('end_date', ''),
                adjust=params.get('adjust', 'qfq')
            )
        
        elif task.task_type == TaskType.DAILY_BASIC:
            return self._fetcher.fetch_daily_basic(
                ts_code=task.symbol,
                start_date=params.get('start_date', ''),
                end_date=params.get('end_date', '')
            )
        
        elif task.task_type == TaskType.FINANCIALS:
            # 财务数据需要根据具体类型调用不同方法
            financial_type = params.get('financial_type', 'income')
            if financial_type == 'income':
                return self._fetcher.fetch_income(
                    ts_code=task.symbol,
                    start_date=params.get('start_date', ''),
                    end_date=params.get('end_date', '')
                )
            elif financial_type == 'balancesheet':
                return self._fetcher.fetch_balancesheet(
                    ts_code=task.symbol,
                    start_date=params.get('start_date', ''),
                    end_date=params.get('end_date', '')
                )
            elif financial_type == 'cashflow':
                return self._fetcher.fetch_cashflow(
                    ts_code=task.symbol,
                    start_date=params.get('start_date', ''),
                    end_date=params.get('end_date', '')
                )
        
        logger.warning(f"Unknown task type: {task.task_type}")
        return None