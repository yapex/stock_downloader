"""
增强的队列管理器实现
使用queue.Queue管理两条管道，支持优先级和智能重试
"""

import asyncio
import queue
import threading
import time
import logging
from typing import Optional, List
from datetime import datetime, timedelta

from .models import DownloadTask, DataBatch, TaskResult, Priority


class EnhancedQueueManager:
    """增强的队列管理器
    
    使用两个 queue.Queue 管理任务和数据流：
    - 任务队列：使用 PriorityQueue 支持优先级调度
    - 数据队列：使用普通 Queue 管理数据批次
    """
    
    def __init__(self,
                 task_queue_size: int = 1000,
                 data_queue_size: int = 5000,
                 queue_timeout: float = 30.0,
                 enable_smart_retry: bool = True):
        """初始化队列管理器
        
        Args:
            task_queue_size: 任务队列最大容量
            data_queue_size: 数据队列最大容量
            queue_timeout: 队列操作超时时间（秒）
            enable_smart_retry: 是否启用智能重试
        """
        self._task_queue_size = task_queue_size
        self._data_queue_size = data_queue_size
        self._queue_timeout = queue_timeout
        self._enable_smart_retry = enable_smart_retry
        
        # 使用优先级队列管理任务
        self._task_queue = queue.PriorityQueue(maxsize=task_queue_size)
        self._data_queue = queue.Queue(maxsize=data_queue_size)
        
        # 重试队列：延迟重试的任务
        self._retry_queue = queue.PriorityQueue(maxsize=task_queue_size)
        
        # 状态管理
        self._running = False
        self._start_time: Optional[datetime] = None
        
        # 指标统计
        self._metrics = {
            'tasks_submitted': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'tasks_retried': 0,
            'data_batches_processed': 0,
            'queue_full_errors': 0
        }
        self._metrics_lock = threading.Lock()
        
        # 重试管理
        self._retry_delays = {
            Priority.HIGH: [1.0, 2.0, 4.0],      # 高优先级：快速重试
            Priority.NORMAL: [2.0, 5.0, 10.0],   # 普通优先级：标准重试
            Priority.LOW: [5.0, 15.0, 30.0]      # 低优先级：慢速重试
        }
        
        # 启动重试处理器
        self._retry_thread = None
        
        self.logger = logging.getLogger(__name__)
    
    def start(self) -> None:
        """启动队列管理器"""
        if self._running:
            return
        
        self._running = True
        self._start_time = datetime.now()
        
        # 启动重试处理线程
        if self._enable_smart_retry:
            self._retry_thread = threading.Thread(
                target=self._process_retries, daemon=True
            )
            self._retry_thread.start()
        
        self.logger.info(f"队列管理器已启动，任务队列大小: {self._task_queue_size}, "
                        f"数据队列大小: {self._data_queue_size}")
    
    def stop(self) -> None:
        """停止队列管理器"""
        if not self._running:
            return
        
        self._running = False
        
        # 清空队列
        self._clear_queue(self._task_queue)
        self._clear_queue(self._data_queue)
        self._clear_queue(self._retry_queue)
        
        self.logger.info("队列管理器已停止")
    
    def _clear_queue(self, q: queue.Queue) -> None:
        """清空队列"""
        while not q.empty():
            try:
                q.get_nowait()
                q.task_done()
            except queue.Empty:
                break
    
    def put_task(self, task: DownloadTask, timeout: Optional[float] = None) -> bool:
        """添加任务到任务队列
        
        Args:
            task: 下载任务
            timeout: 超时时间，None表示使用默认超时
            
        Returns:
            是否成功添加
        """
        if not self._running:
            raise RuntimeError("队列管理器未启动")
        
        timeout = timeout or self._queue_timeout
        
        # 创建优先级项目：(优先级值，时间戳，任务)
        priority_value = -task.priority.value  # 负值使高优先级排在前面
        timestamp = time.time()
        priority_item = (priority_value, timestamp, task)
        
        try:
            self._task_queue.put(priority_item, timeout=timeout)
            
            with self._metrics_lock:
                self._metrics['tasks_submitted'] += 1
            
            self.logger.debug(f"任务已添加到队列: {task.symbol} - {task.task_type.value}")
            return True
            
        except queue.Full:
            with self._metrics_lock:
                self._metrics['queue_full_errors'] += 1
            
            self.logger.warning(f"任务队列已满，无法添加任务: {task.symbol}")
            return False
    
    def get_task(self, timeout: Optional[float] = None) -> Optional[DownloadTask]:
        """从任务队列获取任务
        
        Args:
            timeout: 超时时间，None表示使用默认超时
            
        Returns:
            下载任务或None
        """
        if not self._running:
            return None
        
        timeout = timeout or self._queue_timeout
        
        try:
            priority_item = self._task_queue.get(timeout=timeout)
            _, _, task = priority_item
            return task
        except queue.Empty:
            return None
    
    def task_done(self) -> None:
        """标记任务完成"""
        self._task_queue.task_done()
    
    def put_data(self, data_batch: DataBatch, timeout: Optional[float] = None) -> bool:
        """添加数据批次到数据队列
        
        Args:
            data_batch: 数据批次
            timeout: 超时时间，None表示使用默认超时
            
        Returns:
            是否成功添加
        """
        if not self._running:
            raise RuntimeError("队列管理器未启动")
        
        timeout = timeout or self._queue_timeout
        
        try:
            self._data_queue.put(data_batch, timeout=timeout)
            
            with self._metrics_lock:
                self._metrics['data_batches_processed'] += 1
            
            self.logger.debug(f"数据批次已添加到队列: {data_batch.symbol}, "
                            f"大小: {data_batch.size}")
            return True
            
        except queue.Full:
            with self._metrics_lock:
                self._metrics['queue_full_errors'] += 1
            
            self.logger.warning(f"数据队列已满，无法添加数据批次: {data_batch.symbol}")
            return False
    
    def get_data(self, timeout: Optional[float] = None) -> Optional[DataBatch]:
        """从数据队列获取数据批次
        
        Args:
            timeout: 超时时间，None表示使用默认超时
            
        Returns:
            数据批次或None
        """
        if not self._running:
            return None
        
        timeout = timeout or self._queue_timeout
        
        try:
            return self._data_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def data_done(self) -> None:
        """标记数据处理完成"""
        self._data_queue.task_done()
    
    def schedule_retry(self, task: DownloadTask, error: str) -> bool:
        """调度任务重试
        
        Args:
            task: 失败的任务
            error: 错误信息
            
        Returns:
            是否成功调度重试
        """
        if not self._enable_smart_retry or not task.can_retry():
            with self._metrics_lock:
                self._metrics['tasks_failed'] += 1
            return False
        
        # 创建重试任务
        retry_task = task.increment_retry()
        
        # 计算重试延迟
        delay = self._get_retry_delay(task.priority, task.retry_count)
        retry_time = time.time() + delay
        
        # 添加到重试队列
        retry_item = (retry_time, retry_task)
        
        try:
            self._retry_queue.put(retry_item, timeout=1.0)
            
            with self._metrics_lock:
                self._metrics['tasks_retried'] += 1
            
            self.logger.info(f"任务已调度重试: {task.symbol}, 重试次数: {retry_task.retry_count}, "
                           f"延迟: {delay}秒")
            return True
            
        except queue.Full:
            self.logger.error(f"重试队列已满，无法调度重试: {task.symbol}")
            with self._metrics_lock:
                self._metrics['tasks_failed'] += 1
            return False
    
    def _get_retry_delay(self, priority: Priority, retry_count: int) -> float:
        """获取重试延迟时间"""
        delays = self._retry_delays.get(priority, self._retry_delays[Priority.NORMAL])
        if retry_count >= len(delays):
            return delays[-1]
        return delays[retry_count]
    
    def _process_retries(self) -> None:
        """重试处理线程"""
        while self._running:
            try:
                # 获取需要重试的任务
                retry_item = self._retry_queue.get(timeout=1.0)
                retry_time, task = retry_item
                
                # 检查是否到了重试时间
                current_time = time.time()
                if current_time < retry_time:
                    # 重新放回队列
                    self._retry_queue.put(retry_item)
                    time.sleep(0.1)
                    continue
                
                # 重新添加到任务队列
                if not self.put_task(task, timeout=1.0):
                    # 如果任务队列满了，稍后再试
                    self._retry_queue.put(retry_item)
                    time.sleep(0.5)
                
                self._retry_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"重试处理错误: {e}")
    
    def report_task_completed(self, task: DownloadTask) -> None:
        """报告任务完成"""
        with self._metrics_lock:
            self._metrics['tasks_completed'] += 1
        
        self.logger.debug(f"任务已完成: {task.symbol} - {task.task_type.value}")
    
    def report_task_failed(self, task: DownloadTask, error: str) -> None:
        """报告任务失败"""
        # 先尝试重试，如果不能重试则标记为失败
        if not self.schedule_retry(task, error):
            # schedule_retry内部已经处理了tasks_failed计数，这里不需要重复
            pass
        
        self.logger.error(f"任务失败: {task.symbol} - {task.task_type.value}, "
                         f"错误: {error}")
    
    # 属性接口
    @property
    def task_queue_size(self) -> int:
        """当前任务队列大小"""
        return self._task_queue.qsize()
    
    @property
    def data_queue_size(self) -> int:
        """当前数据队列大小"""
        return self._data_queue.qsize()
    
    @property
    def retry_queue_size(self) -> int:
        """当前重试队列大小"""
        return self._retry_queue.qsize()
    
    @property
    def is_running(self) -> bool:
        """队列管理器是否正在运行"""
        return self._running
    
    @property
    def metrics(self) -> dict:
        """获取当前指标"""
        with self._metrics_lock:
            metrics = self._metrics.copy()
        
        # 添加队列状态
        metrics.update({
            'task_queue_size': self.task_queue_size,
            'data_queue_size': self.data_queue_size,
            'retry_queue_size': self.retry_queue_size,
            'task_queue_utilization': self.task_queue_size / max(self._task_queue_size, 1),
            'data_queue_utilization': self.data_queue_size / max(self._data_queue_size, 1),
        })
        
        # 计算成功率
        total_processed = metrics['tasks_completed'] + metrics['tasks_failed']
        if total_processed > 0:
            metrics['success_rate'] = metrics['tasks_completed'] / total_processed
        else:
            metrics['success_rate'] = 0.0
        
        # 运行时间
        if self._start_time:
            metrics['uptime_seconds'] = (datetime.now() - self._start_time).total_seconds()
        else:
            metrics['uptime_seconds'] = 0.0
        
        return metrics
    
    def join_queues(self, timeout: Optional[float] = None) -> None:
        """等待所有队列处理完成"""
        self._task_queue.join()
        self._data_queue.join()
