"""
内存队列管理器实现
实现IQueueManager接口，提供基于内存的任务和数据队列管理
"""

import asyncio
import queue
import threading
import time
from typing import Optional
from datetime import datetime

from .interfaces import (
    IQueueManager, TaskMessage, DataMessage, 
    PerformanceMetrics, Priority
)


class MemoryQueueManager(IQueueManager):
    """基于内存的队列管理器实现"""
    
    def __init__(self, 
                 task_queue_size: int = 1000,
                 data_queue_size: int = 5000,
                 queue_timeout: float = 30.0):
        """
        初始化队列管理器
        
        Args:
            task_queue_size: 任务队列最大容量
            data_queue_size: 数据队列最大容量
            queue_timeout: 队列操作超时时间（秒）
        """
        self._task_queue_size = task_queue_size
        self._data_queue_size = data_queue_size
        self._queue_timeout = queue_timeout
        
        # 优先级队列：高优先级任务优先执行
        self._task_queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=task_queue_size)
        self._data_queue: queue.Queue = queue.Queue(maxsize=data_queue_size)
        
        # 性能指标
        self._metrics = PerformanceMetrics()
        self._metrics_lock = threading.Lock()
        
        # 运行状态
        self._running = False
        self._start_time: Optional[datetime] = None
    
    async def start(self) -> None:
        """启动队列管理器"""
        if self._running:
            return
            
        self._running = True
        self._start_time = datetime.now()
        
        # 重置指标
        with self._metrics_lock:
            self._metrics.reset()
    
    async def stop(self) -> None:
        """停止队列管理器"""
        self._running = False
        
        # 清空队列
        while not self._task_queue.empty():
            try:
                self._task_queue.get_nowait()
                self._task_queue.task_done()
            except queue.Empty:
                break
        
        while not self._data_queue.empty():
            try:
                self._data_queue.get_nowait() 
                self._data_queue.task_done()
            except queue.Empty:
                break
    
    async def put_task(self, task: TaskMessage) -> None:
        """添加任务到任务队列"""
        if not self._running:
            raise RuntimeError("Queue manager is not running")
        
        # 使用优先级队列，优先级值越小越优先
        priority_value = -task.priority.value  # 反转优先级值
        priority_item = (priority_value, time.time(), task)
        
        try:
            # 使用线程池执行阻塞操作
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, 
                lambda: self._task_queue.put(priority_item, timeout=self._queue_timeout)
            )
            
            # 更新指标
            with self._metrics_lock:
                self._metrics.tasks_total += 1
                
        except queue.Full:
            raise RuntimeError(f"Task queue is full (size: {self._task_queue_size})")
    
    async def get_task(self, timeout: Optional[float] = None) -> Optional[TaskMessage]:
        """从任务队列获取任务"""
        if not self._running:
            return None
        
        timeout = timeout or self._queue_timeout
        
        try:
            # 使用线程池执行阻塞操作
            loop = asyncio.get_event_loop()
            priority_item = await loop.run_in_executor(
                None,
                lambda: self._task_queue.get(timeout=timeout)
            )
            
            # 解包优先级项目
            _, _, task = priority_item
            return task
            
        except queue.Empty:
            return None
    
    async def put_data(self, data: DataMessage) -> None:
        """添加数据到数据队列"""
        if not self._running:
            raise RuntimeError("Queue manager is not running")
        
        try:
            # 使用线程池执行阻塞操作
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._data_queue.put(data, timeout=self._queue_timeout)
            )
            
            # 更新指标
            with self._metrics_lock:
                self._metrics.data_messages_total += 1
                
        except queue.Full:
            raise RuntimeError(f"Data queue is full (size: {self._data_queue_size})")
    
    async def get_data(self, timeout: Optional[float] = None) -> Optional[DataMessage]:
        """从数据队列获取数据"""
        if not self._running:
            return None
        
        timeout = timeout or self._queue_timeout
        
        try:
            # 使用线程池执行阻塞操作
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(
                None,
                lambda: self._data_queue.get(timeout=timeout)
            )
            
            # 更新指标
            with self._metrics_lock:
                self._metrics.data_messages_processed += 1
                
            return data
            
        except queue.Empty:
            return None
    
    def task_done(self) -> None:
        """标记任务完成"""
        self._task_queue.task_done()
    
    def data_done(self) -> None:
        """标记数据处理完成"""
        self._data_queue.task_done()
    
    @property
    def task_queue_size(self) -> int:
        """任务队列大小"""
        return self._task_queue.qsize()
    
    @property
    def data_queue_size(self) -> int:
        """数据队列大小"""
        return self._data_queue.qsize()
    
    @property
    def is_running(self) -> bool:
        """队列管理器是否正在运行"""
        return self._running
    
    @property
    def task_queue_full_ratio(self) -> float:
        """任务队列满载率"""
        if self._task_queue_size == 0:
            return 0.0
        return self.task_queue_size / self._task_queue_size
    
    @property
    def data_queue_full_ratio(self) -> float:
        """数据队列满载率"""
        if self._data_queue_size == 0:
            return 0.0
        return self.data_queue_size / self._data_queue_size
    
    def get_metrics(self) -> PerformanceMetrics:
        """获取性能指标"""
        with self._metrics_lock:
            # 创建指标副本
            metrics = PerformanceMetrics(
                tasks_total=self._metrics.tasks_total,
                tasks_completed=self._metrics.tasks_completed,
                tasks_failed=self._metrics.tasks_failed,
                data_messages_total=self._metrics.data_messages_total,
                data_messages_processed=self._metrics.data_messages_processed
            )
            
            # 更新队列大小信息
            metrics.queue_sizes = {
                "task_queue": self.task_queue_size,
                "data_queue": self.data_queue_size,
                "task_queue_ratio": self.task_queue_full_ratio,
                "data_queue_ratio": self.data_queue_full_ratio
            }
            
            # 计算处理速率（如果有运行时间）
            if self._start_time:
                running_time = (datetime.now() - self._start_time).total_seconds()
                if running_time > 0:
                    metrics.processing_rates = {
                        "tasks_per_second": self._metrics.tasks_total / running_time,
                        "data_messages_per_second": self._metrics.data_messages_total / running_time
                    }
            
            return metrics
    
    def update_task_completed(self) -> None:
        """更新任务完成计数"""
        with self._metrics_lock:
            self._metrics.tasks_completed += 1
    
    def update_task_failed(self) -> None:
        """更新任务失败计数"""
        with self._metrics_lock:
            self._metrics.tasks_failed += 1
    
    def join_task_queue(self, timeout: Optional[float] = None) -> None:
        """等待所有任务完成"""
        self._task_queue.join()
    
    def join_data_queue(self, timeout: Optional[float] = None) -> None:
        """等待所有数据处理完成"""
        self._data_queue.join()


class QueueManagerFactory:
    """队列管理器工厂类"""
    
    @staticmethod
    def create_memory_queue_manager(config: dict) -> MemoryQueueManager:
        """创建内存队列管理器"""
        return MemoryQueueManager(
            task_queue_size=config.get('task_queue_size', 1000),
            data_queue_size=config.get('data_queue_size', 5000),
            queue_timeout=config.get('queue_timeout', 30.0)
        )
    
    @staticmethod
    def create_queue_manager(queue_type: str, config: dict) -> IQueueManager:
        """根据类型创建队列管理器"""
        if queue_type == "memory":
            return QueueManagerFactory.create_memory_queue_manager(config)
        else:
            raise ValueError(f"Unsupported queue type: {queue_type}")
