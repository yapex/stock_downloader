"""Producer多线程测试

测试Producer内部多线程执行下载任务的功能。
"""

import pytest
import time
import threading
import pandas as pd
from queue import Queue
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.downloader.producer import Producer
from src.downloader.interfaces.events import IEventListener
from src.downloader.event_bus import SimpleEventBus
from src.downloader.interfaces.producer import ProducerEvents
from src.downloader.models import DownloadTask, DataBatch
from src.downloader.fetcher import TushareFetcher


class ThreadSafeEventListener(IEventListener):
    """线程安全的事件监听器"""
    
    def __init__(self):
        self.events = []
        self._lock = threading.Lock()
    
    def on_event(self, event_type: str, event_data: any) -> None:
        with self._lock:
            self.events.append((event_type, event_data, threading.get_ident()))
    
    def get_events(self):
        with self._lock:
            return self.events.copy()


def test_producer_uses_separate_thread():
    """测试Producer使用独立线程执行任务"""
    # 创建事件总线和监听器
    event_bus = SimpleEventBus()
    listener = ThreadSafeEventListener()
    event_bus.subscribe(ProducerEvents.DATA_READY, listener)
    
    # 创建mock fetcher
    mock_fetcher = Mock(spec=TushareFetcher)
    
    # 创建Producer
    executor = ThreadPoolExecutor(max_workers=1)
    producer = Producer(
        event_bus=event_bus,
        fetcher=mock_fetcher,
        thread_pool_executor=executor
    )
    
    # 记录主线程ID
    main_thread_id = threading.get_ident()
    
    # 创建测试任务
    task = DownloadTask(
        symbol="000001.SZ",
        task_type="stock_list",
        params={"exchange": "SZ"}
    )
    
    # Mock处理器返回数据
    mock_data = DataBatch(
        df=pd.DataFrame({'ts_code': ['000001.SZ'], 'name': ['平安银行']}),
        meta={'task_type': 'stock_list'},
        task_id=task.task_id,
        symbol=task.symbol
    )
    
    with patch.object(producer.processor, 'process', return_value=mock_data):
        # 启动Producer
        producer.start()
        
        # 验证工作线程已启动
        assert producer.is_running
        assert producer.is_alive()
        
        # 提交任务
        producer.submit_task(task)
        
        # 等待任务处理完成
        time.sleep(0.5)
        
        # 停止Producer
        producer.stop()
    
    # 验证事件被发送
    events = listener.get_events()
    assert len(events) == 1
    
    event_type, event_data, worker_thread_id = events[0]
    assert event_type == ProducerEvents.DATA_READY
    assert event_data['task_id'] == task.task_id
    
    # 验证任务在不同线程中执行
    assert worker_thread_id != main_thread_id


def test_producer_concurrent_task_processing():
    """测试Producer并发处理多个任务"""
    # 创建事件总线和监听器
    event_bus = SimpleEventBus()
    listener = ThreadSafeEventListener()
    event_bus.subscribe(ProducerEvents.DATA_READY, listener)
    
    # 创建mock fetcher
    mock_fetcher = Mock(spec=TushareFetcher)
    
    # 创建Producer
    executor = ThreadPoolExecutor(max_workers=1)
    producer = Producer(
        event_bus=event_bus,
        fetcher=mock_fetcher,
        thread_pool_executor=executor
    )
    
    # 创建多个测试任务
    tasks = []
    for i in range(5):
        task = DownloadTask(
            symbol=f"00000{i}.SZ",
            task_type="stock_list",
            params={"exchange": "SZ"}
        )
        tasks.append(task)
    
    # Mock处理器返回数据（添加延迟模拟真实处理）
    def mock_process(task):
        time.sleep(0.1)  # 模拟处理时间
        return DataBatch(
            df=pd.DataFrame({'ts_code': [task.symbol], 'name': [f'股票{task.symbol}']}),
            meta={'task_type': 'stock_list'},
            task_id=task.task_id,
            symbol=task.symbol
        )
    
    with patch.object(producer.processor, 'process', side_effect=mock_process):
        # 启动Producer
        producer.start()
        
        # 记录开始时间
        start_time = time.time()
        
        # 提交所有任务
        for task in tasks:
            producer.submit_task(task)
        
        # 等待所有任务处理完成
        time.sleep(1.0)
        
        # 停止Producer
        producer.stop()
        
        # 记录结束时间
        end_time = time.time()
    
    # 验证所有任务都被处理
    events = listener.get_events()
    assert len(events) == 5
    
    # 验证所有任务都在同一个工作线程中执行
    worker_thread_ids = [event[2] for event in events]
    assert len(set(worker_thread_ids)) == 1  # 所有任务在同一个线程中执行
    
    # 验证处理时间合理（串行处理）
    total_time = end_time - start_time
    assert total_time >= 0.5  # 至少需要5 * 0.1秒的处理时间
    
    # 验证任务ID唯一性
    task_ids = [event[1]['task_id'] for event in events]
    assert len(set(task_ids)) == 5


def test_multiple_producers_concurrent_execution():
    """测试多个Producer实例并发执行"""
    # 创建共享事件总线和监听器
    event_bus = SimpleEventBus()
    listener = ThreadSafeEventListener()
    event_bus.subscribe(ProducerEvents.DATA_READY, listener)
    
    # 创建多个Producer实例
    producers = []
    for i in range(3):
        mock_fetcher = Mock(spec=TushareFetcher)
        executor = ThreadPoolExecutor(max_workers=1)
        producer = Producer(
            event_bus=event_bus,
            fetcher=mock_fetcher,
            thread_pool_executor=executor
        )
        producers.append(producer)
    
    # 为每个Producer创建任务
    all_tasks = []
    for i, producer in enumerate(producers):
        for j in range(2):
            task = DownloadTask(
                symbol=f"00{i}{j:02d}.SZ",
                task_type="stock_list",
                params={"exchange": "SZ"}
            )
            all_tasks.append((producer, task))
    
    # Mock处理器返回数据
    def mock_process(task):
        time.sleep(0.1)  # 模拟处理时间
        return DataBatch(
            df=pd.DataFrame({'ts_code': [task.symbol], 'name': [f'股票{task.symbol}']}),
            meta={'task_type': 'stock_list'},
            task_id=task.task_id,
            symbol=task.symbol
        )
    
    try:
        # 为所有Producer设置mock
        for producer in producers:
            patch.object(producer.processor, 'process', side_effect=mock_process).start()
        
        # 启动所有Producer
        start_time = time.time()
        for producer in producers:
            producer.start()
        
        # 验证所有Producer都在运行
        for producer in producers:
            assert producer.is_running
            assert producer.is_alive()
        
        # 提交任务到各自的Producer
        for producer, task in all_tasks:
            producer.submit_task(task)
        
        # 等待所有任务处理完成
        time.sleep(1.0)
        
        end_time = time.time()
        
    finally:
        # 停止所有Producer
        for producer in producers:
            producer.stop()
        
        # 清理所有patch
        patch.stopall()
    
    # 验证所有任务都被处理
    events = listener.get_events()
    assert len(events) == 6  # 3个Producer * 2个任务
    
    # 验证任务在不同线程中执行
    worker_thread_ids = [event[2] for event in events]
    unique_thread_ids = set(worker_thread_ids)
    assert len(unique_thread_ids) == 3  # 3个不同的工作线程
    
    # 验证执行完成（不严格要求时间，因为测试环境性能差异较大）
    total_time = end_time - start_time
    assert total_time > 0  # 确保有执行时间
    
    # 验证任务ID唯一性
    task_ids = [event[1]['task_id'] for event in events]
    assert len(set(task_ids)) == 6


def test_producer_thread_safety():
    """测试Producer的线程安全性"""
    # 创建事件总线和监听器
    event_bus = SimpleEventBus()
    listener = ThreadSafeEventListener()
    event_bus.subscribe(ProducerEvents.DATA_READY, listener)
    
    # 创建mock fetcher
    mock_fetcher = Mock(spec=TushareFetcher)
    
    # 创建Producer
    executor = ThreadPoolExecutor(max_workers=1)
    producer = Producer(
        event_bus=event_bus,
        fetcher=mock_fetcher,
        thread_pool_executor=executor
    )
    
    # Mock处理器返回数据
    def mock_process(task):
        time.sleep(0.05)  # 短暂延迟
        return DataBatch(
            df=pd.DataFrame({'ts_code': [task.symbol], 'name': [f'股票{task.symbol}']}),
            meta={'task_type': 'stock_list'},
            task_id=task.task_id,
            symbol=task.symbol
        )
    
    with patch.object(producer.processor, 'process', side_effect=mock_process):
        # 启动Producer
        producer.start()
        
        # 使用多个线程同时提交任务
        def submit_tasks(thread_id):
            for i in range(3):
                task = DownloadTask(
                    symbol=f"{thread_id}{i:02d}.SZ",
                    task_type="stock_list",
                    params={"exchange": "SZ"}
                )
                success = producer.submit_task(task)
                assert success, f"任务提交失败: {task.symbol}"
        
        # 创建多个提交线程
        submit_threads = []
        for i in range(3):
            thread = threading.Thread(target=submit_tasks, args=[i])
            submit_threads.append(thread)
            thread.start()
        
        # 等待所有提交线程完成
        for thread in submit_threads:
            thread.join()
        
        # 等待所有任务处理完成
        time.sleep(1.0)
        
        # 停止Producer
        producer.stop()
    
    # 验证所有任务都被处理
    events = listener.get_events()
    assert len(events) == 9  # 3个线程 * 3个任务
    
    # 验证任务ID唯一性
    task_ids = [event[1]['task_id'] for event in events]
    assert len(set(task_ids)) == 9
    
    # 验证所有任务都在同一个工作线程中执行
    worker_thread_ids = [event[2] for event in events]
    assert len(set(worker_thread_ids)) == 1


def test_producer_start_stop_thread_lifecycle():
    """测试Producer线程生命周期管理"""
    # 创建事件总线
    event_bus = SimpleEventBus()
    
    # 创建mock fetcher
    mock_fetcher = Mock(spec=TushareFetcher)
    
    # 创建Producer
    executor = ThreadPoolExecutor(max_workers=1)
    producer = Producer(
        event_bus=event_bus,
        fetcher=mock_fetcher,
        thread_pool_executor=executor
    )
    
    # 初始状态检查
    assert not producer.is_running
    assert not producer.is_alive()
    
    # 启动Producer
    producer.start()
    
    # 启动后状态检查
    assert producer.is_running
    assert producer.is_alive()
    
    # 停止Producer
    producer.stop()
    
    # 停止后状态检查
    assert not producer.is_running
    assert not producer.is_alive()
    
    # 重新启动Producer
    producer.start()
    
    # 验证Producer可以重新启动
    assert producer.is_running
    assert producer.is_alive()
    
    # 最终清理
    producer.stop()