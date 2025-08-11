"""Producer事件集成测试

测试Producer通过event_bus发送数据的功能。
"""

import pytest
import pandas as pd
from queue import Queue
from unittest.mock import Mock, patch

from src.downloader.producer import Producer
from src.downloader.interfaces.events import SimpleEventBus, IEventListener
from src.downloader.interfaces.producer import ProducerEvents
from src.downloader.models import DownloadTask, DataBatch
from src.downloader.fetcher import TushareFetcher


class EventListener(IEventListener):
    """测试事件监听器"""
    
    def __init__(self):
        self.events = []
    
    def on_event(self, event_type: str, event_data: any) -> None:
        self.events.append((event_type, event_data))


def test_producer_sends_data_ready_event():
    """测试Producer发送DATA_READY事件"""
    # 创建事件总线和监听器
    event_bus = SimpleEventBus()
    listener = EventListener()
    event_bus.subscribe(ProducerEvents.DATA_READY, listener)
    
    # 创建mock fetcher
    mock_fetcher = Mock(spec=TushareFetcher)
    
    # 创建Producer
    from concurrent.futures import ThreadPoolExecutor
    thread_pool_executor = ThreadPoolExecutor(max_workers=2)
    
    producer = Producer(
        fetcher=mock_fetcher,
        thread_pool_executor=thread_pool_executor,
        event_bus=event_bus
    )
    
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
    
    # Mock processor的process方法
    with patch.object(producer.processor, 'process', return_value=mock_data):
        # 处理任务
        producer._process_single_task(task)
    
    # 验证事件被发送
    assert len(listener.events) == 1
    event_type, event_data = listener.events[0]
    assert event_type == ProducerEvents.DATA_READY
    assert event_data['data_batch'] == mock_data
    assert event_data['task_id'] == task.task_id
    assert event_data['symbol'] == task.symbol
    assert 'processing_time' in event_data


def test_producer_sends_task_failed_event():
    """测试Producer发送TASK_FAILED事件"""
    # 创建事件总线和监听器
    event_bus = SimpleEventBus()
    listener = EventListener()
    event_bus.subscribe(ProducerEvents.TASK_FAILED, listener)
    
    # 创建mock fetcher
    mock_fetcher = Mock(spec=TushareFetcher)
    
    # 创建Producer
    from concurrent.futures import ThreadPoolExecutor
    thread_pool_executor = ThreadPoolExecutor(max_workers=2)
    
    producer = Producer(
        fetcher=mock_fetcher,
        thread_pool_executor=thread_pool_executor,
        event_bus=event_bus
    )
    
    # 创建测试任务（设置为不能重试）
    task = DownloadTask(
        symbol="000001.SZ",
        task_type="stock_list",
        params={"exchange": "SZ"},
        max_retries=0  # 不允许重试
    )
    
    # Mock处理器抛出异常
    test_error = Exception("Test error")
    with patch.object(producer.processor, 'process', side_effect=test_error):
        # 处理任务
        producer._process_single_task(task)
    
    # 验证TASK_FAILED事件被发送
    task_failed_events = [e for e in listener.events if e[0] == ProducerEvents.TASK_FAILED]
    assert len(task_failed_events) == 1
    
    event_type, event_data = task_failed_events[0]
    assert event_type == ProducerEvents.TASK_FAILED
    assert event_data['task_id'] == task.task_id
    assert event_data['symbol'] == task.symbol
    assert event_data['error'] == str(test_error)
    assert event_data['task_type'] == task.task_type.value


def test_producer_requires_event_bus():
    """测试Producer需要event_bus参数"""
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import Mock
    
    mock_fetcher = Mock(spec=TushareFetcher)
    thread_pool_executor = ThreadPoolExecutor(max_workers=2)
    
    with pytest.raises(ValueError, match="event_bus is required"):
        Producer(
            fetcher=mock_fetcher,
            thread_pool_executor=thread_pool_executor,
            event_bus=None
        )