"""ProducerV2 使用示例

展示如何使用 ProducerV2 进行数据获取和处理。
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
import pandas as pd

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.downloader.producer_v2 import ProducerV2
from src.downloader.interfaces.events import SimpleEventBus
from src.downloader.interfaces.producer import ProducerEvents
from src.downloader.models import DownloadTask, TaskType, Priority
from src.downloader.fetcher_factory import get_singleton
from src.downloader.config_impl import ConfigManager


def create_mock_fetcher():
    """创建模拟的 fetcher"""
    fetcher = Mock()
    
    # 模拟股票列表数据
    stock_list_data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
        'symbol': ['000001', '000002', '600000'],
        'name': ['平安银行', '万科A', '浦发银行'],
        'area': ['深圳', '深圳', '上海'],
        'industry': ['银行', '房地产', '银行']
    })
    
    # 模拟日K线数据
    daily_data = pd.DataFrame({
        'ts_code': ['000001.SZ'] * 5,
        'trade_date': ['20230101', '20230102', '20230103', '20230104', '20230105'],
        'open': [10.0, 10.1, 10.2, 10.3, 10.4],
        'high': [10.5, 10.6, 10.7, 10.8, 10.9],
        'low': [9.8, 9.9, 10.0, 10.1, 10.2],
        'close': [10.2, 10.3, 10.4, 10.5, 10.6],
        'vol': [1000000, 1100000, 1200000, 1300000, 1400000]
    })
    
    # 设置返回值
    fetcher.fetch_stock_list.return_value = stock_list_data
    fetcher.fetch_daily_history.return_value = daily_data
    fetcher.fetch_daily_basic.return_value = daily_data
    fetcher.fetch_income.return_value = daily_data
    fetcher.fetch_balancesheet.return_value = daily_data
    fetcher.fetch_cashflow.return_value = daily_data
    
    return fetcher


def create_real_fetcher():
    """创建真实的 fetcher（需要 TUSHARE_TOKEN）"""
    try:
        config = ConfigManager()
        return get_singleton(config)
    except Exception as e:
        print(f"无法创建真实 fetcher: {e}")
        print("使用模拟 fetcher 代替")
        return create_mock_fetcher()


def demo_basic_usage():
    """基本使用示例"""
    print("=== ProducerV2 基本使用示例 ===")
    
    # 创建依赖
    fetcher = create_mock_fetcher()
    executor = ThreadPoolExecutor(max_workers=2)
    event_bus = SimpleEventBus()
    
    # 设置事件监听器
    class EventListener:
        def __init__(self, name):
            self.name = name
            
        def on_event(self, event_type, event_data):
            if event_type == ProducerEvents.DATA_READY:
                batch = event_data
                print(f"数据准备完成: {batch.symbol}, 数据条数: {batch.size}")
                print(f"数据列: {batch.columns}")
            elif event_type == ProducerEvents.TASK_COMPLETED:
                task = event_data
                print(f"任务完成: {task.symbol} ({task.task_type.value})")
            elif event_type == ProducerEvents.TASK_FAILED:
                task = event_data
                print(f"任务失败: {task.symbol} ({task.task_type.value})")
    
    listener = EventListener("demo")
    event_bus.subscribe(ProducerEvents.DATA_READY, listener)
    event_bus.subscribe(ProducerEvents.TASK_COMPLETED, listener)
    event_bus.subscribe(ProducerEvents.TASK_FAILED, listener)
    
    # 创建 ProducerV2
    producer = ProducerV2(
        fetcher=fetcher,
        executor=executor,
        event_bus=event_bus,
        queue_size=10
    )
    
    try:
        # 启动生产者
        producer.start()
        print("生产者已启动")
        
        # 提交不同类型的任务
        tasks = [
            DownloadTask(
                symbol="",  # 股票列表不需要 symbol
                task_type=TaskType.STOCK_LIST,
                params={},
                priority=Priority.HIGH
            ),
            DownloadTask(
                symbol="000001.SZ",
                task_type=TaskType.DAILY,
                params={
                    'start_date': '20230101',
                    'end_date': '20230105',
                    'adjust': 'qfq'
                },
                priority=Priority.NORMAL
            ),
            DownloadTask(
                symbol="000001.SZ",
                task_type=TaskType.DAILY_BASIC,
                params={
                    'start_date': '20230101',
                    'end_date': '20230105'
                },
                priority=Priority.LOW
            ),
            DownloadTask(
                symbol="000001.SZ",
                task_type=TaskType.FINANCIALS,
                params={
                    'start_date': '20230101',
                    'end_date': '20230105',
                    'financial_type': 'income'
                }
            )
        ]
        
        # 提交任务
        for task in tasks:
            success = producer.submit_task(task)
            print(f"提交任务 {task.task_type.value}: {'成功' if success else '失败'}")
        
        # 等待任务处理完成
        print("等待任务处理...")
        time.sleep(2)
        
    finally:
        # 停止生产者
        producer.stop()
        executor.shutdown(wait=True)
        print("生产者已停止")


def demo_error_handling():
    """错误处理示例"""
    print("\n=== ProducerV2 错误处理示例 ===")
    
    # 创建会抛出异常的 fetcher
    fetcher = Mock()
    fetcher.fetch_daily_history.side_effect = Exception("网络错误")
    
    executor = ThreadPoolExecutor(max_workers=1)
    event_bus = SimpleEventBus()
    
    # 设置错误监听器
    class ErrorListener:
        def on_event(self, event_type, event_data):
            if event_type == ProducerEvents.TASK_FAILED:
                task = event_data
                print(f"任务失败处理: {task.symbol} - {task.task_type.value}")
                if task.can_retry():
                    print(f"  可以重试，当前重试次数: {task.retry_count}/{task.max_retries}")
                else:
                    print(f"  已达到最大重试次数: {task.max_retries}")
    
    error_listener = ErrorListener()
    event_bus.subscribe(ProducerEvents.TASK_FAILED, error_listener)
    
    producer = ProducerV2(
        fetcher=fetcher,
        executor=executor,
        event_bus=event_bus
    )
    
    try:
        producer.start()
        
        # 提交会失败的任务
        task = DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={'start_date': '20230101', 'end_date': '20230105'}
        )
        
        producer.submit_task(task)
        time.sleep(1)
        
    finally:
        producer.stop()
        executor.shutdown(wait=True)


if __name__ == "__main__":
    demo_basic_usage()
    demo_error_handling()