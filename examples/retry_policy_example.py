"""
智能重试策略和死信处理使用示例

展示如何使用新的重试策略，以及如何处理死信日志。
"""

import asyncio
import time
from queue import Queue
from typing import List
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.downloader.models import DownloadTask, TaskType, Priority
from src.downloader.retry_policy import (
    RetryPolicy, 
    BackoffStrategy,
    NETWORK_RETRY_POLICY, 
    API_LIMIT_RETRY_POLICY,
    DeadLetterLogger
)
from src.downloader.producer_pool import ProducerPool
from src.downloader.consumer_pool import ConsumerPool
from src.downloader.queue_manager import MemoryQueueManager


async def example_basic_retry_policy():
    """基本重试策略示例"""
    print("=== 基本重试策略示例 ===")
    
    # 创建自定义重试策略
    custom_policy = RetryPolicy(
        max_attempts=5,
        backoff=BackoffStrategy.EXPONENTIAL,
        base_delay=2.0,
        max_delay=30.0,
        backoff_factor=2.0,
        retryable_errors=["Connection", "Timeout", "Network"],
        non_retryable_errors=["400", "401", "403", "Invalid parameter"]
    )
    
    # 测试不同的错误
    test_errors = [
        Exception("Connection timeout"),  # 应该重试
        Exception("400 Bad Request"),     # 不应该重试
        Exception("Network error"),       # 应该重试
        Exception("Invalid parameter"),   # 不应该重试
    ]
    
    for i, error in enumerate(test_errors, 1):
        should_retry = custom_policy.should_retry(error, 1)
        delay = custom_policy.get_delay(1) if should_retry else 0
        print(f"错误 {i}: {error}")
        print(f"  是否重试: {should_retry}")
        print(f"  延迟时间: {delay:.2f}秒")
        print()


async def example_dead_letter_handling():
    """死信处理示例"""
    print("=== 死信处理示例 ===")
    
    # 创建死信日志管理器
    dead_letter_logger = DeadLetterLogger("logs/example_dead_letter.jsonl")
    
    # 创建一些失败的任务
    failed_tasks = [
        DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={"start_date": "20230101", "end_date": "20231231"},
            retry_count=3  # 已经重试了3次
        ),
        DownloadTask(
            symbol="000002.SZ", 
            task_type=TaskType.DAILY_BASIC,
            params={"start_date": "20230101"},
            retry_count=2
        ),
    ]
    
    # 模拟错误
    errors = [
        Exception("Connection timeout after 3 retries"),
        Exception("API rate limit exceeded")
    ]
    
    # 写入死信日志
    for task, error in zip(failed_tasks, errors):
        dead_letter_logger.write_dead_letter(task, error)
        print(f"写入死信: {task.symbol} - {error}")
    
    # 读取死信统计
    stats = dead_letter_logger.get_statistics()
    print(f"\n死信统计:")
    print(f"  总数: {stats['total_count']}")
    print(f"  按任务类型: {stats['by_task_type']}")
    print(f"  按错误类型: {stats['by_error_type']}")
    
    # 读取并转换为任务，用于重新处理
    records = dead_letter_logger.read_dead_letters()
    retry_tasks = dead_letter_logger.convert_to_tasks(records)
    
    print(f"\n从死信恢复 {len(retry_tasks)} 个任务:")
    for task in retry_tasks:
        print(f"  {task.symbol} - {task.task_type.value} (重试次数已重置)")


async def example_production_usage():
    """生产环境使用示例"""
    print("=== 生产环境使用示例 ===")
    
    # 创建队列管理器
    queue_manager = MemoryQueueManager()
    
    # 配置网络重试策略的生产者池
    producer_pool = ProducerPool(
        max_producers=2,
        task_queue=queue_manager.task_queue,
        data_queue=queue_manager.data_queue,
        retry_policy_config=NETWORK_RETRY_POLICY,
        dead_letter_path="logs/production_dead_letter.jsonl"
    )
    
    # 创建消费者池
    consumer_pool = ConsumerPool(
        max_consumers=1,
        data_queue=queue_manager.data_queue,
        db_path="data/example.db"
    )
    
    # 创建一些测试任务
    test_tasks = [
        DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={"start_date": "20231201", "end_date": "20231231"}
        ),
        DownloadTask(
            symbol="000002.SZ",
            task_type=TaskType.DAILY_BASIC, 
            params={"start_date": "20231201", "end_date": "20231231"}
        ),
    ]
    
    try:
        # 启动服务
        await queue_manager.start()
        producer_pool.start()
        consumer_pool.start()
        
        print("服务已启动")
        
        # 提交任务
        for task in test_tasks:
            success = producer_pool.submit_task(task)
            if success:
                print(f"任务已提交: {task.symbol}")
            else:
                print(f"任务提交失败: {task.symbol}")
        
        # 等待处理完成
        print("等待任务处理完成...")
        await asyncio.sleep(5)
        
        # 获取统计信息
        producer_stats = producer_pool.get_statistics()
        consumer_stats = consumer_pool.get_statistics()
        
        print(f"\n生产者统计:")
        print(f"  队列大小: {producer_stats['task_queue_size']}")
        print(f"  重试策略: {producer_stats['retry_policy_config']['backoff']}")
        
        print(f"\n消费者统计:")
        print(f"  处理批次: {consumer_stats['total_batches_processed']}")
        print(f"  失败操作: {consumer_stats['total_failed_operations']}")
        
    finally:
        # 清理资源
        consumer_pool.stop()
        producer_pool.stop()
        await queue_manager.stop()
        print("服务已停止")


async def example_dead_letter_cli_usage():
    """死信命令行工具使用示例"""
    print("=== 死信命令行工具使用示例 ===")
    
    print("死信命令行工具可以通过以下方式使用:")
    print()
    print("1. 查看死信记录:")
    print("   python -m src.downloader.dead_letter_cli list --limit 10")
    print()
    print("2. 显示统计信息:")
    print("   python -m src.downloader.dead_letter_cli stats")
    print()
    print("3. 导出失败的股票代码:")
    print("   python -m src.downloader.dead_letter_cli export failed_symbols.txt --task-type daily")
    print()
    print("4. 重试失败的任务:")
    print("   python -m src.downloader.dead_letter_cli retry --task-type daily --limit 5")
    print()
    print("5. 预演重试（不实际执行）:")
    print("   python -m src.downloader.dead_letter_cli retry --dry-run --limit 10")
    print()
    print("6. 清空死信日志:")
    print("   python -m src.downloader.dead_letter_cli clear --yes")


def example_custom_strategies():
    """自定义重试策略示例"""
    print("=== 自定义重试策略示例 ===")
    
    # 保守重试策略 - 适用于重要但不紧急的任务
    conservative_policy = RetryPolicy(
        max_attempts=2,
        backoff=BackoffStrategy.FIXED,
        base_delay=5.0,
        retryable_errors=["timeout", "connection"]
    )
    
    # 激进重试策略 - 适用于关键任务
    aggressive_policy = RetryPolicy(
        max_attempts=10,
        backoff=BackoffStrategy.EXPONENTIAL,
        base_delay=0.5,
        max_delay=60.0,
        backoff_factor=1.5,
        retryable_errors=[
            "Connection", "Timeout", "Network", "SSL", "HTTP",
            "rate limit", "server error", "503", "502", "500"
        ]
    )
    
    # API专用重试策略 - 考虑API限制
    api_policy = RetryPolicy(
        max_attempts=5,
        backoff=BackoffStrategy.LINEAR,
        base_delay=10.0,  # API限制通常需要较长等待
        max_delay=300.0,  # 最多等待5分钟
        backoff_factor=2.0,
        retryable_errors=["rate limit", "quota", "429", "503"],
        non_retryable_errors=["401", "403", "invalid key", "permission"]
    )
    
    policies = [
        ("保守策略", conservative_policy),
        ("激进策略", aggressive_policy), 
        ("API策略", api_policy)
    ]
    
    test_error = Exception("rate limit exceeded")
    
    for name, policy in policies:
        print(f"{name}:")
        print(f"  最大尝试次数: {policy.max_attempts}")
        print(f"  退避策略: {policy.backoff.value}")
        print(f"  是否重试 '{test_error}': {policy.should_retry(test_error, 1)}")
        
        if policy.should_retry(test_error, 1):
            delays = [policy.get_delay(i) for i in range(1, 4)]
            print(f"  延迟时间序列: {[f'{d:.1f}s' for d in delays]}")
        print()


async def main():
    """主函数"""
    print("智能重试策略和死信处理示例\n")
    
    # 基本用法
    await example_basic_retry_policy()
    
    # 死信处理
    await example_dead_letter_handling()
    
    # 自定义策略
    example_custom_strategies()
    
    # 命令行工具用法
    await example_dead_letter_cli_usage()
    
    # 生产环境用法（需要有效的API配置）
    # await example_production_usage()
    
    print("示例完成！")


if __name__ == "__main__":
    asyncio.run(main())
