"""
队列管理器使用示例
演示如何使用 DownloadTask、DataBatch 和 EnhancedQueueManager
"""

import time
import pandas as pd
from datetime import datetime

from src.downloader.models import DownloadTask, DataBatch, TaskType, Priority
from src.downloader.enhanced_queue_manager import EnhancedQueueManager


def create_sample_tasks():
    """创建示例任务"""
    tasks = [
        DownloadTask(
            symbol="000001.SZ",
            task_type=TaskType.DAILY,
            params={"start_date": "2024-01-01", "end_date": "2024-01-31"},
            priority=Priority.HIGH
        ),
        DownloadTask(
            symbol="000002.SZ", 
            task_type=TaskType.DAILY_BASIC,
            params={"start_date": "2024-01-01", "end_date": "2024-01-31"},
            priority=Priority.NORMAL
        ),
        DownloadTask(
            symbol="000003.SZ",
            task_type=TaskType.FINANCIALS,
            params={"year": 2023},
            priority=Priority.LOW
        )
    ]
    return tasks


def create_sample_data_batch(task_id: str, symbol: str):
    """创建示例数据批次"""
    # 模拟股票数据
    data = []
    for i in range(10):
        data.append({
            "date": f"2024-01-{i+1:02d}",
            "open": 10.0 + i * 0.1,
            "high": 10.5 + i * 0.1,
            "low": 9.5 + i * 0.1,
            "close": 10.2 + i * 0.1,
            "volume": 1000000 + i * 10000
        })
    
    df = pd.DataFrame(data)
    
    batch = DataBatch(
        df=df,
        meta={
            "source": "tushare",
            "data_type": "daily",
            "fetch_time": datetime.now().isoformat(),
            "record_count": len(data)
        },
        task_id=task_id,
        symbol=symbol
    )
    
    return batch


def demo_basic_queue_operations():
    """演示基本队列操作"""
    print("=== 基本队列操作演示 ===")
    
    # 创建队列管理器
    manager = EnhancedQueueManager(
        task_queue_size=100,
        data_queue_size=100,
        enable_smart_retry=True
    )
    
    # 启动队列管理器
    manager.start()
    print(f"队列管理器已启动，状态: {manager.is_running}")
    
    # 创建和添加任务
    tasks = create_sample_tasks()
    for task in tasks:
        success = manager.put_task(task)
        print(f"添加任务: {task.symbol} - {task.task_type.value} - {task.priority.name}, 成功: {success}")
    
    print(f"当前任务队列大小: {manager.task_queue_size}")
    
    # 按优先级获取和处理任务
    while manager.task_queue_size > 0:
        task = manager.get_task(timeout=1.0)
        if task:
            print(f"获取任务: {task.symbol} - {task.task_type.value} - 优先级: {task.priority.name}")
            
            # 模拟任务处理
            time.sleep(0.1)
            
            # 创建模拟数据
            data_batch = create_sample_data_batch(task.task_id, task.symbol)
            manager.put_data(data_batch)
            print(f"  -> 生成数据批次: {data_batch.size} 条记录")
            
            # 标记任务完成
            manager.task_done()
            manager.report_task_completed(task)
    
    # 处理数据队列
    print(f"\n处理数据队列，当前大小: {manager.data_queue_size}")
    while manager.data_queue_size > 0:
        data_batch = manager.get_data(timeout=1.0)
        if data_batch:
            print(f"处理数据批次: {data_batch.symbol}, 大小: {data_batch.size}")
            print(f"  -> 列名: {data_batch.columns}")
            print(f"  -> 元数据: {data_batch.meta}")
            
            # 标记数据处理完成
            manager.data_done()
    
    # 获取并显示指标
    metrics = manager.metrics
    print(f"\n=== 队列管理器指标 ===")
    for key, value in metrics.items():
        print(f"{key}: {value}")
    
    # 停止队列管理器
    manager.stop()
    print(f"\n队列管理器已停止，状态: {manager.is_running}")


def demo_retry_mechanism():
    """演示重试机制"""
    print("\n=== 重试机制演示 ===")
    
    manager = EnhancedQueueManager(
        task_queue_size=50,
        data_queue_size=50,
        enable_smart_retry=True
    )
    manager.start()
    
    # 创建一个任务
    task = DownloadTask(
        symbol="000001.SZ",
        task_type=TaskType.DAILY,
        params={"start_date": "2024-01-01"},
        priority=Priority.NORMAL
    )
    
    print(f"原始任务: {task.symbol}, 重试次数: {task.retry_count}")
    
    # 模拟任务失败和重试
    for attempt in range(4):  # 超过最大重试次数
        print(f"\n尝试 #{attempt + 1}")
        
        if attempt == 0:
            # 第一次添加任务
            manager.put_task(task)
        
        # 模拟处理失败
        success = manager.schedule_retry(task, f"网络错误 #{attempt + 1}")
        if success:
            print(f"任务已调度重试: {task.symbol}, 重试次数: {task.retry_count + 1}")
            task = task.increment_retry()  # 更新本地任务状态
        else:
            print(f"任务重试失败（达到最大重试次数）: {task.symbol}")
            break
        
        # 等待一小段时间查看重试队列状态
        time.sleep(0.2)
        print(f"重试队列大小: {manager.retry_queue_size}")
    
    # 显示最终指标
    metrics = manager.metrics
    print(f"\n重试指标:")
    print(f"  任务提交数: {metrics['tasks_submitted']}")
    print(f"  任务重试数: {metrics['tasks_retried']}")
    print(f"  任务失败数: {metrics['tasks_failed']}")
    print(f"  成功率: {metrics['success_rate']:.2%}")
    
    manager.stop()


def demo_priority_handling():
    """演示优先级处理"""
    print("\n=== 优先级处理演示 ===")
    
    manager = EnhancedQueueManager(
        task_queue_size=50,
        data_queue_size=50,
        enable_smart_retry=True
    )
    manager.start()
    
    # 创建不同优先级的任务（倒序添加以测试优先级）
    tasks = [
        DownloadTask("LOW.SZ", TaskType.DAILY, {}, Priority.LOW),
        DownloadTask("NORMAL.SZ", TaskType.DAILY, {}, Priority.NORMAL),
        DownloadTask("HIGH.SZ", TaskType.DAILY, {}, Priority.HIGH),
    ]
    
    # 添加任务
    print("添加任务顺序: LOW -> NORMAL -> HIGH")
    for task in tasks:
        manager.put_task(task)
        print(f"  添加: {task.symbol} - 优先级: {task.priority.name}")
    
    # 获取任务（应按优先级顺序）
    print("\n获取任务顺序（按优先级）:")
    execution_order = []
    while manager.task_queue_size > 0:
        task = manager.get_task(timeout=1.0)
        if task:
            execution_order.append(task.symbol)
            print(f"  获取: {task.symbol} - 优先级: {task.priority.name}")
            manager.task_done()
    
    print(f"\n执行顺序: {' -> '.join(execution_order)}")
    print("预期顺序: HIGH.SZ -> NORMAL.SZ -> LOW.SZ")
    
    manager.stop()


def demo_data_batch_operations():
    """演示数据批次操作"""
    print("\n=== 数据批次操作演示 ===")
    
    # 创建不同类型的数据批次
    
    # 1. 从DataFrame创建
    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "close": [10.0, 10.5, 11.0],
        "volume": [1000, 1500, 2000]
    })
    
    batch1 = DataBatch(
        df=df,
        meta={"source": "method1", "type": "from_dataframe"},
        task_id="task-001",
        symbol="000001.SZ"
    )
    
    print(f"批次1: {batch1.symbol}, 大小: {batch1.size}, 列: {batch1.columns}")
    
    # 2. 从记录列表创建
    records = [
        {"date": "2024-01-04", "close": 11.5, "volume": 2200},
        {"date": "2024-01-05", "close": 12.0, "volume": 2500}
    ]
    
    batch2 = DataBatch.from_records(
        records,
        meta={"source": "method2", "type": "from_records"},
        task_id="task-002",
        symbol="000002.SZ"
    )
    
    print(f"批次2: {batch2.symbol}, 大小: {batch2.size}, 列: {batch2.columns}")
    
    # 3. 创建空批次
    batch3 = DataBatch.empty(
        task_id="task-003",
        symbol="000003.SZ",
        meta={"source": "method3", "type": "empty"}
    )
    
    print(f"批次3: {batch3.symbol}, 大小: {batch3.size}, 是否为空: {batch3.is_empty}")
    
    # 4. 序列化和转换
    print("\n=== 数据批次序列化 ===")
    for i, batch in enumerate([batch1, batch2, batch3], 1):
        print(f"\n批次{i}序列化结果:")
        batch_dict = batch.to_dict()
        for key, value in batch_dict.items():
            print(f"  {key}: {value}")
        
        if not batch.is_empty:
            print(f"  记录数据: {batch.to_records()[:2]}...")  # 只显示前两条


if __name__ == "__main__":
    print("队列管理器使用示例")
    print("=" * 50)
    
    # 运行各个演示
    demo_basic_queue_operations()
    demo_retry_mechanism()
    demo_priority_handling()
    demo_data_batch_operations()
    
    print("\n" + "=" * 50)
    print("演示完成！")
