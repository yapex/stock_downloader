"""
ProducerPool 使用示例

演示如何使用生产者池来并发下载股票数据
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.downloader.producer_pool import ProducerPool
from src.downloader.models import DownloadTask, TaskType, Priority
from src.downloader.logging_setup import setup_logging

# 设置日志
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_basic_usage():
    """基本使用示例"""
    logger.info("=== ProducerPool 基本使用示例 ===")
    
    # 创建生产者池
    pool = ProducerPool(
        max_producers=3,  # 使用3个生产者线程
        retry_policy="requeue",  # 重试策略：重新入队
        max_retries=2,  # 最大重试2次
        fetcher_rate_limit=120  # 每分钟120次API调用
    )
    
    try:
        # 启动生产者池
        logger.info("启动生产者池...")
        pool.start()
        
        # 创建一些测试任务
        tasks = [
            # 获取股票列表任务
            DownloadTask(
                symbol="",
                task_type=TaskType.STOCK_LIST,
                params={},
                priority=Priority.HIGH
            ),
            
            # 获取日K线数据任务
            DownloadTask(
                symbol="000001.SZ",
                task_type=TaskType.DAILY,
                params={
                    'start_date': '20231201',
                    'end_date': '20231210',
                    'adjust': 'hfq'
                },
                priority=Priority.NORMAL
            ),
            
            DownloadTask(
                symbol="000002.SZ",
                task_type=TaskType.DAILY,
                params={
                    'start_date': '20231201',
                    'end_date': '20231210',
                    'adjust': 'hfq'
                },
                priority=Priority.NORMAL
            ),
            
            # 获取每日指标任务
            DownloadTask(
                symbol="000001.SZ",
                task_type=TaskType.DAILY_BASIC,
                params={
                    'start_date': '20231201',
                    'end_date': '20231210'
                },
                priority=Priority.LOW
            )
        ]
        
        # 提交任务
        logger.info(f"提交 {len(tasks)} 个任务...")
        for task in tasks:
            success = pool.submit_task(task)
            if success:
                logger.info(f"任务已提交: {task.symbol} - {task.task_type.value}")
            else:
                logger.warning(f"任务提交失败: {task.symbol} - {task.task_type.value}")
        
        # 监控处理进度
        logger.info("开始监控任务处理进度...")
        processed_count = 0
        start_time = time.time()
        
        while processed_count < len(tasks):
            # 获取处理结果
            data_batch = pool.get_data(timeout=2.0)
            
            if data_batch:
                processed_count += 1
                logger.info(
                    f"任务完成 ({processed_count}/{len(tasks)}): "
                    f"{data_batch.symbol} - {data_batch.meta.get('task_type', 'unknown')}, "
                    f"数据条数: {data_batch.size}"
                )
                
                # 显示部分数据（如果有的话）
                if not data_batch.is_empty and hasattr(data_batch.df, 'head'):
                    logger.info(f"数据预览:\n{data_batch.df.head(3)}")
            
            # 显示统计信息
            stats = pool.get_statistics()
            if processed_count % 2 == 0:  # 每处理2个任务显示一次统计
                logger.info(
                    f"池状态 - 运行中: {stats['running']}, "
                    f"活跃工作线程: {stats['active_workers']}, "
                    f"任务队列: {stats['task_queue_size']}, "
                    f"数据队列: {stats['data_queue_size']}"
                )
            
            # 超时保护
            if time.time() - start_time > 60:  # 60秒超时
                logger.warning("处理超时，停止等待")
                break
        
        # 等待所有任务完成
        logger.info("等待剩余任务完成...")
        pool.wait_for_completion(timeout=30)
        
        # 显示最终统计
        final_stats = pool.get_statistics()
        logger.info("=== 最终统计信息 ===")
        for key, value in final_stats.items():
            logger.info(f"{key}: {value}")
        
    except KeyboardInterrupt:
        logger.info("用户中断，停止处理...")
    
    except Exception as e:
        logger.error(f"处理过程中发生错误: {e}")
        
    finally:
        # 停止生产者池
        logger.info("停止生产者池...")
        pool.stop(timeout=10)
        logger.info("生产者池已停止")


def example_error_handling():
    """错误处理示例"""
    logger.info("=== ProducerPool 错误处理示例 ===")
    
    pool = ProducerPool(
        max_producers=2,
        retry_policy="deadletter",  # 使用死信策略
        max_retries=1  # 只重试1次
    )
    
    try:
        pool.start()
        
        # 创建一些可能失败的任务
        tasks = [
            # 正常任务
            DownloadTask(
                symbol="000001.SZ",
                task_type=TaskType.DAILY,
                params={
                    'start_date': '20231201',
                    'end_date': '20231202'
                }
            ),
            
            # 可能导致错误的任务（无效股票代码）
            DownloadTask(
                symbol="INVALID.CODE",
                task_type=TaskType.DAILY,
                params={
                    'start_date': '20231201',
                    'end_date': '20231202'
                }
            )
        ]
        
        for task in tasks:
            pool.submit_task(task)
            logger.info(f"提交任务: {task.symbol}")
        
        # 等待并处理结果
        time.sleep(10)  # 给足够时间处理
        
        # 获取结果
        results = []
        while True:
            data_batch = pool.get_data(timeout=1.0)
            if data_batch is None:
                break
            results.append(data_batch)
            
            logger.info(
                f"获得结果: {data_batch.symbol}, "
                f"数据条数: {data_batch.size}, "
                f"是否为空: {data_batch.is_empty}"
            )
        
        logger.info(f"总共处理了 {len(results)} 个结果")
        
    except Exception as e:
        logger.error(f"错误处理示例中发生异常: {e}")
        
    finally:
        pool.stop()


def example_performance_monitoring():
    """性能监控示例"""
    logger.info("=== ProducerPool 性能监控示例 ===")
    
    pool = ProducerPool(max_producers=4)
    
    try:
        pool.start()
        
        # 创建大量任务来测试性能
        logger.info("创建大量任务进行性能测试...")
        start_time = time.time()
        
        # 模拟多只股票的日K线数据下载
        stock_codes = ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH", "000858.SZ"]
        
        total_tasks = 0
        for stock_code in stock_codes:
            for i in range(5):  # 每只股票5个时间段
                end_date = datetime(2023, 12, 10)
                start_date = end_date - timedelta(days=i*10 + 5)
                
                task = DownloadTask(
                    symbol=stock_code,
                    task_type=TaskType.DAILY,
                    params={
                        'start_date': start_date.strftime('%Y%m%d'),
                        'end_date': end_date.strftime('%Y%m%d')
                    }
                )
                
                pool.submit_task(task)
                total_tasks += 1
        
        logger.info(f"已提交 {total_tasks} 个任务")
        
        # 定期监控性能
        processed = 0
        last_stats_time = time.time()
        
        while processed < total_tasks:
            data_batch = pool.get_data(timeout=1.0)
            
            if data_batch:
                processed += 1
                
                if processed % 5 == 0:  # 每处理5个任务显示一次进度
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    
                    logger.info(
                        f"进度: {processed}/{total_tasks} "
                        f"({processed/total_tasks*100:.1f}%), "
                        f"速率: {rate:.2f} 任务/秒"
                    )
            
            # 每10秒显示详细统计
            if time.time() - last_stats_time > 10:
                stats = pool.get_statistics()
                logger.info("=== 实时统计 ===")
                logger.info(f"运行时间: {stats['uptime_seconds']:.1f}s")
                logger.info(f"队列状态 - 任务: {stats['task_queue_size']}, 数据: {stats['data_queue_size']}")
                logger.info(f"工作线程: {stats['active_workers']}/{stats['max_producers']}")
                last_stats_time = time.time()
            
            # 超时保护
            if time.time() - start_time > 120:  # 2分钟超时
                logger.warning("性能测试超时")
                break
        
        # 显示最终性能统计
        total_time = time.time() - start_time
        final_rate = processed / total_time if total_time > 0 else 0
        
        logger.info("=== 性能测试结果 ===")
        logger.info(f"总处理任务数: {processed}")
        logger.info(f"总耗时: {total_time:.2f}s")
        logger.info(f"平均处理速率: {final_rate:.2f} 任务/秒")
        logger.info(f"成功率: {processed/total_tasks*100:.1f}%")
        
    finally:
        pool.stop()


if __name__ == "__main__":
    # 检查环境变量
    if not os.getenv("TUSHARE_TOKEN"):
        logger.error("请设置 TUSHARE_TOKEN 环境变量")
        sys.exit(1)
    
    print("ProducerPool 使用示例")
    print("1. 基本使用")
    print("2. 错误处理")
    print("3. 性能监控")
    
    choice = input("请选择示例 (1-3): ").strip()
    
    if choice == "1":
        example_basic_usage()
    elif choice == "2":
        example_error_handling()
    elif choice == "3":
        example_performance_monitoring()
    else:
        logger.info("运行所有示例...")
        example_basic_usage()
        print("\n" + "="*50 + "\n")
        example_error_handling()
        print("\n" + "="*50 + "\n")
        example_performance_monitoring()
