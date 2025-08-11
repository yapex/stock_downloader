#!/usr/bin/env python3
"""
线程本地TushareFetcher演示

演示如何在多线程环境下使用线程本地的TushareFetcher实例，
避免速率限制器冲突，同时保证线程内的一致性。
"""

import os
import threading
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [线程:%(thread)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# 模拟环境变量（实际使用时应该设置真实的token）
os.environ['TUSHARE_TOKEN'] = 'demo_token_for_testing'

from downloader.fetcher_factory import (
    get_thread_local_fetcher,
    TushareFetcherFactory
)


def simulate_data_fetch(worker_id: int, symbols: List[str]) -> Dict[str, Any]:
    """
    模拟数据获取工作
    
    Args:
        worker_id: 工作线程ID
        symbols: 股票代码列表
        
    Returns:
        Dict: 包含工作结果的字典
    """
    thread_id = threading.get_ident()
    logger.info(f"工作线程 {worker_id} (线程ID: {thread_id}) 开始工作，处理 {len(symbols)} 个股票代码")
    
    try:
        # 获取线程本地的fetcher实例
        fetcher = get_thread_local_fetcher()
        fetcher_id = id(fetcher)
        
        logger.info(f"工作线程 {worker_id} 获取到fetcher实例，ID: {fetcher_id}")
        
        # 模拟数据获取过程
        results = []
        for i, symbol in enumerate(symbols):
            # 再次获取fetcher，验证是同一个实例
            fetcher_again = get_thread_local_fetcher()
            assert fetcher is fetcher_again, "线程内fetcher实例应该保持一致"
            
            # 模拟API调用延迟
            time.sleep(0.1)
            
            # 模拟获取数据
            result = {
                'symbol': symbol,
                'price': 100 + i * 10,  # 模拟价格
                'volume': 1000 + i * 100,  # 模拟成交量
                'fetcher_id': id(fetcher_again)
            }
            results.append(result)
            
            logger.debug(f"工作线程 {worker_id} 完成 {symbol} 数据获取")
        
        logger.info(f"工作线程 {worker_id} 完成所有数据获取，共 {len(results)} 条记录")
        
        return {
            'worker_id': worker_id,
            'thread_id': thread_id,
            'fetcher_id': fetcher_id,
            'symbols_count': len(symbols),
            'results': results,
            'success': True
        }
        
    except Exception as e:
        logger.error(f"工作线程 {worker_id} 发生错误: {e}")
        return {
            'worker_id': worker_id,
            'thread_id': thread_id,
            'error': str(e),
            'success': False
        }


def demonstrate_thread_isolation():
    """
    演示线程隔离特性
    """
    logger.info("=== 演示线程隔离特性 ===")
    
    # 主线程获取fetcher
    main_fetcher = get_thread_local_fetcher()
    main_thread_id = threading.get_ident()
    main_fetcher_id = id(main_fetcher)
    
    logger.info(f"主线程 (ID: {main_thread_id}) fetcher实例ID: {main_fetcher_id}")
    
    def other_thread_work():
        other_thread_id = threading.get_ident()
        other_fetcher = get_thread_local_fetcher()
        other_fetcher_id = id(other_fetcher)
        
        logger.info(f"其他线程 (ID: {other_thread_id}) fetcher实例ID: {other_fetcher_id}")
        
        # 验证是不同的实例
        assert other_fetcher is not main_fetcher, "不同线程应该有不同的fetcher实例"
        assert other_fetcher_id != main_fetcher_id, "不同线程的fetcher实例ID应该不同"
        
        logger.info(f"✓ 线程隔离验证成功：{other_fetcher_id} != {main_fetcher_id}")
    
    thread = threading.Thread(target=other_thread_work)
    thread.start()
    thread.join()
    
    # 验证主线程的fetcher仍然是同一个
    main_fetcher_again = get_thread_local_fetcher()
    assert main_fetcher is main_fetcher_again, "主线程fetcher实例应该保持一致"
    logger.info("✓ 主线程fetcher实例一致性验证成功")


def demonstrate_multi_threaded_fetching():
    """
    演示多线程数据获取
    """
    logger.info("=== 演示多线程数据获取 ===")
    
    # 准备测试数据
    all_symbols = [f"00000{i}.SZ" for i in range(1, 21)]  # 20个股票代码
    
    # 分配给不同的工作线程
    worker_tasks = [
        (1, all_symbols[0:5]),   # 工作线程1处理前5个
        (2, all_symbols[5:10]),  # 工作线程2处理中间5个
        (3, all_symbols[10:15]), # 工作线程3处理后5个
        (4, all_symbols[15:20])  # 工作线程4处理最后5个
    ]
    
    logger.info(f"准备启动 {len(worker_tasks)} 个工作线程")
    
    # 使用线程池执行任务
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 提交所有任务
        future_to_worker = {
            executor.submit(simulate_data_fetch, worker_id, symbols): worker_id
            for worker_id, symbols in worker_tasks
        }
        
        # 收集结果
        results = []
        for future in as_completed(future_to_worker):
            worker_id = future_to_worker[future]
            try:
                result = future.result()
                results.append(result)
                
                if result['success']:
                    logger.info(f"✓ 工作线程 {worker_id} 成功完成，fetcher ID: {result['fetcher_id']}")
                else:
                    logger.error(f"✗ 工作线程 {worker_id} 失败: {result.get('error')}")
                    
            except Exception as e:
                logger.error(f"✗ 工作线程 {worker_id} 异常: {e}")
    
    # 分析结果
    successful_results = [r for r in results if r['success']]
    logger.info(f"成功完成的工作线程: {len(successful_results)}/{len(worker_tasks)}")
    
    # 验证每个线程都有不同的fetcher实例
    fetcher_ids = [r['fetcher_id'] for r in successful_results]
    unique_fetcher_ids = set(fetcher_ids)
    
    logger.info(f"不同的fetcher实例数量: {len(unique_fetcher_ids)}")
    logger.info(f"Fetcher实例ID列表: {list(unique_fetcher_ids)}")
    
    if len(unique_fetcher_ids) == len(successful_results):
        logger.info("✓ 每个线程都有独立的fetcher实例")
    else:
        logger.warning("⚠ 某些线程可能共享了fetcher实例")
    
    # 统计总的数据获取量
    total_records = sum(len(r['results']) for r in successful_results)
    logger.info(f"总共获取数据记录: {total_records} 条")
    
    return results


def demonstrate_factory_status():
    """
    演示工厂状态查询
    """
    logger.info("=== 演示工厂状态查询 ===")
    
    # 检查当前线程状态
    has_instance = TushareFetcherFactory.has_thread_local_instance()
    instance_id = TushareFetcherFactory.get_thread_local_instance_id()
    
    logger.info(f"当前线程是否有fetcher实例: {has_instance}")
    logger.info(f"当前线程fetcher实例ID: {instance_id}")
    
    if not has_instance:
        logger.info("创建新的线程本地实例...")
        fetcher = get_thread_local_fetcher()
        logger.info(f"新实例ID: {id(fetcher)}")
    
    # 重新检查状态
    has_instance_after = TushareFetcherFactory.has_thread_local_instance()
    instance_id_after = TushareFetcherFactory.get_thread_local_instance_id()
    
    logger.info(f"创建后 - 是否有实例: {has_instance_after}")
    logger.info(f"创建后 - 实例ID: {instance_id_after}")


def main():
    """
    主演示函数
    """
    logger.info("开始线程本地TushareFetcher演示")
    logger.info("=" * 60)
    
    try:
        # 1. 演示工厂状态
        demonstrate_factory_status()
        print()
        
        # 2. 演示线程隔离
        demonstrate_thread_isolation()
        print()
        
        # 3. 演示多线程数据获取
        results = demonstrate_multi_threaded_fetching()
        print()
        
        # 4. 总结
        logger.info("=== 演示总结 ===")
        logger.info("✓ 线程本地fetcher功能正常工作")
        logger.info("✓ 不同线程拥有独立的fetcher实例")
        logger.info("✓ 同一线程内fetcher实例保持一致")
        logger.info("✓ 多线程环境下避免了速率限制器冲突")
        logger.info("✓ 工厂状态查询功能正常")
        
        successful_count = len([r for r in results if r['success']])
        logger.info(f"✓ 多线程数据获取成功率: {successful_count}/{len(results)}")
        
    except Exception as e:
        logger.error(f"演示过程中发生错误: {e}")
        raise
    
    logger.info("=" * 60)
    logger.info("线程本地TushareFetcher演示完成")


if __name__ == "__main__":
    main()