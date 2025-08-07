#!/usr/bin/env python3
"""
ConsumerPool 使用示例

展示如何使用消费者管理器（ConsumerPool）：
- 配置消费者池参数
- 提交数据批次
- 监控处理进度和统计信息
- 优雅停止和资源清理
"""

import time
import pandas as pd
from pathlib import Path
from queue import Queue

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.downloader.consumer_pool import ConsumerPool
from src.downloader.models import DataBatch


def create_sample_data_batches():
    """创建示例数据批次"""
    batches = []
    
    # 股票列表数据
    stock_list_df = pd.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
        'symbol': ['000001', '000002', '600000'],
        'name': ['平安银行', '万科A', '浦发银行'],
        'area': ['深圳', '深圳', '上海'],
        'industry': ['银行', '房地产', '银行'],
        'market': ['主板', '主板', '主板'],
        'list_date': ['19910403', '19910129', '19991110']
    })
    
    stock_list_batch = DataBatch(
        df=stock_list_df,
        meta={'task_type': 'stock_list'},
        task_id='stock_list_001',
        symbol='system'
    )
    batches.append(stock_list_batch)
    
    # 日线数据
    for i, stock_code in enumerate(['000001.SZ', '000002.SZ', '600000.SH']):
        daily_df = pd.DataFrame({
            'ts_code': [stock_code] * 5,
            'trade_date': ['20241201', '20241202', '20241203', '20241204', '20241205'],
            'open': [10.0 + i, 10.1 + i, 9.9 + i, 10.2 + i, 10.0 + i],
            'high': [10.5 + i, 10.6 + i, 10.4 + i, 10.7 + i, 10.5 + i],
            'low': [9.8 + i, 9.9 + i, 9.7 + i, 10.0 + i, 9.8 + i],
            'close': [10.2 + i, 10.0 + i, 10.1 + i, 10.3 + i, 10.1 + i],
            'volume': [1000000, 1100000, 950000, 1200000, 1050000],
            'amount': [10200000, 11000000, 9595000, 12360000, 10605000]
        })
        
        daily_batch = DataBatch(
            df=daily_df,
            meta={'task_type': 'daily'},
            task_id=f'daily_{stock_code}_{i}',
            symbol=stock_code
        )
        batches.append(daily_batch)
    
    # 基础数据
    for stock_code in ['000001.SZ', '000002.SZ']:
        basic_df = pd.DataFrame({
            'ts_code': [stock_code] * 3,
            'trade_date': ['20241203', '20241204', '20241205'],
            'close': [10.1, 10.3, 10.1],
            'turnover_rate': [2.5, 3.1, 2.8],
            'volume_ratio': [1.2, 1.5, 1.1],
            'pe': [12.5, 12.8, 12.3],
            'pb': [1.8, 1.85, 1.82],
            'ps': [2.1, 2.15, 2.08],
            'dv_ratio': [3.2, 3.15, 3.18],
            'dv_ttm': [3.5, 3.48, 3.52],
            'total_share': [1000000000, 1000000000, 1000000000],
            'float_share': [800000000, 800000000, 800000000],
            'free_share': [750000000, 750000000, 750000000],
            'total_mv': [10100000000, 10300000000, 10100000000],
            'circ_mv': [8080000000, 8240000000, 8080000000]
        })
        
        basic_batch = DataBatch(
            df=basic_df,
            meta={'task_type': 'daily_basic'},
            task_id=f'basic_{stock_code}',
            symbol=stock_code
        )
        batches.append(basic_batch)
    
    return batches


def monitor_progress(pool: ConsumerPool, duration: int = 30):
    """监控处理进度"""
    print("\\n🔄 开始监控处理进度...")
    
    start_time = time.time()
    last_stats = None
    
    while time.time() - start_time < duration:
        stats = pool.get_statistics()
        
        if stats != last_stats:
            print(f"\\n📊 统计信息 (运行时间: {stats['uptime_seconds']:.1f}s):")
            print(f"   - 活跃工作线程: {stats['active_workers']}/{stats['max_consumers']}")
            print(f"   - 数据队列大小: {stats['data_queue_size']}")
            print(f"   - 已处理批次: {stats['total_batches_processed']}")
            print(f"   - 已缓存批次: {stats['total_batches_cached']}")
            print(f"   - 刷新操作: {stats['total_flush_operations']}")
            print(f"   - 失败操作: {stats['total_failed_operations']}")
            print(f"   - 缓存记录数: {stats['total_cached_records']}")
            
            # 显示每个工作线程的详细信息
            for worker_stat in stats['worker_statistics']:
                if worker_stat['batches_processed'] > 0 or worker_stat['cached_records'] > 0:
                    print(f"   Worker {worker_stat['worker_id']}: "
                          f"processed={worker_stat['batches_processed']}, "
                          f"cached={worker_stat['cached_records']}, "
                          f"flushes={worker_stat['flush_operations']}")
            
            last_stats = stats
        
        time.sleep(1)
    
    print("\\n✅ 监控结束")


def main():
    """主函数"""
    print("🚀 ConsumerPool 使用示例")
    print("=" * 50)
    
    # 配置参数
    db_path = Path("data/consumer_pool_example.db")
    db_path.parent.mkdir(exist_ok=True)
    
    config = {
        'max_consumers': 2,
        'batch_size': 10,      # 较小的批量大小，便于观察刷新行为
        'flush_interval': 5.0,  # 5秒刷新间隔
        'db_path': str(db_path),
        'max_retries': 3
    }
    
    print(f"📝 配置参数:")
    for key, value in config.items():
        print(f"   - {key}: {value}")
    
    # 创建消费者池
    pool = ConsumerPool(**config)
    
    try:
        # 启动消费者池
        print("\\n🔧 启动消费者池...")
        pool.start()
        
        # 创建示例数据
        print("\\n📦 创建示例数据批次...")
        batches = create_sample_data_batches()
        print(f"   创建了 {len(batches)} 个数据批次")
        
        # 提交数据批次
        print("\\n📤 提交数据批次...")
        submitted_count = 0
        for i, batch in enumerate(batches):
            success = pool.submit_data(batch, timeout=2.0)
            if success:
                submitted_count += 1
                print(f"   ✅ 批次 {i+1}: {batch.meta.get('task_type')} - {batch.symbol} ({batch.size} 条记录)")
            else:
                print(f"   ❌ 批次 {i+1}: 提交失败")
        
        print(f"\\n📊 成功提交 {submitted_count}/{len(batches)} 个批次")
        
        # 监控处理进度
        monitor_progress(pool, duration=20)
        
        # 等待队列处理完成
        print("\\n⏳ 等待队列处理完成...")
        empty = pool.wait_for_empty_queue(timeout=10.0)
        if empty:
            print("   ✅ 队列处理完成")
        else:
            print("   ⚠️  等待超时，队列可能仍有未处理数据")
        
        # 强制刷新缓存
        print("\\n🔄 强制刷新所有缓存...")
        pool.force_flush_all()
        
        # 最终统计信息
        final_stats = pool.get_statistics()
        print(f"\\n📋 最终统计信息:")
        print(f"   - 总处理批次: {final_stats['total_batches_processed']}")
        print(f"   - 总缓存批次: {final_stats['total_batches_cached']}")
        print(f"   - 总刷新操作: {final_stats['total_flush_operations']}")
        print(f"   - 总失败操作: {final_stats['total_failed_operations']}")
        print(f"   - 运行时间: {final_stats['uptime_seconds']:.1f}s")
        
        # 验证数据存储
        print("\\n🔍 验证数据存储...")
        # 通过第一个工作线程访问存储
        worker = list(pool.workers.values())[0]
        storage = worker.storage
        
        # 检查各类数据表
        tables = storage.list_tables()
        print(f"   数据库中的表: {tables}")
        
        for table in tables:
            if 'daily_' in table:
                data_type, entity_id = table.split('_', 1)
                df = storage.query(data_type, entity_id)
                print(f"   - {table}: {len(df)} 条记录")
        
        # 查看股票列表数据
        stock_list = storage.query('system', 'stock_list')  
        if not stock_list.empty:
            print(f"   - 股票列表: {len(stock_list)} 条记录")
            print(f"     股票: {', '.join(stock_list['name'].tolist())}")
        
        print("\\n✅ 示例运行完成！")
        print(f"💾 数据已保存至: {db_path.resolve()}")
        
    except Exception as e:
        print(f"\\n❌ 运行出错: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 停止消费者池
        print("\\n🛑 停止消费者池...")
        pool.stop(timeout=10.0)
        print("   ✅ 消费者池已停止")


if __name__ == "__main__":
    main()
