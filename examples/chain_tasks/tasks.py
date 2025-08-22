import time
import random
from typing import Dict, Any
from config import huey


@huey.task()
def download_task(symbol: str) -> Dict[str, Any]:
    """下载任务：下载股票数据并触发数据处理"""
    # 随机下载时间：0.5-2.0秒
    download_time = random.uniform(0.5, 2.0)
    print(f"📥 开始下载: {symbol} (预计耗时: {download_time:.1f}s)")
    
    # 模拟下载过程
    time.sleep(download_time)
    
    # 模拟下载结果
    result = {
        'symbol': symbol,
        'data': {'open': 100.0, 'close': 103.0, 'volume': 1000000},
        'status': 'success'
    }
    
    print(f"✅ 下载完成: {symbol}")
    
    # 🔗 链式调用：下载完成后自动触发数据处理
    print(f"🔄 触发数据处理: {symbol}")
    process_data_task(result)
    
    return result


@huey.task()
def process_data_task(download_result: Dict[str, Any]) -> bool:
    """数据处理任务：处理下载的数据并保存"""
    symbol = download_result['symbol']
    data = download_result['data']
    
    # 随机处理时间：0.2-1.0秒
    process_time = random.uniform(0.2, 1.0)
    print(f"🔄 开始处理: {symbol} (预计耗时: {process_time:.1f}s)")
    time.sleep(process_time)
    
    # 模拟保存到数据库
    print(f"💾 保存到数据库: {symbol} (收盘价: {data['close']})")
    print(f"✅ 处理完成: {symbol}")
    
    return True