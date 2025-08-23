"""支持 tqdm 进度条的任务定义"""

import random
import time
from typing import Dict, Any
from config import huey


@huey.task()
def download_task_with_progress(symbol: str) -> Dict[str, Any]:
    """下载任务：下载股票数据并触发数据处理（支持进度显示）"""
    # 随机下载时间：1.0-3.0秒
    download_time = random.uniform(1.0, 3.0)
    
    # 模拟下载过程（分阶段进行，便于进度跟踪）
    stages = [
        ("连接服务器", 0.2),
        ("获取数据", 0.6),
        ("验证数据", 0.15),
        ("格式化数据", 0.05)
    ]
    
    total_time = 0
    for stage_name, ratio in stages:
        stage_time = download_time * ratio
        time.sleep(stage_time)
        total_time += stage_time
    
    # 模拟下载结果
    result = {
        "symbol": symbol,
        "data": {
            "open": round(random.uniform(90, 110), 2),
            "close": round(random.uniform(95, 115), 2),
            "volume": random.randint(500000, 2000000)
        },
        "status": "success",
        "download_time": round(total_time, 2)
    }

    # 🔗 链式调用：下载完成后自动触发数据处理
    process_data_task_with_progress(result)

    return result


@huey.task()
def process_data_task_with_progress(download_result: Dict[str, Any]) -> bool:
    """数据处理任务：处理下载的数据并保存（支持进度显示）"""
    symbol = download_result["symbol"]
    data = download_result["data"]

    # 随机处理时间：0.5-1.5秒
    process_time = random.uniform(0.5, 1.5)
    
    # 模拟数据处理过程（分阶段进行）
    stages = [
        ("数据清洗", 0.3),
        ("数据转换", 0.4),
        ("数据验证", 0.2),
        ("保存数据库", 0.1)
    ]
    
    total_time = 0
    for stage_name, ratio in stages:
        stage_time = process_time * ratio
        time.sleep(stage_time)
        total_time += stage_time
    
    # 模拟随机失败（5%概率）
    if random.random() < 0.05:
        raise Exception(f"数据处理失败: {symbol}")

    return True


@huey.task()
def slow_task_with_progress(duration: int, task_name: str = "SlowTask") -> str:
    """慢任务：用于测试 asyncio 并发执行（支持进度显示）"""
    start_time = time.time()
    
    # 将任务分解为多个小步骤，便于进度跟踪
    steps = 10
    step_duration = duration / steps
    
    for i in range(steps):
        time.sleep(step_duration)
        # 可以在这里添加进度回调，但由于 Huey 任务的限制，
        # 我们主要依赖外部的进度估算
    
    actual_duration = time.time() - start_time
    result = f"{task_name}, 实际耗时 {actual_duration:.2f} 秒"
    
    return result


@huey.task()
def batch_download_task(symbols: list, batch_size: int = 5) -> Dict[str, Any]:
    """批量下载任务：一次处理多个股票代码"""
    results = []
    failed = []
    
    total_symbols = len(symbols)
    
    for i, symbol in enumerate(symbols):
        try:
            # 模拟单个下载
            download_time = random.uniform(0.5, 1.5)
            time.sleep(download_time)
            
            result = {
                "symbol": symbol,
                "data": {
                    "open": round(random.uniform(90, 110), 2),
                    "close": round(random.uniform(95, 115), 2),
                    "volume": random.randint(500000, 2000000)
                },
                "status": "success"
            }
            results.append(result)
            
        except Exception as e:
            failed.append({"symbol": symbol, "error": str(e)})
    
    return {
        "total": total_symbols,
        "success": len(results),
        "failed": len(failed),
        "results": results,
        "errors": failed
    }


@huey.task()
def data_analysis_task(data_batch: Dict[str, Any]) -> Dict[str, Any]:
    """数据分析任务：分析批量数据"""
    results = data_batch.get("results", [])
    
    if not results:
        return {"status": "no_data", "analysis": None}
    
    # 模拟分析过程
    analysis_time = len(results) * 0.1  # 每个结果0.1秒
    time.sleep(analysis_time)
    
    # 计算统计信息
    prices = [r["data"]["close"] for r in results]
    volumes = [r["data"]["volume"] for r in results]
    
    analysis = {
        "count": len(results),
        "avg_price": round(sum(prices) / len(prices), 2),
        "max_price": max(prices),
        "min_price": min(prices),
        "total_volume": sum(volumes),
        "avg_volume": round(sum(volumes) / len(volumes), 0)
    }
    
    return {
        "status": "completed",
        "analysis": analysis,
        "processing_time": round(analysis_time, 2)
    }


@huey.task()
def error_prone_task(failure_rate: float = 0.3) -> str:
    """容易出错的任务：用于测试错误处理"""
    # 模拟处理时间
    time.sleep(random.uniform(0.5, 2.0))
    
    # 根据失败率决定是否抛出异常
    if random.random() < failure_rate:
        error_types = [
            "网络连接超时",
            "数据格式错误",
            "权限不足",
            "服务器内部错误",
            "数据不存在"
        ]
        raise Exception(random.choice(error_types))
    
    return f"任务成功完成 (失败率: {failure_rate*100:.0f}%)"