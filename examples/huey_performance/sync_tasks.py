"""基于Huey的同步任务处理器"""
import time
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from huey_config import huey


class SyncDataBuffer:
    """同步数据缓冲器"""
    
    def __init__(self):
        self._processed_count = 0
        self._batches_processed = 0
    
    def process_data(self, data: List[Dict[str, Any]]) -> None:
        """处理数据"""
        # 模拟数据处理时间
        time.sleep(0.001 * len(data))
        self._processed_count += len(data)
        self._batches_processed += 1
    
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            'processed_count': self._processed_count,
            'batches_processed': self._batches_processed
        }


# 全局数据缓冲器实例
sync_buffer = SyncDataBuffer()


@huey.task()
def process_stock_sync(symbol: str, data_count: int, task_id: str) -> str:
    """同步处理单只股票数据的Huey任务"""
    start_time = time.time()
    
    try:
        # 模拟数据处理时间
        time.sleep(0.001 * data_count)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        return f"sync:{symbol}:{data_count}:{processing_time:.3f}"
    
    except Exception as e:
        end_time = time.time()
        processing_time = end_time - start_time
        
        return f"sync:{symbol}:0:{processing_time:.3f}:ERROR:{str(e)}"


@huey.task()
def process_stock_batch_sync(symbols: List[str], data_count_per_stock: int, batch_id: str) -> List[str]:
    """同步批量处理股票数据的Huey任务"""
    results = []
    
    for i, symbol in enumerate(symbols):
        start_time = time.time()
        task_id = f"{batch_id}_stock_{i}"
        
        try:
            # 直接模拟处理时间，避免调用其他任务函数
            time.sleep(0.001 * data_count_per_stock)
            
            end_time = time.time()
            processing_time = end_time - start_time
            result = f"sync:{symbol}:{data_count_per_stock}:{processing_time:.3f}"
            results.append(result)
        except Exception as e:
            end_time = time.time()
            processing_time = end_time - start_time
            result = f"sync:{symbol}:0:{processing_time:.3f}:ERROR:{str(e)}"
            results.append(result)
    
    return results


class SyncTaskManager:
    """同步任务管理器"""
    
    def __init__(self):
        self._processed_count = 0
        self._batches_processed = 0
    
    def submit_tasks(self, symbols: List[str], data_count_per_stock: int, batch_size: int = 10) -> List[Any]:
        """提交任务"""
        tasks = []
        
        # 按批次提交任务
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            batch_id = f"sync_batch_{i // batch_size}"
            
            task = process_stock_batch_sync(batch_symbols, data_count_per_stock, batch_id)
            tasks.append(task)
        
        return tasks
    
    def wait_for_results(self, tasks: List[Any], timeout: float = 300.0) -> List[str]:
        """等待任务结果"""
        all_results = []
        
        for task in tasks:
            try:
                # 使用Huey的正确方式获取结果
                batch_results = task.get(blocking=True, timeout=timeout)
                if isinstance(batch_results, list):
                    all_results.extend(batch_results)
                else:
                    all_results.append(batch_results)
                    
            except Exception as e:
                # 创建错误结果字符串
                error_result = f"error:UNKNOWN:0:0.0:ERROR:{str(e)}"
                all_results.append(error_result)
        
        return all_results
    
    def get_buffer_stats(self) -> Dict[str, int]:
        """获取缓冲器统计信息"""
        return {
            'processed_count': self._processed_count,
            'batches_processed': self._batches_processed
        }