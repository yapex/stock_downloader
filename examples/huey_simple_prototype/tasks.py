"""简单的 Huey 任务原型验证"""

import pandas as pd
from config import huey


@huey.task()
def download_task(task_type: str, symbol: str) -> dict:
    """模拟下载任务，返回字典格式的数据"""
    print(f"执行下载任务: {task_type}, {symbol}")
    
    # 模拟下载数据，返回字典格式（键名与 process_data_task 参数名对齐）
    data = {
        'task_type': task_type,  # 第一个参数
        'symbol': symbol,        # 第二个参数
        'data_payload': {        # 第三个参数：实际的数据内容
            'symbol_list': [symbol, symbol, symbol],
            'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'price': [100.0, 101.0, 102.0]
        }
    }
    
    print(f"下载完成，数据条数: {len(data['data_payload']['symbol_list'])}")
    return data


@huey.task()
def process_data_task(task_type: str, symbol: str, data_payload: dict) -> bool:
    """模拟数据处理任务
    
    参数名与 download_task 返回的字典键名完全匹配
    
    Args:
        task_type: 任务类型（来自 download_task 返回字典的 'task_type' 键）
        symbol: 股票代码（来自 download_task 返回字典的 'symbol' 键）
        data_payload: 实际数据内容（来自 download_task 返回字典的 'data_payload' 键）
        
    Returns:
        bool: 处理是否成功
    """
    print(f"[处理任务] 开始执行")
    print(f"[处理任务] 接收到的参数: task_type={task_type}, symbol={symbol}")
    print(f"[处理任务] 数据内容: {data_payload}")
    
    # 检查数据格式
    if data_payload and 'symbol_list' in data_payload and 'price' in data_payload:
        print(f"[处理任务] 数据条数: {len(data_payload['symbol_list'])}")
        
        # 模拟处理逻辑
        avg_price = sum(data_payload['price']) / len(data_payload['price'])
        
        print(f"[处理任务] 处理结果 - 平均价格: {avg_price}")
        print(f"[处理任务] 处理完成，成功!")
        
        return True
    else:
        print(f"[处理任务] 数据为空或格式不正确，处理失败")
        return False