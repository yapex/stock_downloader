"""任务生产者 - 测试 pipeline 功能"""

import time
from config import huey
from tasks import download_task, process_data_task


def test_pipeline():
    """测试 pipeline 功能"""
    print("=== 测试 Huey Pipeline 功能 ===")
    
    # 模拟任务类型和股票代码
    task_type = "stock_basic"
    symbol = "000001.SZ"
    
    print(f"创建 pipeline: {task_type} -> {symbol}")
    
    # 创建 pipeline - 参考 mini.py 的成功模式
    # Huey 会自动将 download_task 返回的字典解包并匹配给 process_data_task 的参数
    pipeline = download_task.s(task_type, symbol).then(process_data_task)
    
    print(f"Pipeline 对象: {pipeline}")
    print(f"Pipeline 类型: {type(pipeline)}")
    
    # 提交到队列
    result_group = huey.enqueue(pipeline)
    print(f"提交 pipeline 到队列: {result_group}")
    
    print("\n等待任务完成...")
    
    # 等待结果
    try:
        # 等待一段时间让任务完成
        time.sleep(5)
        
        # 尝试获取结果
        results = result_group.get(blocking=False)
        print(f"Pipeline 执行结果: {results}")
        
    except Exception as e:
        print(f"获取结果时出错: {e}")
    
    print("\n=== 测试完成 ===")


def test_individual_tasks():
    """测试单独的任务"""
    print("\n=== 测试单独任务 ===")
    
    task_type = "stock_basic"
    symbol = "000002.SZ"
    
    # 测试下载任务
    print("提交下载任务...")
    download_result = huey.enqueue(download_task.s(task_type, symbol))
    print(f"下载任务结果对象: {download_result}")
    
    # 等待下载完成
    time.sleep(3)
    
    try:
        download_data = download_result.get(blocking=False)
        print(f"下载任务完成，数据: {download_data}")
        
        if download_data is not None:
            # 测试处理任务 - 直接传递字典的各个键作为参数
            print("\n提交处理任务...")
            process_result = huey.enqueue(process_data_task.s(
                download_data['task_type'],
                download_data['symbol'], 
                download_data['data_payload']
            ))
            print(f"处理任务结果对象: {process_result}")
            
            # 等待处理完成
            time.sleep(3)
            
            process_success = process_result.get(blocking=False)
            print(f"处理任务完成，成功: {process_success}")
            
    except Exception as e:
        print(f"获取单独任务结果时出错: {e}")
    
    print("=== 单独任务测试完成 ===")


if __name__ == "__main__":
    print("开始测试 Huey 简单原型...")
    
    # 测试 pipeline
    test_pipeline()
    
    # 测试单独任务
    test_individual_tasks()
    
    print("\n所有测试完成！")