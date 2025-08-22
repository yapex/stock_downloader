#!/usr/bin/env python3

import os
from src.neo.downloader.fetcher_builder import FetcherBuilder
from src.neo.task_bus.types import TaskType
from src.neo.data_processor.simple_data_processor import SimpleDataProcessor
from src.neo.data_processor.types import TaskResult
from src.neo.downloader.types import DownloadTaskConfig, TaskPriority

def main():
    # 检查环境变量
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        print("❌ 未设置 TUSHARE_TOKEN 环境变量")
        return
    
    print(f"✅ TUSHARE_TOKEN已设置: {token[:10]}...")
    
    # 1. 测试数据获取
    print("\n=== 步骤1: 测试数据获取 ===")
    builder = FetcherBuilder()
    fetcher = builder.build_by_task(TaskType.STOCK_BASIC)
    
    print("🚀 开始获取stock_basic数据...")
    raw_data = fetcher()
    
    if raw_data is not None and not raw_data.empty:
        print(f"✅ 原始数据获取成功，数据行数: {len(raw_data)}")
        print(f"📊 原始数据列: {list(raw_data.columns)}")
        print(f"📝 前3行原始数据:")
        print(raw_data.head(3))
    else:
        print("❌ 原始数据获取失败")
        return
    
    # 2. 测试数据处理器
    print("\n=== 步骤2: 测试数据处理器 ===")
    processor = SimpleDataProcessor(enable_batch=False)
    
    # 创建任务配置
    config = DownloadTaskConfig(
        task_type=TaskType.STOCK_BASIC,
        symbol="",  # stock_basic不需要symbol
        priority=TaskPriority.HIGH,
    )
    
    # 创建任务结果
    task_result = TaskResult(
        config=config,
        success=True,
        data=raw_data,
        error=None
    )
    
    print(f"🔧 开始处理数据...")
    print(f"📊 输入数据: {len(task_result.data)} 行, {len(task_result.data.columns)} 列")
    
    # 3. 测试数据清洗
    print("\n=== 步骤3: 测试数据清洗 ===")
    cleaned_data = processor._clean_data(task_result.data, "stock_basic")
    if cleaned_data is not None:
        print(f"✅ 数据清洗成功: {len(cleaned_data)} 行, {len(cleaned_data.columns)} 列")
        print(f"📊 清洗后数据列: {list(cleaned_data.columns)}")
        print(f"📝 前3行清洗后数据:")
        print(cleaned_data.head(3))
    else:
        print("❌ 数据清洗失败")
        return
    
    # 4. 测试数据转换
    print("\n=== 步骤4: 测试数据转换 ===")
    transformed_data = processor._transform_data(cleaned_data, "stock_basic")
    if transformed_data is not None:
        print(f"✅ 数据转换成功: {len(transformed_data)} 行, {len(transformed_data.columns)} 列")
        print(f"📊 转换后数据列: {list(transformed_data.columns)}")
        print(f"📝 前3行转换后数据:")
        print(transformed_data.head(3))
        
        # 检查关键字段
        required_fields = ['ts_code', 'symbol', 'name']
        missing_fields = [field for field in required_fields if field not in transformed_data.columns]
        
        if missing_fields:
            print(f"❌ 转换后缺少必要字段: {missing_fields}")
        else:
            print(f"✅ 转换后包含所有必要字段: {required_fields}")
    else:
        print("❌ 数据转换失败")
        return
    
    print("\n=== 数据流程测试完成 ===")

if __name__ == '__main__':
    main()