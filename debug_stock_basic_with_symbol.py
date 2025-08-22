#!/usr/bin/env python3
"""调试带symbol的stock_basic任务数据获取问题"""

import os
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.task_bus.types import TaskType

def main():
    # 检查环境变量
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        print("❌ 未设置 TUSHARE_TOKEN 环境变量")
        return
    
    print(f"✅ TUSHARE_TOKEN 已设置")
    
    try:
        # 测试不带symbol的stock_basic（正确方式）
        print("\n=== 测试不带symbol的stock_basic ===")
        fetcher_builder = FetcherBuilder()
        fetcher = fetcher_builder.build_by_task(TaskType.STOCK_BASIC)
        result = fetcher()
        
        if result is not None and not result.empty:
            print(f"✅ 成功获取数据: {len(result)} 行, {len(result.columns)} 列")
            print(f"列名: {list(result.columns)}")
            print(f"包含ts_code: {'ts_code' in result.columns}")
            print(f"包含symbol: {'symbol' in result.columns}")
            print(f"包含name: {'name' in result.columns}")
            print("前3行数据:")
            print(result.head(3))
        else:
            print("❌ 获取数据失败或数据为空")
            
        # 测试带symbol的stock_basic（错误方式）
        print("\n=== 测试带symbol的stock_basic ===")
        fetcher_with_symbol = fetcher_builder.build_by_task(TaskType.STOCK_BASIC, "000001.SZ")
        result_with_symbol = fetcher_with_symbol()
        
        if result_with_symbol is not None and not result_with_symbol.empty:
            print(f"✅ 成功获取数据: {len(result_with_symbol)} 行, {len(result_with_symbol.columns)} 列")
            print(f"列名: {list(result_with_symbol.columns)}")
            print(f"包含ts_code: {'ts_code' in result_with_symbol.columns}")
            print(f"包含symbol: {'symbol' in result_with_symbol.columns}")
            print(f"包含name: {'name' in result_with_symbol.columns}")
            print("数据内容:")
            print(result_with_symbol)
        else:
            print("❌ 获取数据失败或数据为空")
            
    except Exception as e:
        print(f"❌ 执行过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()