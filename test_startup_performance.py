#!/usr/bin/env python3
"""
测试下载器启动性能的脚本
用于分析从启动到进度条出现之间各个步骤的耗时
"""

import time
import logging
import sys
from pathlib import Path
from unittest.mock import Mock, patch

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_engine_initialization_steps():
    """测试引擎初始化各个步骤的耗时"""
    print("=== 下载器启动步骤耗时分析 ===")
    
    # 模拟配置
    config = {
        "downloader": {
            "symbols": ["600519", "000001"],  # 使用固定股票列表避免数据库查询
            "max_producers": 1,
            "max_consumers": 1,
            "retry_policy": {
                "max_retries": 3,
                "base_delay": 1.0,
                "max_delay": 60.0,
                "backoff_factor": 2.0
            }
        },
        "tasks": [
            {
                "type": "daily",
                "enabled": True,
                "date_range": {
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-02"
                }
            }
        ]
    }
    
    start_time = time.time()
    
    try:
        # 步骤1: 导入模块
        step_start = time.time()
        from src.downloader.engine import DownloadEngine
        print(f"[{time.time() - start_time:.3f}s] 步骤1: 导入模块耗时 {time.time() - step_start:.3f}s")
        
        # 步骤2: 创建引擎实例
        step_start = time.time()
        engine = DownloadEngine(
            config=config,
            group_name="performance_test"
        )
        print(f"[{time.time() - start_time:.3f}s] 步骤2: 创建引擎实例耗时 {time.time() - step_start:.3f}s")
        
        # 步骤3: 模拟运行过程中的各个步骤
        print(f"[{time.time() - start_time:.3f}s] 步骤3: 开始模拟运行过程...")
        
        # 模拟 _prepare_target_symbols
        step_start = time.time()
        enabled_tasks = [task for task in config["tasks"] if task.get("enabled", False)]
        target_symbols = engine._prepare_target_symbols(enabled_tasks)
        print(f"[{time.time() - start_time:.3f}s] 步骤3a: 准备目标股票列表耗时 {time.time() - step_start:.3f}s，获得 {len(target_symbols)} 只股票")
        
        # 模拟 _setup_queues
        step_start = time.time()
        engine._setup_queues()
        print(f"[{time.time() - start_time:.3f}s] 步骤3b: 初始化队列耗时 {time.time() - step_start:.3f}s")
        
        # 模拟 _build_download_tasks
        step_start = time.time()
        download_tasks = engine._build_download_tasks(target_symbols, enabled_tasks)
        print(f"[{time.time() - start_time:.3f}s] 步骤3c: 构建下载任务耗时 {time.time() - step_start:.3f}s，生成 {len(download_tasks)} 个任务")
        
        # 模拟进度条初始化
        step_start = time.time()
        from src.downloader.progress_manager import progress_manager
        progress_manager.initialize(
            total_tasks=len(download_tasks),
            description=f"处理 performance_test 组任务",
        )
        print(f"[{time.time() - start_time:.3f}s] 步骤3d: 进度条初始化耗时 {time.time() - step_start:.3f}s")
        
        # 清理进度条
        progress_manager.finish()
        
        total_time = time.time() - start_time
        print(f"\n总耗时: {total_time:.3f} 秒")
        
        if total_time > 2.0:
            print("⚠️  启动时间较长，可能影响用户体验")
        else:
            print("✅ 启动时间正常")
            
    except Exception as e:
        print(f"[{time.time() - start_time:.3f}s] 错误: {e}")
        import traceback
        traceback.print_exc()

def test_database_query_simulation():
    """模拟数据库查询性能测试"""
    print("\n=== 模拟数据库查询性能测试 ===")
    
    # 模拟不同大小的股票列表查询
    stock_counts = [100, 500, 1000, 5000]
    
    for count in stock_counts:
        start_time = time.time()
        
        # 模拟生成股票代码列表
        mock_stocks = [f"{i:06d}.SZ" if i % 2 == 0 else f"{i:06d}.SH" for i in range(count)]
        
        # 模拟数据库查询延迟
        time.sleep(count * 0.0001)  # 模拟每个股票0.1ms的查询时间
        
        query_time = time.time() - start_time
        print(f"模拟查询 {count} 个股票代码耗时: {query_time:.3f} 秒")
        
        if query_time > 1.0:
            print(f"  ⚠️  {count} 个股票的查询时间过长")

def test_config_symbols_all_simulation():
    """测试 symbols: 'all' 配置的影响"""
    print("\n=== 测试 symbols: 'all' 配置的影响 ===")
    
    # 模拟配置
    config_with_all = {
        "downloader": {
            "symbols": "all",  # 这会触发数据库查询
            "max_producers": 1,
            "max_consumers": 1,
        },
        "tasks": [
            {
                "type": "daily",
                "enabled": True,
            }
        ]
    }
    
    config_with_list = {
        "downloader": {
            "symbols": ["600519", "000001"],  # 固定列表
            "max_producers": 1,
            "max_consumers": 1,
        },
        "tasks": [
            {
                "type": "daily",
                "enabled": True,
            }
        ]
    }
    
    try:
        from src.downloader.engine import DownloadEngine
        
        # 测试固定列表配置
        start_time = time.time()
        engine_list = DownloadEngine(config=config_with_list, group_name="test")
        enabled_tasks = [task for task in config_with_list["tasks"] if task.get("enabled", False)]
        symbols_list = engine_list._prepare_target_symbols(enabled_tasks)
        list_time = time.time() - start_time
        print(f"固定股票列表配置耗时: {list_time:.3f}s，获得 {len(symbols_list)} 只股票")
        
        # 测试 "all" 配置（会尝试查询数据库）
        start_time = time.time()
        engine_all = DownloadEngine(config=config_with_all, group_name="test")
        enabled_tasks = [task for task in config_with_all["tasks"] if task.get("enabled", False)]
        symbols_all = engine_all._prepare_target_symbols(enabled_tasks)
        all_time = time.time() - start_time
        print(f"symbols='all' 配置耗时: {all_time:.3f}s，获得 {len(symbols_all)} 只股票")
        
        if all_time > list_time * 2:
            print(f"⚠️  symbols='all' 配置比固定列表慢 {all_time/list_time:.1f} 倍")
            print("   建议：如果不需要所有股票，使用固定股票列表可以显著提升启动速度")
        
    except Exception as e:
        print(f"测试过程中出现错误: {e}")

if __name__ == "__main__":
    test_database_query_simulation()
    test_config_symbols_all_simulation()
    test_engine_initialization_steps()