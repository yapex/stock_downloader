#!/usr/bin/env python3
"""调试文件扫描逻辑的测试脚本"""

import tempfile
import shutil
import time
import os
from pathlib import Path
import sys

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from neo.helpers.metadata_sync import MetadataSyncManager


def create_test_directory():
    """创建测试目录结构"""
    temp_dir = tempfile.mkdtemp(prefix="test_metadata_scan_")
    print(f"创建临时目录: {temp_dir}")
    
    parquet_path = Path(temp_dir) / "parquet"
    parquet_path.mkdir()
    
    # 创建 stock_adj_hfq 表目录和文件
    table_dir = parquet_path / "stock_adj_hfq"
    table_dir.mkdir()
    
    # 创建年份分区
    years = ["2020", "2021", "2022"]
    files_created = []
    
    for year in years:
        year_dir = table_dir / f"year={year}"
        year_dir.mkdir()
        
        # 每个年份创建3个parquet文件
        for i in range(3):
            parquet_file = year_dir / f"data_{i:02d}.parquet"
            parquet_file.write_text(f"mock parquet data for {year}, file {i}")
            
            # 设置不同的时间戳
            timestamp = time.time() - (len(files_created) * 60)  # 每个文件相差1分钟
            os.utime(parquet_file, (timestamp, timestamp))
            
            files_created.append(parquet_file)
            print(f"创建文件: {parquet_file} (时间戳: {timestamp})")
    
    print(f"总计创建 {len(files_created)} 个文件")
    return temp_dir, parquet_path, files_created


def test_different_scan_methods(parquet_path):
    """测试不同的扫描方法"""
    print("\n=== 测试不同扫描方法 ===")
    
    # 初始化管理器 (metadata_db_path, parquet_base_path)
    metadata_db = parquet_path.parent / "test_metadata.db"
    manager = MetadataSyncManager(str(metadata_db), str(parquet_path))
    
    table_name = "stock_adj_hfq"
    table_dir = parquet_path / table_name
    
    print(f"表目录: {table_dir}")
    print(f"目录存在: {table_dir.exists()}")
    
    if table_dir.exists():
        # 方法1: 直接 glob
        files_glob = list(table_dir.glob("**/*.parquet"))
        print(f"glob('**/*.parquet') 找到 {len(files_glob)} 个文件")
        
        # 方法2: rglob
        files_rglob = list(table_dir.rglob("*.parquet"))
        print(f"rglob('*.parquet') 找到 {len(files_rglob)} 个文件")
        
        # 方法3: _fast_file_scan
        scan_result = manager._fast_file_scan(table_name, 0)
        print(f"_fast_file_scan 找到 {scan_result['file_count']} 个文件")
        print(f"扫描方法: {scan_result['method']}")
        
        # 方法4: _quick_table_check
        quick_result = manager._quick_table_check(table_name, 0)
        print(f"_quick_table_check 找到 {quick_result['file_count']} 个文件")
        print(f"检查方法: {quick_result['method']}")
        
        # 方法5: _get_file_info
        info_result = manager._get_file_info(table_name, 0)
        print(f"_get_file_info 找到 {info_result['file_count']} 个文件")
        
        # 详细列出找到的文件
        print(f"\nglob 找到的文件:")
        for f in files_glob:
            print(f"  - {f}")
        
        print(f"\nrglob 找到的文件:")
        for f in files_rglob:
            print(f"  - {f}")


def test_cache_behavior(parquet_path):
    """测试缓存行为"""
    print("\n=== 测试缓存行为 ===")
    
    metadata_db = parquet_path.parent / "test_metadata.db"
    manager = MetadataSyncManager(str(metadata_db), str(parquet_path))
    table_name = "stock_adj_hfq"
    
    # 第一次扫描
    print("第一次扫描...")
    result1 = manager._quick_table_check(table_name, 0)
    print(f"结果1: {result1}")
    
    # 保存缓存
    manager._save_cache()
    print("缓存已保存")
    
    # 创建新管理器实例（模拟重启）
    manager2 = MetadataSyncManager(str(metadata_db), str(parquet_path))
    
    # 第二次扫描（应该使用缓存）
    print("第二次扫描（新实例，使用缓存）...")
    result2 = manager2._quick_table_check(table_name, 0)
    print(f"结果2: {result2}")
    
    print(f"缓存内容: {manager2.cache}")


def main():
    temp_dir = None
    try:
        # 创建测试环境
        temp_dir, parquet_path, files_created = create_test_directory()
        
        # 测试扫描方法
        test_different_scan_methods(parquet_path)
        
        # 测试缓存
        test_cache_behavior(parquet_path)
        
    except Exception as e:
        print(f"发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if temp_dir and Path(temp_dir).exists():
            print(f"\n清理临时目录: {temp_dir}")
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    main()
