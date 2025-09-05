#!/usr/bin/env python3
"""详细诊断元数据同步流程的脚本"""

import tempfile
import shutil
import time
import os
import duckdb
from pathlib import Path
import sys
import re

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from neo.helpers.metadata_sync import MetadataSyncManager


class SyncProcessDebugger:
    
    def __init__(self):
        # 创建临时环境
        self.temp_dir = tempfile.mkdtemp(prefix="debug_sync_")
        self.parquet_path = Path(self.temp_dir) / "parquet"
        self.parquet_path.mkdir()
        self.metadata_db = Path(self.temp_dir) / "test_metadata.db"
        
        # 创建管理器
        self.manager = MetadataSyncManager(str(self.metadata_db), str(self.parquet_path))
        
        print(f"临时目录: {self.temp_dir}")
        print(f"Parquet路径: {self.parquet_path}")
        print(f"元数据库路径: {self.metadata_db}")
    
    def cleanup(self):
        """清理临时环境"""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
            print(f"已清理临时目录: {self.temp_dir}")
    
    def create_test_data(self):
        """创建测试数据文件"""
        print("\n=== 创建测试数据 ===")
        
        # 创建 stock_adj_hfq 表目录
        table_dir = self.parquet_path / "stock_adj_hfq"
        table_dir.mkdir()
        
        # 创建年份分区和文件
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
                timestamp = time.time() - (len(files_created) * 60)
                os.utime(parquet_file, (timestamp, timestamp))
                
                files_created.append(parquet_file)
                print(f"创建文件: {parquet_file.relative_to(self.parquet_path)} (时间戳: {timestamp})")
        
        print(f"总计创建 {len(files_created)} 个文件")
        return files_created
    
    def test_physical_partitions(self):
        """测试物理分区检测"""
        print("\n=== 测试物理分区检测 ===")
        
        table_name = "stock_adj_hfq"
        physical_partitions = self.manager._get_physical_partitions(table_name)
        
        print(f"检测到的物理分区: {sorted(physical_partitions)}")
        
        # 验证分区检测
        expected_partitions = {"year=2020", "year=2021", "year=2022"}
        if physical_partitions == expected_partitions:
            print("✓ 物理分区检测正确")
        else:
            print(f"✗ 物理分区检测有问题，期望: {expected_partitions}")
        
        return physical_partitions
    
    def test_view_creation_sql(self, physical_partitions):
        """测试视图创建SQL的生成"""
        print("\n=== 测试视图创建SQL ===")
        
        table_name = "stock_adj_hfq"
        
        # 检测分区结构并生成正确的路径模式
        partition_paths = []
        for p in sorted(list(physical_partitions)):
            # 检查是否存在嵌套结构 (ts_code=XXX/year=YYYY)
            nested_path = self.parquet_path / table_name / "*" / p
            direct_path = self.parquet_path / table_name / p
            
            print(f"检查分区 {p}:")
            print(f"  嵌套路径模式: {nested_path}")
            print(f"  直接路径: {direct_path}")
            
            # 检查嵌套结构
            nested_matches = list(nested_path.parent.parent.glob(f"*/{p}/*.parquet"))
            direct_matches = list(direct_path.glob("*.parquet"))
            
            print(f"  嵌套匹配: {len(nested_matches)} 个文件")
            print(f"  直接匹配: {len(direct_matches)} 个文件")
            
            if nested_matches:
                partition_paths.append(f"'{nested_path}/*.parquet'")
                print(f"  -> 使用嵌套路径")
            elif direct_matches:
                partition_paths.append(f"'{direct_path}/*.parquet'")
                print(f"  -> 使用直接路径")
                for f in direct_matches[:3]:  # 显示前3个文件
                    print(f"    文件: {f}")
        
        partition_paths_str = ", ".join(partition_paths)
        
        sql = f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT * FROM read_parquet([{partition_paths_str}], union_by_name=true);
        """
        
        print(f"\n生成的SQL:")
        print(sql)
        
        return sql.strip()
    
    def test_duckdb_sql_execution(self, sql):
        """测试DuckDB SQL执行"""
        print("\n=== 测试DuckDB SQL执行 ===")
        
        try:
            with duckdb.connect(database=str(self.metadata_db)) as con:
                print("执行视图创建SQL...")
                con.execute(sql)
                print("✓ SQL执行成功")
                
                # 检查视图是否创建成功
                table_name = "stock_adj_hfq"
                view_exists = self.manager._view_exists(con, table_name)
                print(f"视图存在检查: {view_exists}")
                
                if view_exists:
                    # 尝试查询视图
                    try:
                        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                        row_count = result[0] if result else 0
                        print(f"✓ 视图查询成功，行数: {row_count}")
                    except Exception as e:
                        print(f"✗ 视图查询失败: {e}")
                        return False
                    
                    # 检查视图的SQL定义
                    view_sql_result = con.execute(
                        f"SELECT sql FROM duckdb_views() WHERE view_name = '{table_name}'"
                    ).fetchone()
                    
                    if view_sql_result:
                        view_sql = view_sql_result[0]
                        print(f"视图SQL定义: {view_sql}")
                        
                        # 提取分区信息
                        view_partitions = self.manager._get_view_partitions(con, table_name)
                        print(f"从视图SQL提取的分区: {sorted(view_partitions)}")
                    
                    return True
                else:
                    print("✗ 视图未创建成功")
                    return False
                    
        except Exception as e:
            print(f"✗ SQL执行出错: {e}")
            return False
    
    def test_full_sync_process(self):
        """测试完整同步流程"""
        print("\n=== 测试完整同步流程 ===")
        
        try:
            # 执行同步（强制全量扫描，忽略时间检查）
            self.manager.sync(force_full_scan=True, mtime_check_minutes=0)
            
            print("✓ 同步流程执行完成")
            
            # 验证同步结果
            with duckdb.connect(database=str(self.metadata_db)) as con:
                table_name = "stock_adj_hfq"
                
                # 检查视图存在
                view_exists = self.manager._view_exists(con, table_name)
                print(f"同步后视图存在: {view_exists}")
                
                if view_exists:
                    # 查询行数
                    result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    row_count = result[0] if result else 0
                    print(f"同步后行数: {row_count}")
                    
                    # 检查分区
                    view_partitions = self.manager._get_view_partitions(con, table_name)
                    print(f"同步后视图分区: {sorted(view_partitions)}")
                    
                    return True
                
        except Exception as e:
            print(f"✗ 完整同步流程出错: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_specific_issue_scenarios(self):
        """测试特定问题场景"""
        print("\n=== 测试特定问题场景 ===")
        
        # 场景1: 检查缓存对同步的影响
        print("\n场景1: 测试缓存影响")
        
        # 第一次同步（会创建缓存）
        print("第一次同步...")
        self.manager.sync(force_full_scan=False, mtime_check_minutes=30)
        
        # 保存缓存内容
        cache_content = dict(self.manager.cache)
        print(f"缓存内容: {cache_content}")
        
        # 第二次同步（应该使用缓存）
        print("第二次同步（使用缓存）...")
        self.manager.sync(force_full_scan=False, mtime_check_minutes=30)
        
        # 场景2: 强制全量扫描
        print("\n场景2: 强制全量扫描")
        self.manager.sync(force_full_scan=True, mtime_check_minutes=0)
        
        # 场景3: 检查文件扫描一致性
        print("\n场景3: 文件扫描一致性检查")
        table_name = "stock_adj_hfq"
        
        # 使用不同方法扫描
        quick_check = self.manager._quick_table_check(table_name, 0)
        fast_scan = self.manager._fast_file_scan(table_name, 0)
        file_info = self.manager._get_file_info(table_name, 0)
        
        print(f"quick_table_check: {quick_check['file_count']} 文件")
        print(f"fast_file_scan: {fast_scan['file_count']} 文件") 
        print(f"get_file_info: {file_info['file_count']} 文件")
        
        # 检查一致性
        if quick_check['file_count'] == fast_scan['file_count'] == file_info['file_count']:
            print("✓ 文件扫描结果一致")
        else:
            print("✗ 文件扫描结果不一致！")


def main():
    debugger = None
    try:
        debugger = SyncProcessDebugger()
        
        # 执行测试步骤
        files_created = debugger.create_test_data()
        
        # 测试各个环节
        physical_partitions = debugger.test_physical_partitions()
        sql = debugger.test_view_creation_sql(physical_partitions)
        duckdb_success = debugger.test_duckdb_sql_execution(sql)
        
        if duckdb_success:
            debugger.test_full_sync_process()
            debugger.test_specific_issue_scenarios()
        
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if debugger:
            debugger.cleanup()


if __name__ == "__main__":
    main()
