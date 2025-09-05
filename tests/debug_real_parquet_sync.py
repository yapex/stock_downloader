#!/usr/bin/env python3
"""使用真实parquet文件测试元数据同步流程"""

import tempfile
import shutil
import time
import os
import duckdb
import pandas as pd
from pathlib import Path
import sys

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from neo.helpers.metadata_sync import MetadataSyncManager


class RealParquetSyncDebugger:
    
    def __init__(self):
        # 创建临时环境
        self.temp_dir = tempfile.mkdtemp(prefix="debug_real_parquet_")
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
    
    def create_real_parquet_files(self):
        """创建真实的parquet文件"""
        print("\n=== 创建真实Parquet文件 ===")
        
        # 创建 stock_adj_hfq 表目录
        table_dir = self.parquet_path / "stock_adj_hfq"
        table_dir.mkdir()
        
        files_created = []
        
        # 创建年份分区和真实parquet文件
        years = [2020, 2021, 2022]
        
        for year in years:
            year_dir = table_dir / f"year={year}"
            year_dir.mkdir()
            
            # 每个年份创建3个parquet文件，包含真实数据
            for i in range(3):
                # 创建示例数据
                data = {
                    'ts_code': [f'00000{j}.SH' for j in range(i*10, (i+1)*10)],
                    'trade_date': [f'{year}0{j%9+1:02d}01' for j in range(10)],
                    'close': [10.0 + j * 0.1 for j in range(10)],
                    'open': [9.8 + j * 0.1 for j in range(10)],
                    'high': [10.2 + j * 0.1 for j in range(10)],
                    'low': [9.6 + j * 0.1 for j in range(10)],
                    'vol': [1000 + j * 100 for j in range(10)],
                    'year': [year] * 10
                }
                
                df = pd.DataFrame(data)
                parquet_file = year_dir / f"data_{i:02d}.parquet"
                df.to_parquet(parquet_file, index=False)
                
                # 设置时间戳
                timestamp = time.time() - (len(files_created) * 60)
                os.utime(parquet_file, (timestamp, timestamp))
                
                files_created.append(parquet_file)
                print(f"创建Parquet文件: {parquet_file.relative_to(self.parquet_path)} ({len(df)} 行)")
        
        print(f"总计创建 {len(files_created)} 个真实parquet文件")
        return files_created
    
    def test_duckdb_direct_read(self):
        """测试DuckDB直接读取parquet文件"""
        print("\n=== 测试DuckDB直接读取 ===")
        
        try:
            with duckdb.connect() as con:
                # 测试读取单个文件
                single_file = self.parquet_path / "stock_adj_hfq/year=2020/data_00.parquet"
                result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{single_file}')").fetchone()
                print(f"单个文件行数: {result[0]}")
                
                # 测试读取年份分区
                year_pattern = self.parquet_path / "stock_adj_hfq/year=2020/*.parquet"
                result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{year_pattern}')").fetchone()
                print(f"2020年分区总行数: {result[0]}")
                
                # 测试读取所有文件（使用通配符）
                all_pattern = self.parquet_path / "stock_adj_hfq/*/*.parquet"
                result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{all_pattern}')").fetchone()
                print(f"所有文件总行数: {result[0]}")
                
                return True
                
        except Exception as e:
            print(f"✗ DuckDB直接读取失败: {e}")
            return False
    
    def test_sync_with_real_files(self):
        """使用真实parquet文件测试同步"""
        print("\n=== 使用真实文件测试同步 ===")
        
        try:
            # 执行强制全量扫描同步
            print("执行强制全量同步...")
            self.manager.sync(force_full_scan=True, mtime_check_minutes=0)
            
            # 验证同步结果
            with duckdb.connect(database=str(self.metadata_db)) as con:
                table_name = "stock_adj_hfq"
                
                # 检查视图存在
                view_exists = self.manager._view_exists(con, table_name)
                print(f"同步后视图存在: {view_exists}")
                
                if view_exists:
                    # 查询总行数
                    result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    total_rows = result[0] if result else 0
                    print(f"同步后总行数: {total_rows}")
                    
                    # 按年份分组查询
                    result = con.execute(f"SELECT year, COUNT(*) FROM {table_name} GROUP BY year ORDER BY year").fetchall()
                    print("按年份统计:")
                    for year, count in result:
                        print(f"  {year}: {count} 行")
                    
                    # 检查分区
                    view_partitions = self.manager._get_view_partitions(con, table_name)
                    print(f"视图分区: {sorted(view_partitions)}")
                    
                    # 显示视图SQL
                    view_sql_result = con.execute(
                        f"SELECT sql FROM duckdb_views() WHERE view_name = '{table_name}'"
                    ).fetchone()
                    if view_sql_result:
                        print(f"视图SQL:\n{view_sql_result[0]}")
                    
                    return True
                else:
                    print("✗ 视图同步失败")
                    return False
                    
        except Exception as e:
            print(f"✗ 真实文件同步测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_incremental_sync(self):
        """测试增量同步"""
        print("\n=== 测试增量同步 ===")
        
        try:
            # 添加新的parquet文件
            new_year_dir = self.parquet_path / "stock_adj_hfq/year=2023"
            new_year_dir.mkdir()
            
            # 创建2023年的数据
            data = {
                'ts_code': [f'10000{j}.SH' for j in range(10)],
                'trade_date': ['20230101'] * 10,
                'close': [15.0 + j * 0.1 for j in range(10)],
                'open': [14.8 + j * 0.1 for j in range(10)],
                'high': [15.2 + j * 0.1 for j in range(10)],
                'low': [14.6 + j * 0.1 for j in range(10)],
                'vol': [2000 + j * 100 for j in range(10)],
                'year': [2023] * 10
            }
            
            df = pd.DataFrame(data)
            new_file = new_year_dir / "data_00.parquet"
            df.to_parquet(new_file, index=False)
            print(f"添加新文件: {new_file.relative_to(self.parquet_path)} ({len(df)} 行)")
            
            # 执行增量同步
            print("执行增量同步...")
            self.manager.sync(force_full_scan=False, mtime_check_minutes=60)
            
            # 验证增量同步结果
            with duckdb.connect(database=str(self.metadata_db)) as con:
                table_name = "stock_adj_hfq"
                
                # 查询总行数（应该增加）
                result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                total_rows = result[0] if result else 0
                print(f"增量同步后总行数: {total_rows}")
                
                # 按年份分组查询（应该包含2023）
                result = con.execute(f"SELECT year, COUNT(*) FROM {table_name} GROUP BY year ORDER BY year").fetchall()
                print("增量同步后按年份统计:")
                for year, count in result:
                    print(f"  {year}: {count} 行")
                
                return True
                
        except Exception as e:
            print(f"✗ 增量同步测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    debugger = None
    try:
        debugger = RealParquetSyncDebugger()
        
        # 创建真实parquet文件
        files_created = debugger.create_real_parquet_files()
        
        # 测试DuckDB直接读取
        if not debugger.test_duckdb_direct_read():
            return
        
        # 测试完整同步流程
        if debugger.test_sync_with_real_files():
            # 测试增量同步
            debugger.test_incremental_sync()
        
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if debugger:
            debugger.cleanup()


if __name__ == "__main__":
    main()
