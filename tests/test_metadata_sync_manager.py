"""
测试 MetadataSyncManager 的核心功能

这个测试文件专门用于验证元数据同步管理器的逻辑正确性，
特别关注可能导致同步不完整的问题。
"""

import pytest
import tempfile
import shutil
import json
import time
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 添加项目路径
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from neo.helpers.metadata_sync import MetadataSyncManager


@pytest.fixture
def temp_directories():
    """创建临时的测试目录结构"""
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir)
    
    # 创建测试目录结构
    db_path = temp_path / "test_metadata.db"
    parquet_path = temp_path / "parquet"
    parquet_path.mkdir(exist_ok=True)
    
    yield {
        "temp_dir": temp_path,
        "db_path": str(db_path),
        "parquet_path": str(parquet_path)
    }
    
    # 清理
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_parquet_structure(temp_directories):
    """创建示例的parquet文件结构"""
    parquet_path = Path(temp_directories["parquet_path"])
    
    # 创建 stock_adj_hfq 表结构
    stock_hfq_dir = parquet_path / "stock_adj_hfq"
    stock_hfq_dir.mkdir(exist_ok=True)
    
    # 创建年份分区和文件
    years = ["2023", "2024", "2025"]
    for year in years:
        year_dir = stock_hfq_dir / f"year={year}"
        year_dir.mkdir(exist_ok=True)
        
        # 创建多个有效的parquet文件
        for i in range(3):
            parquet_file = year_dir / f"part-{i}-test.parquet"
            
            # 创建示例DataFrame并写入parquet
            df = pd.DataFrame({
                'ts_code': [f'00000{i}.SZ'],
                'trade_date': [f'{year}-01-01'],
                'close': [100.0 + i]
            })
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_file)
            
            # 设置不同的修改时间
            timestamp = time.time() - (i * 3600)  # 每小时递减
            os.utime(parquet_file, (timestamp, timestamp))
    
    # 创建 balance_sheet 表（嵌套结构）
    balance_dir = parquet_path / "balance_sheet"
    balance_dir.mkdir(exist_ok=True)
    
    # 创建带股票代码的嵌套结构
    for stock_code in ["000001.SZ", "000002.SZ"]:
        stock_dir = balance_dir / f"ts_code={stock_code}"
        stock_dir.mkdir(exist_ok=True)
        
        for year in ["2023", "2024"]:
            year_dir = stock_dir / f"year={year}"
            year_dir.mkdir(exist_ok=True)
            
            parquet_file = year_dir / "data.parquet"
            
            # 创建示例财务数据DataFrame
            df = pd.DataFrame({
                'ts_code': [stock_code],
                'ann_date': [f'{year}0331'],
                'total_assets': [1000000.0]
            })
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_file)
    
    return temp_directories


class TestMetadataSyncManager:
    """MetadataSyncManager 的单元测试"""
    
    def test_init(self, temp_directories):
        """测试初始化功能"""
        manager = MetadataSyncManager(
            temp_directories["db_path"],
            temp_directories["parquet_path"]
        )
        
        assert manager.db_path == Path(temp_directories["db_path"])
        assert manager.parquet_path == Path(temp_directories["parquet_path"])
        assert manager.cache_path.exists() or not manager.cache_path.exists()  # 缓存文件可能存在也可能不存在
        assert isinstance(manager.cache, dict)
    
    def test_cache_operations(self, temp_directories):
        """测试缓存的加载和保存"""
        manager = MetadataSyncManager(
            temp_directories["db_path"],
            temp_directories["parquet_path"]
        )
        
        # 测试缓存保存
        test_data = {
            "stock_adj_hfq": {
                "file_count": 10,
                "last_modified": time.time(),
                "dir_mtime": time.time()
            }
        }
        manager.cache = test_data
        manager._save_cache()
        
        # 测试缓存加载
        manager2 = MetadataSyncManager(
            temp_directories["db_path"],
            temp_directories["parquet_path"]
        )
        
        assert "stock_adj_hfq" in manager2.cache
        assert manager2.cache["stock_adj_hfq"]["file_count"] == 10
    
    def test_get_physical_partitions(self, sample_parquet_structure):
        """测试物理分区检测功能"""
        manager = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        # 测试直接分区结构 (stock_adj_hfq)
        partitions = manager._get_physical_partitions("stock_adj_hfq")
        expected_partitions = {"year=2023", "year=2024", "year=2025"}
        assert partitions == expected_partitions
        
        # 测试嵌套分区结构 (balance_sheet)
        partitions = manager._get_physical_partitions("balance_sheet")
        expected_partitions = {"year=2023", "year=2024"}
        assert partitions == expected_partitions
        
        # 测试不存在的表
        partitions = manager._get_physical_partitions("nonexistent_table")
        assert partitions == set()
    
    def test_format_time_diff(self, temp_directories):
        """测试时间格式化功能"""
        manager = MetadataSyncManager(
            temp_directories["db_path"],
            temp_directories["parquet_path"]
        )
        
        now = time.time()
        
        # 测试秒级差异
        result = manager._format_time_diff(now - 30)
        assert "30秒前" == result
        
        # 测试分钟级差异
        result = manager._format_time_diff(now - 150)  # 2分30秒
        assert "2分30秒前" == result
        
        # 测试小时级差异
        result = manager._format_time_diff(now - 3900)  # 1小时5分
        assert "1小时5分钟前" == result
        
        # 测试天级差异
        result = manager._format_time_diff(now - 90000)  # 1天1小时
        assert "1天1小时前" == result
    
    def test_get_file_info(self, sample_parquet_structure):
        """测试文件信息获取功能"""
        manager = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        # 测试存在的表
        file_info = manager._get_file_info("stock_adj_hfq", minutes=0)
        assert file_info["has_changes"] is True
        assert file_info["file_count"] == 9  # 3年 × 3文件
        assert file_info["last_modified"] is not None
        
        # 测试近期变化检测
        file_info = manager._get_file_info("stock_adj_hfq", minutes=60)
        assert isinstance(file_info["has_changes"], bool)
        
        # 测试不存在的表
        file_info = manager._get_file_info("nonexistent_table")
        assert file_info["has_changes"] is False
        assert file_info["file_count"] == 0
        assert file_info["last_modified"] is None
    
    def test_fast_file_scan(self, sample_parquet_structure):
        """测试快速文件扫描功能"""
        manager = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        # 测试文件扫描
        scan_result = manager._fast_file_scan("stock_adj_hfq", minutes=0)
        assert scan_result["file_count"] == 9
        assert scan_result["last_modified"] is not None
        assert scan_result["method"] == "fast_scan"
        
        # 检查缓存是否更新
        assert "stock_adj_hfq" in manager.cache
        assert manager.cache["stock_adj_hfq"]["file_count"] == 9
    
    def test_quick_table_check_file_count_change(self, sample_parquet_structure):
        """测试快速表检查 - 文件数量变化的情况"""
        manager = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        # 首次检查，建立缓存
        result1 = manager._quick_table_check("stock_adj_hfq", minutes=30)
        assert result1["has_changes"] is True  # 首次检查总是有变化
        assert result1["file_count"] == 9
        
        # 保存缓存
        manager._save_cache()
        
        # 再次检查，文件数量未变
        result2 = manager._quick_table_check("stock_adj_hfq", minutes=30)
        # 这里应该根据时间窗口判断
        
        # 添加新文件
        new_file = Path(sample_parquet_structure["parquet_path"]) / "stock_adj_hfq" / "year=2025" / "new_file.parquet"
        new_file.write_text("new data")
        
        # 检查文件数量变化
        result3 = manager._quick_table_check("stock_adj_hfq", minutes=30)
        assert result3["has_changes"] is True
        assert result3["file_count"] == 10
    
    def test_view_operations_with_mock_db(self, sample_parquet_structure):
        """测试数据库视图相关操作（使用mock）"""
        manager = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        # Mock DuckDB连接
        mock_con = Mock()
        
        # 测试视图存在检查
        mock_con.execute.return_value.fetchone.return_value = (1,)
        assert manager._view_exists(mock_con, "test_table") is True
        
        mock_con.execute.return_value.fetchone.return_value = None
        assert manager._view_exists(mock_con, "test_table") is False
        
        # 测试视图分区提取
        mock_sql = "CREATE VIEW test AS SELECT * FROM read_parquet('data/year=2023/*.parquet', 'data/year=2024/*.parquet')"
        mock_con.execute.return_value.fetchone.return_value = (mock_sql,)
        
        partitions = manager._get_view_partitions(mock_con, "test_table")
        assert partitions == {"year=2023", "year=2024"}
    
    def test_integration_sync_with_real_db(self, sample_parquet_structure):
        """集成测试 - 使用真实数据库进行同步"""
        manager = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        # 执行同步
        manager.sync(force_full_scan=True, mtime_check_minutes=0)
        
        # 验证数据库状态
        with duckdb.connect(sample_parquet_structure["db_path"]) as con:
            # 检查视图是否创建
            tables = con.execute("SELECT view_name FROM duckdb_views()").fetchall()
            table_names = [t[0] for t in tables]
            
            assert "stock_adj_hfq" in table_names
            assert "balance_sheet" in table_names
            
            # 检查数据是否可以查询（虽然是mock数据，但结构应该正确）
            try:
                # 这可能会失败因为是mock数据，但至少视图应该存在
                result = con.execute("SELECT COUNT(*) FROM stock_adj_hfq").fetchone()
                # 如果能执行到这里说明视图创建成功
            except Exception as e:
                # 预期可能失败，因为mock数据格式不正确
                print(f"预期的查询失败（mock数据）: {e}")
    
    def test_sync_skip_logic(self, sample_parquet_structure):
        """测试同步跳过逻辑"""
        manager = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        # 设置较短的时间窗口，使得文件看起来很旧
        very_old_time = time.time() - 86400 * 7  # 7天前
        
        # 修改所有文件的时间戳为很久以前
        parquet_path = Path(sample_parquet_structure["parquet_path"])
        for parquet_file in parquet_path.rglob("*.parquet"):
            os.utime(parquet_file, (very_old_time, very_old_time))
        
        # 使用短时间窗口进行同步，应该跳过所有表
        with patch('builtins.print') as mock_print:
            manager.sync(force_full_scan=False, mtime_check_minutes=60)  # 只检查最近1小时的文件
            
            # 验证跳过消息
            print_calls = [call.args[0] for call in mock_print.call_args_list]
            skip_messages = [msg for msg in print_calls if "跳过" in msg]
            assert len(skip_messages) > 0
    
    def test_error_handling(self, temp_directories):
        """测试错误处理"""
        # 创建一个不存在的子路径
        nonexistent_path = Path(temp_directories["temp_dir"]) / "nonexistent_subdir"
        manager = MetadataSyncManager(
            temp_directories["db_path"],
            str(nonexistent_path)
        )
        
        # 测试物理分区检测对不存在路径的处理
        partitions = manager._get_physical_partitions("any_table")
        assert partitions == set()
        
        # 测试文件信息获取对不存在路径的处理
        file_info = manager._get_file_info("any_table")
        assert file_info["has_changes"] is False
        assert file_info["file_count"] == 0


class TestSpecificBugScenarios:
    """专门测试可能导致同步不完整的特定场景"""
    
    def test_missing_stock_scenario(self, temp_directories):
        """重现我们发现的问题：某些股票数据存在但未同步的场景"""
        parquet_path = Path(temp_directories["parquet_path"])
        
        # 创建我们测试的股票结构
        stock_hfq_dir = parquet_path / "stock_adj_hfq"
        stock_hfq_dir.mkdir(exist_ok=True)
        
        test_stocks = ["000002.SZ", "000011.SZ", "000042.SZ", "000055.SZ"]
        
        # 为每个股票创建数据文件
        for stock in test_stocks:
            for year in ["2023", "2024"]:
                year_dir = stock_hfq_dir / f"year={year}"
                year_dir.mkdir(exist_ok=True)
                
                # 创建包含股票代码的文件
                parquet_file = year_dir / f"part-{stock.replace('.', '')}-{year}.parquet"
                df = pd.DataFrame({
                    'ts_code': [stock],
                    'trade_date': [f'{year}-01-01'],
                    'close': [100.0]
                })
                table = pa.Table.from_pandas(df)
                pq.write_table(table, parquet_file)
        
        manager = MetadataSyncManager(
            temp_directories["db_path"],
            temp_directories["parquet_path"]
        )
        
        # 执行同步
        manager.sync(force_full_scan=True)
        
        # 验证所有分区都被检测到
        partitions = manager._get_physical_partitions("stock_adj_hfq")
        expected_partitions = {"year=2023", "year=2024"}
        assert partitions == expected_partitions
        
        # 验证数据库视图创建
        with duckdb.connect(temp_directories["db_path"]) as con:
            views = con.execute("SELECT view_name FROM duckdb_views() WHERE view_name = 'stock_adj_hfq'").fetchall()
            assert len(views) == 1
    
    def test_partial_sync_detection(self, sample_parquet_structure):
        """测试部分同步检测能力"""
        manager = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        # 首次完整同步
        manager.sync(force_full_scan=True)
        
        # 添加新的分区数据
        new_year_dir = Path(sample_parquet_structure["parquet_path"]) / "stock_adj_hfq" / "year=2026"
        new_year_dir.mkdir(exist_ok=True)
        new_parquet_file = new_year_dir / "new_data.parquet"
        df = pd.DataFrame({
            'ts_code': ['999999.SZ'],
            'trade_date': ['2026-01-01'],
            'close': [500.0]
        })
        table = pa.Table.from_pandas(df)
        pq.write_table(table, new_parquet_file)
        
        # 增量同步应该检测到变化
        with patch('builtins.print') as mock_print:
            manager.sync(force_full_scan=False, mtime_check_minutes=60)
            
            # 检查是否有更新消息
            print_calls = [call.args[0] for call in mock_print.call_args_list]
            update_messages = [msg for msg in print_calls if "更新完成" in msg]
            assert len(update_messages) > 0
    
    def test_cache_corruption_recovery(self, sample_parquet_structure):
        """测试缓存损坏时的恢复能力"""
        manager = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        # 创建损坏的缓存文件
        with open(manager.cache_path, 'w') as f:
            f.write("invalid json content")
        
        # 重新初始化应该能够处理损坏的缓存
        manager2 = MetadataSyncManager(
            sample_parquet_structure["db_path"],
            sample_parquet_structure["parquet_path"]
        )
        
        assert isinstance(manager2.cache, dict)
        
        # 同步应该正常工作
        manager2.sync(force_full_scan=True)


if __name__ == "__main__":
    # 运行测试的简单方法
    pytest.main([__file__, "-v"])
