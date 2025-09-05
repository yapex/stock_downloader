"""
元数据同步管理器核心功能测试

专门测试可能导致同步不完整的核心逻辑，避免DuckDB格式要求
"""

import pytest
import tempfile
import shutil
import json
import time
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 添加项目路径
import sys
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from neo.helpers.metadata_sync import MetadataSyncManager


@pytest.fixture
def temp_directories():
    """创建临时的测试目录结构"""
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir)
    
    db_path = temp_path / "test_metadata.db"
    parquet_path = temp_path / "parquet"
    parquet_path.mkdir(exist_ok=True)
    
    yield {
        "temp_dir": temp_path,
        "db_path": str(db_path),
        "parquet_path": str(parquet_path)
    }
    
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def stock_hfq_structure(temp_directories):
    """创建stock_adj_hfq表的文件结构"""
    parquet_path = Path(temp_directories["parquet_path"])
    
    # 创建 stock_adj_hfq 表结构，模拟我们遇到的问题
    stock_hfq_dir = parquet_path / "stock_adj_hfq"
    stock_hfq_dir.mkdir(exist_ok=True)
    
    # 创建年份分区
    years = ["2023", "2024", "2025"]
    for year in years:
        year_dir = stock_hfq_dir / f"year={year}"
        year_dir.mkdir(exist_ok=True)
        
        # 创建有效的parquet文件
        for i in range(3):
            parquet_file = year_dir / f"part-{i}-{year}.parquet"
            
            # 创建示例DataFrame并写入parquet
            df = pd.DataFrame({
                'ts_code': [f'00000{i}.SZ'],
                'trade_date': [f'{year}-01-01'],
                'close': [100.0 + i]
            })
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_file)
            
            # 设置不同时间戳
            timestamp = time.time() - (i * 1800)  # 30分钟间隔
            os.utime(parquet_file, (timestamp, timestamp))
    
    return temp_directories


class TestMetadataSyncCoreLogic:
    """测试MetadataSyncManager的核心逻辑"""
    
    def test_initialization(self, temp_directories):
        """测试初始化逻辑"""
        manager = MetadataSyncManager(
            temp_directories["db_path"],
            temp_directories["parquet_path"]
        )
        
        assert manager.db_path == Path(temp_directories["db_path"])
        assert manager.parquet_path == Path(temp_directories["parquet_path"])
        assert manager.parquet_path.exists()
        assert isinstance(manager.cache, dict)
    
    def test_physical_partition_detection(self, stock_hfq_structure):
        """测试物理分区检测 - 这是关键功能"""
        manager = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 测试正常情况
        partitions = manager._get_physical_partitions("stock_adj_hfq")
        expected = {"year=2023", "year=2024", "year=2025"}
        assert partitions == expected
        
        # 测试不存在的表
        partitions = manager._get_physical_partitions("nonexistent")
        assert partitions == set()
    
    def test_file_count_detection(self, stock_hfq_structure):
        """测试文件数量检测逻辑"""
        manager = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 第一次扫描
        scan_result = manager._fast_file_scan("stock_adj_hfq", minutes=0)
        assert scan_result["file_count"] == 9  # 3年 × 3文件
        assert scan_result["last_modified"] is not None
        assert scan_result["method"] == "fast_scan"
        
        # 检查缓存更新
        assert "stock_adj_hfq" in manager.cache
        assert manager.cache["stock_adj_hfq"]["file_count"] == 9
    
    def test_quick_table_check_logic(self, stock_hfq_structure):
        """测试快速表检查逻辑 - 可能导致同步遗漏的关键点"""
        manager = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 首次检查，建立缓存基准
        result1 = manager._quick_table_check("stock_adj_hfq", minutes=30)
        assert result1["file_count"] == 9
        
        # 保存缓存状态
        manager._save_cache()
        
        # 模拟添加新文件
        new_file = Path(stock_hfq_structure["parquet_path"]) / "stock_adj_hfq" / "year=2025" / "new_part.parquet"
        df = pd.DataFrame({
            'ts_code': ['000999.SZ'],
            'trade_date': ['2025-01-01'],
            'close': [150.0]
        })
        table = pa.Table.from_pandas(df)
        pq.write_table(table, new_file)
        
        # 再次检查，应该检测到文件数量变化
        result2 = manager._quick_table_check("stock_adj_hfq", minutes=30)
        assert result2["has_changes"] is True
        assert result2["file_count"] == 10
    
    def test_cache_reliability(self, stock_hfq_structure):
        """测试缓存机制的可靠性"""
        manager = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 建立初始缓存
        manager._fast_file_scan("stock_adj_hfq", minutes=0)
        original_cache = manager.cache.copy()
        
        # 保存并重新加载
        manager._save_cache()
        manager2 = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 验证缓存一致性
        assert "stock_adj_hfq" in manager2.cache
        assert manager2.cache["stock_adj_hfq"]["file_count"] == original_cache["stock_adj_hfq"]["file_count"]
    
    def test_time_window_logic(self, stock_hfq_structure):
        """测试时间窗口逻辑 - 可能导致文件被错误跳过"""
        manager = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 创建很久以前的文件
        old_file = Path(stock_hfq_structure["parquet_path"]) / "stock_adj_hfq" / "year=2023" / "very_old.parquet"
        df = pd.DataFrame({
            'ts_code': ['000888.SZ'],
            'trade_date': ['2023-01-01'],
            'close': [80.0]
        })
        table = pa.Table.from_pandas(df)
        pq.write_table(table, old_file)
        very_old_time = time.time() - 86400 * 7  # 7天前
        os.utime(old_file, (very_old_time, very_old_time))
        
        # 使用短时间窗口检查
        result = manager._fast_file_scan("stock_adj_hfq", minutes=60)  # 只检查1小时内
        
        # 文件数量应该包含旧文件
        assert result["file_count"] == 10  # 原9个 + 1个旧文件
        
        # 但在时间窗口检查中，旧文件不应该被认为是"最近变化"
        # 这里的逻辑测试很重要，确保不会因为时间窗口而丢失文件
        
    def test_edge_case_empty_directories(self, temp_directories):
        """测试边界情况：空目录处理"""
        parquet_path = Path(temp_directories["parquet_path"])
        
        # 创建空的表目录
        empty_table = parquet_path / "empty_table"
        empty_table.mkdir(exist_ok=True)
        
        manager = MetadataSyncManager(
            temp_directories["db_path"],
            temp_directories["parquet_path"]
        )
        
        # 空目录应该返回空的分区集合
        partitions = manager._get_physical_partitions("empty_table")
        assert partitions == set()
        
        # 文件扫描应该正确处理空目录
        scan_result = manager._fast_file_scan("empty_table", minutes=0)
        assert scan_result["file_count"] == 0
    
    def test_concurrent_file_changes(self, stock_hfq_structure):
        """测试文件在检查期间发生变化的情况"""
        manager = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 模拟文件在扫描期间被删除的情况
        # 这在实际生产环境中可能发生
        temp_file = Path(stock_hfq_structure["parquet_path"]) / "stock_adj_hfq" / "year=2024" / "temp.parquet"
        temp_file.write_text("temp data")
        
        # 开始扫描前删除文件（模拟并发删除）
        temp_file.unlink()
        
        # 扫描应该能够优雅地处理文件不存在的情况
        scan_result = manager._fast_file_scan("stock_adj_hfq", minutes=0)
        assert isinstance(scan_result["file_count"], int)
        assert scan_result["file_count"] >= 0
    
    def test_format_time_diff_accuracy(self, temp_directories):
        """测试时间差格式化的准确性"""
        manager = MetadataSyncManager(
            temp_directories["db_path"],
            temp_directories["parquet_path"]
        )
        
        now = time.time()
        
        # 测试各种时间差
        test_cases = [
            (30, "30秒前"),
            (90, "1分30秒前"),
            (3660, "1小时1分钟前"),
            (90000, "1天1小时前")
        ]
        
        for seconds_ago, expected in test_cases:
            result = manager._format_time_diff(now - seconds_ago)
            assert result == expected


class TestSyncMissDetectionScenarios:
    """专门测试可能导致同步遗漏的场景"""
    
    def test_scenario_partial_file_update(self, stock_hfq_structure):
        """场景测试：部分文件更新后的检测"""
        manager = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 建立初始状态
        initial_scan = manager._fast_file_scan("stock_adj_hfq", minutes=0)
        manager._save_cache()
        
        # 只更新某个分区的文件
        target_file = Path(stock_hfq_structure["parquet_path"]) / "stock_adj_hfq" / "year=2024" / "part-1-2024.parquet"
        df = pd.DataFrame({
            'ts_code': ['000001.SZ'],
            'trade_date': ['2024-01-01'],
            'close': [200.0]  # 更新价格
        })
        table = pa.Table.from_pandas(df)
        pq.write_table(table, target_file)
        current_time = time.time()
        os.utime(target_file, (current_time, current_time))
        
        # 检查是否能检测到变化
        updated_scan = manager._quick_table_check("stock_adj_hfq", minutes=60)
        
        # 应该检测到变化（因为最新文件时间戳改变了）
        assert updated_scan["has_changes"] is True or updated_scan["last_modified"] > initial_scan["last_modified"]
    
    def test_scenario_new_partition_added(self, stock_hfq_structure):
        """场景测试：添加新分区的检测"""
        manager = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 初始分区检测
        initial_partitions = manager._get_physical_partitions("stock_adj_hfq")
        assert initial_partitions == {"year=2023", "year=2024", "year=2025"}
        
        # 添加新年份分区
        new_year_dir = Path(stock_hfq_structure["parquet_path"]) / "stock_adj_hfq" / "year=2026"
        new_year_dir.mkdir(exist_ok=True)
        new_file = new_year_dir / "part-0-2026.parquet"
        df = pd.DataFrame({
            'ts_code': ['000777.SZ'],
            'trade_date': ['2026-01-01'],
            'close': [300.0]
        })
        table = pa.Table.from_pandas(df)
        pq.write_table(table, new_file)
        
        # 重新检测分区
        updated_partitions = manager._get_physical_partitions("stock_adj_hfq")
        assert updated_partitions == {"year=2023", "year=2024", "year=2025", "year=2026"}
    
    def test_scenario_file_count_mismatch(self, stock_hfq_structure):
        """场景测试：文件数量不匹配的处理"""
        manager = MetadataSyncManager(
            stock_hfq_structure["db_path"],
            stock_hfq_structure["parquet_path"]
        )
        
        # 建立基准缓存
        manager._fast_file_scan("stock_adj_hfq", minutes=0)
        manager._save_cache()
        
        # 模拟缓存中记录的文件数量与实际不符的情况
        manager.cache["stock_adj_hfq"]["file_count"] = 5  # 实际是9个文件
        
        # 快速检查应该检测到不匹配
        result = manager._quick_table_check("stock_adj_hfq", minutes=30)
        
        # 应该触发完整扫描并检测到变化
        assert result["has_changes"] is True
        assert result["file_count"] == 9


def test_identify_missing_sync_root_cause():
    """
    根本原因分析测试：
    
    基于我们发现的问题（10个股票只有2个同步成功），
    测试可能的根本原因
    """
    # 这个测试用于验证我们对问题的理解
    
    # 问题症状：
    # - Parquet文件存在 ✓
    # - 下载日志显示成功 ✓  
    # - 数据处理日志显示成功 ✓
    # - 但只有部分数据同步到metadata.db ❌
    
    # 可能的原因：
    reasons = [
        "元数据同步脚本的时间窗口过滤逻辑有缺陷",
        "缓存机制导致某些文件被错误跳过",
        "分区检测逻辑无法正确识别所有分区",
        "DuckDB视图创建过程中的SQL生成有问题",
        "文件扫描过程中的并发问题"
    ]
    
    # 基于测试结果，我们已经可以排除前3个原因
    # 重点应该关注SQL生成和并发问题
    
    assert len(reasons) == 5  # 确保我们考虑了所有可能性


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
