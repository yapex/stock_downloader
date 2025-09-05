#!/usr/bin/env python3
"""专门测试文件扫描功能的简化测试"""

import pytest
import tempfile
import shutil
import time
import os
from pathlib import Path
import sys

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from neo.helpers.metadata_sync import MetadataSyncManager


class TestFileScan:
    
    def setup_method(self):
        """每个测试方法前的准备"""
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp(prefix="test_scan_")
        self.parquet_path = Path(self.temp_dir) / "parquet"
        self.parquet_path.mkdir()
        
        # 创建测试文件
        self._create_test_files()
        
        # 创建管理器
        metadata_db = Path(self.temp_dir) / "test_metadata.db"
        self.manager = MetadataSyncManager(str(metadata_db), str(self.parquet_path))
    
    def teardown_method(self):
        """每个测试方法后的清理"""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def _create_test_files(self):
        """创建测试文件"""
        # 创建 stock_adj_hfq 表目录
        table_dir = self.parquet_path / "stock_adj_hfq"
        table_dir.mkdir()
        
        # 创建年份分区和文件
        years = ["2020", "2021", "2022"]
        self.files_created = []
        
        for year in years:
            year_dir = table_dir / f"year={year}"
            year_dir.mkdir()
            
            # 每个年份创建3个parquet文件
            for i in range(3):
                parquet_file = year_dir / f"data_{i:02d}.parquet"
                parquet_file.write_text(f"mock parquet data for {year}, file {i}")
                
                # 设置时间戳
                timestamp = time.time() - (len(self.files_created) * 60)
                os.utime(parquet_file, (timestamp, timestamp))
                
                self.files_created.append(parquet_file)
    
    def test_glob_scan(self):
        """测试 glob 扫描"""
        table_dir = self.parquet_path / "stock_adj_hfq"
        files = list(table_dir.glob("**/*.parquet"))
        assert len(files) == 9, f"glob应该找到9个文件，实际找到{len(files)}个"
    
    def test_rglob_scan(self):
        """测试 rglob 扫描"""
        table_dir = self.parquet_path / "stock_adj_hfq"
        files = list(table_dir.rglob("*.parquet"))
        assert len(files) == 9, f"rglob应该找到9个文件，实际找到{len(files)}个"
    
    def test_fast_file_scan(self):
        """测试 _fast_file_scan 方法"""
        result = self.manager._fast_file_scan("stock_adj_hfq", 0)
        assert result["file_count"] == 9, f"_fast_file_scan应该找到9个文件，实际找到{result['file_count']}个"
        assert result["method"] == "fast_scan"
        assert result["last_modified"] is not None
    
    def test_quick_table_check(self):
        """测试 _quick_table_check 方法"""
        result = self.manager._quick_table_check("stock_adj_hfq", 0)
        assert result["file_count"] == 9, f"_quick_table_check应该找到9个文件，实际找到{result['file_count']}个"
        assert result["last_modified"] is not None
    
    def test_get_file_info(self):
        """测试 _get_file_info 方法"""
        result = self.manager._get_file_info("stock_adj_hfq", 0)
        assert result["file_count"] == 9, f"_get_file_info应该找到9个文件，实际找到{result['file_count']}个"
        assert result["last_modified"] is not None
    
    def test_file_creation_verification(self):
        """验证文件确实被创建了"""
        # 直接检查创建的文件列表
        assert len(self.files_created) == 9, f"应该创建9个文件，实际创建{len(self.files_created)}个"
        
        # 检查文件存在性
        existing_files = []
        for f in self.files_created:
            if f.exists():
                existing_files.append(f)
        
        assert len(existing_files) == 9, f"9个文件中只有{len(existing_files)}个存在"
        
        # 打印详细信息用于调试
        print(f"\n创建的文件: {len(self.files_created)}")
        for f in self.files_created:
            print(f"  - {f} (存在: {f.exists()})")
    
    def test_directory_structure(self):
        """测试目录结构"""
        table_dir = self.parquet_path / "stock_adj_hfq"
        assert table_dir.exists(), "表目录应该存在"
        
        years = ["2020", "2021", "2022"]
        for year in years:
            year_dir = table_dir / f"year={year}"
            assert year_dir.exists(), f"年份目录 {year} 应该存在"
            
            files_in_year = list(year_dir.glob("*.parquet"))
            assert len(files_in_year) == 3, f"年份 {year} 应该有3个文件，实际有{len(files_in_year)}个"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
