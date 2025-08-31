"""任务过滤器测试模块"""

import tempfile
import os
from pathlib import Path
from unittest.mock import patch
import pytest

from neo.helpers.task_filter import TaskFilter


class TestTaskFilter:
    """TaskFilter 测试类"""
    
    def setup_method(self):
        """测试前准备"""
        # 创建临时配置文件
        self.temp_config = tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False)
        self.temp_config.write("""
[task_filter]
exchange_whitelist = ["SH", "SZ"]
        """)
        self.temp_config.close()
        
        self.task_filter = TaskFilter(Path(self.temp_config.name))
    
    def teardown_method(self):
        """测试后清理"""
        os.unlink(self.temp_config.name)
    
    def test_should_process_symbol_valid_sh(self):
        """测试处理上海交易所股票代码"""
        assert self.task_filter.should_process_symbol("600000.SH") is True
    
    def test_should_process_symbol_valid_sz(self):
        """测试处理深圳交易所股票代码"""
        assert self.task_filter.should_process_symbol("000001.SZ") is True
    
    def test_should_process_symbol_invalid_exchange(self):
        """测试不处理其他交易所股票代码"""
        assert self.task_filter.should_process_symbol("000001.BJ") is False
        assert self.task_filter.should_process_symbol("000001.HK") is False
    
    def test_should_process_symbol_invalid_format(self):
        """测试无效格式的股票代码"""
        assert self.task_filter.should_process_symbol("") is False
        assert self.task_filter.should_process_symbol("600000") is False
        assert self.task_filter.should_process_symbol(None) is False
    
    def test_filter_symbols(self):
        """测试过滤股票代码列表"""
        symbols = [
            "600000.SH",
            "000001.SZ",
            "000001.BJ",
            "600001.SH",
            "000002.HK"
        ]
        
        filtered = self.task_filter.filter_symbols(symbols)
        expected = ["600000.SH", "000001.SZ", "600001.SH"]
        
        assert filtered == expected
    
    def test_filter_symbols_empty_list(self):
        """测试过滤空列表"""
        assert self.task_filter.filter_symbols([]) == []
    
    def test_filter_data(self):
        """测试过滤数据列表"""
        data = [
            {"ts_code": "600000.SH", "name": "浦发银行"},
            {"ts_code": "000001.SZ", "name": "平安银行"},
            {"ts_code": "000001.BJ", "name": "北交所股票"},
            {"ts_code": "000002.HK", "name": "港股"}
        ]
        
        filtered = self.task_filter.filter_data(data)
        expected = [
            {"ts_code": "600000.SH", "name": "浦发银行"},
            {"ts_code": "000001.SZ", "name": "平安银行"}
        ]
        
        assert filtered == expected
    
    def test_filter_data_custom_column(self):
        """测试使用自定义列名过滤数据"""
        data = [
            {"symbol": "600000.SH", "name": "浦发银行"},
            {"symbol": "000001.BJ", "name": "北交所股票"}
        ]
        
        filtered = self.task_filter.filter_data(data, symbol_column="symbol")
        expected = [{"symbol": "600000.SH", "name": "浦发银行"}]
        
        assert filtered == expected
    
    def test_filter_data_missing_column(self):
        """测试数据中缺少股票代码列"""
        data = [
            {"name": "浦发银行"},
            {"ts_code": "600000.SH", "name": "招商银行"}
        ]
        
        filtered = self.task_filter.filter_data(data)
        expected = [{"ts_code": "600000.SH", "name": "招商银行"}]
        
        assert filtered == expected
    
    def test_get_exchange_whitelist(self):
        """测试获取交易所白名单"""
        whitelist = self.task_filter.get_exchange_whitelist()
        assert whitelist == ["SH", "SZ"]
        
        # 确保返回的是副本
        whitelist.append("BJ")
        assert self.task_filter.get_exchange_whitelist() == ["SH", "SZ"]
    
    def test_default_config(self):
        """测试默认配置"""
        # 创建没有task_filter配置的文件
        temp_config = tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False)
        temp_config.write("[other_section]\nkey = 'value'")
        temp_config.close()
        
        try:
            task_filter = TaskFilter(Path(temp_config.name))
            assert task_filter.get_exchange_whitelist() == ["SH", "SZ"]
        finally:
            os.unlink(temp_config.name)
    
    def test_custom_whitelist(self):
        """测试自定义白名单配置"""
        # 创建自定义配置文件
        temp_config = tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False)
        temp_config.write("""
[task_filter]
exchange_whitelist = ["SH", "BJ"]
        """)
        temp_config.close()
        
        try:
            task_filter = TaskFilter(Path(temp_config.name))
            assert task_filter.should_process_symbol("600000.SH") is True
            assert task_filter.should_process_symbol("000001.BJ") is True
            assert task_filter.should_process_symbol("000001.SZ") is False
        finally:
            os.unlink(temp_config.name)