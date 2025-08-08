"""
测试DeadLetterLogger的log_missing_symbols方法

包含正常流程、边界情况和异常场景的完整测试。
"""

import json
import tempfile
import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock

from src.downloader.retry_policy import DeadLetterLogger, DeadLetterRecord
from src.downloader.models import TaskType, Priority


class TestDeadLetterLoggerMissingSymbols:
    """测试DeadLetterLogger的log_missing_symbols方法"""
    
    def setup_method(self):
        """每个测试前的设置"""
        # 使用临时文件避免污染实际日志
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl')
        self.temp_file.close()
        self.logger = DeadLetterLogger(log_path=self.temp_file.name)
    
    def teardown_method(self):
        """每个测试后的清理"""
        Path(self.temp_file.name).unlink(missing_ok=True)
    
    def read_jsonl_file(self) -> list:
        """读取JSONL文件内容"""
        records = []
        with open(self.temp_file.name, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records
    
    def test_log_missing_symbols_normal(self):
        """测试正常批量写入缺失股票"""
        btype = "daily"
        symbols = ["000001.SZ", "000002.SZ", "600000.SH"]
        
        self.logger.log_missing_symbols(btype, symbols)
        
        # 验证文件内容
        records = self.read_jsonl_file()
        assert len(records) == 3
        
        for i, record in enumerate(records):
            assert record["symbol"] == symbols[i]
            assert record["task_type"] == "daily"
            assert record["error_type"] == "MISSING_DATA"
            assert record["error_message"] == f"Symbol {symbols[i]} not found in data source"
            assert record["params"] == {}
            assert record["priority"] == Priority.NORMAL.value
            assert record["retry_count"] == 0
            assert record["max_retries"] == 0
            assert "task_id" in record
            assert "failed_at" in record
            assert "original_created_at" in record
    
    def test_log_missing_symbols_all_task_types(self):
        """测试所有任务类型的缺失股票写入"""
        test_cases = [
            ("daily", ["000001.SZ"]),
            ("daily_basic", ["000002.SZ"]),
            ("financials", ["600000.SH"]),
            ("stock_list", ["399001.SZ"])
        ]
        
        for btype, symbols in test_cases:
            # 清空文件
            Path(self.temp_file.name).write_text("")
            
            self.logger.log_missing_symbols(btype, symbols)
            
            records = self.read_jsonl_file()
            assert len(records) == 1
            assert records[0]["task_type"] == btype
            assert records[0]["symbol"] == symbols[0]
    
    def test_log_missing_symbols_empty_list(self):
        """测试空股票列表"""
        btype = "daily"
        symbols = []
        
        self.logger.log_missing_symbols(btype, symbols)
        
        # 验证没有写入任何记录
        records = self.read_jsonl_file()
        assert len(records) == 0
    
    def test_log_missing_symbols_single_symbol(self):
        """测试单个股票代码"""
        btype = "daily"
        symbols = ["000001.SZ"]
        
        self.logger.log_missing_symbols(btype, symbols)
        
        records = self.read_jsonl_file()
        assert len(records) == 1
        assert records[0]["symbol"] == "000001.SZ"
    
    def test_log_missing_symbols_large_batch(self):
        """测试大批量股票代码"""
        btype = "daily"
        symbols = [f"{i:06d}.SZ" for i in range(1, 101)]  # 100个股票代码
        
        self.logger.log_missing_symbols(btype, symbols)
        
        records = self.read_jsonl_file()
        assert len(records) == 100
        
        # 验证所有记录
        for i, record in enumerate(records):
            expected_symbol = f"{i+1:06d}.SZ"
            assert record["symbol"] == expected_symbol
            assert record["error_type"] == "MISSING_DATA"
    
    def test_log_missing_symbols_invalid_task_type(self):
        """测试无效任务类型"""
        btype = "invalid_task_type"
        symbols = ["000001.SZ"]
        
        with pytest.raises(ValueError, match="is not a valid TaskType"):
            self.logger.log_missing_symbols(btype, symbols)
        
        # 验证没有写入任何记录
        records = self.read_jsonl_file()
        assert len(records) == 0
    
    def test_log_missing_symbols_special_characters(self):
        """测试包含特殊字符的股票代码"""
        btype = "daily"
        symbols = ["000001.SZ", "ST中国", "退市大控", "*ST海润"]
        
        self.logger.log_missing_symbols(btype, symbols)
        
        records = self.read_jsonl_file()
        assert len(records) == 4
        
        for i, record in enumerate(records):
            assert record["symbol"] == symbols[i]
            # 验证JSON序列化正确处理中文字符
            assert record["error_message"] == f"Symbol {symbols[i]} not found in data source"
    
    def test_log_missing_symbols_duplicate_symbols(self):
        """测试重复的股票代码"""
        btype = "daily"
        symbols = ["000001.SZ", "000002.SZ", "000001.SZ", "000002.SZ"]
        
        self.logger.log_missing_symbols(btype, symbols)
        
        records = self.read_jsonl_file()
        assert len(records) == 4  # 应该写入所有记录，包括重复的
        
        # 验证每个记录都有唯一的task_id
        task_ids = [record["task_id"] for record in records]
        assert len(set(task_ids)) == 4  # 所有task_id都应该不同
    
    def test_log_missing_symbols_append_mode(self):
        """测试追加写入模式"""
        btype = "daily"
        
        # 第一次写入
        symbols1 = ["000001.SZ", "000002.SZ"]
        self.logger.log_missing_symbols(btype, symbols1)
        
        # 第二次写入
        symbols2 = ["600000.SH", "600001.SH"]
        self.logger.log_missing_symbols(btype, symbols2)
        
        records = self.read_jsonl_file()
        assert len(records) == 4
        
        written_symbols = [record["symbol"] for record in records]
        assert written_symbols == symbols1 + symbols2
    
    @patch('src.downloader.retry_policy.open')
    def test_log_missing_symbols_file_write_error(self, mock_open):
        """测试文件写入异常"""
        mock_open.side_effect = IOError("Permission denied")
        
        btype = "daily"
        symbols = ["000001.SZ"]
        
        with pytest.raises(IOError, match="Permission denied"):
            self.logger.log_missing_symbols(btype, symbols)
    
    def test_log_missing_symbols_json_serialization(self):
        """测试JSON序列化的正确性"""
        btype = "daily"
        symbols = ["000001.SZ"]
        
        self.logger.log_missing_symbols(btype, symbols)
        
        # 直接读取文件内容验证JSON格式
        with open(self.temp_file.name, 'r', encoding='utf-8') as f:
            line = f.readline().strip()
        
        # 验证是有效的JSON
        data = json.loads(line)
        
        # 验证所有必需字段
        required_fields = [
            "task_id", "symbol", "task_type", "params", "priority",
            "retry_count", "max_retries", "error_message", "error_type",
            "failed_at", "original_created_at"
        ]
        
        for field in required_fields:
            assert field in data, f"Missing required field: {field}"
        
        # 验证字段值类型和内容
        assert isinstance(data["task_id"], str)
        assert isinstance(data["symbol"], str)
        assert isinstance(data["task_type"], str)
        assert isinstance(data["params"], dict)
        assert isinstance(data["priority"], int)
        assert isinstance(data["retry_count"], int)
        assert isinstance(data["max_retries"], int)
        assert isinstance(data["error_message"], str)
        assert isinstance(data["error_type"], str)
        assert isinstance(data["failed_at"], str)
        assert isinstance(data["original_created_at"], str)
        
        # 验证时间戳格式
        datetime.fromisoformat(data["failed_at"])
        datetime.fromisoformat(data["original_created_at"])
    
    def test_log_missing_symbols_consistency_with_existing_format(self):
        """测试与现有DeadLetterRecord格式的一致性"""
        # 先使用现有方法写入一个记录
        from src.downloader.models import DownloadTask
        
        existing_task = DownloadTask(
            symbol="test_symbol",
            task_type=TaskType.DAILY,
            params={"test": "value"},
            priority=Priority.HIGH
        )
        
        self.logger.write_dead_letter(existing_task, Exception("Test error"))
        
        # 然后使用新方法写入记录
        self.logger.log_missing_symbols("daily", ["000001.SZ"])
        
        records = self.read_jsonl_file()
        assert len(records) == 2
        
        existing_record = records[0]
        new_record = records[1]
        
        # 验证两个记录具有相同的字段结构
        assert set(existing_record.keys()) == set(new_record.keys())
        
        # 验证新记录的特殊字段值
        assert new_record["error_type"] == "MISSING_DATA"
        assert new_record["retry_count"] == 0
        assert new_record["max_retries"] == 0
        assert new_record["params"] == {}
        assert new_record["priority"] == Priority.NORMAL.value
    
    def test_log_missing_symbols_logging(self):
        """测试日志输出"""
        btype = "daily"
        symbols = ["000001.SZ", "000002.SZ"]
        
        with patch.object(self.logger, 'logger') as mock_logger:
            self.logger.log_missing_symbols(btype, symbols)
            
            # 验证info日志被调用
            mock_logger.info.assert_called_once_with(
                f"Logged {len(symbols)} missing symbols for task type {btype}"
            )
    
    def test_log_missing_symbols_error_logging(self):
        """测试错误日志输出"""
        btype = "invalid_type"
        symbols = ["000001.SZ"]
        
        with patch.object(self.logger, 'logger') as mock_logger:
            with pytest.raises(ValueError):
                self.logger.log_missing_symbols(btype, symbols)
            
            # 验证错误日志被调用
            mock_logger.error.assert_called_once()
            assert "Invalid task type 'invalid_type'" in str(mock_logger.error.call_args)
    
    def test_log_missing_symbols_thread_safety(self):
        """测试多线程安全性（基础测试）"""
        import threading
        import time
        
        btype = "daily"
        symbols_list = [
            ["000001.SZ", "000002.SZ"],
            ["600000.SH", "600001.SH"],
            ["300001.SZ", "300002.SZ"]
        ]
        
        def write_symbols(symbols):
            self.logger.log_missing_symbols(btype, symbols)
        
        # 创建多个线程同时写入
        threads = []
        for symbols in symbols_list:
            thread = threading.Thread(target=write_symbols, args=(symbols,))
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证所有记录都被写入
        records = self.read_jsonl_file()
        assert len(records) == 6  # 总共6个股票代码
        
        # 验证每个记录的完整性
        for record in records:
            assert record["task_type"] == btype
            assert record["error_type"] == "MISSING_DATA"
            assert "task_id" in record
            assert "symbol" in record
