"""测试数据处理器统计功能"""

import pandas as pd
import time
from unittest.mock import Mock, patch

from neo.data_processor.simple_data_processor import SimpleDataProcessor


class TestDataProcessorStats:
    """测试数据处理器统计功能"""

    def setup_method(self):
        """测试前设置"""
        self.mock_db_operator = Mock()
        # 在测试中禁用批量模式，确保统计信息立即更新
        self.processor = SimpleDataProcessor(
            db_operator=self.mock_db_operator, enable_batch=False
        )

    def test_initial_stats(self):
        """测试初始统计状态"""
        stats = self.processor.get_stats()

        assert stats["total_processed"] == 0
        assert stats["successful_processed"] == 0
        assert stats["failed_processed"] == 0
        assert stats["total_rows_processed"] == 0
        assert stats["success_rate"] == 0
        assert stats["processing_rate"] == 0
        assert stats["task_type_stats"] == {}

    def test_successful_processing_stats(self):
        """测试成功处理的统计更新"""
        # 准备测试数据
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )

        # 模拟数据库操作成功
        self.mock_db_operator.upsert.return_value = None

        # 处理任务
        result = self.processor.process("stock_basic", test_data)

        # 验证处理结果
        assert result is True

        # 验证统计信息
        stats = self.processor.get_stats()
        assert stats["total_processed"] == 1
        assert stats["successful_processed"] == 1
        assert stats["failed_processed"] == 0
        assert stats["total_rows_processed"] == 1
        assert stats["success_rate"] == 100.0

        # 验证任务类型统计
        assert "stock_basic" in stats["task_type_stats"]
        task_stats = stats["task_type_stats"]["stock_basic"]
        assert task_stats["count"] == 1
        assert task_stats["success"] == 1
        assert task_stats["rows"] == 1

    def test_failed_processing_stats(self):
        """测试失败处理的统计更新"""
        # 模拟数据库操作失败
        self.mock_db_operator.upsert.side_effect = Exception("数据库操作失败")

        # 准备测试数据
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )

        # 处理任务
        result = self.processor.process("stock_basic", test_data)

        # 验证处理结果
        assert result is False

        # 验证统计信息
        stats = self.processor.get_stats()
        assert stats["total_processed"] == 1
        assert stats["successful_processed"] == 0
        assert stats["failed_processed"] == 1
        assert stats["total_rows_processed"] == 0
        assert stats["success_rate"] == 0.0

        # 验证任务类型统计
        assert "stock_basic" in stats["task_type_stats"]
        task_stats = stats["task_type_stats"]["stock_basic"]
        assert task_stats["count"] == 1
        assert task_stats["success"] == 0
        assert task_stats["rows"] == 0

    def test_multiple_task_types_stats(self):
        """测试多种任务类型的统计"""
        # 准备不同类型的测试数据
        stock_basic_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )

        daily_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240101"],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
            }
        )

        # 模拟数据库操作成功
        self.mock_db_operator.upsert.return_value = None

        # 处理股票基础信息任务
        self.processor.process("stock_basic", stock_basic_data)

        # 处理日线数据任务
        self.processor.process("stock_daily", daily_data)

        # 验证统计信息
        stats = self.processor.get_stats()
        assert stats["total_processed"] == 2
        assert stats["successful_processed"] == 2
        assert stats["failed_processed"] == 0
        assert stats["total_rows_processed"] == 2
        assert stats["success_rate"] == 100.0

        # 验证任务类型统计
        assert len(stats["task_type_stats"]) == 2
        assert "stock_basic" in stats["task_type_stats"]
        assert "stock_daily" in stats["task_type_stats"]

        basic_stats = stats["task_type_stats"]["stock_basic"]
        assert basic_stats["count"] == 1
        assert basic_stats["success"] == 1
        assert basic_stats["rows"] == 1

        daily_stats = stats["task_type_stats"]["stock_daily"]
        assert daily_stats["count"] == 1
        assert daily_stats["success"] == 1
        assert daily_stats["rows"] == 1

    @patch("builtins.print")
    def test_stats_output_timing(self, mock_print):
        """测试统计信息输出时机"""
        # 设置较短的输出间隔用于测试
        self.processor.stats_output_interval = 0.1

        # 准备测试数据
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )

        # 模拟数据库操作成功
        self.mock_db_operator.upsert.return_value = None

        # 第一次处理，不应该输出统计信息
        self.processor.process("stock_basic", test_data)

        # 检查是否没有输出统计信息（只有处理信息）
        stats_calls = [
            call
            for call in mock_print.call_args_list
            if "📈 数据处理统计信息" in str(call)
        ]
        assert len(stats_calls) == 0

        # 等待超过输出间隔
        time.sleep(0.2)

        # 第二次处理，应该输出统计信息
        self.processor.process("stock_basic", test_data)

        # 检查是否输出了统计信息
        stats_calls = [
            call
            for call in mock_print.call_args_list
            if "📈 数据处理统计信息" in str(call)
        ]
        assert len(stats_calls) > 0

    def test_processing_rate_calculation(self):
        """测试处理速率计算"""
        # 准备测试数据
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )

        # 模拟数据库操作成功
        self.mock_db_operator.upsert.return_value = None

        # 处理多个任务
        for _ in range(3):
            self.processor.process("stock_basic", test_data)
            time.sleep(0.1)  # 模拟处理时间

        # 验证处理速率
        stats = self.processor.get_stats()
        assert stats["processing_rate"] > 0
        assert stats["total_processed"] == 3
