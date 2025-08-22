"""测试数据处理器统计功能"""

import pandas as pd
import time
from unittest.mock import Mock, patch

from neo.data_processor.simple_data_processor import SimpleDataProcessor
from neo.data_processor.types import TaskResult
from neo.downloader.types import DownloadTaskConfig, TaskType, TaskPriority


class TestDataProcessorStats:
    """测试数据处理器统计功能"""

    def setup_method(self):
        """测试前设置"""
        self.mock_db_operator = Mock()
        self.processor = SimpleDataProcessor(db_operator=self.mock_db_operator)

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

        config = DownloadTaskConfig(
            task_type=TaskType.STOCK_BASIC,
            symbol="000001.SZ",
            priority=TaskPriority.HIGH,
        )

        task_result = TaskResult(
            config=config, success=True, data=test_data, error=None
        )

        # 模拟数据库操作成功
        self.mock_db_operator.upsert.return_value = None

        # 处理任务
        result = self.processor.process(task_result)

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
        assert "STOCK_BASIC" in stats["task_type_stats"]
        task_stats = stats["task_type_stats"]["STOCK_BASIC"]
        assert task_stats["count"] == 1
        assert task_stats["success"] == 1
        assert task_stats["rows"] == 1

    def test_failed_processing_stats(self):
        """测试失败处理的统计更新"""
        config = DownloadTaskConfig(
            task_type=TaskType.STOCK_BASIC,
            symbol="000001.SZ",
            priority=TaskPriority.HIGH,
        )

        # 创建失败的任务结果
        task_result = TaskResult(
            config=config, success=False, data=None, error="下载失败"
        )

        # 处理任务
        result = self.processor.process(task_result)

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
        assert "STOCK_BASIC" in stats["task_type_stats"]
        task_stats = stats["task_type_stats"]["STOCK_BASIC"]
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
        stock_basic_config = DownloadTaskConfig(
            task_type=TaskType.STOCK_BASIC,
            symbol="000001.SZ",
            priority=TaskPriority.HIGH,
        )

        stock_basic_result = TaskResult(
            config=stock_basic_config, success=True, data=stock_basic_data, error=None
        )

        self.processor.process(stock_basic_result)

        # 处理日线数据任务
        daily_config = DownloadTaskConfig(
            task_type=TaskType.STOCK_DAILY,
            symbol="000001.SZ",
            priority=TaskPriority.HIGH,
        )

        daily_result = TaskResult(
            config=daily_config, success=True, data=daily_data, error=None
        )

        self.processor.process(daily_result)

        # 验证统计信息
        stats = self.processor.get_stats()
        assert stats["total_processed"] == 2
        assert stats["successful_processed"] == 2
        assert stats["failed_processed"] == 0
        assert stats["total_rows_processed"] == 2
        assert stats["success_rate"] == 100.0

        # 验证任务类型统计
        assert len(stats["task_type_stats"]) == 2
        assert "STOCK_BASIC" in stats["task_type_stats"]
        assert "STOCK_DAILY" in stats["task_type_stats"]

        basic_stats = stats["task_type_stats"]["STOCK_BASIC"]
        assert basic_stats["count"] == 1
        assert basic_stats["success"] == 1
        assert basic_stats["rows"] == 1

        daily_stats = stats["task_type_stats"]["STOCK_DAILY"]
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

        config = DownloadTaskConfig(
            task_type=TaskType.STOCK_BASIC,
            symbol="000001.SZ",
            priority=TaskPriority.HIGH,
        )

        task_result = TaskResult(
            config=config, success=True, data=test_data, error=None
        )

        # 模拟数据库操作成功
        self.mock_db_operator.upsert.return_value = None

        # 第一次处理，不应该输出统计信息
        self.processor.process(task_result)

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
        self.processor.process(task_result)

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

        config = DownloadTaskConfig(
            task_type=TaskType.STOCK_BASIC,
            symbol="000001.SZ",
            priority=TaskPriority.HIGH,
        )

        task_result = TaskResult(
            config=config, success=True, data=test_data, error=None
        )

        # 模拟数据库操作成功
        self.mock_db_operator.upsert.return_value = None

        # 处理多个任务
        for _ in range(3):
            self.processor.process(task_result)
            time.sleep(0.1)  # 模拟处理时间

        # 验证处理速率
        stats = self.processor.get_stats()
        assert stats["processing_rate"] > 0
        assert stats["total_processed"] == 3
