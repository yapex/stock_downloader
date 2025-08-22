"""批量处理功能测试

测试 SimpleDataProcessor 的批量处理能力。
"""

import pandas as pd
from unittest.mock import Mock, patch

from neo.data_processor.simple_data_processor import SimpleDataProcessor
from neo.data_processor.types import TaskResult
from neo.downloader.types import DownloadTaskConfig, TaskType, TaskPriority


class TestBatchProcessing:
    """批量处理功能测试类"""

    def setup_method(self):
        """测试前设置"""
        self.mock_db_operator = Mock()
        # 启用批量模式，设置较小的批量大小便于测试
        self.processor = SimpleDataProcessor(
            db_operator=self.mock_db_operator, enable_batch=True
        )
        # 手动设置较小的批量大小用于测试
        self.processor.batch_size = 3
        self.processor.flush_interval_seconds = 10

    def test_batch_buffer_initialization(self):
        """测试批量缓冲区初始化"""
        assert self.processor.enable_batch is True
        assert self.processor.batch_size == 3
        assert len(self.processor.batch_buffers) == 0
        assert self.processor.stats["buffered_items"] == 0
        assert self.processor.stats["batch_flushes"] == 0

    def test_add_to_buffer(self):
        """测试添加数据到缓冲区"""
        # 创建测试数据
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )

        # 添加数据到缓冲区
        success = self.processor._add_to_buffer(test_data, "stock_basic")

        assert success is True
        assert len(self.processor.batch_buffers["stock_basic"]) == 1
        assert self.processor.stats["buffered_items"] == 1

        # 验证数据内容
        buffered_data = self.processor.batch_buffers["stock_basic"][0]
        assert len(buffered_data) == 1
        assert buffered_data.iloc[0]["ts_code"] == "000001.SZ"

    def test_batch_processing_without_flush(self):
        """测试批量处理但不触发刷新"""
        # 创建股票基础信息数据
        stock_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )

        config = DownloadTaskConfig(
            task_type=TaskType.stock_basic,
            symbol="000001.SZ",
            priority=TaskPriority.HIGH,
        )

        result = TaskResult(config=config, success=True, data=stock_data, error=None)

        # 处理任务
        success = self.processor.process(result)

        assert success is True
        assert self.processor.stats["total_processed"] == 1
        assert self.processor.stats["successful_processed"] == 1
        assert self.processor.stats["buffered_items"] == 1
        assert self.processor.stats["batch_flushes"] == 0  # 未触发刷新
        assert self.processor.stats["total_rows_processed"] == 0  # 行数统计在刷新时更新

        # 验证数据库未被调用
        self.mock_db_operator.upsert.assert_not_called()

    def test_batch_processing_with_auto_flush(self):
        """测试批量处理自动刷新"""
        # 创建多个任务数据，触发自动刷新
        for i in range(3):  # 达到 batch_size = 3
            stock_data = pd.DataFrame(
                {
                    "ts_code": [f"00000{i + 1}.SZ"],
                    "symbol": [f"00000{i + 1}"],
                    "name": [f"股票{i + 1}"],
                }
            )

            config = DownloadTaskConfig(
                task_type=TaskType.stock_basic,
                symbol=f"00000{i + 1}.SZ",
                priority=TaskPriority.HIGH,
            )

            result = TaskResult(
                config=config, success=True, data=stock_data, error=None
            )

            self.processor.process(result)

        # 验证自动刷新被触发
        assert self.processor.stats["total_processed"] == 3
        assert self.processor.stats["successful_processed"] == 3
        assert self.processor.stats["batch_flushes"] == 1
        assert self.processor.stats["buffered_items"] == 0  # 刷新后缓冲区清空
        assert self.processor.stats["total_rows_processed"] == 3

        # 验证数据库被调用
        self.mock_db_operator.upsert.assert_called_once()
        call_args = self.mock_db_operator.upsert.call_args
        assert call_args[0][0] == "stock_basic"  # 表名
        assert len(call_args[0][1]) == 3  # 合并后的数据行数

    def test_manual_flush_all(self):
        """测试手动刷新所有缓冲区"""
        # 添加不同类型的数据到缓冲区
        stock_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )

        daily_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240101"],
                "open": [10.0],
                "high": [11.0],
                "low": [9.5],
                "close": [10.5],
            }
        )

        # 添加到缓冲区
        self.processor._add_to_buffer(stock_data, "stock_basic")
        self.processor._add_to_buffer(daily_data, "stock_daily")

        assert self.processor.stats["buffered_items"] == 2

        # 手动刷新所有缓冲区
        success = self.processor.flush_all()

        assert success is True
        assert self.processor.stats["batch_flushes"] == 2  # 两个任务类型各刷新一次
        assert self.processor.stats["buffered_items"] == 0
        assert self.processor.stats["total_rows_processed"] == 2

        # 验证数据库被调用两次
        assert self.mock_db_operator.upsert.call_count == 2

    def test_mixed_task_types_batching(self):
        """测试不同任务类型的混合批量处理"""
        # 创建不同类型的任务
        tasks = [
            (
                TaskType.stock_basic,
                "stock_basic",
                {"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行"},
            ),
            (
                TaskType.stock_basic,
                "stock_basic",
                {"ts_code": "000002.SZ", "symbol": "000002", "name": "万科A"},
            ),
            (
                TaskType.stock_daily,
                "daily",
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240101",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                },
            ),
            (
                TaskType.stock_daily,
                "daily",
                {
                    "ts_code": "000002.SZ",
                    "trade_date": "20240101",
                    "open": 20.0,
                    "high": 21.0,
                    "low": 19.5,
                    "close": 20.5,
                },
            ),
        ]

        for task_type, api_method, data_dict in tasks:
            data = pd.DataFrame([data_dict])
            config = DownloadTaskConfig(
                task_type=task_type,
                symbol=data_dict.get("ts_code", "test"),
                priority=TaskPriority.HIGH,
            )

            result = TaskResult(config=config, success=True, data=data, error=None)

            self.processor.process(result)

        # 验证缓冲区状态
        assert len(self.processor.batch_buffers["stock_basic"]) == 2
        assert len(self.processor.batch_buffers["stock_daily"]) == 2
        assert self.processor.stats["buffered_items"] == 4
        assert self.processor.stats["batch_flushes"] == 0  # 未达到批量大小

        # 手动刷新
        self.processor.flush_all()

        # 验证刷新结果
        assert self.processor.stats["batch_flushes"] == 2
        assert self.processor.stats["buffered_items"] == 0
        assert self.processor.stats["total_rows_processed"] == 4

    def test_get_stats_with_batch_info(self):
        """测试获取包含批量处理信息的统计数据"""
        # 添加一些数据到缓冲区
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )
        self.processor._add_to_buffer(test_data, "stock_basic")

        stats = self.processor.get_stats()

        # 验证批量处理相关统计信息
        assert "batch_enabled" in stats
        assert "batch_flushes" in stats
        assert "buffered_items" in stats
        assert "buffer_status" in stats
        assert "batch_size" in stats
        assert "flush_interval_seconds" in stats

        assert stats["batch_enabled"] is True
        assert stats["batch_size"] == 3
        assert stats["buffered_items"] == 1
        assert "stock_basic" in stats["buffer_status"]
        assert stats["buffer_status"]["stock_basic"]["tasks"] == 1
        assert stats["buffer_status"]["stock_basic"]["rows"] == 1

    @patch("time.time")
    def test_time_based_flush(self, mock_time):
        """测试基于时间的刷新机制"""
        # 设置初始时间
        mock_time.return_value = 1000.0

        # 重新初始化处理器以使用模拟时间
        self.processor.last_flush_time = 1000.0

        # 添加数据到缓冲区
        test_data = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"]}
        )
        self.processor._add_to_buffer(test_data, "stock_basic")

        # 时间推进，但未超过刷新间隔
        mock_time.return_value = 1005.0
        assert not self.processor._should_flush("stock_basic")

        # 时间推进，超过刷新间隔
        mock_time.return_value = 1015.0  # 超过 flush_interval_seconds = 10
        assert self.processor._should_flush("stock_basic")

    def test_large_data_auto_flush(self):
        """测试大量数据自动触发刷新（修复后的行数计算逻辑）"""
        # 创建一个包含大量数据的 DataFrame（超过 batch_size=3）
        large_data = pd.DataFrame(
            {
                "ts_code": [f"00000{i}.SZ" for i in range(1, 101)],  # 100 行数据
                "symbol": [f"00000{i}" for i in range(1, 101)],
                "name": [f"股票{i}" for i in range(1, 101)],
            }
        )

        config = DownloadTaskConfig(
            task_type=TaskType.stock_basic,
            symbol="",
            priority=TaskPriority.HIGH,
        )

        result = TaskResult(config=config, success=True, data=large_data, error=None)

        # 处理任务
        success = self.processor.process(result)

        # 验证自动刷新被触发（因为100行 > batch_size=3）
        assert success is True
        assert self.processor.stats["batch_flushes"] == 1
        assert self.processor.stats["buffered_items"] == 0  # 刷新后缓冲区清空
        assert self.processor.stats["total_rows_processed"] == 100

        # 验证数据库被调用
        self.mock_db_operator.upsert.assert_called_once()
        call_args = self.mock_db_operator.upsert.call_args
        assert call_args[0][0] == "stock_basic"  # 表名
        assert len(call_args[0][1]) == 100  # 数据行数

    def test_should_flush_by_row_count(self):
        """测试 _should_flush 方法按数据行数判断（而非 DataFrame 数量）"""
        # 添加一个包含大量数据的 DataFrame
        large_data = pd.DataFrame(
            {
                "ts_code": [f"00000{i}.SZ" for i in range(1, 51)],  # 50 行数据
                "symbol": [f"00000{i}" for i in range(1, 51)],
                "name": [f"股票{i}" for i in range(1, 51)],
            }
        )

        # 添加到缓冲区
        self.processor._add_to_buffer(large_data, "stock_basic")

        # 验证缓冲区状态
        assert len(self.processor.batch_buffers["stock_basic"]) == 1  # 只有1个DataFrame
        total_rows = sum(len(df) for df in self.processor.batch_buffers["stock_basic"])
        assert total_rows == 50  # 但有50行数据

        # 验证 _should_flush 按行数判断（50 > batch_size=3）
        assert self.processor._should_flush("stock_basic") is True

        # 测试边界情况：正好等于 batch_size
        small_data = pd.DataFrame(
            {
                "ts_code": [f"test{i}.SZ" for i in range(1, 4)],  # 3 行数据
                "symbol": [f"test{i}" for i in range(1, 4)],
                "name": [f"测试{i}" for i in range(1, 4)],
            }
        )

        # 清空缓冲区并添加小数据
        self.processor.batch_buffers["test_type"] = []
        self.processor._add_to_buffer(small_data, "test_type")

        # 验证正好等于 batch_size 时也会触发刷新
        assert self.processor._should_flush("test_type") is True

        # 测试小于 batch_size 的情况
        tiny_data = pd.DataFrame(
            {"ts_code": ["tiny.SZ"], "symbol": ["tiny"], "name": ["微小"]}
        )

        self.processor.batch_buffers["tiny_type"] = []
        self.processor._add_to_buffer(tiny_data, "tiny_type")

        # 验证小于 batch_size 时不会触发刷新
        assert self.processor._should_flush("tiny_type") is False

    def test_flush_buffer_by_row_count(self):
        """测试 _flush_buffer 方法按数据行数判断是否需要刷新"""
        # 添加小量数据（不足 batch_size）
        small_data = pd.DataFrame(
            {"ts_code": ["small.SZ"], "symbol": ["small"], "name": ["小数据"]}
        )

        self.processor._add_to_buffer(small_data, "stock_basic")

        # 非强制刷新，数据量不足，应该不会刷新
        result = self.processor._flush_buffer("stock_basic", force=False)
        assert result is True  # 返回True但实际未刷新
        assert self.processor.stats["batch_flushes"] == 0
        self.mock_db_operator.upsert.assert_not_called()

        # 强制刷新，应该会刷新
        result = self.processor._flush_buffer("stock_basic", force=True)
        assert result is True
        assert self.processor.stats["batch_flushes"] == 1
        self.mock_db_operator.upsert.assert_called_once()

        # 重置mock
        self.mock_db_operator.reset_mock()
        self.processor.stats["batch_flushes"] = 0

        # 添加大量数据（超过 batch_size）
        large_data = pd.DataFrame(
            {
                "ts_code": [
                    f"large{i}.SZ" for i in range(1, 11)
                ],  # 10 行数据 > batch_size=3
                "symbol": [f"large{i}" for i in range(1, 11)],
                "name": [f"大数据{i}" for i in range(1, 11)],
            }
        )

        self.processor._add_to_buffer(large_data, "stock_basic")

        # 非强制刷新，数据量足够，应该会刷新
        result = self.processor._flush_buffer("stock_basic", force=False)
        assert result is True
        assert self.processor.stats["batch_flushes"] == 1
        self.mock_db_operator.upsert.assert_called_once()

        # 验证数据行数
        call_args = self.mock_db_operator.upsert.call_args
        assert len(call_args[0][1]) == 10  # 数据行数

    def test_multiple_dataframes_row_count_calculation(self):
        """测试多个 DataFrame 的行数计算逻辑"""
        # 添加多个小的 DataFrame，总行数超过 batch_size
        data1 = pd.DataFrame(
            {"ts_code": ["001.SZ"], "symbol": ["001"], "name": ["股票1"]}
        )
        data2 = pd.DataFrame(
            {"ts_code": ["002.SZ"], "symbol": ["002"], "name": ["股票2"]}
        )
        data3 = pd.DataFrame(
            {"ts_code": ["003.SZ"], "symbol": ["003"], "name": ["股票3"]}
        )
        data4 = pd.DataFrame(
            {"ts_code": ["004.SZ"], "symbol": ["004"], "name": ["股票4"]}
        )

        # 逐个添加到缓冲区
        self.processor._add_to_buffer(data1, "stock_basic")
        self.processor._add_to_buffer(data2, "stock_basic")
        self.processor._add_to_buffer(data3, "stock_basic")

        # 此时有3个DataFrame，每个1行，总共3行，正好等于batch_size=3
        assert len(self.processor.batch_buffers["stock_basic"]) == 3  # 3个DataFrame
        total_rows = sum(len(df) for df in self.processor.batch_buffers["stock_basic"])
        assert total_rows == 3  # 总共3行
        assert self.processor._should_flush("stock_basic") is True  # 应该刷新

        # 添加第4个DataFrame
        self.processor._add_to_buffer(data4, "stock_basic")

        # 此时有4个DataFrame，总共4行，超过batch_size=3
        assert len(self.processor.batch_buffers["stock_basic"]) == 4  # 4个DataFrame
        total_rows = sum(len(df) for df in self.processor.batch_buffers["stock_basic"])
        assert total_rows == 4  # 总共4行
        assert self.processor._should_flush("stock_basic") is True  # 应该刷新
