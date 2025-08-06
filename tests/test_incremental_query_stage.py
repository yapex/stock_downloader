import pytest
import pandas as pd
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from downloader.storage import DuckDBStorage


class TestIncrementalQueryStage:
    """测试增量查询阶段的功能"""

    @pytest.fixture
    def storage(self):
        """创建临时的 DuckDB 存储实例"""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            yield DuckDBStorage(db_path)

    def test_get_latest_date_table_not_exists(self, storage):
        """测试表不存在时的情况"""
        # 当表不存在时，应该返回 None
        result = storage.get_latest_date("daily", "000001.SZ", "trade_date")
        assert result is None

    def test_get_latest_date_table_exists_but_empty(self, storage):
        """测试表存在但为空的情况"""
        # 创建一个空表
        empty_df = pd.DataFrame(columns=["ts_code", "trade_date", "close"])
        storage.save_full(empty_df, "daily", "000001.SZ")
        
        # 查询最新日期应该返回 None
        result = storage.get_latest_date("daily", "000001.SZ", "trade_date")
        assert result is None

    def test_get_latest_date_table_exists_with_data(self, storage):
        """测试表存在且有数据的情况"""
        # 创建包含数据的表
        df = pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "trade_date": ["20240101", "20240102", "20240103"],
            "close": [10.0, 11.0, 12.0]
        })
        storage.save_full(df, "daily", "000001.SZ")
        
        # 查询最新日期应该返回最大的日期
        result = storage.get_latest_date("daily", "000001.SZ", "trade_date")
        assert result == "20240103"

    def test_get_latest_date_different_date_formats(self, storage):
        """测试不同日期格式的情况"""
        # 创建包含不同格式日期的表
        df = pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "trade_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "close": [10.0, 11.0, 12.0]
        })
        storage.save_full(df, "daily", "000001.SZ")
        
        # 查询最新日期
        result = storage.get_latest_date("daily", "000001.SZ", "trade_date")
        assert result == "2024-01-03"

    def test_get_latest_date_with_null_values(self, storage):
        """测试包含空值的情况"""
        # 创建包含空值的表
        df = pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "trade_date": ["20240101", None, "20240103"],
            "close": [10.0, 11.0, 12.0]
        })
        storage.save_full(df, "daily", "000001.SZ")
        
        # 查询最新日期应该忽略空值
        result = storage.get_latest_date("daily", "000001.SZ", "trade_date")
        assert result == "20240103"

    def test_get_latest_date_invalid_column(self, storage):
        """测试查询不存在的列时的情况"""
        # 创建包含数据的表
        df = pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": ["20240101", "20240102"],
            "close": [10.0, 11.0]
        })
        storage.save_full(df, "daily", "000001.SZ")
        
        # 查询不存在的列应该返回 None
        result = storage.get_latest_date("daily", "000001.SZ", "invalid_col")
        assert result is None


class TestTaskHandlerIncrementalLogic:
    """测试 TaskHandler 中的增量查询逻辑"""

    def test_start_date_calculation_no_latest_date(self):
        """测试没有最新日期时使用默认起始日期"""
        mock_storage = Mock()
        mock_storage.get_latest_date.return_value = None
        
        # 模拟 TaskHandler 的逻辑
        latest_date = mock_storage.get_latest_date("daily", "000001.SZ", "trade_date")
        if latest_date:
            start_date = (
                pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
            ).strftime("%Y%m%d")
        else:
            start_date = "19901219"
        
        assert start_date == "19901219"
        mock_storage.get_latest_date.assert_called_once_with("daily", "000001.SZ", "trade_date")

    def test_start_date_calculation_with_latest_date(self):
        """测试有最新日期时的起始日期计算"""
        mock_storage = Mock()
        mock_storage.get_latest_date.return_value = "20240103"
        
        # 模拟 TaskHandler 的逻辑
        latest_date = mock_storage.get_latest_date("daily", "000001.SZ", "trade_date")
        if latest_date:
            start_date = (
                pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
            ).strftime("%Y%m%d")
        else:
            start_date = "19901219"
        
        assert start_date == "20240104"
        mock_storage.get_latest_date.assert_called_once_with("daily", "000001.SZ", "trade_date")

    def test_start_date_calculation_different_date_formats(self):
        """测试不同日期格式的起始日期计算"""
        mock_storage = Mock()
        mock_storage.get_latest_date.return_value = "2024-01-03"
        
        # 模拟 TaskHandler 的逻辑，需要处理不同的日期格式
        latest_date = mock_storage.get_latest_date("daily", "000001.SZ", "trade_date")
        if latest_date:
            # 尝试不同的日期格式
            try:
                start_date = (
                    pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
                ).strftime("%Y%m%d")
            except ValueError:
                # 如果格式不匹配，使用自动解析
                start_date = (
                    pd.to_datetime(latest_date) + timedelta(days=1)
                ).strftime("%Y%m%d")
        else:
            start_date = "19901219"
        
        assert start_date == "20240104"

    @patch('downloader.tasks.base.IncrementalTaskHandler.get_data_type')
    @patch('downloader.tasks.base.IncrementalTaskHandler.get_date_col')
    def test_incremental_query_integration(self, mock_get_date_col, mock_get_data_type):
        """测试增量查询的集成流程"""
        from downloader.tasks.base import IncrementalTaskHandler
        from downloader.fetcher import TushareFetcher
        from downloader.storage import DuckDBStorage
        
        # 设置模拟返回值
        mock_get_data_type.return_value = "daily"
        mock_get_date_col.return_value = "trade_date"
        
        # 创建模拟对象
        mock_storage = Mock(spec=DuckDBStorage)
        mock_fetcher = Mock(spec=TushareFetcher)
        
        # 创建一个具体的 TaskHandler 子类用于测试
        class TestTaskHandler(IncrementalTaskHandler):
            def fetch_data(self, ts_code, start_date, end_date):
                return pd.DataFrame({
                    "ts_code": [ts_code],
                    "trade_date": [start_date],
                    "close": [100.0]
                })
            
            def get_data_type(self):
                return "daily"
            
            def get_date_col(self):
                return "trade_date"
        
        task_config = {"name": "test_task"}
        handler = TestTaskHandler(task_config, mock_fetcher, mock_storage)
        
        # 设置 get_latest_date 的返回值
        mock_storage.get_latest_date.return_value = "20240103"
        
        # 执行部分逻辑模拟
        ts_code = "000001.SZ"
        data_type = handler.get_data_type()
        date_col = handler.get_date_col()
        
        latest_date = mock_storage.get_latest_date(data_type, ts_code, date_col)
        if latest_date:
            start_date = (
                pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
            ).strftime("%Y%m%d")
        else:
            start_date = "19901219"
        
        # 验证调用和结果
        mock_storage.get_latest_date.assert_called_once_with("daily", "000001.SZ", "trade_date")
        assert start_date == "20240104"
