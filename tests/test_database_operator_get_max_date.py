"""ParquetDBQueryer get_max_date 方法单元测试"""

from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

import pytest

from src.neo.database.operator import ParquetDBQueryer
from src.neo.database.schema_loader import SchemaLoader


class TestParquetDBQueryerGetMaxDate:
    """ParquetDBQueryer get_max_date 方法测试类"""

    @pytest.fixture
    def mock_schema_loader(self):
        """创建 mock SchemaLoader"""
        return Mock(spec=SchemaLoader)

    @pytest.fixture
    def operator(self, mock_schema_loader):
        """创建 ParquetDBQueryer 实例"""
        return ParquetDBQueryer(
            schema_loader=mock_schema_loader, parquet_base_path="/tmp/test_parquet"
        )

    @patch("src.neo.database.operator.duckdb.connect")
    @patch("pathlib.Path.rglob")
    @patch("pathlib.Path.exists")
    def test_get_max_date_with_ts_code_primary_key(
        self, mock_exists, mock_rglob, mock_connect, operator, mock_schema_loader
    ):
        """测试有 ts_code 主键的表查询最大日期"""
        # 设置 mock schema
        mock_table_config = Mock()
        mock_table_config.table_name = "stock_daily"
        mock_table_config.date_col = "trade_date"
        mock_table_config.primary_key = ["ts_code", "trade_date"]
        mock_schema_loader.load_schema.return_value = mock_table_config

        # Mock parquet files exist
        mock_exists.return_value = True
        mock_rglob.return_value = [Path("/tmp/test.parquet")]

        # Mock duckdb connection and query results
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ("000001.SZ", "20231201"),
            ("000002.SZ", "20231130"),
        ]

        result = operator.get_max_date("stock_daily", ["000001", "000002"])

        assert result == {"000001": "20231201", "000002": "20231130"}
        # 验证调用了 duckdb
        mock_connect.assert_called_once_with(":memory:")
        mock_conn.execute.assert_called_once()

    @patch("src.neo.database.operator.duckdb.connect")
    @patch("pathlib.Path.rglob")
    @patch("pathlib.Path.exists")
    def test_get_max_date_without_ts_code_primary_key(
        self, mock_exists, mock_rglob, mock_connect, operator, mock_schema_loader
    ):
        """测试不包含 ts_code 主键的表查询最大日期"""
        # 设置 mock schema
        mock_table_config = Mock()
        mock_table_config.table_name = "trade_cal"
        mock_table_config.date_col = "cal_date"
        mock_table_config.primary_key = ["cal_date"]
        mock_schema_loader.load_schema.return_value = mock_table_config

        # Mock parquet files exist
        mock_exists.return_value = True
        mock_rglob.return_value = [Path("/tmp/test.parquet")]

        # Mock duckdb connection and query results
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = ("20231201",)

        result = operator.get_max_date("trade_cal", ["000001"])

        # 对于非股票表，所有股票代码返回相同的最大日期
        assert result == {"000001": "20231201"}
        # 验证调用了 duckdb
        mock_connect.assert_called_once_with(":memory:")
        mock_conn.execute.assert_called_once()

    def test_get_max_date_empty_symbols_list(self, operator, mock_schema_loader):
        """测试空股票代码列表"""
        # 设置 mock schema
        mock_table_config = Mock()
        mock_table_config.table_name = "stock_daily"
        mock_table_config.date_col = "trade_date"
        mock_table_config.primary_key = ["ts_code", "trade_date"]
        mock_schema_loader.load_schema.return_value = mock_table_config

        result = operator.get_max_date("stock_daily", [])

        # 验证返回空字典
        assert result == {}

    def test_get_max_date_table_not_in_schema(self, operator, mock_schema_loader):
        """测试表不在 schema 中的情况"""
        # 模拟 schema 加载器抛出 KeyError
        mock_schema_loader.load_schema.side_effect = KeyError("表配置不存在")

        result = operator.get_max_date("non_existent_table", ["000001"])

        # 验证返回空字典
        assert result == {}

    def test_get_max_date_no_date_col_in_schema(self, operator, mock_schema_loader):
        """测试 schema 中没有 date_col 的情况"""
        # 设置 mock schema 没有 date_col
        mock_table_config = Mock()
        mock_table_config.table_name = "table_without_date_col"
        mock_table_config.primary_key = ["ts_code"]
        # 没有 date_col 属性
        del mock_table_config.date_col
        mock_schema_loader.load_schema.return_value = mock_table_config

        result = operator.get_max_date("table_without_date_col", ["000001"])

        # 验证返回空字典
        assert result == {}

    @patch("pathlib.Path.exists")
    def test_get_max_date_no_parquet_files(
        self, mock_exists, operator, mock_schema_loader
    ):
        """测试没有parquet文件的情况"""
        # 设置 mock schema
        mock_table_config = Mock()
        mock_table_config.table_name = "stock_daily"
        mock_table_config.date_col = "trade_date"
        mock_table_config.primary_key = ["ts_code", "trade_date"]
        mock_schema_loader.load_schema.return_value = mock_table_config

        # Mock parquet files don't exist
        mock_exists.return_value = False

        result = operator.get_max_date("stock_daily", ["000001"])

        # 验证返回空字典
        assert result == {}
