import pytest
import pandas as pd
from unittest.mock import Mock, patch
from contextlib import contextmanager
import duckdb
from downloader.database.db_oprator import DBOperator
from downloader.database.db_table_create import SchemaTableCreator
from downloader.database.db_connection import get_memory_conn
from pathlib import Path


class TestDBOperator:
    @pytest.fixture
    def schema_file_path(self):
        return Path.cwd() / "stock_schema.toml"

    @pytest.fixture
    def db_operator(self, schema_file_path):
        # 为每个测试创建独立的内存数据库连接，但在同一个测试中复用
        conn = duckdb.connect(":memory:")

        @contextmanager
        def memory_conn_context():
            try:
                yield conn
            finally:
                pass  # 不关闭连接，让测试中的多个操作可以复用

        operator = DBOperator(str(schema_file_path), memory_conn_context)

        # 在fixture清理时关闭连接
        yield operator
        conn.close()

    @pytest.fixture
    def sample_stock_basic_data(self):
        return pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ"],
                "symbol": ["000001", "000002"],
                "name": ["平安银行", "万科A"],
                "area": ["深圳", "深圳"],
                "industry": ["银行", "房地产开发"],
                "cnspell": ["PAYH", "WKA"],
                "market": ["主板", "主板"],
                "list_date": ["19910403", "19910129"],
                "act_name": ["平安银行股份有限公司", "万科企业股份有限公司"],
                "act_ent_type": ["股份有限公司", "股份有限公司"],
            }
        )

    def test_upsert_empty_data(self, db_operator):
        """测试空数据的处理"""
        empty_data = pd.DataFrame()
        # 应该不抛出异常，只是记录日志
        db_operator.upsert("stock_basic", empty_data)

    def test_upsert_missing_primary_key(self, db_operator, sample_stock_basic_data):
        """测试缺少主键的表"""
        # 模拟一个没有主键的表配置
        mock_config = Mock()
        mock_config.table_name = "test_table"
        mock_config.primary_key = []
        mock_config.columns = [{"name": "col1", "type": "TEXT"}]

        db_operator.stock_schema = {"test_table": mock_config}
        
        # 先创建表
        with db_operator.conn() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT)")

        with pytest.raises(ValueError, match="未定义主键"):
            db_operator.upsert("test_table", sample_stock_basic_data)

    def test_upsert_missing_columns(self, db_operator):
        """测试DataFrame缺少必需列"""
        incomplete_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "symbol": ["000001"],
                # 缺少其他必需列
            }
        )
        
        # 先创建表
        db_operator.create_table("stock_basic")

        with pytest.raises(ValueError, match="DataFrame 缺少以下列"):
            db_operator.upsert("stock_basic", incomplete_data)

    def test_upsert_single_record(self, db_operator, sample_stock_basic_data):
        """测试单条记录的upsert操作"""
        # 先创建表
        db_operator.create_table("stock_basic")

        # 插入单条记录
        single_record = sample_stock_basic_data.iloc[:1]
        db_operator.upsert("stock_basic", single_record)

        # 验证数据已插入
        with db_operator.conn() as conn:
            result = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()
            assert result[0] == 1

        # 再次插入相同记录（应该更新而不是重复插入）
        updated_record = single_record.copy()
        updated_record.loc[0, "name"] = "平安银行更新"
        db_operator.upsert("stock_basic", updated_record)

        # 验证记录数量没有增加，但数据已更新
        with db_operator.conn() as conn:
            result = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()
            assert result[0] == 1

            name_result = conn.execute(
                "SELECT name FROM stock_basic WHERE ts_code = '000001.SZ'"
            ).fetchone()
            assert name_result[0] == "平安银行更新"

    def test_upsert_batch_records(self, db_operator, sample_stock_basic_data):
        """测试批量记录的upsert操作"""
        # 先创建表
        db_operator.create_table("stock_basic")

        # 插入批量记录
        db_operator.upsert("stock_basic", sample_stock_basic_data)

        # 验证数据已插入
        with db_operator.conn() as conn:
            result = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()
            assert result[0] == 2

        # 更新部分记录并添加新记录
        updated_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000003.SZ"],  # 一个已存在，一个新的
                "symbol": ["000001", "000003"],
                "name": ["平安银行更新", "国农科技"],
                "area": ["深圳", "深圳"],
                "industry": ["银行", "农业"],
                "cnspell": ["PAYH", "GNKJ"],
                "market": ["主板", "主板"],
                "list_date": ["19910403", "19910114"],
                "act_name": ["平安银行股份有限公司", "国农科技股份有限公司"],
                "act_ent_type": ["股份有限公司", "股份有限公司"],
            }
        )

        db_operator.upsert("stock_basic", updated_data)

        # 验证总记录数为3（2个原有 + 1个新增，1个更新）
        with db_operator.conn() as conn:
            result = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()
            assert result[0] == 3

            # 验证更新的记录
            name_result = conn.execute(
                "SELECT name FROM stock_basic WHERE ts_code = '000001.SZ'"
            ).fetchone()
            assert name_result[0] == "平安银行更新"

            # 验证新增的记录
            new_result = conn.execute(
                "SELECT name FROM stock_basic WHERE ts_code = '000003.SZ'"
            ).fetchone()
            assert new_result[0] == "国农科技"

    def test_upsert_transaction_rollback(self, db_operator, sample_stock_basic_data):
        """测试事务回滚功能"""
        # 先创建表
        db_operator.create_table("stock_basic")

        # 模拟执行过程中出现异常
        with patch.object(
            db_operator, "_upsert_batch_records", side_effect=Exception("模拟异常")
        ):
            with pytest.raises(Exception, match="模拟异常"):
                db_operator.upsert("stock_basic", sample_stock_basic_data)

        # 验证事务已回滚，表中没有数据
        with db_operator.conn() as conn:
            result = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()
            assert result[0] == 0

    def test_extract_column_names_dict(self, db_operator):
        """测试从字典格式提取列名"""
        columns_dict = {
            "ts_code": {"type": "TEXT", "primary_key": True},
            "name": {"type": "TEXT"},
        }
        result = db_operator._extract_column_names(columns_dict)
        assert result == ["ts_code", "name"]

    def test_extract_column_names_list(self, db_operator):
        """测试从列表格式提取列名"""
        columns_list = [
            {"name": "ts_code", "type": "TEXT"},
            {"name": "symbol", "type": "TEXT"},
        ]
        result = db_operator._extract_column_names(columns_list)
        assert result == ["ts_code", "symbol"]

    def test_extract_column_names_invalid_format(self, db_operator):
        """测试无效格式的列配置"""
        with pytest.raises(ValueError, match="不支持的列配置格式"):
            db_operator._extract_column_names("invalid_format")

    def test_get_max_date_with_date_col(self, db_operator):
        """测试有 date_col 的表查询最大日期"""
        # 创建 stock_daily 表并插入测试数据
        db_operator.create_table("stock_daily")

        test_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ"],
                "trade_date": ["20240101", "20240102", "20240103"],
                "open": [10.0, 11.0, 12.0],
                "high": [10.5, 11.5, 12.5],
                "low": [9.5, 10.5, 11.5],
                "close": [10.2, 11.2, 12.2],
                "pre_close": [9.8, 10.2, 11.2],
                "change": [0.4, 1.0, 1.0],
                "pct_chg": [4.08, 9.80, 8.93],
                "vol": [1000, 2000, 3000],
                "amount": [10000, 22000, 36000],
            }
        )

        db_operator.upsert("stock_daily", test_data)

        # 查询最大日期
        max_date = db_operator.get_max_date("stock_daily")
        assert max_date == "20240103"

    def test_get_max_date_without_date_col(self, db_operator, caplog):
        """测试没有 date_col 的表查询最大日期"""
        import logging

        caplog.set_level(logging.WARNING)

        # stock_basic 表没有 date_col
        result = db_operator.get_max_date("stock_basic")

        assert result is None
        assert "表 'stock_basic' 未定义 date_col 字段，无法查询最大日期" in caplog.text

    def test_get_max_date_empty_table(self, db_operator, caplog):
        """测试空表查询最大日期"""
        import logging

        caplog.set_level(logging.WARNING)

        # 创建 stock_daily 表但不插入数据
        db_operator.create_table("stock_daily")

        max_date = db_operator.get_max_date("stock_daily")

        assert max_date is None
        assert "表 'stock_daily' 为空或 trade_date 字段无有效数据" in caplog.text

    def test_get_max_date_invalid_table(self, db_operator):
        """测试不存在的表查询最大日期"""
        with pytest.raises(
            ValueError, match="表配置 'invalid_table' 不存在于 schema 中"
        ):
            db_operator.get_max_date("invalid_table")

    def test_get_max_date_table_not_exists(self, db_operator):
        """测试表在 schema 中存在但数据库中不存在的情况"""
        # stock_daily 在 schema 中存在但数据库中未创建
        with pytest.raises(Exception):  # 应该抛出数据库相关异常
            db_operator.get_max_date("stock_daily")

    def test_get_all_symbols_with_data(self, db_operator, sample_stock_basic_data):
        """测试有数据的 stock_basic 表查询所有 symbol"""
        # 创建 stock_basic 表并插入测试数据
        db_operator.create_table("stock_basic")
        db_operator.upsert("stock_basic", sample_stock_basic_data)

        # 查询所有 symbol
        symbols = db_operator.get_all_symbols()

        expected_codes = ["000001.SZ", "000002.SZ"]
        assert len(symbols) == 2
        assert set(symbols) == set(expected_codes)

    def test_get_all_symbols_empty_table(self, db_operator):
        """测试空的 stock_basic 表查询所有 symbol"""
        # 创建 stock_basic 表但不插入数据
        db_operator.create_table("stock_basic")

        # 查询所有 symbol
        symbols = db_operator.get_all_symbols()

        assert symbols == []

    def test_get_all_symbols_table_not_exists(self, db_operator):
        """测试表在数据库中不存在的情况"""
        # stock_basic 表未创建
        with pytest.raises(Exception):  # 应该抛出数据库相关异常
            db_operator.get_all_symbols()
