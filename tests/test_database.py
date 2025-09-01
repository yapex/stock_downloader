"""数据库相关功能的综合测试"""

import pytest
import threading
import tempfile
import os
import pandas as pd
from unittest.mock import patch, Mock
from pathlib import Path

from neo.database.connection import get_memory_conn, DatabaseConnectionManager, get_conn
from neo.database.operator import ParquetDBQueryer
from neo.containers import AppContainer


class TestDatabaseConnection:
    """数据库连接测试类"""

    def test_same_thread_same_connection(self):
        """测试同一线程中多次调用 get_memory_conn 返回相同的连接"""
        # 第一次获取连接
        with get_memory_conn() as conn1:
            # 创建测试表并插入数据
            conn1.execute("CREATE TABLE test_table (id INTEGER, name TEXT)")
            conn1.execute("INSERT INTO test_table VALUES (1, 'test')")

            # 第二次获取连接
            with get_memory_conn() as conn2:
                # 验证是同一个连接（数据应该存在）
                result = conn2.execute("SELECT * FROM test_table").fetchall()
                assert len(result) == 1
                assert result[0] == (1, "test")

                # 验证连接对象是同一个
                assert conn1 is conn2

    def test_different_threads_different_connections(self):
        """测试不同线程中调用 get_memory_conn 返回不同的连接"""
        connections = {}

        def get_connection_in_thread(thread_id):
            with get_memory_conn() as conn:
                connections[thread_id] = conn

        # 创建两个线程
        thread1 = threading.Thread(target=get_connection_in_thread, args=(1,))
        thread2 = threading.Thread(target=get_connection_in_thread, args=(2,))

        # 启动线程
        thread1.start()
        thread2.start()

        # 等待线程完成
        thread1.join()
        thread2.join()

        # 验证不同线程获取的是不同的连接
        assert connections[1] is not connections[2]

    def test_thread_isolation(self):
        """测试线程间数据隔离"""
        results = {}

        def thread_operation(thread_id):
            with get_memory_conn() as conn:
                # 每个线程创建自己的表
                conn.execute(f"CREATE TABLE thread_{thread_id}_table (id INTEGER)")
                conn.execute(
                    f"INSERT INTO thread_{thread_id}_table VALUES ({thread_id})"
                )

                # 尝试查询其他线程的表（应该失败）
                other_thread_id = 2 if thread_id == 1 else 1
                try:
                    conn.execute(
                        f"SELECT * FROM thread_{other_thread_id}_table"
                    ).fetchall()
                    results[thread_id] = "found_other_table"
                except Exception:
                    results[thread_id] = "isolated"

        # 创建两个线程
        thread1 = threading.Thread(target=thread_operation, args=(1,))
        thread2 = threading.Thread(target=thread_operation, args=(2,))

        # 启动线程
        thread1.start()
        thread2.start()

        # 等待线程完成
        thread1.join()
        thread2.join()

        # 验证线程间数据隔离
        assert results[1] == "isolated"
        assert results[2] == "isolated"


class TestDatabaseConnectionManager:
    """DatabaseConnectionManager 测试类"""

    def test_init_with_default_path(self):
        """测试使用默认路径初始化"""
        with patch("neo.database.connection.get_config") as mock_get_config:
            mock_config = mock_get_config.return_value
            mock_config.database.path = "/test/path/db.duckdb"
            manager = DatabaseConnectionManager()
            assert manager.db_path == "/test/path/db.duckdb"

    def test_init_with_custom_path(self):
        """测试使用自定义路径初始化"""
        custom_path = "/custom/path/db.duckdb"
        manager = DatabaseConnectionManager(custom_path)
        assert manager.db_path == custom_path

    def test_get_connection_success(self):
        """测试成功获取连接"""
        manager = DatabaseConnectionManager(":memory:")
        with manager.get_connection() as conn:
            assert conn is not None
            # 测试连接可用性
            result = conn.execute("SELECT 1").fetchone()
            assert result[0] == 1

    @patch("neo.database.connection.duckdb.connect")
    def test_get_connection_exception_handling(self, mock_connect):
        """测试连接异常处理"""
        # 模拟连接失败
        mock_connect.side_effect = Exception("Connection failed")

        manager = DatabaseConnectionManager(":memory:")
        with pytest.raises(Exception, match="Connection failed"):
            with manager.get_connection():
                pass


class TestGetConnFunction:
    """get_conn 函数测试类"""

    def test_get_conn_with_default_path(self):
        """测试使用默认路径获取连接"""
        with get_conn(":memory:") as conn:
            assert conn is not None
            result = conn.execute("SELECT 1").fetchone()
            assert result[0] == 1

    def test_get_conn_with_custom_path(self):
        """测试使用自定义路径获取连接"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "test.duckdb")

            with get_conn(db_path) as conn:
                conn.execute("CREATE TABLE test (id INTEGER)")
                conn.execute("INSERT INTO test VALUES (1)")
                result = conn.execute("SELECT COUNT(*) FROM test").fetchone()
                assert result[0] == 1


class TestParquetDBQueryer:
    """数据库操作器测试类"""

    @pytest.fixture
    def schema_file_path(self):
        return Path.cwd() / "stock_schema.toml"

    @pytest.fixture
    def mock_schema_loader(self):
        """模拟 schema 加载器"""
        mock_loader = Mock()
        # 模拟 stock_basic 表配置（无 date_col）
        stock_basic_config = Mock()
        stock_basic_config.table_name = "stock_basic"
        stock_basic_config.date_col = None

        # 模拟 stock_daily 表配置（有 date_col）
        stock_daily_config = Mock()
        stock_daily_config.table_name = "stock_daily"
        stock_daily_config.date_col = "trade_date"

        # 模拟 trade_cal 表配置
        trade_cal_config = Mock()
        trade_cal_config.table_name = "trade_cal"
        trade_cal_config.date_col = "cal_date"

        def load_schema_side_effect(table_key):
            if table_key == "stock_basic":
                return stock_basic_config
            elif table_key == "stock_daily":
                return stock_daily_config
            elif table_key == "trade_cal":
                return trade_cal_config
            else:
                raise KeyError(f"Table {table_key} not found")

        mock_loader.load_schema.side_effect = load_schema_side_effect
        return mock_loader

    @pytest.fixture
    def db_operator(self, schema_file_path):
        # ParquetDBQueryer 不需要连接参数
        from neo.database.schema_loader import SchemaLoader

        schema_loader = SchemaLoader(str(schema_file_path))
        operator = ParquetDBQueryer(schema_loader)
        return operator

    @pytest.fixture
    def mock_db_operator(self, mock_schema_loader):
        """使用模拟 schema 加载器的数据库操作器"""
        with tempfile.TemporaryDirectory() as temp_dir:
            operator = ParquetDBQueryer(mock_schema_loader, temp_dir)
            return operator

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

    def test_init_with_default_path(self, mock_schema_loader):
        """测试使用默认路径初始化"""
        with patch("neo.database.operator.get_config") as mock_get_config:
            mock_config = Mock()
            mock_config.get.return_value = "data/parquet"
            mock_get_config.return_value = mock_config

            operator = ParquetDBQueryer(mock_schema_loader)
            assert operator.parquet_base_path == Path("data/parquet")
            assert operator.schema_loader == mock_schema_loader

    def test_init_with_custom_path(self, mock_schema_loader):
        """测试使用自定义路径初始化"""
        custom_path = "/custom/parquet/path"
        operator = ParquetDBQueryer(mock_schema_loader, custom_path)
        assert operator.parquet_base_path == Path(custom_path)
        assert operator.schema_loader == mock_schema_loader

    def test_create_default(self):
        """测试使用默认参数创建 ParquetDBQueryer"""
        with (
            patch("neo.database.operator.get_config") as mock_get_config,
            patch("neo.database.operator.SchemaLoader") as mock_schema_loader_class,
        ):
            mock_config = Mock()
            mock_config.get.return_value = "data/parquet"
            mock_get_config.return_value = mock_config

            mock_schema_loader = Mock()
            mock_schema_loader_class.return_value = mock_schema_loader

            operator = ParquetDBQueryer.create_default()

            # 验证实例创建成功
            assert isinstance(operator, ParquetDBQueryer)
            assert hasattr(operator, "get_all_symbols")
            assert hasattr(operator, "get_max_date")
            mock_schema_loader_class.assert_called_once()

    def test_table_exists_in_schema_true(self, mock_db_operator):
        """测试表在 schema 中存在的情况"""
        result = mock_db_operator._table_exists_in_schema("stock_basic")
        assert result is True

    def test_table_exists_in_schema_false(self, mock_db_operator):
        """测试表在 schema 中不存在的情况"""
        result = mock_db_operator._table_exists_in_schema("nonexistent_table")
        assert result is False

    def test_get_table_config(self, mock_db_operator):
        """测试获取表配置"""
        config = mock_db_operator._get_table_config("stock_basic")
        assert config.table_name == "stock_basic"
        assert config.date_col is None

    def test_get_parquet_path_pattern(self, mock_db_operator):
        """测试获取 Parquet 文件路径模式"""
        pattern = mock_db_operator._get_parquet_path_pattern("stock_daily")
        expected = str(
            mock_db_operator.parquet_base_path / "stock_daily" / "**" / "*.parquet"
        )
        assert pattern == expected

    def test_parquet_files_exist_false_no_directory(self, mock_db_operator):
        """测试 Parquet 文件不存在的情况 - 目录不存在"""
        result = mock_db_operator._parquet_files_exist("nonexistent_table")
        assert result is False

    def test_parquet_files_exist_false_no_parquet_files(self, mock_db_operator):
        """测试 Parquet 文件不存在的情况 - 目录存在但无 parquet 文件"""
        # 创建目录但不创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "empty_table"
        table_path.mkdir(parents=True, exist_ok=True)

        result = mock_db_operator._parquet_files_exist("empty_table")
        assert result is False

    def test_parquet_files_exist_true(self, mock_db_operator):
        """测试 Parquet 文件存在的情况"""
        # 创建目录和 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "test_table"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        result = mock_db_operator._parquet_files_exist("test_table")
        assert result is True

    def test_get_max_date_table_not_in_schema(self, mock_db_operator):
        """测试表不在 schema 中的情况"""
        result = mock_db_operator.get_max_date("nonexistent_table", ["000001.SZ"])
        assert result == {}

    def test_get_max_date_without_date_col(self, mock_db_operator):
        """测试在没有日期列的表上获取最大日期"""
        result = mock_db_operator.get_max_date("stock_basic", ["000001.SZ"])
        assert result == {}

    def test_get_max_date_no_parquet_files(self, mock_db_operator):
        """测试 Parquet 文件不存在时获取最大日期"""
        result = mock_db_operator.get_max_date("stock_daily", ["000001.SZ"])
        assert result == {}

    @patch("neo.database.operator.duckdb.connect")
    def test_get_max_date_with_data(self, mock_connect, mock_db_operator):
        """测试有数据时获取最大日期"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "stock_daily"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # Mock 必要的方法
        mock_db_operator._table_exists_in_schema = Mock(return_value=True)
        mock_db_operator._parquet_files_exist = Mock(return_value=True)

        # Mock table config
        mock_table_config = Mock()
        mock_table_config.table_name = "stock_daily"
        mock_table_config.date_col = "trade_date"
        mock_table_config.primary_key = ["ts_code", "trade_date"]
        mock_db_operator._get_table_config = Mock(return_value=mock_table_config)
        mock_db_operator._get_parquet_path_pattern = Mock(
            return_value="/path/to/*.parquet"
        )

        # 模拟 DuckDB 连接和查询结果
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ("000001.SZ", "20240101"),
            ("000002.SZ", "20240102"),
        ]

        result = mock_db_operator.get_max_date(
            "stock_daily", ["000001.SZ", "000002.SZ"]
        )

        expected = {"000001.SZ": "20240101", "000002.SZ": "20240102"}
        assert result == expected
        mock_conn.close.assert_called_once()

    @patch("neo.database.operator.duckdb.connect")
    def test_get_max_date_with_null_values(self, mock_connect, mock_db_operator):
        """测试查询结果包含 null 值的情况"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "stock_daily"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # Mock 必要的方法
        mock_db_operator._table_exists_in_schema = Mock(return_value=True)
        mock_db_operator._parquet_files_exist = Mock(return_value=True)

        # Mock table config
        mock_table_config = Mock()
        mock_table_config.table_name = "stock_daily"
        mock_table_config.date_col = "trade_date"
        mock_table_config.primary_key = ["ts_code", "trade_date"]
        mock_db_operator._get_table_config = Mock(return_value=mock_table_config)
        mock_db_operator._get_parquet_path_pattern = Mock(
            return_value="/path/to/*.parquet"
        )

        # 模拟 DuckDB 连接和查询结果（包含 null 值）
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ("000001.SZ", "20240101"),
            ("000002.SZ", None),  # null 值应该被过滤
        ]

        result = mock_db_operator.get_max_date(
            "stock_daily", ["000001.SZ", "000002.SZ"]
        )

        expected = {"000001.SZ": "20240101"}  # 只包含非 null 值
        assert result == expected
        mock_conn.close.assert_called_once()

    @patch("neo.database.operator.duckdb.connect")
    def test_get_max_date_exception_handling(self, mock_connect, mock_db_operator):
        """测试查询异常处理"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "stock_daily"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟 DuckDB 连接成功但查询异常
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.side_effect = Exception("Query failed")

        result = mock_db_operator.get_max_date("stock_daily", ["000001.SZ"])
        assert result == {}  # 异常时返回空字典
        mock_conn.close.assert_called_once()  # 确保连接被关闭

    def test_get_all_symbols_table_not_in_schema(self, mock_db_operator):
        """测试 stock_basic 表不在 schema 中的情况"""
        # 修改 mock 使 stock_basic 不存在
        mock_db_operator.schema_loader.load_schema.side_effect = KeyError(
            "stock_basic not found"
        )

        result = mock_db_operator.get_all_symbols()
        assert result == []

    def test_get_all_symbols_no_parquet_files(self, mock_db_operator):
        """测试 stock_basic Parquet 文件不存在的情况"""
        result = mock_db_operator.get_all_symbols()
        assert result == []

    @patch("neo.database.operator.duckdb.connect")
    def test_get_all_symbols_with_data(self, mock_connect, mock_db_operator):
        """测试有数据时获取所有股票代码"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "stock_basic"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟 DuckDB 连接和查询结果
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ("000001.SZ",),
            ("000002.SZ",),
            ("000003.SZ",),
        ]

        result = mock_db_operator.get_all_symbols()

        expected = ["000001.SZ", "000002.SZ", "000003.SZ"]
        assert result == expected
        mock_conn.close.assert_called_once()

    @patch("neo.database.operator.duckdb.connect")
    def test_get_all_symbols_with_empty_values(self, mock_connect, mock_db_operator):
        """测试查询结果包含空值的情况"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "stock_basic"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟 DuckDB 连接和查询结果（包含空值）
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ("000001.SZ",),
            (None,),  # null 值应该被过滤
            ("",),  # 空字符串应该被过滤
            ("000002.SZ",),
        ]

        result = mock_db_operator.get_all_symbols()

        expected = ["000001.SZ", "000002.SZ"]  # 只包含有效值
        assert result == expected

    @patch("neo.database.operator.duckdb.connect")
    def test_get_all_symbols_exception_handling(self, mock_connect, mock_db_operator):
        """测试查询异常处理"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "stock_basic"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟 DuckDB 连接异常
        mock_connect.side_effect = Exception("Database connection failed")

        result = mock_db_operator.get_all_symbols()
        assert result == []  # 异常时返回空列表

    def test_get_all_symbols_cache_behavior(self, mock_db_operator):
        """测试 get_all_symbols 的缓存行为"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "stock_basic"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        with patch("neo.database.operator.duckdb.connect") as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            mock_conn.execute.return_value.fetchall.return_value = [("000001.SZ",)]

            # 第一次调用
            result1 = mock_db_operator.get_all_symbols()
            # 第二次调用应该使用缓存
            result2 = mock_db_operator.get_all_symbols()

            assert result1 == result2 == ["000001.SZ"]
            # 由于缓存，DuckDB 连接只应该被调用一次
            mock_connect.assert_called_once()

    def test_get_latest_trading_day_table_not_in_schema(self, mock_db_operator):
        """测试 trade_cal 表不在 schema 中的情况"""

        # 修改 mock 使 trade_cal 不存在
        def side_effect(table_key):
            if table_key == "trade_cal":
                raise KeyError("trade_cal not found")
            return Mock()

        mock_db_operator.schema_loader.load_schema.side_effect = side_effect

        result = mock_db_operator.get_latest_trading_day()
        assert result is None

    def test_get_latest_trading_day_no_parquet_files(self, mock_db_operator):
        """测试 trade_cal Parquet 文件不存在的情况"""
        result = mock_db_operator.get_latest_trading_day()
        assert result is None

    @patch("neo.database.operator.datetime")
    @patch("neo.database.operator.duckdb.connect")
    def test_get_latest_trading_day_today_is_trading_day(
        self, mock_connect, mock_datetime, mock_db_operator
    ):
        """测试今天是交易日的情况"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "trade_cal"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟当前日期
        mock_now = Mock()
        mock_now.strftime.return_value = "20240115"
        mock_datetime.now.return_value = mock_now

        # 模拟 DuckDB 连接和查询结果（今天是交易日）
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = (
            1,
            "20240112",
        )  # is_open=1, pretrade_date

        result = mock_db_operator.get_latest_trading_day()

        assert result == "20240115"  # 返回今天
        mock_conn.close.assert_called_once()

    @patch("neo.database.operator.datetime")
    @patch("neo.database.operator.duckdb.connect")
    def test_get_latest_trading_day_today_is_not_trading_day(
        self, mock_connect, mock_datetime, mock_db_operator
    ):
        """测试今天不是交易日的情况"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "trade_cal"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟当前日期
        mock_now = Mock()
        mock_now.strftime.return_value = "20240113"  # 假设是周六
        mock_datetime.now.return_value = mock_now

        # 模拟 DuckDB 连接和查询结果（今天不是交易日）
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = (
            0,
            "20240112",
        )  # is_open=0, pretrade_date

        result = mock_db_operator.get_latest_trading_day()

        assert result == "20240112"  # 返回上一个交易日
        mock_conn.close.assert_called_once()

    @patch("neo.database.operator.datetime")
    @patch("neo.database.operator.duckdb.connect")
    def test_get_latest_trading_day_no_today_data_fallback(
        self, mock_connect, mock_datetime, mock_db_operator
    ):
        """测试今天数据不存在时的回退查询"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "trade_cal"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟当前日期
        mock_now = Mock()
        mock_now.strftime.return_value = "20240120"
        mock_datetime.now.return_value = mock_now

        # 模拟 DuckDB 连接和查询结果
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        # 第一次查询（今天的数据）返回 None
        # 第二次查询（回退查询）返回最新交易日
        mock_conn.execute.return_value.fetchone.side_effect = [None, ("20240119",)]

        result = mock_db_operator.get_latest_trading_day()

        assert result == "20240119"  # 返回最新交易日
        mock_conn.close.assert_called_once()

    @patch("neo.database.operator.datetime")
    @patch("neo.database.operator.duckdb.connect")
    def test_get_latest_trading_day_no_data_at_all(
        self, mock_connect, mock_datetime, mock_db_operator
    ):
        """测试完全没有数据的情况"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "trade_cal"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟当前日期
        mock_now = Mock()
        mock_now.strftime.return_value = "20240120"
        mock_datetime.now.return_value = mock_now

        # 模拟 DuckDB 连接和查询结果（都返回 None）
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = None

        result = mock_db_operator.get_latest_trading_day()

        assert result is None
        mock_conn.close.assert_called_once()

    @patch("neo.database.operator.datetime")
    @patch("neo.database.operator.duckdb.connect")
    def test_get_latest_trading_day_with_custom_exchange(
        self, mock_connect, mock_datetime, mock_db_operator
    ):
        """测试使用自定义交易所的情况"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "trade_cal"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟当前日期
        mock_now = Mock()
        mock_now.strftime.return_value = "20240115"
        mock_datetime.now.return_value = mock_now

        # 模拟 DuckDB 连接和查询结果
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = (1, "20240112")

        result = mock_db_operator.get_latest_trading_day("SZSE")

        assert result == "20240115"
        # 验证 SQL 中包含正确的交易所参数
        call_args = mock_conn.execute.call_args[0][0]
        assert "exchange = 'SZSE'" in call_args
        mock_conn.close.assert_called_once()

    @patch("neo.database.operator.datetime")
    @patch("neo.database.operator.duckdb.connect")
    def test_get_latest_trading_day_exception_handling(
        self, mock_connect, mock_datetime, mock_db_operator
    ):
        """测试查询异常处理"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "trade_cal"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        # 模拟当前日期
        mock_now = Mock()
        mock_now.strftime.return_value = "20240115"
        mock_datetime.now.return_value = mock_now

        # 模拟 DuckDB 连接异常
        mock_connect.side_effect = Exception("Database connection failed")

        result = mock_db_operator.get_latest_trading_day()

        assert result is None  # 异常时返回 None

    def test_get_latest_trading_day_cache_behavior(self, mock_db_operator):
        """测试 get_latest_trading_day 的缓存行为"""
        # 创建 parquet 文件
        table_path = mock_db_operator.parquet_base_path / "trade_cal"
        table_path.mkdir(parents=True, exist_ok=True)
        parquet_file = table_path / "test.parquet"
        parquet_file.touch()

        with (
            patch("neo.database.operator.datetime") as mock_datetime,
            patch("neo.database.operator.duckdb.connect") as mock_connect,
        ):
            # 模拟当前日期
            mock_now = Mock()
            mock_now.strftime.return_value = "20240115"
            mock_datetime.now.return_value = mock_now

            # 模拟 DuckDB 连接和查询结果
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            mock_conn.execute.return_value.fetchone.return_value = (1, "20240112")

            # 第一次调用
            result1 = mock_db_operator.get_latest_trading_day()
            # 第二次调用应该使用缓存
            result2 = mock_db_operator.get_latest_trading_day()

            assert result1 == result2 == "20240115"
            # 由于缓存，DuckDB 连接只应该被调用一次
            mock_connect.assert_called_once()

    # 保留原有的测试用例
    def test_get_max_date_without_date_col_original(self, db_operator):
        """测试在没有日期列的表上获取最大日期（原有测试）"""
        # stock_basic 表没有日期列，应该返回空字典
        max_dates = db_operator.get_max_date("stock_basic", ["000001.SZ"])
        assert max_dates == {}

    def test_create_default_original(self):
        """测试使用默认参数创建 ParquetDBQueryer（原有测试）"""
        # 使用默认参数创建
        operator = ParquetDBQueryer.create_default()

        # 验证实例创建成功
        assert isinstance(operator, ParquetDBQueryer)
        # ParquetDBQueryer 不需要 conn 参数，验证实例类型即可
        assert hasattr(operator, "get_all_symbols")
        assert hasattr(operator, "get_max_date")


class TestDatabaseIntegration:
    """数据库集成测试类"""

    def test_container_db_operator_functionality(self):
        """测试从容器获取的 ParquetDBQueryer 功能"""
        container = AppContainer()
        # 使用 db_queryer 来测试 ParquetDBQueryer 功能
        db_operator = container.db_queryer()

        # 验证实例类型
        assert isinstance(db_operator, ParquetDBQueryer)

        # 验证具有预期的查询方法
        assert hasattr(db_operator, "get_all_symbols")
        assert hasattr(db_operator, "get_max_date")

    def test_container_provides_different_operator_instances(self):
        """测试容器提供不同的操作器实例"""
        container = AppContainer()
        # 使用 db_queryer 来测试 ParquetDBQueryer 功能
        operator1 = container.db_queryer()
        operator2 = container.db_queryer()

        # 验证是不同的实例（非单例模式）
        assert operator1 is not operator2
        assert isinstance(operator1, ParquetDBQueryer)
        assert isinstance(operator2, ParquetDBQueryer)
