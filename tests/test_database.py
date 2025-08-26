"""数据库相关功能的综合测试"""

import pytest
import threading
import tempfile
import os
import pandas as pd
from unittest.mock import patch, MagicMock, Mock
from contextlib import contextmanager
import duckdb

from neo.database.connection import get_memory_conn, DatabaseConnectionManager, get_conn
from neo.database.operator import DBOperator
from neo.database.table_creator import SchemaTableCreator
from neo.database.types import TableName
from neo.containers import AppContainer
from pathlib import Path


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
                conn.execute(f"INSERT INTO thread_{thread_id}_table VALUES ({thread_id})")

                # 尝试查询其他线程的表（应该失败）
                other_thread_id = 2 if thread_id == 1 else 1
                try:
                    conn.execute(f"SELECT * FROM thread_{other_thread_id}_table").fetchall()
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


class TestDBOperator:
    """数据库操作器测试类"""

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
        empty_df = pd.DataFrame()
        db_operator.upsert("stock_basic", empty_df)
        # 空数据不应该引发异常

    def test_upsert_missing_primary_key(self, db_operator, sample_stock_basic_data):
        """测试缺少主键的数据"""
        # 删除主键列
        data_without_pk = sample_stock_basic_data.drop(columns=["ts_code"])

        with pytest.raises(Exception) as exc_info:
            db_operator.upsert("stock_basic", data_without_pk)

        # 验证异常信息包含主键相关内容
        error_msg = str(exc_info.value).lower()
        assert any(
            keyword in error_msg
            for keyword in ["primary", "key", "ts_code", "column", "missing"]
        )

    def test_get_max_date_for_multiple_symbols(self, db_operator):
        """测试为多个特定股票代码批量获取最大日期"""
        # 准备包含多个股票和日期的数据
        test_data = pd.DataFrame({
            'ts_code': ['000001.SZ', '000001.SZ', '000002.SZ', '000003.SZ'],
            'trade_date': ['20240101', '20240105', '20240110', '20240115'],
            # 添加其他必需列
            'open': [10.0, 11.0, 20.0, 30.0],
            'high': [10.8, 11.8, 20.8, 30.8],
            'low': [9.8, 10.8, 19.8, 29.8],
            'close': [10.5, 11.5, 20.5, 30.5],
            'pre_close': [10.0, 10.5, 20.0, 30.0],
            'change': [0.5, 1.0, 0.5, 0.5],
            'pct_chg': [5.0, 9.52, 2.5, 1.67],
            'vol': [1000, 1100, 2000, 3000],
            'amount': [10500, 12650, 41000, 91500],
        })
        db_operator.upsert("stock_daily", test_data)

        # 批量查询 '000001.SZ' 和 '000003.SZ' 的最大日期
        max_dates = db_operator.get_max_date("stock_daily", ts_codes=['000001.SZ', '000003.SZ'])
        assert max_dates == {
            '000001.SZ': '20240105',
            '000003.SZ': '20240115'
        }

        # 查询一个存在的和一个不存在的
        max_dates_mixed = db_operator.get_max_date("stock_daily", ts_codes=['000002.SZ', '999999.SH'])
        assert max_dates_mixed == {
            '000002.SZ': '20240110'
        }

        # 查询全表的最新日期
        max_date_all = db_operator.get_max_date("stock_daily")
        assert max_date_all == {'__all__': '20240115'}

    def test_get_max_date_without_date_col(self, db_operator):
        """测试在没有日期列的表上获取最大日期"""
        # stock_basic 表没有日期列，应该返回空字典
        max_dates = db_operator.get_max_date("stock_basic")
        assert max_dates == {}

    def test_get_all_symbols_with_data(self, db_operator, sample_stock_basic_data):
        """测试获取所有股票代码（有数据）"""
        # 插入测试数据
        db_operator.upsert("stock_basic", sample_stock_basic_data)

        # 获取所有股票代码
        symbols = db_operator.get_all_symbols()

        # 验证结果
        expected_symbols = ["000001.SZ", "000002.SZ"]
        assert set(symbols) == set(expected_symbols)
        assert len(symbols) == 2

    def test_get_all_symbols_empty_table(self, db_operator):
        """测试获取所有股票代码（空表）"""
        # 先创建stock_basic表
        db_operator.create_table("stock_basic")
        
        # 获取空表的股票代码
        symbols = db_operator.get_all_symbols()

        # 验证结果
        assert symbols == []

    def test_create_default(self):
        """测试使用默认参数创建 DBOperator"""
        with patch("neo.database.operator.get_conn") as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            # 使用默认参数创建
            operator = DBOperator.create_default()

            # 验证实例创建成功
            assert isinstance(operator, DBOperator)
            assert operator.conn is not None


class TestSchemaTableCreator:
    """表创建器测试类"""

    @pytest.fixture
    def schema_file(self):
        """创建临时 schema 文件"""
        schema_content = """
[tables.stock_basic]
table_name = "stock_basic"
primary_key = ["ts_code"]
description = "股票基本信息表字段"
columns = [
    { name = "ts_code", type = "TEXT" },
    { name = "symbol", type = "TEXT" },
    { name = "name", type = "TEXT" }
]

[tables.stock_daily]
table_name = "stock_daily"
primary_key = ["ts_code", "trade_date"]
description = "股票日线数据字段"
columns = [
    { name = "ts_code", type = "TEXT" },
    { name = "trade_date", type = "TEXT" },
    { name = "open", type = "REAL" },
    { name = "close", type = "REAL" }
]
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(schema_content)
            temp_file = f.name

        yield temp_file

        # 清理临时文件
        os.unlink(temp_file)

    @pytest.fixture
    def shared_memory_conn(self):
        """创建共享的内存数据库连接"""
        conn = duckdb.connect(":memory:")

        @contextmanager
        def memory_conn_context():
            try:
                yield conn
            finally:
                pass

        yield memory_conn_context
        conn.close()

    @pytest.fixture
    def creator(self, schema_file, shared_memory_conn):
        return SchemaTableCreator(schema_file, shared_memory_conn)

    def test_create_table_with_valid_enum_table_name(self, creator):
        """测试使用有效的枚举表名创建表"""
        creator.create_table(TableName.STOCK_BASIC.value)
        # 如果没有异常，则测试通过

    def test_create_table_with_invalid_table_name(self, creator):
        """测试使用无效表名创建表"""
        # create_table方法对于无效表名会返回False而不是抛出异常
        result = creator.create_table("invalid_table")
        assert result is False

    def test_create_table_generates_correct_sql(self, creator):
        """测试生成正确的 SQL"""
        with patch.object(creator, "conn") as mock_conn_func:
            mock_conn = MagicMock()
            mock_conn_func.return_value.__enter__.return_value = mock_conn

            creator.create_table(TableName.STOCK_BASIC.value)

            # 验证执行了 SQL
            mock_conn.execute.assert_called()
            sql_call = mock_conn.execute.call_args[0][0]

            # 验证 SQL 包含预期内容
            assert "CREATE TABLE" in sql_call.upper()
            assert "stock_basic" in sql_call
            assert "ts_code" in sql_call
            assert "PRIMARY KEY" in sql_call.upper()

    def test_create_table_in_memory_database_real(self, creator):
        """测试在真实内存数据库中创建表"""
        # 创建表
        creator.create_table(TableName.STOCK_BASIC.value)

        # 验证表已创建
        with creator.conn() as conn:
            # 查询表信息
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'stock_basic'"
            ).fetchall()
            assert len(result) == 1
            assert result[0][0] == "stock_basic"

    def test_create_all_tables(self, schema_file):
        """测试创建所有表"""
        conn = duckdb.connect(":memory:")

        @contextmanager
        def memory_conn_context():
            try:
                yield conn
            finally:
                pass

        try:
            creator = SchemaTableCreator(schema_file, memory_conn_context)
            creator.create_all_tables()

            # 验证所有表都已创建
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            table_names = [table[0] for table in tables]

            assert "stock_basic" in table_names
            assert "stock_daily" in table_names

        finally:
            conn.close()

    def test_drop_table_success(self, creator):
        """测试成功删除表"""
        # 先创建表
        creator.create_table(TableName.STOCK_BASIC.value)

        # 删除表
        creator.drop_table(TableName.STOCK_BASIC.value)

        # 验证表已删除
        with creator.conn() as conn:
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'stock_basic'"
            ).fetchall()
            assert len(result) == 0


class TestDatabaseIntegration:
    """数据库集成测试类"""

    def test_container_db_operator_functionality(self):
        """测试从容器获取的 DBOperator 功能"""
        container = AppContainer()
        db_operator = container.db_operator()

        # 验证实例类型
        assert isinstance(db_operator, DBOperator)

        # 验证具有预期的方法
        assert hasattr(db_operator, "upsert")
        assert hasattr(db_operator, "get_max_date")
        assert hasattr(db_operator, "get_all_symbols")

    def test_container_provides_different_operator_instances(self):
        """测试容器提供不同的操作器实例"""
        container = AppContainer()
        operator1 = container.db_operator()
        operator2 = container.db_operator()

        # 验证是不同的实例（非单例模式）
        assert operator1 is not operator2
        assert isinstance(operator1, DBOperator)
        assert isinstance(operator2, DBOperator)