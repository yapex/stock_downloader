import pytest
import tempfile
import os
from unittest.mock import patch, MagicMock
from downloader.database.db_table_create import SchemaTableCreator, TableName


class TestSchemaTableCreator:
    """SchemaTableCreator 测试类"""

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
        from contextlib import contextmanager
        import duckdb

        # 创建一个持久的内存数据库连接
        conn = duckdb.connect(":memory:")

        @contextmanager
        def get_shared_conn():
            yield conn

        return get_shared_conn

    @pytest.fixture
    def creator(self, schema_file, shared_memory_conn):
        """创建 SchemaTableCreator 实例"""
        return SchemaTableCreator(schema_file_path=schema_file, conn=shared_memory_conn)

    def test_create_table_with_valid_enum_table_name(self, creator):
        """测试使用有效枚举表名创建表"""
        result = creator.create_table(TableName.STOCK_BASIC.value)
        assert result is True

    def test_create_table_with_invalid_table_name(self, creator):
        """测试使用无效表名创建表"""
        result = creator.create_table("invalid_table")
        assert result is False

    def test_create_table_with_non_existent_schema_table(self, creator):
        """测试使用不存在于 schema 中的有效枚举表名"""
        # INCOME_STATEMENT 在枚举中存在但不在测试 schema 文件中
        result = creator.create_table(TableName.INCOME_STATEMENT.value)
        assert result is False

    def test_create_table_sql_execution_error(self, creator):
        """测试 SQL 执行错误的情况"""
        with patch.object(creator, "conn") as mock_conn:
            mock_context = MagicMock()
            mock_conn.return_value.__enter__.return_value = mock_context
            mock_context.execute.side_effect = Exception("SQL execution failed")

            result = creator.create_table(TableName.STOCK_BASIC.value)
            assert result is False

    def test_create_table_generates_correct_sql(self, creator):
        """测试生成的 SQL 语句是否正确"""
        with patch.object(creator, "conn") as mock_conn:
            mock_context = MagicMock()
            mock_conn.return_value.__enter__.return_value = mock_context

            creator.create_table(TableName.STOCK_BASIC.value)

            # 验证 execute 被调用
            mock_context.execute.assert_called_once()

            # 获取调用的 SQL 语句
            sql_call = mock_context.execute.call_args[0][0]

            # 验证 SQL 包含预期内容
            assert "CREATE TABLE IF NOT EXISTS stock_basic" in sql_call
            assert "ts_code VARCHAR" in sql_call
            assert "symbol VARCHAR" in sql_call
            assert "name VARCHAR" in sql_call
            assert "PRIMARY KEY (ts_code)" in sql_call

    def test_create_table_with_composite_primary_key(self, creator):
        """测试复合主键表的创建"""
        with patch.object(creator, "conn") as mock_conn:
            mock_context = MagicMock()
            mock_conn.return_value.__enter__.return_value = mock_context

            creator.create_table(TableName.STOCK_DAILY.value)

            # 获取调用的 SQL 语句
            sql_call = mock_context.execute.call_args[0][0]

            # 验证复合主键
            assert "PRIMARY KEY (ts_code, trade_date)" in sql_call

    def test_table_name_enum_values(self):
        """测试 TableName 枚举包含所有预期值"""
        expected_tables = {
            "stock_basic",
            "stock_daily",
            "stock_adj_qfq",
            "daily_basic",
            "income_statement",
            "balance_sheet",
            "cash_flow",
        }

        actual_tables = {table.value for table in TableName}
        assert actual_tables == expected_tables

    def test_table_name_enum_validation(self):
        """测试 TableName 枚举验证功能"""
        # 有效的表名
        valid_table = TableName("stock_basic")
        assert valid_table == TableName.STOCK_BASIC

        # 无效的表名应该抛出 ValueError
        with pytest.raises(ValueError):
            TableName("invalid_table_name")

    def test_create_table_in_memory_database_real(self, creator):
        """测试在真实内存数据库中创建表"""
        # 创建表
        result = creator.create_table(TableName.STOCK_BASIC.value)
        assert result is True

        # 验证表是否真的被创建 - 直接在同一个连接中检查
        with creator.conn() as conn:
            # 先尝试直接查询表来验证是否存在
            try:
                conn.execute("SELECT COUNT(*) FROM stock_basic").fetchall()
                table_exists = True
            except Exception:
                table_exists = False

            assert table_exists, "表 stock_basic 应该存在但查询失败"

            # 验证表结构 - 使用 DESCRIBE
            columns_result = conn.execute("DESCRIBE stock_basic").fetchall()

            # DuckDB DESCRIBE 返回格式: (column_name, column_type, null, key, default, extra)
            expected_columns = ["ts_code", "symbol", "name"]
            actual_columns = [row[0] for row in columns_result]

            assert len(actual_columns) == len(expected_columns)
            for expected_col in expected_columns:
                assert expected_col in actual_columns

    def test_create_table_with_composite_primary_key_real(self, creator):
        """测试在真实内存数据库中创建复合主键表"""
        # 创建表
        result = creator.create_table(TableName.STOCK_DAILY.value)
        assert result is True

        # 验证表是否真的被创建
        with creator.conn() as conn:
            # 查询表是否存在
            tables_result = conn.execute("SHOW TABLES").fetchall()
            table_names = [row[0] for row in tables_result]
            assert "stock_daily" in table_names

            # 验证表结构包含预期的列
            columns_result = conn.execute("DESCRIBE stock_daily").fetchall()
            column_names = [row[0] for row in columns_result]
            expected_columns = ["ts_code", "trade_date", "open", "close"]
            for expected_col in expected_columns:
                assert expected_col in column_names

    def test_insert_data_into_created_table_real(self, creator):
        """测试向真实创建的表中插入数据"""
        # 创建表
        result = creator.create_table(TableName.STOCK_BASIC.value)
        assert result is True

        # 插入测试数据
        with creator.conn() as conn:
            conn.execute(
                "INSERT INTO stock_basic (ts_code, symbol, name) VALUES (?, ?, ?)",
                ("000001.SZ", "000001", "平安银行"),
            )

            # 验证数据插入成功
            result = conn.execute("SELECT * FROM stock_basic").fetchall()
            assert len(result) == 1
            assert result[0] == ("000001.SZ", "000001", "平安银行")

    def test_duplicate_table_creation_real(self, creator):
        """测试重复创建表的情况"""
        # 第一次创建
        result1 = creator.create_table(TableName.STOCK_BASIC.value)
        assert result1 is True

        # 第二次创建（应该成功，因为使用了 IF NOT EXISTS）
        result2 = creator.create_table(TableName.STOCK_BASIC.value)
        assert result2 is True

        # 验证表仍然存在
        with creator.conn() as conn:
            tables_result = conn.execute("SHOW TABLES").fetchall()
            table_names = [row[0] for row in tables_result]
            assert "stock_basic" in table_names
            # 验证只有一个 stock_basic 表
            stock_basic_count = table_names.count('stock_basic')
            assert stock_basic_count == 1

    def test_dependency_injection_with_memory_conn(self, schema_file):
        """测试通过依赖注入使用内存数据库连接"""
        from downloader.database.db_connection import get_memory_conn
        
        # 通过依赖注入创建使用内存数据库的 SchemaTableCreator
        creator = SchemaTableCreator(schema_file_path=schema_file, conn=get_memory_conn)
        
        # 验证依赖注入成功，conn 属性被正确设置
        assert creator.conn == get_memory_conn
        
        # 验证可以成功创建表
        result = creator.create_table(TableName.STOCK_BASIC.value)
        assert result is True

    def test_dependency_injection_different_connections(self, schema_file):
        """测试可以注入不同的连接函数"""
        from downloader.database.db_connection import get_memory_conn, get_conn
        
        # 创建使用内存数据库的实例
        creator_memory = SchemaTableCreator(schema_file_path=schema_file, conn=get_memory_conn)
        assert creator_memory.conn == get_memory_conn
        
        # 创建使用默认连接的实例
        creator_default = SchemaTableCreator(schema_file_path=schema_file, conn=get_conn)
        assert creator_default.conn == get_conn
        
        # 验证两个实例使用不同的连接函数
        assert creator_memory.conn != creator_default.conn
        
        # 验证都可以成功创建表
        result1 = creator_memory.create_table(TableName.STOCK_BASIC.value)
        assert result1 is True

    def test_create_all_tables(self, schema_file):
        """测试创建所有表的功能"""
        from downloader.database.db_connection import get_memory_conn
        
        creator = SchemaTableCreator(schema_file_path=schema_file, conn=get_memory_conn)
        
        # 调用创建所有表的方法
        results = creator.create_all_tables()
        
        # 验证返回结果是字典
        assert isinstance(results, dict)
        
        # 验证包含了schema中定义的表
        assert "stock_basic" in results
        assert "stock_daily" in results
        
        # 验证这些表都创建成功
        assert results["stock_basic"] is True
        assert results["stock_daily"] is True
        
        # 验证表确实被创建了
        with get_memory_conn() as conn:
            # 检查表是否存在
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            assert "stock_basic" in table_names
            assert "stock_daily" in table_names

    def test_create_all_tables_with_missing_schema(self, schema_file):
        """测试当某些表在schema中不存在时的处理"""
        from downloader.database.db_connection import get_memory_conn
        import tempfile
        import os
        
        # 创建一个只包含部分表的schema文件
        partial_schema_content = """
[tables.stock_basic]
table_name = "stock_basic"
primary_key = ["ts_code"]
description = "股票基本信息表字段"
columns = [
    { name = "ts_code", type = "TEXT" },
    { name = "symbol", type = "TEXT" },
    { name = "name", type = "TEXT" }
]
"""
        
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(partial_schema_content)
            partial_schema_file = f.name
        
        try:
            creator = SchemaTableCreator(schema_file_path=partial_schema_file, conn=get_memory_conn)
            results = creator.create_all_tables()
            
            # 验证只有存在于schema中的表创建成功
            assert results["stock_basic"] is True
            
            # 验证不存在于schema中的表标记为失败
            assert results["stock_daily"] is False
            assert results["stock_adj_qfq"] is False
            
        finally:
            # 清理临时文件
            os.unlink(partial_schema_file)
