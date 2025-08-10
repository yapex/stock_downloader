"""测试数据库连接装饰器"""

import pytest
import tempfile
import os
from pathlib import Path

from downloader.database import (
    DuckDBConnectionFactory,
    DatabaseConnection,
    with_db_connection,
    with_read_connection,
    with_write_connection
)


class TestConnectionDecorator:
    """测试连接装饰器功能"""
    
    def test_with_db_connection_basic(self):
        """测试基本的连接装饰器功能"""
        
        @with_db_connection(":memory:")
        def create_and_query(conn: DatabaseConnection):
            conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO test VALUES (1, 'test')")
            result = conn.execute("SELECT COUNT(*) FROM test").fetchone()
            return result[0]
        
        count = create_and_query()
        assert count == 1
    
    def test_with_read_connection(self, tmp_path: Path):
        """测试只读连接装饰器"""
        db_path = str(tmp_path / "test_read.db")
        
        # 先创建数据
        @with_write_connection(db_path)
        def setup_data(conn: DatabaseConnection):
            conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO test VALUES (1, 'test'), (2, 'test2')")
        
        setup_data()
        
        # 然后读取数据
        @with_read_connection(db_path)
        def read_data(conn: DatabaseConnection):
            result = conn.execute("SELECT COUNT(*) FROM test").fetchone()
            return result[0]
        
        count = read_data()
        assert count == 2
    
    def test_with_write_connection(self, tmp_path: Path):
        """测试写连接装饰器"""
        db_path = str(tmp_path / "test_write.db")
        
        @with_write_connection(db_path)
        def create_and_insert(conn: DatabaseConnection):
            conn.execute("CREATE TABLE test (id INTEGER, value TEXT)")
            conn.execute("INSERT INTO test VALUES (1, 'first'), (2, 'second')")
            result = conn.execute("SELECT COUNT(*) FROM test").fetchone()
            return result[0]
        
        count = create_and_insert()
        assert count == 2
    
    def test_connection_auto_close(self):
        """测试连接自动关闭"""
        connection_ref = None
        
        @with_db_connection(":memory:")
        def capture_connection(conn: DatabaseConnection):
            nonlocal connection_ref
            connection_ref = conn
            conn.execute("CREATE TABLE test (id INTEGER)")
            return "success"
        
        result = capture_connection()
        assert result == "success"
        
        # 连接应该已经被关闭，尝试使用会失败
        with pytest.raises(Exception):
            connection_ref.execute("SELECT 1")
    
    def test_exception_handling(self):
        """测试异常情况下连接仍然被关闭"""
        connection_ref = None
        
        @with_db_connection(":memory:")
        def failing_function(conn: DatabaseConnection):
            nonlocal connection_ref
            connection_ref = conn
            conn.execute("CREATE TABLE test (id INTEGER)")
            raise ValueError("Test exception")
        
        with pytest.raises(ValueError, match="Test exception"):
            failing_function()
        
        # 即使发生异常，连接也应该被关闭
        with pytest.raises(Exception):
            connection_ref.execute("SELECT 1")
    
    def test_custom_factory(self):
        """测试使用自定义工厂"""
        factory = DuckDBConnectionFactory()
        
        @with_db_connection(":memory:", factory=factory)
        def test_with_custom_factory(conn: DatabaseConnection):
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.execute("INSERT INTO test VALUES (42)")
            result = conn.execute("SELECT id FROM test").fetchone()
            return result[0]
        
        value = test_with_custom_factory()
        assert value == 42
    
    def test_multiple_decorators_independence(self, tmp_path: Path):
        """测试多个装饰器的独立性"""
        db_path = str(tmp_path / "test_multi.db")
        
        @with_write_connection(db_path)
        def setup_table(conn: DatabaseConnection):
            conn.execute("CREATE TABLE test (id INTEGER, value TEXT)")
        
        @with_write_connection(db_path)
        def insert_data(conn: DatabaseConnection, value: str):
            conn.execute("INSERT INTO test VALUES (?, ?)", [1, value])
        
        @with_read_connection(db_path)
        def read_data(conn: DatabaseConnection):
            result = conn.execute("SELECT value FROM test WHERE id = 1").fetchone()
            return result[0] if result else None
        
        # 每个装饰器都使用独立的连接
        setup_table()
        insert_data("test_value")
        value = read_data()
        
        assert value == "test_value"
    
    def test_decorator_with_parameters(self):
        """测试装饰器传递参数"""
        
        @with_db_connection(":memory:")
        def function_with_params(conn: DatabaseConnection, table_name: str, value: int):
            conn.execute(f"CREATE TABLE {table_name} (id INTEGER)")
            conn.execute(f"INSERT INTO {table_name} VALUES (?)", [value])
            result = conn.execute(f"SELECT id FROM {table_name}").fetchone()
            return result[0]
        
        result = function_with_params("my_table", 123)
        assert result == 123