import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch

from downloader.database import (
    DuckDBConnectionFactory, 
    connect_db_ctx, 
    connect_db,
    with_db_connection,
    with_read_connection,
    with_write_connection
)


@pytest.fixture
def temp_db_path():
    """创建临时数据库文件路径"""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, 'test.db')
        yield db_path


@pytest.fixture
def connection_factory():
    """创建连接工厂实例"""
    return DuckDBConnectionFactory()


def test_connection_factory_initialization():
    """测试连接工厂初始化"""
    factory = DuckDBConnectionFactory()
    assert factory is not None


def test_create_connection_for_reading(connection_factory, temp_db_path):
    """测试创建读连接"""
    conn = connection_factory.create_for_reading(temp_db_path)
    assert conn is not None
    
    # 测试简单查询
    result = conn.execute("SELECT 1 as test").fetchone()
    assert result[0] == 1
    
    conn.close()


def test_create_connection_for_writing(connection_factory, temp_db_path):
    """测试创建写连接"""
    conn = connection_factory.create_for_writing(temp_db_path)
    assert conn is not None
    
    # 测试创建表和插入数据
    conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO test_table VALUES (1, 'test')")
    
    # 验证数据已插入
    result = conn.execute("SELECT * FROM test_table").fetchone()
    assert result[0] == 1
    assert result[1] == 'test'
    
    conn.close()


def test_connect_db_function(temp_db_path):
    """测试 connect_db 函数"""
    conn = connect_db(temp_db_path)
    assert conn is not None
    
    # 测试查询
    result = conn.execute("SELECT 1").fetchone()
    assert result[0] == 1
    
    conn.close()


def test_connect_db_memory():
    """测试内存数据库连接"""
    conn = connect_db(':memory:')
    assert conn is not None
    
    # 测试查询
    result = conn.execute("SELECT 1").fetchone()
    assert result[0] == 1
    
    conn.close()


def test_connect_db_ctx_context_manager(temp_db_path):
    """测试数据库连接上下文管理器"""
    with connect_db_ctx(temp_db_path) as conn:
        assert conn is not None
        result = conn.execute("SELECT 1").fetchone()
        assert result[0] == 1


def test_connect_db_ctx_with_exception(temp_db_path):
    """测试上下文管理器的异常处理"""
    try:
        with connect_db_ctx(temp_db_path) as conn:
            conn.execute("CREATE TABLE test_table (id INTEGER)")
            conn.execute("INSERT INTO test_table VALUES (1)")
            # 故意引发异常
            raise ValueError("Test exception")
    except ValueError:
        pass
    
    # 验证连接已正确关闭（通过创建新连接验证）
    with connect_db_ctx(temp_db_path) as conn:
        # 如果之前的连接没有正确关闭，这里可能会有问题
        result = conn.execute("SELECT 1").fetchone()
        assert result[0] == 1


def test_with_db_connection_decorator(temp_db_path):
    """测试数据库连接装饰器"""
    @with_db_connection(temp_db_path)
    def test_function(conn):
        return conn.execute("SELECT 42").fetchone()[0]
    
    result = test_function()
    assert result == 42


def test_with_read_connection_decorator(temp_db_path):
    """测试只读连接装饰器"""
    # 先创建一些数据
    with connect_db_ctx(temp_db_path) as conn:
        conn.execute("CREATE TABLE read_test (value INTEGER)")
        conn.execute("INSERT INTO read_test VALUES (123)")
    
    @with_read_connection(temp_db_path)
    def read_function(conn):
        return conn.execute("SELECT value FROM read_test").fetchone()[0]
    
    result = read_function()
    assert result == 123


def test_with_write_connection_decorator(temp_db_path):
    """测试写连接装饰器"""
    @with_write_connection(temp_db_path)
    def write_function(conn):
        conn.execute("CREATE TABLE write_test (value INTEGER)")
        conn.execute("INSERT INTO write_test VALUES (456)")
        return conn.execute("SELECT value FROM write_test").fetchone()[0]
    
    result = write_function()
    assert result == 456


def test_connection_register_dataframe(connection_factory, temp_db_path):
    """测试连接注册DataFrame功能"""
    import pandas as pd
    
    conn = connection_factory.create_for_writing(temp_db_path)
    
    # 创建测试DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C']
    })
    
    # 注册DataFrame
    conn.register('test_df', df)
    
    # 查询注册的DataFrame
    result = conn.execute("SELECT * FROM test_df ORDER BY id").fetchall()
    assert len(result) == 3
    assert result[0][0] == 1
    assert result[0][1] == 'A'
    
    # 取消注册
    conn.unregister('test_df')
    
    conn.close()


def test_connection_factory_multiple_connections(connection_factory, temp_db_path):
    """测试连接工厂创建多个连接"""
    # 创建多个读连接
    conn1 = connection_factory.create_for_reading(temp_db_path)
    conn2 = connection_factory.create_for_reading(temp_db_path)
    
    # 测试两个连接都能正常工作
    result1 = conn1.execute("SELECT 1").fetchone()[0]
    result2 = conn2.execute("SELECT 2").fetchone()[0]
    
    assert result1 == 1
    assert result2 == 2
    
    conn1.close()
    conn2.close()


def test_database_file_creation_with_directory():
    """测试数据库文件创建时需要手动创建目录"""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, 'subdir', 'test.db')
        
        # 确保目录不存在
        assert not os.path.exists(os.path.dirname(db_path))
        
        # 手动创建目录
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # 创建连接
        conn = connect_db(db_path)
        assert conn is not None
        
        # 验证目录和文件都存在
        assert os.path.exists(os.path.dirname(db_path))
        assert os.path.exists(db_path)
        
        conn.close()