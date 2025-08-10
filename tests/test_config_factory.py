#!/usr/bin/env python3
"""
测试配置工厂

提供测试环境下的存储配置，遵循DuckDB连接管理最佳实践。
"""

from typing import Optional
from pathlib import Path

from downloader.storage import PartitionedStorage
from downloader.database import DuckDBConnectionFactory, DatabaseConnectionFactory
from downloader.logger_interface import LoggerFactory, LoggerInterface


class TestStorageFactory:
    """测试存储工厂
    
    专门为测试环境提供存储实例，默认使用内存数据库以提高测试速度和隔离性。
    """
    
    @staticmethod
    def create_memory_storage(
        logger: Optional[LoggerInterface] = None,
        db_factory: Optional[DatabaseConnectionFactory] = None
    ) -> PartitionedStorage:
        """创建内存数据库存储实例
        
        Args:
            logger: 日志接口，默认创建测试日志器
            db_factory: 数据库连接工厂，默认使用DuckDB工厂
            
        Returns:
            PartitionedStorage: 使用内存数据库的存储实例
        """
        if logger is None:
            logger = LoggerFactory.create_logger("test_memory_storage")
        if db_factory is None:
            db_factory = DuckDBConnectionFactory()
            
        # 使用:memory:创建内存数据库，每次都是全新环境
        return PartitionedStorage(":memory:", db_factory, logger)
    
    @staticmethod
    def create_temp_file_storage(
        temp_path: Path,
        logger: Optional[LoggerInterface] = None,
        db_factory: Optional[DatabaseConnectionFactory] = None
    ) -> PartitionedStorage:
        """创建临时文件数据库存储实例
        
        Args:
            temp_path: 临时文件路径
            logger: 日志接口，默认创建测试日志器
            db_factory: 数据库连接工厂，默认使用DuckDB工厂
            
        Returns:
            PartitionedStorage: 使用临时文件的存储实例
        """
        if logger is None:
            logger = LoggerFactory.create_logger("test_file_storage")
        if db_factory is None:
            db_factory = DuckDBConnectionFactory()
            
        return PartitionedStorage(str(temp_path), db_factory, logger)
    
    @staticmethod
    def create_test_config_with_memory_db() -> dict:
        """创建使用内存数据库的测试配置
        
        Returns:
            dict: 测试配置字典
        """
        return {
            "database": {
                "path": ":memory:"
            },
            "storage": {
                "db_path": ":memory:"
            },
            "downloader": {
                "symbols": ["000001.SZ", "000002.SZ"],
                "max_concurrent_tasks": 2
            },
            "tasks": [],
            "defaults": {}
        }
    
    @staticmethod
    def create_test_config_with_temp_file(temp_path: Path) -> dict:
        """创建使用临时文件的测试配置
        
        Args:
            temp_path: 临时文件路径
            
        Returns:
            dict: 测试配置字典
        """
        return {
            "database": {
                "path": str(temp_path)
            },
            "storage": {
                "db_path": str(temp_path)
            },
            "downloader": {
                "symbols": ["000001.SZ", "000002.SZ"],
                "max_concurrent_tasks": 2
            },
            "tasks": [],
            "defaults": {}
        }


class TestConnectionBestPractices:
    """连接管理最佳实践示例
    
    展示如何在不同场景下正确使用DuckDB连接。
    """
    
    @staticmethod
    def demonstrate_memory_db_usage():
        """演示内存数据库的正确使用方式"""
        # 创建内存存储 - 每次都是全新环境
        storage = TestStorageFactory.create_memory_storage()
        
        # 使用存储进行操作
        # 连接会在需要时自动创建，使用共享写连接
        
        return storage
    
    @staticmethod
    def demonstrate_read_write_separation():
        """演示读写连接分离的使用方式"""
        from downloader.database import DuckDBConnectionFactory
        import tempfile
        import os
        
        # 生成临时文件路径，但不预先创建文件
        temp_dir = tempfile.gettempdir()
        db_path = os.path.join(temp_dir, f"test_demo_{os.getpid()}.db")
        
        try:
            factory = DuckDBConnectionFactory()
            
            # 创建写连接（单例）
            write_conn = factory.create_for_writing(db_path)
            write_conn.execute("CREATE TABLE demo (id INTEGER, name TEXT)")
            write_conn.execute("INSERT INTO demo VALUES (1, 'test')")
            
            # 创建读连接（独立的短生命周期连接）
            read_conn = factory.create_for_reading(db_path)
            result = read_conn.execute("SELECT COUNT(*) FROM demo").fetchone()
            
            read_conn.close()
            return write_conn, read_conn, result[0]
        finally:
            # 清理临时文件
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    @staticmethod
    def demonstrate_thread_safety():
        """演示线程安全的连接使用
        
        基于DuckDB最佳实践，现在每次都创建新连接：
        - 每次调用都创建独立的连接
        - 连接使用完毕后立即关闭
        - 避免连接共享带来的线程安全问题
        """
        from downloader.database import DuckDBConnectionFactory
        
        factory = DuckDBConnectionFactory()
        
        # 每次都创建新连接
        results = []
        for i in range(3):
            conn = factory.create_for_writing(":memory:")
            conn.execute("CREATE TABLE IF NOT EXISTS test (value INTEGER)")
            conn.execute("INSERT INTO test VALUES (1)")
            result = conn.execute("SELECT COUNT(*) FROM test").fetchone()
            results.append((i, id(conn), result[0] if result else None))
            conn.close()
        
        return results
    
    @staticmethod
    def demonstrate_connection_lifecycle():
        """演示连接生命周期管理"""
        from downloader.database import DuckDBConnectionFactory
        
        factory = DuckDBConnectionFactory()
        
        # 每次调用都创建新连接
        conn1 = factory.create_for_writing(":memory:")
        conn2 = factory.create_for_writing(":memory:")
        
        # 现在每次都是新连接
        assert conn1 is not conn2
        
        # 使用完毕后需要手动关闭
        conn1.close()
        conn2.close()
        
        return factory


# 便捷函数
def create_test_memory_storage() -> PartitionedStorage:
    """创建测试用内存存储的便捷函数"""
    return TestStorageFactory.create_memory_storage()


def create_test_config_memory() -> dict:
    """创建内存数据库测试配置的便捷函数"""
    return TestStorageFactory.create_test_config_with_memory_db()


# 实际测试用例
def test_create_memory_storage():
    """测试创建内存存储"""
    storage = TestStorageFactory.create_memory_storage()
    assert storage is not None
    assert storage.db_path == Path(":memory:")


def test_create_temp_file_storage(tmp_path):
    """测试创建临时文件存储"""
    temp_file = tmp_path / "test.db"
    storage = TestStorageFactory.create_temp_file_storage(temp_file)
    assert storage is not None
    assert storage.db_path == temp_file


def test_memory_config_creation():
    """测试内存数据库配置创建"""
    config = TestStorageFactory.create_test_config_with_memory_db()
    assert config["database"]["path"] == ":memory:"
    assert config["storage"]["db_path"] == ":memory:"


def test_temp_file_config_creation(tmp_path):
    """测试临时文件配置创建"""
    temp_file = tmp_path / "test.db"
    config = TestStorageFactory.create_test_config_with_temp_file(temp_file)
    assert config["database"]["path"] == str(temp_file)
    assert config["storage"]["db_path"] == str(temp_file)


def test_connection_best_practices_memory_usage():
    """测试内存数据库使用最佳实践"""
    storage = TestConnectionBestPractices.demonstrate_memory_db_usage()
    assert storage is not None
    # 验证存储对象正常工作
    assert storage.db_path is not None


def test_connection_best_practices_thread_safety():
    """测试线程安全最佳实践"""
    results = TestConnectionBestPractices.demonstrate_thread_safety()
    assert len(results) == 3
    # 验证每次都创建新连接（即用即创，用完即弃）
    connection_ids = [conn_id for _, conn_id, _ in results]
    assert len(set(connection_ids)) == 3, "每次调用都应该创建新连接"
    # 验证所有操作都成功执行
    query_results = [result for _, _, result in results]
    assert all(result == 1 for result in query_results), "所有查询都应该成功"


def test_connection_best_practices_lifecycle():
    """测试连接生命周期最佳实践"""
    factory = TestConnectionBestPractices.demonstrate_connection_lifecycle()
    assert factory is not None


def test_convenience_functions():
    """测试便捷函数"""
    storage = create_test_memory_storage()
    assert storage is not None
    
    config = create_test_config_memory()
    assert config["database"]["path"] == ":memory:"


def test_read_write_separation():
    """测试读写分离功能"""
    write_conn, read_conn, count = TestConnectionBestPractices.demonstrate_read_write_separation()
    assert write_conn is not read_conn  # 现在是独立的连接
    assert count == 1  # 验证数据正确