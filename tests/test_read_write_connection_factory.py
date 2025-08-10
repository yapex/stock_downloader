#!/usr/bin/env python3
"""
测试读写连接工厂

验证基于DuckDB最佳实践的读写连接分离实现。
"""

import pytest
import threading
import time
from pathlib import Path

from downloader.database import DuckDBConnectionFactory
from downloader.logger_interface import LoggerFactory


class TestReadWriteConnectionFactory:
    """测试读写连接工厂的实现"""
    
    def test_create_for_reading_short_lived_connections(self, tmp_path: Path):
        """测试读连接的短生命周期特性"""
        db_path = str(tmp_path / "test_read.db")
        factory = DuckDBConnectionFactory()
        
        # 先创建数据库文件
        write_conn = factory.create_for_writing(db_path)
        write_conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER)")
        write_conn.execute("INSERT INTO test VALUES (1)")
        write_conn.close()
        
        # 创建多个读连接请求 - 现在每次都是新连接
        read_conn1 = factory.create_for_reading(db_path)
        read_conn2 = factory.create_for_reading(db_path)
        
        # 在新的策略下，读连接应该是独立的（短生命周期）
        assert read_conn1 is not read_conn2, "读连接应该是独立的短生命周期连接"
        
        # 注意：由于DuckDB限制，读连接也是读写模式
        # 在应用层面应该控制只读行为，但连接本身支持写操作
        
        # 验证可以执行读操作
        result = read_conn1.execute("SELECT COUNT(*) FROM test").fetchone()
        assert result[0] == 1
        
        read_conn1.close()
        read_conn2.close()
    
    def test_create_for_writing_short_lived_connections(self, tmp_path: Path):
        """测试写连接的短生命周期特性"""
        db_path = str(tmp_path / "test_write.db")
        factory = DuckDBConnectionFactory()
        
        # 创建多个写连接请求
        write_conn1 = factory.create_for_writing(db_path)
        write_conn2 = factory.create_for_writing(db_path)
        
        # 现在每次都创建新连接（即用即创，用完即弃）
        assert write_conn1 is not write_conn2
        
        # 验证可以执行写操作
        write_conn1.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER)")
        write_conn1.execute("INSERT INTO test VALUES (1)")
        write_conn1.close()
        
        # 第二个连接可以看到第一个连接的数据
        result = write_conn2.execute("SELECT COUNT(*) FROM test").fetchone()
        assert result[0] == 1
        write_conn2.close()
    
    def test_different_databases_different_write_connections(self, tmp_path: Path):
        """测试不同数据库有不同的写连接"""
        db_path1 = str(tmp_path / "test1.db")
        db_path2 = str(tmp_path / "test2.db")
        factory = DuckDBConnectionFactory()
        
        # 不同数据库的写连接应该是不同的实例
        write_conn1 = factory.create_for_writing(db_path1)
        write_conn2 = factory.create_for_writing(db_path2)
        
        assert write_conn1 is not write_conn2
        
        # 验证可以独立操作
        write_conn1.execute("CREATE TABLE test1 (id INTEGER)")
        write_conn2.execute("CREATE TABLE test2 (id INTEGER)")
        
        write_conn1.execute("INSERT INTO test1 VALUES (1)")
        write_conn2.execute("INSERT INTO test2 VALUES (2)")
        
        # 验证数据隔离
        result1 = write_conn1.execute("SELECT id FROM test1").fetchone()
        result2 = write_conn2.execute("SELECT id FROM test2").fetchone()
        
        assert result1[0] == 1
        assert result2[0] == 2
    
    def test_thread_safety_write_connection(self, tmp_path: Path):
        """测试写连接的线程安全性"""
        db_path = str(tmp_path / "test_thread.db")
        factory = DuckDBConnectionFactory()
        results = []
        results_lock = threading.Lock()
        
        # 先创建表，避免并发创建冲突
        setup_conn = factory.create_for_writing(db_path)
        try:
            setup_conn.execute("CREATE TABLE thread_test (thread_name VARCHAR, value INTEGER)")
        finally:
            setup_conn.close()
        
        def get_write_connection():
            """在线程中获取写连接并执行操作"""
            conn = factory.create_for_writing(db_path)
            thread_name = threading.current_thread().name
            
            try:
                # 插入线程特定的数据
                thread_id = int(thread_name.split('-')[1])
                conn.execute("INSERT INTO thread_test VALUES (?, ?)", [thread_name, thread_id])
                
                # 验证数据插入成功
                result = conn.execute("SELECT COUNT(*) FROM thread_test WHERE thread_name = ?", [thread_name]).fetchone()
                
                with results_lock:
                    results.append((thread_name, result[0]))
            finally:
                # 使用完毕后关闭连接
                conn.close()
        
        # 创建多个线程同时请求写连接
        threads = []
        for i in range(3):
            thread = threading.Thread(target=get_write_connection, name=f"Thread-{i}")
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证每个线程都成功执行了独立的操作
        assert len(results) == 3
        for thread_name, count in results:
            assert count == 1, f"线程 {thread_name} 应该插入了1条记录"
        
        # 验证所有数据都被正确插入
        final_conn = factory.create_for_writing(db_path)
        try:
            total_count = final_conn.execute("SELECT COUNT(*) FROM thread_test").fetchone()[0]
            assert total_count == 3, "应该总共插入了3条记录"
        finally:
            final_conn.close()
    
    def test_mixed_read_write_operations(self, tmp_path: Path):
        """测试混合读写操作"""
        db_path = str(tmp_path / "test_mixed.db")
        factory = DuckDBConnectionFactory()
        
        # 获取写连接并初始化数据
        write_conn = factory.create_for_writing(db_path)
        write_conn.execute("CREATE TABLE mixed_test (id INTEGER, value TEXT)")
        write_conn.execute("INSERT INTO mixed_test VALUES (1, 'test1'), (2, 'test2')")
        write_conn.close()
        
        # 获取读连接并验证数据
        read_conn = factory.create_for_reading(db_path)
        result = read_conn.execute("SELECT COUNT(*) FROM mixed_test").fetchone()
        assert result[0] == 2
        read_conn.close()
        
        # 基于新策略，读写连接应该是不同的实例
        assert read_conn is not write_conn
        
        read_conn.close()
    
    def test_memory_database_connections(self):
        """测试内存数据库的连接管理"""
        factory = DuckDBConnectionFactory()
        
        # 创建内存数据库的写连接
        write_conn1 = factory.create_for_writing(":memory:")
        write_conn2 = factory.create_for_writing(":memory:")
        
        # 现在每次都创建新连接
        assert write_conn1 is not write_conn2
        
        # 创建读连接（独立的短生命周期连接）
        read_conn1 = factory.create_for_reading(":memory:")
        read_conn2 = factory.create_for_reading(":memory:")
        
        # 读连接应该是独立的
        assert read_conn1 is not read_conn2
        assert read_conn1 is not write_conn1
        
        # 验证可以执行操作
        write_conn1.execute("CREATE TABLE memory_test (id INTEGER)")
        write_conn1.execute("INSERT INTO memory_test VALUES (1)")
        
        result = write_conn1.execute("SELECT COUNT(*) FROM memory_test").fetchone()
        assert result[0] == 1
        
        # 关闭连接
        write_conn1.close()
        write_conn2.close()
        read_conn1.close()
        read_conn2.close()


class TestConnectionFactoryBestPractices:
    """测试连接工厂最佳实践"""
    
    def test_backward_compatibility(self, tmp_path: Path):
        """测试向后兼容性"""
        db_path1 = str(tmp_path / "test_compat1.db")
        db_path2 = str(tmp_path / "test_compat2.db")
        factory = DuckDBConnectionFactory()
        
        # 测试写连接
        conn1 = factory.create_connection(db_path1, read_only=False)
        conn1.execute("CREATE TABLE compat_test (id INTEGER)")
        conn1.execute("INSERT INTO compat_test VALUES (1)")
        conn1.close()
        
        # 先创建第二个数据库文件
        temp_conn = factory.create_connection(db_path2, read_only=False)
        temp_conn.execute("CREATE TABLE temp_test (id INTEGER)")
        temp_conn.close()
        
        # 然后以只读模式打开
        conn2 = factory.create_connection(db_path2, read_only=True)
        
        # 验证只读连接不能写入数据
        with pytest.raises(Exception):  # 只读连接不能写入
            conn2.execute("INSERT INTO temp_test VALUES (1)")
        
        conn2.close()
    
    def test_factory_initialization(self):
        """测试工厂初始化"""
        factory = DuckDBConnectionFactory()
        
        # 验证工厂可以正常创建连接
        conn = factory.create_for_writing(":memory:")
        assert conn is not None
        conn.close()
        
        # 验证每次调用都创建新连接
        conn1 = factory.create_for_reading(":memory:")
        conn2 = factory.create_for_reading(":memory:")
        assert conn1 is not conn2
        conn1.close()
        conn2.close()