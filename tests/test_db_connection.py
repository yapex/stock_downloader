import pytest
import threading
import time
from contextlib import contextmanager
from downloader2.db_connection import get_memory_conn


class TestDbConnection:
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
                assert result[0] == (1, 'test')
                
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
        assert 1 in connections
        assert 2 in connections
        assert connections[1] is not connections[2]

    def test_thread_isolation(self):
        """测试不同线程之间的数据隔离性"""
        results = {}
        
        def create_and_query_table(thread_id, table_name, data):
            with get_memory_conn() as conn:
                # 创建表并插入数据
                conn.execute(f"CREATE TABLE {table_name} (id INTEGER, value TEXT)")
                conn.execute(f"INSERT INTO {table_name} VALUES (?, ?)", (thread_id, data))
                
                # 查询数据
                result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
                results[thread_id] = result
                
                # 尝试查询其他线程的表（应该不存在）
                other_table = "thread1_table" if thread_id == 2 else "thread2_table"
                try:
                    conn.execute(f"SELECT * FROM {other_table}").fetchall()
                    results[f"{thread_id}_can_see_other"] = True
                except Exception:
                    results[f"{thread_id}_can_see_other"] = False
        
        # 创建两个线程，使用不同的表名和数据
        thread1 = threading.Thread(
            target=create_and_query_table, 
            args=(1, "thread1_table", "data1")
        )
        thread2 = threading.Thread(
            target=create_and_query_table, 
            args=(2, "thread2_table", "data2")
        )
        
        # 启动线程
        thread1.start()
        thread2.start()
        
        # 等待线程完成
        thread1.join()
        thread2.join()
        
        # 验证每个线程只能看到自己的数据
        assert results[1] == [(1, 'data1')]
        assert results[2] == [(2, 'data2')]
        
        # 验证线程间数据隔离
        assert results["1_can_see_other"] is False
        assert results["2_can_see_other"] is False

    def test_thread_local_persistence_within_thread(self):
        """测试同一线程内连接的持久性"""
        def test_persistence():
            # 第一次连接：创建表
            with get_memory_conn() as conn:
                conn.execute("CREATE TABLE persistent_test (id INTEGER)")
                conn.execute("INSERT INTO persistent_test VALUES (1)")
            
            # 第二次连接：验证数据仍然存在
            with get_memory_conn() as conn:
                result = conn.execute("SELECT COUNT(*) FROM persistent_test").fetchone()
                assert result[0] == 1
            
            # 第三次连接：添加更多数据
            with get_memory_conn() as conn:
                conn.execute("INSERT INTO persistent_test VALUES (2)")
                result = conn.execute("SELECT COUNT(*) FROM persistent_test").fetchone()
                assert result[0] == 2
        
        # 在单独的线程中运行测试
        thread = threading.Thread(target=test_persistence)
        thread.start()
        thread.join()

    def test_multiple_concurrent_threads(self):
        """测试多个并发线程的连接隔离"""
        num_threads = 5
        results = {}
        
        def worker(thread_id):
            with get_memory_conn() as conn:
                # 每个线程创建自己的表
                table_name = f"table_{thread_id}"
                conn.execute(f"CREATE TABLE {table_name} (thread_id INTEGER)")
                conn.execute(f"INSERT INTO {table_name} VALUES (?)", (thread_id,))
                
                # 验证只能看到自己的表
                result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
                results[thread_id] = result[0][0]
                
                # 验证看不到其他线程的表
                other_tables_visible = 0
                for other_id in range(num_threads):
                    if other_id != thread_id:
                        try:
                            conn.execute(f"SELECT * FROM table_{other_id}").fetchall()
                            other_tables_visible += 1
                        except Exception:
                            pass
                
                results[f"{thread_id}_other_visible"] = other_tables_visible
        
        # 创建并启动多个线程
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证结果
        for i in range(num_threads):
            assert results[i] == i  # 每个线程看到自己的数据
            assert results[f"{i}_other_visible"] == 0  # 看不到其他线程的表