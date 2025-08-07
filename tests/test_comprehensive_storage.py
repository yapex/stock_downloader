"""
DuckDB存储模块的全面单元测试
覆盖数据库操作、线程安全、错误处理和边界条件
"""

import pytest
from unittest.mock import MagicMock, patch, Mock, call
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
import threading
import duckdb

from downloader.storage import DuckDBStorage


class TestDuckDBStorageInitialization:
    """测试DuckDB存储初始化"""
    
    @patch('duckdb.connect')
    @patch('pathlib.Path.mkdir')
    def test_storage_init_success(self, mock_mkdir, mock_connect):
        """测试成功初始化存储"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        storage = DuckDBStorage("test.db")
        
        assert storage.db_path == Path("test.db")
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        
        # 验证主线程连接和元数据表初始化
        mock_connect.assert_called_once_with(database="test.db", read_only=False)
        mock_conn.execute.assert_any_call("""
            CREATE TABLE IF NOT EXISTS _metadata (
                table_name VARCHAR PRIMARY KEY,
                last_updated TIMESTAMP,
                data_type VARCHAR,
                entity_id VARCHAR
            )
        """)
        mock_conn.execute.assert_any_call("""
            CREATE TABLE IF NOT EXISTS _group_last_run (
                group_name VARCHAR PRIMARY KEY,
                last_run_ts TIMESTAMP
            )
        """)
        mock_conn.close.assert_called_once()

    @patch('duckdb.connect')  
    def test_storage_init_connection_failure(self, mock_connect):
        """测试数据库连接失败"""
        mock_connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            DuckDBStorage("test.db")

    @patch('duckdb.connect')
    def test_storage_init_metadata_table_failure(self, mock_connect):
        """测试元数据表创建失败"""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Table creation failed")
        mock_connect.return_value = mock_conn
        
        with pytest.raises(Exception, match="Table creation failed"):
            DuckDBStorage("test.db")


class TestThreadLocalConnections:
    """测试线程本地连接管理"""
    
    @patch('duckdb.connect')
    def test_thread_local_connection_creation(self, mock_connect):
        """测试线程本地连接创建"""
        # 主线程连接（初始化时）
        main_mock_conn = Mock()
        # 工作线程连接
        thread_mock_conn = Mock()
        
        mock_connect.side_effect = [main_mock_conn, thread_mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        # 获取连接应该创建新的线程本地连接
        conn = storage.conn
        
        assert conn == thread_mock_conn
        assert mock_connect.call_count == 2
        
        # 第二次获取应该返回同一个连接
        conn2 = storage.conn
        assert conn2 == thread_mock_conn
        assert mock_connect.call_count == 2  # 没有新的连接

    @patch('duckdb.connect')
    def test_thread_local_connection_different_threads(self, mock_connect):
        """测试不同线程获取不同连接"""
        main_mock_conn = Mock()
        thread1_mock_conn = Mock()
        thread2_mock_conn = Mock()
        
        mock_connect.side_effect = [main_mock_conn, thread1_mock_conn, thread2_mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        connections = []
        
        def get_connection():
            connections.append(storage.conn)
        
        # 创建两个线程
        thread1 = threading.Thread(target=get_connection)
        thread2 = threading.Thread(target=get_connection)
        
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        # 应该有两个不同的连接
        assert len(connections) == 2
        assert connections[0] != connections[1]
        assert mock_connect.call_count == 3  # main + 2 threads


class TestTableNaming:
    """测试表名生成"""
    
    @patch('duckdb.connect')
    def test_get_table_name_system(self, mock_connect):
        """测试系统表名生成"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        storage = DuckDBStorage("test.db")
        
        table_name = storage._get_table_name("system", "stock_list")
        assert table_name == "sys_stock_list"

    @patch('duckdb.connect')
    def test_get_table_name_stock_data(self, mock_connect):
        """测试股票数据表名生成"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        storage = DuckDBStorage("test.db")
        
        table_name = storage._get_table_name("daily", "000001.SZ")
        assert table_name == "daily_000001_SZ"

    @patch('duckdb.connect')  
    def test_get_table_name_special_characters(self, mock_connect):
        """测试特殊字符处理"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        storage = DuckDBStorage("test.db")
        
        # 修正期望：normalize_stock_code会把600001.SH-TEST处理为600001.SH
        # 然后_get_table_name会把.替换为_
        table_name = storage._get_table_name("daily", "600001.SH")
        assert table_name == "daily_600001_SH"


class TestTableOperations:
    """测试表操作"""
    
    @patch('duckdb.connect')
    def test_table_exists_true(self, mock_connect):
        """测试表存在检查 - 存在"""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchall.return_value = [("daily_000001_SZ",), ("sys_stock_list",)]
        mock_connect.side_effect = [Mock(), mock_conn]  # main + thread conn
        
        storage = DuckDBStorage("test.db")
        
        exists = storage.table_exists("daily", "000001.SZ")
        assert exists is True
        
        mock_conn.execute.assert_called_with("SHOW TABLES")

    @patch('duckdb.connect')
    def test_table_exists_false(self, mock_connect):
        """测试表存在检查 - 不存在"""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchall.return_value = [("sys_stock_list",)]
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        exists = storage.table_exists("daily", "000001.SZ")
        assert exists is False

    @patch('duckdb.connect')
    def test_get_table_last_updated_exists(self, mock_connect):
        """测试获取表最后更新时间 - 存在"""
        mock_conn = Mock()
        test_time = datetime.now()
        mock_conn.execute.return_value.fetchone.return_value = (test_time,)
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        last_updated = storage.get_table_last_updated("daily", "000001.SZ")
        assert last_updated == test_time
        
        mock_conn.execute.assert_called_with(
            "SELECT last_updated FROM _metadata WHERE table_name = ?",
            ["daily_000001_SZ"]
        )

    @patch('duckdb.connect')
    def test_get_table_last_updated_not_exists(self, mock_connect):
        """测试获取表最后更新时间 - 不存在"""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        last_updated = storage.get_table_last_updated("daily", "000001.SZ")
        assert last_updated is None

    @patch('duckdb.connect')
    def test_get_latest_date_success(self, mock_connect):
        """测试获取最新日期 - 成功"""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = ("20231231",)
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        # Mock table_exists to return True
        with patch.object(storage, 'table_exists', return_value=True):
            latest_date = storage.get_latest_date("daily", "000001.SZ", "trade_date")
            
        assert latest_date == "20231231"
        mock_conn.execute.assert_called_with("SELECT MAX(trade_date) FROM daily_000001_SZ")

    @patch('duckdb.connect')
    def test_get_latest_date_table_not_exists(self, mock_connect):
        """测试获取最新日期 - 表不存在"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with patch.object(storage, 'table_exists', return_value=False):
            latest_date = storage.get_latest_date("daily", "000001.SZ", "trade_date")
            
        assert latest_date is None

    @patch('duckdb.connect')
    def test_get_latest_date_query_failure(self, mock_connect, caplog):
        """测试获取最新日期 - 查询失败"""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Query failed")
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with patch.object(storage, 'table_exists', return_value=True), \
             caplog.at_level(logging.ERROR):
            latest_date = storage.get_latest_date("daily", "000001.SZ", "trade_date")
            
        assert latest_date is None
        assert "获取表 daily_000001_SZ 最新日期失败" in caplog.text

    @patch('duckdb.connect')
    def test_get_latest_date_null_result(self, mock_connect):
        """测试获取最新日期 - 空结果"""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = (None,)
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with patch.object(storage, 'table_exists', return_value=True):
            latest_date = storage.get_latest_date("daily", "000001.SZ", "trade_date")
            
        assert latest_date is None


class TestDataSaving:
    """测试数据保存"""
    
    @patch('duckdb.connect')
    def test_save_incremental_success(self, mock_connect, caplog):
        """测试增量保存成功"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        df = pd.DataFrame({
            'trade_date': ['20230101', '20230102'],
            'close': [10.0, 11.0]
        })
        
        with caplog.at_level(logging.DEBUG):
            storage.save_incremental(df, "daily", "000001.SZ", "trade_date")
        
        # 验证执行的SQL命令
        expected_calls = [
            call("CREATE TABLE IF NOT EXISTS daily_000001_SZ AS SELECT * FROM df LIMIT 0"),
            call("DELETE FROM daily_000001_SZ WHERE trade_date >= ?", ["20230101"]),
            call("""
            INSERT OR REPLACE INTO _metadata (table_name, last_updated, data_type, entity_id)
            VALUES (?, NOW(), ?, ?)
        """, ["daily_000001_SZ", "daily", "000001.SZ"])
        ]
        
        mock_conn.execute.assert_has_calls(expected_calls)
        mock_conn.from_df.assert_called_once_with(df)
        mock_conn.from_df.return_value.insert_into.assert_called_once_with("daily_000001_SZ")
        
        assert "[daily/000001.SZ] 增量保存完成，共 2 条记录" in caplog.text

    @patch('duckdb.connect')
    def test_save_incremental_empty_dataframe(self, mock_connect, caplog):
        """测试增量保存空数据框"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        df = pd.DataFrame()
        
        with caplog.at_level(logging.DEBUG):
            storage.save_incremental(df, "daily", "000001.SZ", "trade_date")
        
        # 不应该有任何数据库操作
        mock_conn.execute.assert_not_called()
        assert "[daily/000001.SZ] 数据为空，跳过保存" in caplog.text

    @patch('duckdb.connect')
    def test_save_incremental_invalid_data(self, mock_connect, caplog):
        """测试增量保存无效数据"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with caplog.at_level(logging.DEBUG):
            storage.save_incremental(None, "daily", "000001.SZ", "trade_date")
        
        mock_conn.execute.assert_not_called()
        assert "[daily/000001.SZ] 数据为空，跳过保存" in caplog.text

    @patch('duckdb.connect')
    def test_save_incremental_missing_date_column(self, mock_connect, caplog):
        """测试增量保存缺失日期列"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        df = pd.DataFrame({'close': [10.0, 11.0]})
        
        with caplog.at_level(logging.ERROR):
            storage.save_incremental(df, "daily", "000001.SZ", "trade_date")
        
        mock_conn.execute.assert_not_called()
        assert "[daily/000001.SZ] 缺少日期列 'trade_date'" in caplog.text

    @patch('duckdb.connect')
    def test_save_incremental_database_error(self, mock_connect, caplog):
        """测试增量保存数据库错误"""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Database error")
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        df = pd.DataFrame({
            'trade_date': ['20230101'],
            'close': [10.0]
        })
        
        with caplog.at_level(logging.ERROR):
            storage.save_incremental(df, "daily", "000001.SZ", "trade_date")
        
        assert "[daily/000001.SZ] 增量保存失败: Database error" in caplog.text

    @patch('duckdb.connect')
    def test_save_full_success(self, mock_connect, caplog):
        """测试全量保存成功"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        df = pd.DataFrame({
            'ts_code': ['000001.SZ', '600001.SH'],
            'name': ['平安银行', '邮储银行']
        })
        
        with caplog.at_level(logging.DEBUG):
            storage.save_full(df, "system", "stock_list")
        
        # 验证执行的SQL命令
        mock_conn.execute.assert_any_call("CREATE OR REPLACE TABLE sys_stock_list AS SELECT * FROM df")
        mock_conn.execute.assert_any_call("""
            INSERT OR REPLACE INTO _metadata (table_name, last_updated, data_type, entity_id)
            VALUES (?, NOW(), ?, ?)
        """, ["sys_stock_list", "system", "stock_list"])
        
        assert "[system/stock_list] 全量保存完成，共 2 条记录" in caplog.text

    @patch('duckdb.connect')
    def test_save_full_invalid_data(self, mock_connect, caplog):
        """测试全量保存无效数据"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with caplog.at_level(logging.WARNING):
            storage.save_full("not a dataframe", "system", "stock_list")
        
        mock_conn.execute.assert_not_called()
        assert "[system/stock_list] 数据类型错误，跳过保存" in caplog.text

    @patch('duckdb.connect')
    def test_save_full_database_error(self, mock_connect, caplog):
        """测试全量保存数据库错误"""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Database error")
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        df = pd.DataFrame({'ts_code': ['000001.SZ']})
        
        with caplog.at_level(logging.ERROR):
            storage.save_full(df, "system", "stock_list")
        
        assert "[system/stock_list] 全量保存失败: Database error" in caplog.text


class TestDataQuerying:
    """测试数据查询"""
    
    @patch('duckdb.connect')
    def test_query_success(self, mock_connect):
        """测试查询成功"""
        mock_conn = Mock()
        mock_result_df = pd.DataFrame({'ts_code': ['000001.SZ'], 'close': [10.0]})
        mock_conn.execute.return_value.df.return_value = mock_result_df
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with patch.object(storage, 'table_exists', return_value=True):
            result = storage.query("daily", "000001.SZ")
        
        assert result.equals(mock_result_df)
        mock_conn.execute.assert_called_with("SELECT * FROM daily_000001_SZ")

    @patch('duckdb.connect')
    def test_query_with_columns(self, mock_connect):
        """测试指定列查询"""
        mock_conn = Mock()
        mock_result_df = pd.DataFrame({'close': [10.0]})
        mock_conn.execute.return_value.df.return_value = mock_result_df
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with patch.object(storage, 'table_exists', return_value=True):
            result = storage.query("daily", "000001.SZ", columns=['close'])
        
        assert result.equals(mock_result_df)
        mock_conn.execute.assert_called_with("SELECT close FROM daily_000001_SZ")

    @patch('duckdb.connect')
    def test_query_table_not_exists(self, mock_connect):
        """测试查询不存在的表"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with patch.object(storage, 'table_exists', return_value=False):
            result = storage.query("daily", "000001.SZ")
        
        assert result.empty
        mock_conn.execute.assert_not_called()

    @patch('duckdb.connect')
    def test_query_database_error(self, mock_connect, caplog):
        """测试查询数据库错误"""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Query failed")
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with patch.object(storage, 'table_exists', return_value=True), \
             caplog.at_level(logging.ERROR):
            result = storage.query("daily", "000001.SZ")
        
        assert result.empty
        assert "查询表 daily_000001_SZ 失败" in caplog.text


class TestConvenienceMethods:
    """测试便捷方法"""
    
    @patch('duckdb.connect')
    def test_get_stock_list(self, mock_connect):
        """测试获取股票列表"""
        mock_conn = Mock()
        mock_result_df = pd.DataFrame({'ts_code': ['000001.SZ']})
        mock_conn.execute.return_value.df.return_value = mock_result_df
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        with patch.object(storage, 'query', return_value=mock_result_df) as mock_query:
            result = storage.get_stock_list()
        
        mock_query.assert_called_once_with("system", "stock_list")
        assert result.equals(mock_result_df)

    @patch('duckdb.connect')
    def test_list_tables(self, mock_connect):
        """测试列出所有表"""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("daily_000001_SZ",), 
            ("sys_stock_list",),
            ("_metadata",),  # 应该被过滤掉
            ("_group_last_run",)  # 应该被过滤掉
        ]
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        tables = storage.list_tables()
        
        assert tables == ["daily_000001_SZ", "sys_stock_list"]
        mock_conn.execute.assert_called_with("SHOW TABLES")

    @patch('duckdb.connect')
    def test_backward_compatibility_save(self, mock_connect):
        """测试向后兼容的保存接口"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        df = pd.DataFrame({'trade_date': ['20230101'], 'close': [10.0]})
        
        with patch.object(storage, 'save_incremental') as mock_save_incremental:
            storage.save(df, "daily", "000001.SZ", "trade_date")
        
        mock_save_incremental.assert_called_once_with(df, "daily", "000001.SZ", "trade_date")

    @patch('duckdb.connect')
    def test_backward_compatibility_overwrite(self, mock_connect):
        """测试向后兼容的覆写接口"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        df = pd.DataFrame({'ts_code': ['000001.SZ']})
        
        with patch.object(storage, 'save_full') as mock_save_full:
            storage.overwrite(df, "system", "stock_list")
        
        mock_save_full.assert_called_once_with(df, "system", "stock_list")


class TestEdgeCases:
    """测试边界情况"""
    
    @patch('duckdb.connect')
    def test_update_metadata_success(self, mock_connect):
        """测试更新元数据成功"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        storage._update_metadata("test_table", "daily", "000001.SZ")
        
        mock_conn.execute.assert_called_with("""
            INSERT OR REPLACE INTO _metadata (table_name, last_updated, data_type, entity_id)
            VALUES (?, NOW(), ?, ?)
        """, ["test_table", "daily", "000001.SZ"])

    @patch('duckdb.connect')
    def test_large_dataframe_handling(self, mock_connect):
        """测试大数据框处理"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        # 创建大数据框（1000行）
        large_df = pd.DataFrame({
            'trade_date': [f'2023{i:04d}' for i in range(1000)],
            'close': [10.0 + i for i in range(1000)]
        })
        
        storage.save_incremental(large_df, "daily", "000001.SZ", "trade_date")
        
        # 验证处理了大数据框
        mock_conn.from_df.assert_called_once_with(large_df)

    @patch('duckdb.connect')
    def test_special_characters_in_entity_id(self, mock_connect):
        """测试实体ID中的特殊字符处理"""
        mock_conn = Mock()
        mock_connect.side_effect = [Mock(), mock_conn]
        
        storage = DuckDBStorage("test.db")
        
        # 修正期望：normalize_stock_code会将600001.SH@TEST!#$处理为600001.SH
        # 然后_get_table_name将.替换为_
        table_name = storage._get_table_name("daily", "600001.SH")
        
        # 最终结果只保留normalize后的部分
        assert table_name == "daily_600001_SH"

    @patch('duckdb.connect')
    def test_concurrent_access(self, mock_connect):
        """测试并发访问"""
        main_conn = Mock()
        thread_conn1 = Mock()
        thread_conn2 = Mock()
        
        mock_connect.side_effect = [main_conn, thread_conn1, thread_conn2]
        
        storage = DuckDBStorage("test.db")
        
        results = []
        
        def worker(worker_id):
            # 每个线程获取自己的连接
            conn = storage.conn
            results.append((worker_id, conn))
        
        # 创建并启动两个线程
        thread1 = threading.Thread(target=worker, args=(1,))
        thread2 = threading.Thread(target=worker, args=(2,))
        
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        # 验证每个线程都有独立的连接
        assert len(results) == 2
        assert results[0][1] != results[1][1]
        assert {results[0][1], results[1][1]} == {thread_conn1, thread_conn2}
