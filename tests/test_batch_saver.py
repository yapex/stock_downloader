import pytest
import pandas as pd
from unittest.mock import Mock, patch
from contextlib import contextmanager
import duckdb

from downloader.consumer.batch_saver import BatchSaver
from downloader.database.db_oprator import DBOperator
from downloader.producer.fetcher_builder import TaskType
from pathlib import Path


class TestBatchSaver:
    @pytest.fixture
    def schema_file_path(self):
        return Path.cwd() / "stock_schema.toml"

    @pytest.fixture
    def db_operator(self, schema_file_path):
        # 为每个测试创建独立的内存数据库连接
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
    def batch_saver(self, db_operator):
        return BatchSaver(db_operator)

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

    def test_save_batch_success(self, batch_saver, sample_stock_basic_data):
        """测试成功保存批量数据"""
        # 先创建表
        batch_saver.db_operator.create_table("stock_basic")
        
        # 保存数据
        result = batch_saver.save_batch(TaskType.STOCK_BASIC, sample_stock_basic_data)
        
        assert result is True
        
        # 验证数据已保存
        with batch_saver.db_operator.conn() as conn:
            count_result = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()
            assert count_result[0] == 2

    def test_save_batch_empty_data(self, batch_saver):
        """测试保存空数据"""
        empty_data = pd.DataFrame()
        
        result = batch_saver.save_batch(TaskType.STOCK_BASIC, empty_data)
        
        assert result is True  # 空数据应该返回成功

    def test_save_batch_table_not_exists(self, batch_saver, sample_stock_basic_data):
        """测试表不存在时的错误处理"""
        # 不创建表，直接保存数据
        result = batch_saver.save_batch(TaskType.STOCK_BASIC, sample_stock_basic_data)
        
        assert result is False  # 应该返回失败

    def test_save_batch_database_error(self, batch_saver, sample_stock_basic_data):
        """测试数据库错误处理"""
        # Mock db_operator.upsert 抛出异常
        batch_saver.db_operator.upsert = Mock(side_effect=Exception("Database error"))
        
        result = batch_saver.save_batch(TaskType.STOCK_BASIC, sample_stock_basic_data)
        
        assert result is False
        batch_saver.db_operator.upsert.assert_called_once_with("stock_basic", sample_stock_basic_data)

    def test_save_batch_different_task_types(self, batch_saver, db_operator):
        """测试不同任务类型的保存"""
        # 创建股票日线数据
        daily_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240101"],
                "open": [10.0],
                "high": [11.0],
                "low": [9.5],
                "close": [10.5],
                "pre_close": [10.0],
                "change": [0.5],
                "pct_chg": [5.0],
                "vol": [1000000.0],
                "amount": [10500000.0],
            }
        )
        
        # 创建表
        db_operator.create_table("stock_daily")
        
        # 保存数据
        result = batch_saver.save_batch(TaskType.STOCK_DAILY, daily_data)
        
        assert result is True
        
        # 验证数据已保存
        with db_operator.conn() as conn:
            count_result = conn.execute("SELECT COUNT(*) FROM stock_daily").fetchone()
            assert count_result[0] == 1

    def test_batch_saver_initialization(self, db_operator):
        """测试 BatchSaver 初始化"""
        batch_saver = BatchSaver(db_operator)
        
        assert batch_saver.db_operator is db_operator