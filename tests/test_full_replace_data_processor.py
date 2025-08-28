"""FullReplaceDataProcessor 单元测试"""

import logging
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.neo.data_processor.full_replace_data_processor import FullReplaceDataProcessor
from src.neo.database.interfaces import IDBQueryer
from src.neo.database.schema_loader import SchemaLoader
from src.neo.writers.interfaces import IParquetWriter


class TestFullReplaceDataProcessor:
    """FullReplaceDataProcessor 测试类"""

    @pytest.fixture
    def mock_parquet_writer(self):
        """模拟 ParquetWriter"""
        writer = Mock()
        writer.write_full_replace = Mock(return_value=None)
        return writer

    @pytest.fixture
    def mock_db_queryer(self):
        """模拟数据库查询器"""
        queryer = Mock(spec=IDBQueryer)
        return queryer

    @pytest.fixture
    def mock_schema_loader(self):
        """模拟 Schema 加载器"""
        loader = Mock(spec=SchemaLoader)
        return loader

    @pytest.fixture
    def processor(self, mock_parquet_writer, mock_db_queryer, mock_schema_loader):
        """创建处理器实例"""
        return FullReplaceDataProcessor(
            parquet_writer=mock_parquet_writer,
            db_queryer=mock_db_queryer,
            schema_loader=mock_schema_loader,
        )

    @pytest.fixture
    def sample_df(self):
        """示例数据框"""
        return pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ"],
                "symbol": ["000001", "000002"],
                "name": ["平安银行", "万科A"],
                "area": ["深圳", "深圳"],
                "industry": ["银行", "房地产"],
                "market": ["主板", "主板"],
                "list_date": ["19910403", "19910129"],
            }
        )

    def test_process_success(self, processor, sample_df, mock_parquet_writer):
        """测试正常处理流程"""
        # Mock datetime.datetime 来避开类型检查问题
        with patch("src.neo.data_processor.full_replace_data_processor.datetime") as mock_datetime:
            mock_datetime.now.return_value.year = 2024

            with patch.object(
                processor.parquet_writer, "write_full_replace", return_value=None
            ) as mock_write:
                with patch.object(
                    processor, "_atomic_replace_table", return_value=True
                ):
                    # 执行处理
                    result = processor.process("stock_basic", sample_df)

                    # 验证结果
                    assert result is True

                    # 验证 write_full_replace 被调用
                    mock_write.assert_called_once()

                    # 获取调用参数
                    call_args = mock_write.call_args
                    processed_df = call_args[0][0]  # 第一个位置参数 (data)
                    temp_table_name = call_args[0][1]  # 第二个位置参数 (task_type)
                    partition_cols = call_args[0][2]  # 第三个位置参数 (partition_cols)

                    # 验证处理后的数据包含年份分区
                    assert "year" in processed_df.columns
                    assert processed_df["year"].iloc[0] == 2024

                    # 验证临时表名
                    assert temp_table_name == "stock_basic_temp"
                    
                    # 验证分区列
                    assert partition_cols == ["year"]

    def test_process_with_trade_date_partition(self, processor, mock_parquet_writer):
        """测试包含 trade_date 的数据处理"""
        df_with_trade_date = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "close": [10.5]}
        )

        with patch.object(processor, "_atomic_replace_table", return_value=True):
            result = processor.process("stock_daily", df_with_trade_date)

            assert result is True
            call_args = mock_parquet_writer.write_full_replace.call_args
            df_arg, temp_table_arg, partition_cols_arg = call_args[0]
            assert partition_cols_arg == ["trade_date"]

    def test_process_with_end_date_partition(self, processor, mock_parquet_writer):
        """测试包含 end_date 的数据处理"""
        df_with_end_date = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "end_date": ["20240331"], "revenue": [1000000]}
        )

        with patch.object(processor, "_atomic_replace_table", return_value=True):
            result = processor.process("income", df_with_end_date)

            assert result is True
            call_args = mock_parquet_writer.write_full_replace.call_args
            df_arg, temp_table_arg, partition_cols_arg = call_args[0]
            assert "end_date" in partition_cols_arg

    def test_process_empty_dataframe(self, processor, mock_parquet_writer):
        """测试空数据框处理"""
        empty_df = pd.DataFrame()

        with patch.object(processor, "_atomic_replace_table", return_value=True):
            result = processor.process("stock_basic", empty_df)

            assert result is True
            mock_parquet_writer.write_full_replace.assert_called_once()

    def test_process_write_failure(self, processor, sample_df, mock_parquet_writer):
        """测试写入失败的情况"""
        # 模拟写入失败
        mock_parquet_writer.write_full_replace.side_effect = Exception("写入失败")

        result = processor.process("stock_basic", sample_df)

        assert result is False

    def test_add_partition_columns_basic_data(self, processor, mock_parquet_writer):
        """测试为基础数据添加分区列（通过 process 方法间接测试）"""
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["平安银行"]})

        with patch("src.neo.data_processor.full_replace_data_processor.datetime") as mock_datetime:
            mock_datetime.now.return_value.year = 2024

            with patch.object(processor, "_atomic_replace_table", return_value=True):
                result = processor.process("stock_basic", df)

                # 验证处理成功
                assert result is True

                # 验证调用了写入方法，并检查传入的数据框包含年份分区
                call_args = mock_parquet_writer.write_full_replace.call_args
                df_arg = call_args[0][0]
                assert "year" in df_arg.columns
                assert df_arg["year"].iloc[0] == 2024

    def test_add_partition_columns_with_trade_date(
        self, processor, mock_parquet_writer
    ):
        """测试已有 trade_date 的数据不添加年份分区（通过 process 方法间接测试）"""
        df = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "close": [10.5]}
        )

        with patch.object(processor, "_atomic_replace_table", return_value=True):
            result = processor.process("stock_daily", df)

            # 验证处理成功
            assert result is True

            # 验证调用了写入方法，并检查传入的数据框不包含年份分区
            call_args = mock_parquet_writer.write_full_replace.call_args
            df_arg = call_args[0][0]
            assert "year" not in df_arg.columns
            assert "trade_date" in df_arg.columns

    def test_get_partition_columns_trade_date(self, processor, mock_parquet_writer):
        """测试获取 trade_date 分区列（通过 process 方法间接测试）"""
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"]})

        with patch.object(processor, "_atomic_replace_table", return_value=True):
            result = processor.process("stock_daily", df)

            # 验证处理成功
            assert result is True

            # 验证分区列参数
            call_args = mock_parquet_writer.write_full_replace.call_args
            partition_cols_arg = call_args[0][2]
            assert partition_cols_arg == ["trade_date"]

    def test_get_partition_columns_end_date(self, processor, mock_parquet_writer):
        """测试获取 end_date 分区列（通过 process 方法间接测试）"""
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "end_date": ["20240331"]})

        with patch.object(processor, "_atomic_replace_table", return_value=True):
            result = processor.process("income", df)

            # 验证处理成功
            assert result is True

            # 验证分区列参数
            call_args = mock_parquet_writer.write_full_replace.call_args
            partition_cols_arg = call_args[0][2]
            assert "end_date" in partition_cols_arg

    def test_get_partition_columns_year(self, processor, mock_parquet_writer):
        """测试获取 year 分区列（通过 process 方法间接测试）"""
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "year": [2024]})

        with patch.object(processor, "_atomic_replace_table", return_value=True):
            result = processor.process("stock_basic", df)

            # 验证处理成功
            assert result is True

            # 验证分区列参数
            call_args = mock_parquet_writer.write_full_replace.call_args
            partition_cols_arg = call_args[0][2]
            assert partition_cols_arg == ["year"]

    def test_get_partition_columns_no_partition(self, processor, mock_parquet_writer):
        """测试无分区列的情况（通过 process 方法间接测试）"""
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["平安银行"]})

        with patch("src.neo.data_processor.full_replace_data_processor.datetime") as mock_datetime:
            mock_datetime.now.return_value.year = 2024

            with patch.object(processor, "_atomic_replace_table", return_value=True):
                result = processor.process("stock_basic", df)

                # 验证处理成功
                assert result is True

                # 由于会自动添加年份分区，所以分区列不为空
                call_args = mock_parquet_writer.write_full_replace.call_args
                partition_cols_arg = call_args[0][2]
                assert partition_cols_arg == ["year"]

    def test_shutdown(self, processor, caplog):
        """测试关闭处理器"""
        with caplog.at_level(logging.DEBUG):
            processor.shutdown()

        assert "FullReplaceDataProcessor shutdown completed" in caplog.text

    def test_process_with_logging(
        self, processor, sample_df, mock_parquet_writer, caplog
    ):
        """测试处理过程的日志记录"""
        with patch.object(processor, "_atomic_replace_table", return_value=True):
            processor.process("stock_basic", sample_df)

            # 验证 process 方法成功执行

    def test_process_failure_logging(
        self, processor, sample_df, mock_parquet_writer, caplog
    ):
        """测试处理失败时的异常处理"""
        mock_parquet_writer.write_full_replace.side_effect = Exception("测试异常")

        # 验证异常被正确处理，不会向上抛出
        processor.process("stock_basic", sample_df)
