"""SimpleDataProcessor 测试

测试同步数据处理器的核心功能
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, ANY

from src.neo.data_processor.simple_data_processor import SimpleDataProcessor
from src.neo.writers.interfaces import IParquetWriter


@pytest.fixture
def mock_parquet_writer() -> MagicMock:
    """模拟 Parquet 写入器"""
    mock = MagicMock(spec=IParquetWriter)
    mock.write.return_value = None
    mock.write_full_replace.return_value = None
    mock.write_full_replace_by_symbol.return_value = None
    return mock


@pytest.fixture
def sample_data() -> pd.DataFrame:
    """示例数据"""
    return pd.DataFrame(
        {
            "trade_date": ["2023-01-01", "2023-01-02"],
            "ts_code": ["600519.SH", "000001.SZ"],
            "value": [100.0, 200.0],
        }
    )


def test_create_default():
    """测试创建默认配置的同步数据处理器"""
    processor = SimpleDataProcessor.create_default()
    assert processor is not None
    assert processor.parquet_writer is not None


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy"
)
def test_process_success(
    mock_get_strategy, mock_parquet_writer: MagicMock, sample_data: pd.DataFrame
):
    """测试成功处理数据"""
    mock_get_strategy.return_value = "incremental"

    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process("stock_daily", "600519.SH", sample_data)

    assert result is True
    mock_parquet_writer.write.assert_called_once_with(ANY, "stock_daily", ["year"])


def test_process_empty_data(mock_parquet_writer: MagicMock):
    """测试处理空数据"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    empty_data = pd.DataFrame()
    result = processor.process("test_task", "600519.SH", empty_data)

    assert result is False
    mock_parquet_writer.write.assert_not_called()


def test_process_none_data(mock_parquet_writer: MagicMock):
    """测试处理 None 数据"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process("test_task", "600519.SH", None)

    assert result is False
    mock_parquet_writer.write.assert_not_called()


def test_shutdown(mock_parquet_writer: MagicMock):
    """测试关闭功能"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    processor.shutdown()


@pytest.fixture
def sample_data_with_end_date() -> pd.DataFrame:
    """示例数据，包含 end_date 列"""
    return pd.DataFrame(
        {
            "end_date": ["20221231", "20230331"],
            "ts_code": ["600000.SH", "000001.SZ"],
            "value": [100.0, 200.0],
        }
    )


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy"
)
def test_process_with_end_date(
    mock_get_strategy,
    mock_parquet_writer: MagicMock,
    sample_data_with_end_date: pd.DataFrame,
):
    """测试处理包含 end_date 列的数据"""
    mock_get_strategy.return_value = "incremental"

    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process("finance_report", "600000.SH", sample_data_with_end_date)

    assert result is True
    mock_parquet_writer.write.assert_called_once_with(ANY, "finance_report", ["year"])


@pytest.fixture
def sample_data_no_date_cols() -> pd.DataFrame:
    """示例数据，不包含日期列"""
    return pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy"
)
def test_process_no_date_columns(
    mock_get_strategy,
    mock_parquet_writer: MagicMock,
    sample_data_no_date_cols: pd.DataFrame,
):
    """测试处理不包含日期列的数据"""
    mock_get_strategy.return_value = "incremental"

    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process(
        "some_other_data", "any_symbol", sample_data_no_date_cols
    )

    assert result is True
    mock_parquet_writer.write.assert_called_once_with(
        sample_data_no_date_cols, "some_other_data", []
    )


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy"
)
def test_process_exception_handling(
    mock_get_strategy, mock_parquet_writer: MagicMock, sample_data: pd.DataFrame
):
    """测试 process 方法中的异常处理"""
    mock_get_strategy.return_value = "incremental"
    mock_parquet_writer.write.side_effect = Exception("Test write error")
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)

    with patch("src.neo.data_processor.simple_data_processor.logger") as mock_logger:
        result = processor.process("stock_daily", "600519.SH", sample_data)

        assert result is False
        mock_logger.error.assert_called_once()
        assert "Test write error" in mock_logger.error.call_args[0][0]


# --- 重构后的核心逻辑测试 ---


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._should_update_by_symbol"
)
@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy"
)
def test_full_replace_calls_global_method_when_not_by_symbol(
    mock_get_strategy,
    mock_should_update,
    mock_parquet_writer: MagicMock,
    sample_data: pd.DataFrame,
):
    """测试全量替换：当配置为不按 symbol 更新时，应调用全局替换方法"""
    mock_get_strategy.return_value = "full_replace"
    mock_should_update.return_value = False  # 模拟 update_by_symbol = false

    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process("stock_basic", "any_symbol", sample_data)

    assert result is True
    mock_parquet_writer.write_full_replace.assert_called_once_with(
        ANY, "stock_basic", ["year"]
    )
    mock_parquet_writer.write_full_replace_by_symbol.assert_not_called()


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._should_update_by_symbol"
)
@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy"
)
def test_full_replace_calls_symbol_method_when_by_symbol(
    mock_get_strategy,
    mock_should_update,
    mock_parquet_writer: MagicMock,
    sample_data: pd.DataFrame,
):
    """测试全量替换：当配置为按 symbol 更新时，应调用定向替换方法"""
    mock_get_strategy.return_value = "full_replace"
    mock_should_update.return_value = True  # 模拟 update_by_symbol = true
    symbol_to_test = "600519.SH"

    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process("stock_daily", symbol_to_test, sample_data)

    assert result is True
    mock_parquet_writer.write_full_replace_by_symbol.assert_called_once_with(
        ANY, "stock_daily", ["year"], symbol_to_test
    )
    mock_parquet_writer.write_full_replace.assert_not_called()


def test_create_default_with_writer(mock_parquet_writer):
    """测试 create_default 方法在提供了 writer 的情况下"""
    processor = SimpleDataProcessor.create_default(parquet_writer=mock_parquet_writer)
    assert processor.parquet_writer is mock_parquet_writer


def test_get_update_strategy_exception(mock_parquet_writer):
    """测试 _get_update_strategy 方法的异常处理"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    processor.config = MagicMock()
    processor.config.get.side_effect = Exception("Config error")
    strategy = processor._get_update_strategy("any_task")
    assert strategy == "incremental"


def test_should_update_by_symbol_exception(mock_parquet_writer):
    """测试 _should_update_by_symbol 方法的异常处理"""
    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    processor.config = MagicMock()
    processor.config.get.side_effect = Exception("Config error")
    result = processor._should_update_by_symbol("any_task")
    assert result is True


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._should_update_by_symbol"
)
@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy"
)
def test_process_no_date_full_replace_by_symbol(
    mock_get_strategy,
    mock_should_update,
    mock_parquet_writer: MagicMock,
    sample_data_no_date_cols: pd.DataFrame,
):
    """测试无日期列时，全量替换且按 symbol 更新"""
    mock_get_strategy.return_value = "full_replace"
    mock_should_update.return_value = True
    symbol_to_test = "any_symbol"

    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process(
        "some_other_data", symbol_to_test, sample_data_no_date_cols
    )

    assert result is True
    mock_parquet_writer.write_full_replace_by_symbol.assert_called_once_with(
        sample_data_no_date_cols, "some_other_data", [], symbol_to_test
    )


@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._should_update_by_symbol"
)
@patch(
    "src.neo.data_processor.simple_data_processor.SimpleDataProcessor._get_update_strategy"
)
def test_process_no_date_full_replace_not_by_symbol(
    mock_get_strategy,
    mock_should_update,
    mock_parquet_writer: MagicMock,
    sample_data_no_date_cols: pd.DataFrame,
):
    """测试无日期列时，全量替换且不按 symbol 更新"""
    mock_get_strategy.return_value = "full_replace"
    mock_should_update.return_value = False

    processor = SimpleDataProcessor(parquet_writer=mock_parquet_writer)
    result = processor.process(
        "some_other_data", "any_symbol", sample_data_no_date_cols
    )

    assert result is True
    mock_parquet_writer.write_full_replace.assert_called_once_with(
        sample_data_no_date_cols, "some_other_data", []
    )
