import pandas as pd
import pytest
from pathlib import Path
import logging
from unittest.mock import Mock

from downloader.storage import DuckDBStorage
from downloader.database import DuckDBConnectionFactory
from downloader.logger_interface import LoggerFactory
from downloader.engine import DownloadEngine
from downloader.fetcher import TushareFetcher


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """提供一个临时的数据库文件路径，并确保测试后清理。"""
    return tmp_path / "test.db"


@pytest.fixture
def storage(db_path: Path) -> DuckDBStorage:
    """创建一个 DuckDBStorage 实例。"""
    db_factory = DuckDBConnectionFactory()
    logger = LoggerFactory.create_logger("test_storage")
    return DuckDBStorage(db_path, db_factory, logger)


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """提供一个用于测试的 DataFrame。"""
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": ["2023-01-01", "2023-01-02"],
            "close": [100.0, 101.0],
        }
    )


@pytest.fixture
def updated_df() -> pd.DataFrame:
    """提供一个用于测试更新操作的 DataFrame。"""
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": ["2023-01-02", "2023-01-03"],
            "close": [102.0, 103.0],  # 2023-01-02 的值已更新
        }
    )


def test_initialization(db_path: Path):
    """测试初始化时是否创建了数据库文件。"""
    assert not db_path.exists()
    db_factory = DuckDBConnectionFactory()
    logger = LoggerFactory.create_logger("test_storage")
    DuckDBStorage(db_path, db_factory, logger)
    assert db_path.exists()


# test_get_table_name 已移除，因为新的分区表架构不再需要动态表名生成


def test_save_and_get_latest_date(storage: DuckDBStorage, sample_df: pd.DataFrame):
    """测试基本的保存和获取最新日期的功能。"""
    storage.save_daily_data(sample_df)
    latest_date = storage.get_latest_date_by_stock("000001.SZ", "daily")
    assert latest_date == "2023-01-02"


def test_save_upsert_logic(
    storage: DuckDBStorage, sample_df: pd.DataFrame, updated_df: pd.DataFrame
):
    """测试增量保存（UPSERT）的逻辑是否正确。"""
    # 1. 初始保存
    storage.save_daily_data(sample_df)

    # 2. 使用新数据进行增量保存
    storage.save_daily_data(updated_df)

    # 3. 验证结果 - 使用新的分区表查询方法
    result_df = storage.query_daily_data_by_stock("000001.SZ")

    # 检查总行数是否正确（应该是 3 行）
    assert len(result_df) == 3
    # 检查 2023-01-02 的数据是否已更新
    assert result_df[result_df["trade_date"] == "2023-01-02"]["close"].iloc[0] == 102.0
    # 检查最新日期是否正确
    assert result_df["trade_date"].max() == "2023-01-03"


def test_save_daily_data_upsert(storage: DuckDBStorage, sample_df: pd.DataFrame):
    """测试日线数据的增量保存功能。"""
    # 先保存一些数据
    storage.save_daily_data(sample_df)

    # 创建一个新的 DataFrame 用于增量保存
    new_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["2024-01-01"],
            "close": [200.0],
        }
    )
    storage.save_daily_data(new_df)

    # 验证 - 使用新的分区表查询方法
    result_df = storage.query_daily_data_by_stock("000001.SZ")
    assert len(result_df) == 3  # 原有2条 + 新增1条
    # 验证新增的数据
    new_data = result_df[result_df["trade_date"] == "2024-01-01"]
    assert len(new_data) == 1
    assert new_data["close"].iloc[0] == 200.0


def test_get_latest_date_on_empty_table(storage: DuckDBStorage):
    """测试在空表或不存在的表上获取最新日期。"""
    latest_date = storage.get_latest_date_by_stock("999999.SZ", "daily")
    assert latest_date is None


def test_save_empty_dataframe(storage: DuckDBStorage):
    """测试保存空 DataFrame 时不应执行任何操作。"""
    empty_df = pd.DataFrame()
    storage.save_daily_data(empty_df)

    # 验证数据库中没有数据（分区表已预创建）
    result_df = storage.query_daily_data_by_stock("000001.SZ")
    assert len(result_df) == 0


def test_save_dataframe_without_date_col(storage: DuckDBStorage, caplog):
    """测试当 DataFrame 缺少日期列时应记录错误并跳过。"""
    df = pd.DataFrame({"close": [1.0, 2.0]})
    with caplog.at_level(logging.ERROR, logger="downloader.storage"):
        storage.save_daily_data(df)
        # 验证异常被正确处理

    # 验证数据库中没有数据（分区表已预创建）
    result_df = storage.query_daily_data_by_stock("000001.SZ")
    assert len(result_df) == 0


def test_engine_run_end_to_end(tmp_path: Path, monkeypatch):
    """测试 engine.run() 的端到端功能，使用 monkeypatch 伪造 fetcher 返回空 DataFrame。"""
    # 创建模拟的配置
    config = {
        "tasks": [{"name": "测试任务", "type": "test_task", "enabled": True}],
        "defaults": {},
        "downloader": {"symbols": ["000001.SZ"], "max_concurrent_tasks": 1},
    }

    # 创建模拟的 fetcher，返回空 DataFrame
    mock_fetcher = Mock(spec=TushareFetcher)
    mock_fetcher.fetch_stock_list.return_value = pd.DataFrame()
    mock_fetcher.fetch_daily_history.return_value = pd.DataFrame()

    # 创建真实的存储实例
    db_path = tmp_path / "test_engine.db"
    db_factory = DuckDBConnectionFactory()
    logger = LoggerFactory.create_logger("test_storage")
    storage = DuckDBStorage(db_path, db_factory, logger)

    # 创建引擎实例
    engine = DownloadEngine(
        config=config, fetcher=mock_fetcher, storage=storage, force_run=False
    )

    # 模拟任务处理器注册表为空（这样就不会真正执行任务）
    monkeypatch.setattr(engine, "task_registry", {})

    # 执行引擎，应该不会抛出异常
    try:
        engine.run()
        # 如果能到达这里，说明 run() 方法执行成功
        assert True
    except Exception as e:
        pytest.fail(f"engine.run() 应该能正常执行，但抛出异常: {e}")


def test_get_table_last_updated_system_type(storage: DuckDBStorage):
    """测试 get_table_last_updated 方法对 system 数据类型的支持"""
    # 创建测试用的股票列表数据
    stock_list_df = pd.DataFrame(
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

    # 保存股票列表数据
    storage.save_stock_list(stock_list_df)

    # 测试获取 system 类型的最后更新时间
    last_updated = storage.get_table_last_updated("system", "stock_list")

    # 验证返回的是 datetime 对象且不为 None
    assert last_updated is not None
    from datetime import datetime

    assert isinstance(last_updated, datetime)

    # 验证时间是最近的（在过去1分钟内）
    from datetime import timedelta

    now = datetime.now()
    assert now - last_updated < timedelta(minutes=1)


def test_database_connect_context_smoke():
    """冒烟测试：验证 connect_db 上下文管理器可用"""
    from downloader.database import connect_db

    with connect_db(":memory:") as conn:
        result = conn.execute("SELECT 1")
        # 验证连接可用，能执行简单查询
        assert result is not None


def test_get_stock_list_returns_only_ts_code(storage: DuckDBStorage):
    """测试 get_stock_list 方法只返回 ts_code 列"""
    # 创建测试用的股票列表数据
    stock_list_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000003.SZ"],
            "symbol": ["000001", "000002", "000003"],
            "name": ["平安银行", "万科A", "国农科技"],
            "area": ["深圳", "深圳", "深圳"],
            "industry": ["银行", "房地产", "农业"],
            "market": ["主板", "主板", "主板"],
            "list_date": ["19910403", "19910129", "19970515"],
        }
    )

    # 保存股票列表数据
    storage.save_stock_list(stock_list_df)

    # 调用 get_stock_list 方法
    result_df = storage.get_stock_list()

    # 验证返回的 DataFrame 只包含 ts_code 列
    assert list(result_df.columns) == ["ts_code"]

    # 验证返回的数据内容正确
    expected_ts_codes = ["000001.SZ", "000002.SZ", "000003.SZ"]
    assert result_df["ts_code"].tolist() == expected_ts_codes

    # 验证数据按 ts_code 排序
    assert result_df["ts_code"].is_monotonic_increasing


def test_save_financial_data(storage: DuckDBStorage):
    """测试保存财务数据功能"""
    # 创建测试用的财务数据
    financial_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "ann_date": ["20231201", "20231201"],
            "end_date": ["20230930", "20230930"],
            "total_revenue": [1000000, 2000000],
            "net_profit": [100000, 200000],
        }
    )

    # 保存财务数据
    result = storage.save_financial_data(financial_df)
    assert result is True

    # 验证数据已保存
    query_result = storage.query_financial_data_by_stock("000001.SZ")
    assert len(query_result) == 1
    assert query_result["total_revenue"].iloc[0] == 1000000


def test_save_financial_data_empty(storage: DuckDBStorage):
    """测试保存空财务数据"""
    empty_df = pd.DataFrame()
    result = storage.save_financial_data(empty_df)
    assert result is False


def test_save_financial_data_missing_fields(storage: DuckDBStorage):
    """测试保存缺少必需字段的财务数据"""
    invalid_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "total_revenue": [1000000],
            # 缺少 ann_date 和 end_date
        }
    )

    result = storage.save_financial_data(invalid_df)
    assert result is False


def test_save_daily_data_incremental(storage: DuckDBStorage):
    """测试增量保存日线数据"""
    daily_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20231201"],
            "close": [100.0],
            "open": [99.0],
        }
    )

    result = storage.save_daily_data_incremental(daily_df)
    assert result is True


def test_get_all_stock_codes(storage: DuckDBStorage):
    """测试获取所有股票代码"""
    # 先保存一些股票列表数据
    stock_list_df = pd.DataFrame(
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
    storage.save_stock_list(stock_list_df)

    # 获取所有股票代码
    codes = storage.get_all_stock_codes()
    assert len(codes) == 2
    assert "000001.SZ" in codes
    assert "000002.SZ" in codes


def test_get_summary(storage: DuckDBStorage):
    """测试获取数据摘要"""
    # 保存一些测试数据
    daily_df = pd.DataFrame(
        {"ts_code": ["000001.SZ"], "trade_date": ["20231201"], "close": [100.0]}
    )
    storage.save_daily_data(daily_df)

    # 获取摘要
    summary = storage.get_summary()
    assert isinstance(summary, list)
    assert len(summary) > 0
    assert "table_name" in summary[0]
    assert "record_count" in summary[0]


def test_list_business_tables(storage: DuckDBStorage):
    """测试列出业务表"""
    # 保存一些测试数据
    daily_df = pd.DataFrame(
        {"ts_code": ["000001.SZ"], "trade_date": ["20231201"], "close": [100.0]}
    )
    storage.save_daily_data(daily_df)

    # 列出业务表
    tables = storage.list_business_tables()
    assert isinstance(tables, list)


def test_save_fundamental_data(storage: DuckDBStorage):
    """测试保存基本面数据"""
    fundamental_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20231201"],
            "pe": [15.5],
            "pb": [1.2],
        }
    )

    result = storage.save_fundamental_data(fundamental_df)
    assert result is True

    # 验证数据已保存
    query_result = storage.query_fundamental_data_by_stock("000001.SZ")
    assert len(query_result) == 1
    assert query_result["pe"].iloc[0] == 15.5


def test_query_fundamental_data_by_stock_with_date_range(storage: DuckDBStorage):
    """测试按日期范围查询基本面数据"""
    # 先保存一些数据
    fundamental_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": ["20231201", "20231202"],
            "pe": [15.5, 16.0],
            "pb": [1.2, 1.3],
        }
    )
    storage.save_fundamental_data(fundamental_df)

    # 查询指定日期范围的数据
    result = storage.query_fundamental_data_by_stock(
        "000001.SZ", start_date="20231201", end_date="20231201"
    )
    assert len(result) == 1
    assert result["trade_date"].iloc[0] == "20231201"


def test_save_daily_data_empty_dataframe(storage: DuckDBStorage):
    """测试保存空的日线数据"""
    empty_df = pd.DataFrame()
    result = storage.save_daily_data(empty_df)
    assert result is False


def test_save_daily_data_invalid_input(storage: DuckDBStorage):
    """测试保存无效输入的日线数据"""
    result = storage.save_daily_data(None)
    assert result is False

    result = storage.save_daily_data("not a dataframe")
    assert result is False


def test_save_financial_data_empty_dataframe(storage: DuckDBStorage):
    """测试保存空的财务数据"""
    empty_df = pd.DataFrame()
    result = storage.save_financial_data(empty_df)
    assert result is False


def test_save_financial_data_invalid_input(storage: DuckDBStorage):
    """测试保存无效输入的财务数据"""
    result = storage.save_financial_data(None)
    assert result is False


def test_save_stock_list_empty_dataframe(storage: DuckDBStorage):
    """测试保存空的股票列表"""
    empty_df = pd.DataFrame()
    result = storage.save_stock_list(empty_df)
    assert result is False


def test_save_stock_list_invalid_input(storage: DuckDBStorage):
    """测试保存无效输入的股票列表"""
    result = storage.save_stock_list(None)
    assert result is False


def test_query_daily_data_by_stock_exception_handling(storage: DuckDBStorage):
    """测试查询日线数据时的异常处理"""
    # 查询不存在的股票
    result = storage.query_daily_data_by_stock("INVALID.CODE")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_query_daily_data_by_date_range_exception_handling(storage: DuckDBStorage):
    """测试按日期范围查询日线数据时的异常处理"""
    # 查询无效日期范围
    result = storage.query_daily_data_by_date_range("invalid_date", "invalid_date")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_query_financial_data_by_stock_exception_handling(storage: DuckDBStorage):
    """测试查询财务数据时的异常处理"""
    # 查询不存在的股票
    result = storage.query_financial_data_by_stock("INVALID.CODE")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_query_daily_data_by_stocks_exception_handling(storage: DuckDBStorage):
    """测试按股票列表查询日线数据时的异常处理"""
    # 查询不存在的股票列表
    result = storage.query_daily_data_by_stocks(["INVALID1.CODE", "INVALID2.CODE"])
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_get_latest_date_by_stock_unsupported_data_type(storage: DuckDBStorage):
    """测试获取最新日期时使用不支持的数据类型"""
    result = storage.get_latest_date_by_stock("000001.SZ", "unsupported_type")
    assert result is None


def test_get_latest_date_by_stock_exception_handling(storage: DuckDBStorage):
    """测试获取最新日期时的异常处理"""
    # 查询不存在的股票
    result = storage.get_latest_date_by_stock("INVALID.CODE", "daily")
    assert result is None


def test_batch_get_latest_dates_unsupported_data_type(storage: DuckDBStorage):
    """测试批量获取最新日期时使用不支持的数据类型"""
    result = storage.batch_get_latest_dates(["000001.SZ"], "unsupported_type")
    assert isinstance(result, dict)
    assert len(result) == 0


def test_batch_get_latest_dates_exception_handling(storage: DuckDBStorage):
    """测试批量获取最新日期时的异常处理"""
    # 查询不存在的股票列表
    result = storage.batch_get_latest_dates(["INVALID1.CODE", "INVALID2.CODE"], "daily")
    assert isinstance(result, dict)
    assert len(result) == 0


def test_query_daily_data_by_date_range_with_ts_codes(storage: DuckDBStorage):
    """测试按日期范围和股票代码查询日线数据"""
    # 准备测试数据
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20230101", "20230101"],
            "close": [100.0, 200.0],
        }
    )
    storage.save_daily_data(df)

    # 查询特定股票的数据
    result = storage.query_daily_data_by_date_range(
        "20230101", "20230101", ["000001.SZ"]
    )
    assert len(result) == 1
    assert result["ts_code"].iloc[0] == "000001.SZ"


def test_query_daily_data_by_stock_with_date_range(storage: DuckDBStorage):
    """测试按股票查询日线数据（带日期范围）"""
    # 准备测试数据
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "trade_date": ["20230101", "20230102", "20230103"],
            "close": [100.0, 101.0, 102.0],
        }
    )
    storage.save_daily_data(df)

    # 查询指定日期范围的数据
    result = storage.query_daily_data_by_stock("000001.SZ", "20230102", "20230103")
    assert len(result) == 2
    assert result["trade_date"].min() == "20230102"
    assert result["trade_date"].max() == "20230103"


def test_query_financial_data_by_stock_with_date_range(storage: DuckDBStorage):
    """测试按股票查询财务数据（带日期范围）"""
    # 准备测试数据
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "ann_date": ["20230331", "20230630"],
            "end_date": ["20230331", "20230630"],
            "revenue": [1000000, 1100000],
        }
    )
    storage.save_financial_data(df)

    # 查询指定日期范围的数据
    result = storage.query_financial_data_by_stock("000001.SZ", "20230331", "20230630")
    assert len(result) == 2


def test_query_daily_data_by_stocks_with_date_range(storage: DuckDBStorage):
    """测试按股票列表查询日线数据（带日期范围）"""
    # 准备测试数据
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000001.SZ"],
            "trade_date": ["20230101", "20230101", "20230102"],
            "close": [100.0, 200.0, 101.0],
        }
    )
    storage.save_daily_data(df)

    # 查询指定股票和日期范围的数据
    result = storage.query_daily_data_by_stocks(
        ["000001.SZ", "000002.SZ"], "20230101", "20230101"
    )
    assert len(result) == 2


def test_get_latest_date_by_stock_financial_type(storage: DuckDBStorage):
    """测试获取财务数据的最新日期"""
    # 准备测试数据
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "ann_date": ["20230331"],
            "end_date": ["20230331"],
            "revenue": [1000000],
        }
    )
    storage.save_financial_data(df)

    # 获取最新日期
    result = storage.get_latest_date_by_stock("000001.SZ", "financial")
    assert result == "20230331"


def test_get_latest_date_by_stock_fundamental_type(storage: DuckDBStorage):
    """测试获取基本面数据的最新日期"""
    # 准备测试数据
    df = pd.DataFrame(
        {"ts_code": ["000001.SZ"], "trade_date": ["20230101"], "pe": [10.5]}
    )
    storage.save_fundamental_data(df)

    # 获取最新日期
    result = storage.get_latest_date_by_stock("000001.SZ", "fundamental")
    assert result == "20230101"


def test_batch_get_latest_dates_financial_type(storage: DuckDBStorage):
    """测试批量获取财务数据的最新日期"""
    # 准备测试数据
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "ann_date": ["20230331", "20230331"],
            "end_date": ["20230331", "20230331"],
            "revenue": [1000000, 2000000],
        }
    )
    storage.save_financial_data(df)

    # 批量获取最新日期
    result = storage.batch_get_latest_dates(["000001.SZ", "000002.SZ"], "financial")
    assert len(result) == 2
    assert result["000001.SZ"] == "20230331"
    assert result["000002.SZ"] == "20230331"


def test_batch_get_latest_dates_fundamental_type(storage: DuckDBStorage):
    """测试批量获取基本面数据的最新日期"""
    # 准备测试数据
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20230101", "20230101"],
            "pe": [10.5, 15.2],
        }
    )
    storage.save_fundamental_data(df)

    # 批量获取最新日期
    result = storage.batch_get_latest_dates(["000001.SZ", "000002.SZ"], "fundamental")
    assert len(result) == 2
    assert result["000001.SZ"] == "20230101"
    assert result["000002.SZ"] == "20230101"


def test_save_financial_data_no_matching_columns(storage: DuckDBStorage):
    """测试保存财务数据时没有匹配的列"""
    # 创建一个没有任何匹配列的DataFrame
    df = pd.DataFrame({"invalid_column1": ["value1"], "invalid_column2": ["value2"]})

    result = storage.save_financial_data(df)
    assert result is False


def test_get_stock_list_exception_handling(storage: DuckDBStorage):
    """测试获取股票列表时的异常处理"""
    # 在没有数据的情况下获取股票列表
    result = storage.get_stock_list()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_save_daily_data_database_exception(storage: DuckDBStorage, monkeypatch):
    """测试保存日线数据时数据库异常"""
    from unittest.mock import Mock, patch

    # 模拟数据库连接异常
    mock_conn = Mock()
    mock_conn.execute.side_effect = Exception("Database error")

    with patch.object(storage._db_factory, "get_write_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20230101"], "close": [100.0]}
        )

        result = storage.save_daily_data(df)
        assert result is False


def test_save_financial_data_database_exception(storage: DuckDBStorage, monkeypatch):
    """测试保存财务数据时数据库异常"""
    from unittest.mock import Mock, patch

    # 模拟数据库连接异常
    mock_conn = Mock()
    mock_conn.execute.side_effect = Exception("Database error")

    with patch.object(storage._db_factory, "get_write_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "ann_date": ["20230331"],
                "end_date": ["20230331"],
                "revenue": [1000000],
            }
        )

        result = storage.save_financial_data(df)
        assert result is False


def test_save_stock_list_database_exception(storage: DuckDBStorage, monkeypatch):
    """测试保存股票列表时数据库异常"""
    from unittest.mock import Mock, patch

    # 模拟数据库连接异常
    mock_conn = Mock()
    mock_conn.execute.side_effect = Exception("Database error")

    with patch.object(storage._db_factory, "get_write_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "symbol": ["平安银行"],
                "name": ["平安银行"],
                "area": ["深圳"],
                "industry": ["银行"],
                "list_date": ["19910403"],
            }
        )

        result = storage.save_stock_list(df)
        assert result is False


def test_init_partitioned_tables_index_creation_warning(storage: DuckDBStorage, caplog):
    """测试分区表索引创建失败时的警告日志"""
    from unittest.mock import Mock, patch
    import logging

    # 模拟索引创建失败
    mock_conn = Mock()
    mock_conn.execute.side_effect = [
        None,
        None,
        None,
        None,
        None,
        Exception("Index creation failed"),
    ]

    with patch.object(storage._db_factory, "get_write_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        with caplog.at_level(logging.WARNING):
            storage._init_partitioned_tables()

        # 检查是否记录了警告日志
        assert any("创建分区表索引失败" in record.message for record in caplog.records)


def test_update_partition_metadata_exception_handling(storage: DuckDBStorage):
    """测试更新分区元数据时的异常处理"""
    from unittest.mock import Mock, patch

    # 模拟数据库连接异常
    mock_conn = Mock()
    mock_conn.execute.side_effect = Exception("Metadata update failed")

    with patch.object(storage._db_factory, "get_write_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20230101"]})

        # 这个方法应该不会抛出异常，而是静默处理
        try:
            storage._update_partition_metadata("daily_data", df)
        except Exception:
            pytest.fail(
                "_update_partition_metadata should handle exceptions gracefully"
            )


def test_save_fundamental_data_success(storage: DuckDBStorage):
    """测试保存基本面数据成功"""
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20230101"],
            "pe": [15.5],
            "pb": [1.2],
        }
    )

    result = storage.save_fundamental_data(df)
    assert result is True


def test_save_fundamental_data_empty_df(storage: DuckDBStorage):
    """测试保存空的基本面数据"""
    df = pd.DataFrame()
    result = storage.save_fundamental_data(df)
    assert result is False  # 空DataFrame应该返回False


def test_save_fundamental_data_exception(storage: DuckDBStorage):
    """测试保存基本面数据异常处理"""
    from unittest.mock import Mock, patch

    mock_conn = Mock()
    mock_conn.execute.side_effect = Exception("Database error")

    with patch.object(storage._db_factory, "get_write_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20230101"], "pe": [15.5]}
        )

        result = storage.save_fundamental_data(df)
        assert result is False


def test_query_fundamental_data_by_stock_success(storage: DuckDBStorage):
    """测试按股票查询基本面数据成功"""
    # 先保存一些基本面数据
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20230101"],
            "pe": [15.5],
            "pb": [1.2],
        }
    )
    storage.save_fundamental_data(df)

    # 查询数据
    result = storage.query_fundamental_data_by_stock("000001.SZ")
    assert not result.empty
    assert result.iloc[0]["ts_code"] == "000001.SZ"


def test_query_fundamental_data_by_stock_with_date_range_new(storage: DuckDBStorage):
    """测试按股票和日期范围查询基本面数据"""
    # 先保存一些基本面数据
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": ["20230101", "20230201"],
            "pe": [15.5, 16.0],
            "pb": [1.2, 1.3],
        }
    )
    storage.save_fundamental_data(df)

    # 查询指定日期范围的数据
    result = storage.query_fundamental_data_by_stock(
        "000001.SZ", start_date="20230101", end_date="20230131"
    )
    assert len(result) == 1
    assert result.iloc[0]["trade_date"] == "20230101"


def test_query_fundamental_data_by_stock_exception(storage: DuckDBStorage):
    """测试查询基本面数据异常处理"""
    from unittest.mock import Mock, patch

    mock_conn = Mock()
    mock_conn.execute.side_effect = Exception("Database error")

    with patch.object(storage._db_factory, "get_read_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        result = storage.query_fundamental_data_by_stock("000001.SZ")
        assert result.empty


def test_get_all_stock_codes_from_other_tables(storage: DuckDBStorage):
    """测试从其他表获取股票代码（当sys_stock_list为空时）"""
    # 确保sys_stock_list表为空，但其他表有数据
    daily_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20230101", "20230101"],
            "close": [10.0, 20.0],
        }
    )
    storage.save_daily_data(daily_df)

    financial_df = pd.DataFrame(
        {
            "ts_code": ["000003.SZ"],
            "ann_date": ["20230331"],
            "end_date": ["20230331"],
            "revenue": [1000000],
        }
    )
    storage.save_financial_data(financial_df)

    fundamental_df = pd.DataFrame(
        {"ts_code": ["000004.SZ"], "trade_date": ["20230101"], "pe": [15.5]}
    )
    storage.save_fundamental_data(fundamental_df)

    # 使用 get_all_stock_codes 方法应该包含所有表中的股票
    stock_codes = set(storage.get_all_stock_codes())

    expected_codes = {"000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"}
    assert expected_codes.issubset(stock_codes)


def test_get_summary_exception(storage: DuckDBStorage):
    """测试获取摘要信息异常处理"""
    from unittest.mock import Mock, patch

    mock_conn = Mock()
    mock_conn.execute.side_effect = Exception("Database error")

    with patch.object(storage._db_factory, "get_read_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        summary = storage.get_summary()
        # 异常时应该返回空的摘要信息
        assert isinstance(summary, list)
        assert len(summary) == 0


def test_list_business_tables_from_financial_and_fundamental(storage: DuckDBStorage):
    """测试从financial_data和fundamental_data表获取股票代码"""
    # 保存财务数据
    financial_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "ann_date": ["20230331"],
            "end_date": ["20230331"],
            "revenue": [1000000],
        }
    )
    storage.save_financial_data(financial_df)

    # 保存基本面数据
    fundamental_df = pd.DataFrame(
        {"ts_code": ["000002.SZ"], "trade_date": ["20230101"], "pe": [15.5]}
    )
    storage.save_fundamental_data(fundamental_df)

    # 获取业务表列表
    tables = storage.list_business_tables()

    # 应该包含这些股票的信息
    stock_codes = [table["stock_code"] for table in tables]
    assert "000001.SZ" in stock_codes
    assert "000002.SZ" in stock_codes

    # 检查业务类型
    business_types = [table["business_type"] for table in tables]
    assert "financials" in business_types
    assert "daily_basic" in business_types


def test_query_daily_data_by_stock_database_exception(storage: DuckDBStorage):
    """测试查询日线数据时数据库异常处理"""
    from unittest.mock import patch, MagicMock

    mock_conn_context = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Database query failed")
    mock_conn_context.__enter__.return_value = mock_conn
    mock_conn_context.__exit__.return_value = None

    with patch.object(
        storage._db_factory, "get_read_connection", return_value=mock_conn_context
    ):
        result = storage.query_daily_data_by_stock("000001.SZ")
        assert result.empty


def test_query_daily_data_by_date_range_database_exception(storage: DuckDBStorage):
    """测试按日期范围查询日线数据时数据库异常处理"""
    from unittest.mock import patch, MagicMock

    mock_conn_context = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Database query failed")
    mock_conn_context.__enter__.return_value = mock_conn
    mock_conn_context.__exit__.return_value = None

    with patch.object(
        storage._db_factory, "get_read_connection", return_value=mock_conn_context
    ):
        result = storage.query_daily_data_by_date_range("20230101", "20230131")
        assert result.empty


def test_query_financial_data_by_stock_database_exception(storage: DuckDBStorage):
    """测试查询财务数据时数据库异常处理"""
    from unittest.mock import patch, MagicMock

    mock_conn_context = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Database query failed")
    mock_conn_context.__enter__.return_value = mock_conn
    mock_conn_context.__exit__.return_value = None

    with patch.object(
        storage._db_factory, "get_read_connection", return_value=mock_conn_context
    ):
        result = storage.query_financial_data_by_stock("000001.SZ")
        assert result.empty


def test_query_daily_data_by_stocks_database_exception(storage: DuckDBStorage):
    """测试查询多股票日线数据时数据库异常处理"""
    from unittest.mock import patch, MagicMock

    mock_conn_context = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Database query failed")
    mock_conn_context.__enter__.return_value = mock_conn
    mock_conn_context.__exit__.return_value = None

    with patch.object(
        storage._db_factory, "get_read_connection", return_value=mock_conn_context
    ):
        result = storage.query_daily_data_by_stocks(["000001.SZ", "000002.SZ"])
        assert result.empty


def test_get_latest_date_by_stock_database_exception(storage: DuckDBStorage):
    """测试获取股票最新日期时数据库异常处理"""
    from unittest.mock import patch, MagicMock

    mock_conn_context = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Database query failed")
    mock_conn_context.__enter__.return_value = mock_conn
    mock_conn_context.__exit__.return_value = None

    with patch.object(
        storage._db_factory, "get_read_connection", return_value=mock_conn_context
    ):
        result = storage.get_latest_date_by_stock("000001.SZ", "daily")
        assert result is None


def test_batch_get_latest_dates_database_exception(storage: DuckDBStorage):
    """测试批量获取最新日期时数据库异常处理"""
    from unittest.mock import patch, MagicMock

    mock_conn_context = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Database query failed")
    mock_conn_context.__enter__.return_value = mock_conn
    mock_conn_context.__exit__.return_value = None

    with patch.object(
        storage._db_factory, "get_read_connection", return_value=mock_conn_context
    ):
        result = storage.batch_get_latest_dates(["000001.SZ", "000002.SZ"], "daily")
        assert result == {}


def test_save_stock_list_missing_required_fields(storage: DuckDBStorage):
    """测试保存股票列表时缺少必需字段"""
    # 创建缺少 ts_code 字段的 DataFrame
    invalid_df = pd.DataFrame({"symbol": ["000001"], "name": ["平安银行"]})

    result = storage.save_stock_list(invalid_df)
    assert result is False
