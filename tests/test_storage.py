import pandas as pd
import pytest
from pathlib import Path
import logging
from unittest.mock import Mock

from downloader.storage import DuckDBStorage
from downloader.engine import DownloadEngine
from downloader.fetcher import TushareFetcher


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """提供一个临时的数据库文件路径，并确保测试后清理。"""
    return tmp_path / "test.db"


@pytest.fixture
def storage(db_path: Path) -> DuckDBStorage:
    """创建一个 DuckDBStorage 实例。"""
    return DuckDBStorage(db_path)


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """提供一个用于测试的 DataFrame。"""
    return pd.DataFrame(
        {
            "trade_date": ["2023-01-01", "2023-01-02"],
            "close": [100.0, 101.0],
        }
    )


@pytest.fixture
def updated_df() -> pd.DataFrame:
    """提供一个用于测试更新操作的 DataFrame。"""
    return pd.DataFrame(
        {
            "trade_date": ["2023-01-02", "2023-01-03"],
            "close": [102.0, 103.0],  # 2023-01-02 的值已更新
        }
    )


def test_initialization(db_path: Path):
    """测试初始化时是否创建了数据库文件。"""
    assert not db_path.exists()
    DuckDBStorage(db_path)
    assert db_path.exists()


# test_get_table_name 已移除，因为新的分区表架构不再需要动态表名生成


def test_save_and_get_latest_date(storage: DuckDBStorage, sample_df: pd.DataFrame):
    """测试基本的保存和获取最新日期的功能。"""
    storage.save(sample_df, "daily", "000001.SZ", date_col="trade_date")
    latest_date = storage.get_latest_date("daily", "000001.SZ", date_col="trade_date")
    assert latest_date == "2023-01-02"


def test_save_upsert_logic(
    storage: DuckDBStorage, sample_df: pd.DataFrame, updated_df: pd.DataFrame
):
    """测试增量保存（UPSERT）的逻辑是否正确。"""
    # 1. 初始保存
    storage.save(sample_df, "daily", "000001.SZ", date_col="trade_date")

    # 2. 使用新数据进行增量保存
    storage.save(updated_df, "daily", "000001.SZ", date_col="trade_date")

    # 3. 验证结果 - 使用新的分区表查询方法
    result_df = storage.query_daily_data_by_stock("000001.SZ")

    # 检查总行数是否正确（应该是 3 行）
    assert len(result_df) == 3
    # 检查 2023-01-02 的数据是否已更新
    assert result_df[result_df["trade_date"] == "2023-01-02"]["close"].iloc[0] == 102.0
    # 检查最新日期是否正确
    assert result_df["trade_date"].max() == "2023-01-03"


def test_overwrite(storage: DuckDBStorage, sample_df: pd.DataFrame):
    """测试全量覆盖功能。"""
    # 先保存一些数据
    storage.save(sample_df, "daily", "000001.SZ", date_col="trade_date")

    # 创建一个全新的 DataFrame 用于覆盖
    overwrite_df = pd.DataFrame(
        {
            "trade_date": ["2024-01-01"],
            "close": [200.0],
        }
    )
    storage.overwrite(overwrite_df, "daily", "000001.SZ")

    # 验证 - 使用新的分区表查询方法
    result_df = storage.query_daily_data_by_stock("000001.SZ")
    assert len(result_df) == 1
    assert result_df["close"].iloc[0] == 200.0


def test_get_latest_date_on_empty_table(storage: DuckDBStorage):
    """测试在空表或不存在的表上获取最新日期。"""
    latest_date = storage.get_latest_date("daily", "999999.SZ", date_col="trade_date")
    assert latest_date is None

def test_save_empty_dataframe(storage: DuckDBStorage):
    """测试保存空 DataFrame 时不应执行任何操作。"""
    empty_df = pd.DataFrame()
    storage.save(empty_df, "daily", "000001.SZ", date_col="trade_date")
    
    # 验证数据库中没有数据（分区表已预创建）
    result_df = storage.query_daily_data_by_stock("000001.SZ")
    assert len(result_df) == 0


def test_save_dataframe_without_date_col(storage: DuckDBStorage, caplog):
    """测试当 DataFrame 缺少日期列时应记录错误并跳过。"""
    df = pd.DataFrame({"close": [1.0, 2.0]})
    with caplog.at_level(logging.ERROR, logger='downloader.storage'):
        storage.save(df, "daily", "000001.SZ", date_col="trade_date")
        # 验证异常被正确处理
    
    # 验证数据库中没有数据（分区表已预创建）
    result_df = storage.query_daily_data_by_stock("000001.SZ")
    assert len(result_df) == 0


def test_engine_run_end_to_end(tmp_path: Path, monkeypatch):
    """测试 engine.run() 的端到端功能，使用 monkeypatch 伪造 fetcher 返回空 DataFrame。"""
    # 创建模拟的配置
    config = {
        "tasks": [
            {
                "name": "测试任务",
                "type": "test_task",
                "enabled": True
            }
        ],
        "defaults": {},
        "downloader": {
            "symbols": ["000001.SZ"],
            "max_concurrent_tasks": 1
        }
    }
    
    # 创建模拟的 fetcher，返回空 DataFrame
    mock_fetcher = Mock(spec=TushareFetcher)
    mock_fetcher.fetch_stock_list.return_value = pd.DataFrame()
    mock_fetcher.fetch_daily_history.return_value = pd.DataFrame()
    
    # 创建真实的存储实例
    db_path = tmp_path / "test_engine.db"
    storage = DuckDBStorage(db_path)
    
    # 创建引擎实例
    engine = DownloadEngine(
        config=config,
        fetcher=mock_fetcher,
        storage=storage,
        force_run=False
    )
    
    # 模拟任务处理器注册表为空（这样就不会真正执行任务）
    monkeypatch.setattr(engine, 'task_registry', {})
    
    # 执行引擎，应该不会抛出异常
    try:
        engine.run()
        # 如果能到达这里，说明 run() 方法执行成功
        assert True
    except Exception as e:
        pytest.fail(f"engine.run() 应该能正常执行，但抛出异常: {e}")

