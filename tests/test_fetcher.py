import pandas as pd
import pytest
from unittest.mock import MagicMock
from dotenv import load_dotenv

# 确保测试环境能加载.env文件
load_dotenv()

from downloader.fetcher import TushareFetcher


@pytest.fixture
def mock_pro_api(monkeypatch):
    """一个模拟的 Tushare Pro API 对象。"""
    mock_pro = MagicMock()

    # 为每个需要的方法设置返回值
    mock_pro.trade_cal.return_value = pd.DataFrame({"cal_date": ["20230101"]})
    mock_pro.stock_basic.return_value = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600519.SH"],
            "symbol": ["000001", "600519"],
            "name": ["平安银行", "贵州茅台"],
        }
    )

    # 替换全局的 tushare.pro_api 函数
    monkeypatch.setattr("tushare.pro_api", lambda: mock_pro)
    return mock_pro


@pytest.fixture
def mock_pro_bar(monkeypatch):
    """一个模拟的 tushare.pro_bar 函数。"""
    mock_bar_func = MagicMock()
    mock_bar_func.return_value = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20230101"],
            "open": [15.0],
            "close": [15.5],
        }
    )
    monkeypatch.setattr("tushare.pro_bar", mock_bar_func)
    return mock_bar_func


# --- 测试用例 ---


def test_fetcher_initialization_success(mock_pro_api):
    """测试当Token有效时，TushareFetcher能否成功初始化。"""
    fetcher = TushareFetcher()
    assert fetcher.pro is not None
    # 验证初始化时是否调用了 trade_cal
    fetcher.pro.trade_cal.assert_called_once_with(exchange="SSE", limit=1)


def test_fetcher_initialization_no_token(monkeypatch):
    """测试当Token未设置时，是否抛出ValueError。"""
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    with pytest.raises(ValueError, match="错误：未设置 TUSHARE_TOKEN 环境变量。"):
        TushareFetcher()


def test_fetch_stock_list(mock_pro_api):
    """测试 fetch_stock_list 能否正确调用API并返回原始DataFrame。"""
    fetcher = TushareFetcher()
    result_df = fetcher.fetch_stock_list()

    fetcher.pro.stock_basic.assert_called_once()
    assert isinstance(result_df, pd.DataFrame)
    assert "ts_code" in result_df.columns
    assert "name" in result_df.columns


def test_fetch_daily_history(mock_pro_bar):
    """测试 fetch_daily_history 能否正确调用 ts.pro_bar。"""
    # 即使不使用 mock_pro_api，由于 set_token 是全局的，
    # 我们需要确保 tushare.pro_api 不会真的被调用。
    # 一个简单的方法是也把它 mock 掉。
    fetcher = TushareFetcher()
    ts_code = "000001.SZ"
    start_date = "20230101"
    end_date = "20230131"

    df = fetcher.fetch_daily_history(ts_code, start_date, end_date, adjust="qfq")

    mock_pro_bar.assert_called_once_with(
        ts_code=ts_code,
        adj="qfq",
        start_date=start_date,
        end_date=end_date,
        asset="E",
        freq="D",
    )
    assert isinstance(df, pd.DataFrame)
    assert "trade_date" in df.columns
