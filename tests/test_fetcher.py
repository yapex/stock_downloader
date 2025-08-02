import pandas as pd
import pytest
from unittest.mock import MagicMock
from dotenv import load_dotenv
import logging
import tushare as ts  # 导入 tushare 以便 mock

load_dotenv()

from downloader.fetcher import TushareFetcher


@pytest.fixture
def mock_pro_api(monkeypatch):
    """
    一个模拟的 Tushare Pro API 对象，并模拟正确的初始化流程。
    """
    # 1. 创建一个 mock pro 对象
    mock_pro = MagicMock()
    mock_pro.trade_cal.return_value = pd.DataFrame({"cal_date": ["20230101"]})
    mock_pro.stock_basic.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})

    # 2. ---> 核心修正：模拟 ts.pro_api() 的无参数调用 <---
    #    现在 lambda 不再需要 token 参数
    monkeypatch.setattr(ts, "pro_api", lambda: mock_pro)

    # 3. ---> 核心修正：模拟 ts.set_token()，让它什么都不做 <---
    monkeypatch.setattr(ts, "set_token", lambda token: None)

    return mock_pro


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


def test_fetch_daily_history_success(monkeypatch):
    """测试 fetch_daily_history 在正常情况下的行为。"""
    mock_bar = MagicMock(return_value=pd.DataFrame({"trade_date": ["20230101"]}))
    monkeypatch.setattr(ts, "pro_bar", mock_bar)

    # 即使这个测试不直接用 mock_pro_api, TushareFetcher 的 __init__ 也需要它
    # 我们可以通过 mock 掉 __init__ 来简化，但保持原样更接近真实
    fetcher = TushareFetcher()
    df = fetcher.fetch_daily_history("000001.SZ", "20230101", "20230131", adjust="qfq")

    mock_bar.assert_called_once()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_fetch_daily_history_handles_api_returning_none(monkeypatch, caplog):
    """【回归测试】测试当 ts.pro_bar 返回 None 时，能规范化为空 DataFrame。"""
    mock_bar_returns_none = MagicMock(return_value=None)
    monkeypatch.setattr(ts, "pro_bar", mock_bar_returns_none)
    caplog.set_level(logging.WARNING)

    fetcher = TushareFetcher()
    result_df = fetcher.fetch_daily_history(
        "000002.SZ", "20230101", "20230131", adjust="none"
    )

    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty
    assert "Tushare API for 000002.SZ 返回了 None" in caplog.text


def test_fetch_daily_history_handles_api_exception(monkeypatch, caplog):
    """测试当 ts.pro_bar 抛出异常时，能捕获并返回 None。"""
    mock_bar_raises_exception = MagicMock(side_effect=ConnectionError("模拟网络错误"))
    monkeypatch.setattr(ts, "pro_bar", mock_bar_raises_exception)
    caplog.set_level(logging.ERROR)

    fetcher = TushareFetcher()
    result_df = fetcher.fetch_daily_history(
        "000003.SZ", "20230101", "20230131", adjust="hfq"
    )

    assert result_df is None
    assert "获取 000003.SZ 的日线数据时发生异常" in caplog.text
    assert "ConnectionError" in caplog.text
