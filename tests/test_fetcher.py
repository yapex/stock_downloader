import logging
import pandas as pd
import pytest
import tushare as ts  # 导入 tushare 以便 mock
from dotenv import load_dotenv
from unittest.mock import MagicMock, patch

from downloader.fetcher import TushareFetcher

load_dotenv()


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


def test_fetcher_initialization_api_failure(monkeypatch):
    """测试当API验证失败时，TushareFetcher是否抛出异常。"""
    mock_set_token = MagicMock()
    mock_pro_api = MagicMock(side_effect=Exception("API init failure"))
    with (
        patch("tushare.set_token", mock_set_token),
        patch("tushare.pro_api", mock_pro_api),
    ):
        with pytest.raises(Exception, match="API init failure"):
            TushareFetcher()


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


def test_fetch_stock_list_exception_handling(mock_pro_api, caplog):
    """测试 fetch_stock_list 异常时返回 None。"""
    mock_pro_api.stock_basic.side_effect = Exception("fetch error")
    fetcher = TushareFetcher()
    result_df = fetcher.fetch_stock_list()
    assert result_df is None
    assert "获取A股列表失败" in caplog.text


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


# ===================================================================
#           核心修正：修复两个失败的测试
# ===================================================================


@pytest.mark.parametrize("adjust", ["none", "qfq", "hfq"])
def test_fetch_daily_history_handles_api_returning_none(adjust, monkeypatch, caplog):
    """【最小化测试】测试 ts.pro_bar 返回 None 时的返回值是否正确。"""
    mock_bar_returns_none = MagicMock(return_value=None)
    monkeypatch.setattr(ts, "pro_bar", mock_bar_returns_none)

    fetcher = TushareFetcher()
    result_df = fetcher.fetch_daily_history(
        "000002.SZ", "20230101", "20230131", adjust=adjust
    )

    # 断言返回值是空的DataFrame
    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty

    # ---> 核心修正：断言正确的警告日志 <---
    assert "Tushare API for 000002.SZ 返回了 None" in caplog.text


@pytest.mark.parametrize("adjust", ["none", "qfq", "hfq"])
def test_fetch_daily_history_handles_api_exception(adjust, monkeypatch, caplog):
    """【最小化测试】测试 ts.pro_bar 抛出异常时的返回值是否正确。"""
    mock_bar_raises_exception = MagicMock(side_effect=ConnectionError("模拟网络错误"))
    monkeypatch.setattr(ts, "pro_bar", mock_bar_raises_exception)

    caplog.set_level(logging.ERROR)

    fetcher = TushareFetcher()
    result_df = fetcher.fetch_daily_history(
        "000003.SZ", "20230101", "20230131", adjust=adjust
    )

    # 断言返回值是 None
    assert result_df is None

    assert "获取 000003.SZ 的日线数据失败: 模拟网络错误" in caplog.text


def test_fetch_income_balancesheet_cashflow(mock_pro_api, caplog):
    """测试财务函数 fetch_income, fetch_balancesheet, fetch_cashflow 
    的正常返回和异常处理
    """
    mock_df = pd.DataFrame({"ann_date": ["20220101"]})

    # 测试正常返回情况
    mock_pro_api.income.return_value = mock_df
    mock_pro_api.balancesheet.return_value = mock_df
    mock_pro_api.cashflow.return_value = mock_df

    fetcher = TushareFetcher()

    # 测试各个财务方法的正常调用
    result_income = fetcher.fetch_income("600519", "20220101", "20221231")
    result_balance = fetcher.fetch_balancesheet("600519", "20220101", "20221231")
    result_cashflow = fetcher.fetch_cashflow("600519", "20220101", "20221231")

    assert result_income.equals(mock_df)
    assert result_balance.equals(mock_df)
    assert result_cashflow.equals(mock_df)

    # 测试异常抛出处理
    mock_pro_api.income.side_effect = Exception("模拟错误")
    mock_pro_api.balancesheet.side_effect = Exception("模拟错误")
    mock_pro_api.cashflow.side_effect = Exception("模拟错误")

    with caplog.at_level(logging.ERROR):
        result_income = fetcher.fetch_income("600519", "20220101", "20221231")
        result_balance = fetcher.fetch_balancesheet("600519", "20220101", "20221231")
        result_cashflow = fetcher.fetch_cashflow("600519", "20220101", "20221231")

        assert result_income is None
        assert result_balance is None  
        assert result_cashflow is None

        assert "获取 600519.SH 的利润表失败" in caplog.text
        assert "获取 600519.SH 的资产负债表失败" in caplog.text
        assert "获取 600519.SH 的现金流量表失败" in caplog.text


# ===================================================================
#           测试 daily_basic 方法
# ===================================================================


def test_fetch_daily_basic_success(mock_pro_api):
    """测试 fetch_daily_basic 正常情况。"""
    mock_df = pd.DataFrame(
        {"ts_code": ["600519.SH"], "trade_date": ["20230101"], "close": [100.0]}
    )
    mock_pro_api.daily_basic.return_value = mock_df

    fetcher = TushareFetcher()
    result = fetcher.fetch_daily_basic("600519", "20230101", "20230131")

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    mock_pro_api.daily_basic.assert_called_once_with(
        ts_code="600519.SH",
        start_date="20230101",
        end_date="20230131",
        fields="ts_code,trade_date,close,turnover_rate,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_mv,circ_mv",
    )


def test_fetch_daily_basic_returns_none(mock_pro_api):
    """测试 fetch_daily_basic 当 API 返回 None 时。"""
    mock_pro_api.daily_basic.return_value = None

    fetcher = TushareFetcher()
    result = fetcher.fetch_daily_basic("600519", "20230101", "20230131")

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_fetch_daily_basic_exception(mock_pro_api, caplog):
    """测试 fetch_daily_basic 异常处理。"""
    mock_pro_api.daily_basic.side_effect = Exception("API error")

    fetcher = TushareFetcher()
    result = fetcher.fetch_daily_basic("600519", "20230101", "20230131")

    assert result is None
    assert "获取 600519.SH 的每日指标失败" in caplog.text


# ===================================================================
#           测试财务报表方法
# ===================================================================


@pytest.mark.parametrize(
    "method_name,api_method",
    [
        ("fetch_income", "income"),
        ("fetch_balancesheet", "balancesheet"),
        ("fetch_cashflow", "cashflow"),
    ],
)
def test_financial_methods_success(mock_pro_api, method_name, api_method):
    """测试财务报表方法的正常情况。"""
    mock_df = pd.DataFrame(
        {"ts_code": ["600519.SH"], "ann_date": ["20230430"], "revenue": [1000000]}
    )
    getattr(mock_pro_api, api_method).return_value = mock_df

    fetcher = TushareFetcher()
    method = getattr(fetcher, method_name)
    result = method("600519", "20230101", "20231231")

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    getattr(mock_pro_api, api_method).assert_called_once_with(
        ts_code="600519.SH", start_date="20230101", end_date="20231231"
    )


@pytest.mark.parametrize(
    "method_name,api_method",
    [
        ("fetch_income", "income"),
        ("fetch_balancesheet", "balancesheet"),
        ("fetch_cashflow", "cashflow"),
    ],
)
def test_financial_methods_empty_data(mock_pro_api, method_name, api_method):
    """测试财务报表方法返回空数据或None。"""
    getattr(mock_pro_api, api_method).return_value = None

    fetcher = TushareFetcher()
    method = getattr(fetcher, method_name)
    result = method("600519", "20230101", "20231231")

    assert result is None


@pytest.mark.parametrize(
    "method_name,api_method,error_msg",
    [
        ("fetch_income", "income", "利润表"),
        ("fetch_balancesheet", "balancesheet", "资产负债表"),
        ("fetch_cashflow", "cashflow", "现金流量表"),
    ],
)
def test_financial_methods_exception(
    mock_pro_api, method_name, api_method, error_msg, caplog
):
    """测试财务报表方法的异常处理。"""
    getattr(mock_pro_api, api_method).side_effect = Exception("API error")

    fetcher = TushareFetcher()
    method = getattr(fetcher, method_name)
    result = method("600519", "20230101", "20231231")

    assert result is None
    assert f"获取 600519.SH 的{error_msg}失败" in caplog.text


# ===================================================================
#           测试数据排序功能
# ===================================================================


def test_fetch_daily_history_data_sorting(monkeypatch):
    """测试 fetch_daily_history 是否正确排序数据。"""
    # 创建乱序的测试数据
    unsorted_df = pd.DataFrame(
        {"trade_date": ["20230103", "20230101", "20230102"], "close": [100, 98, 99]}
    )
    mock_bar = MagicMock(return_value=unsorted_df)
    monkeypatch.setattr(ts, "pro_bar", mock_bar)

    fetcher = TushareFetcher()
    result = fetcher.fetch_daily_history(
        "600519", "20230101", "20230103", adjust="none"
    )

    # 验证数据已按日期排序
    expected_dates = ["20230101", "20230102", "20230103"]
    assert list(result["trade_date"]) == expected_dates


def test_fetch_daily_basic_data_sorting(mock_pro_api):
    """测试 fetch_daily_basic 是否正确排序数据。"""
    unsorted_df = pd.DataFrame(
        {"trade_date": ["20230103", "20230101", "20230102"], "close": [100, 98, 99]}
    )
    mock_pro_api.daily_basic.return_value = unsorted_df

    fetcher = TushareFetcher()
    result = fetcher.fetch_daily_basic("600519", "20230101", "20230103")

    # 验证数据已按日期排序
    expected_dates = ["20230101", "20230102", "20230103"]
    assert list(result["trade_date"]) == expected_dates


# ===================================================================
#           测试股票代码标准化
# ===================================================================


def test_stock_code_normalization_in_methods(monkeypatch):
    """测试各方法中股票代码的标准化处理。"""
    mock_bar = MagicMock(return_value=pd.DataFrame({"trade_date": ["20230101"]}))
    monkeypatch.setattr(ts, "pro_bar", mock_bar)

    fetcher = TushareFetcher()

    # 测试未标准化的代码被正确转换
    fetcher.fetch_daily_history("600519", "20230101", "20230131", adjust="none")

    # 验证调用时使用了标准化后的代码
    mock_bar.assert_called_with(
        ts_code="600519.SH",
        adj=None,
        start_date="20230101",
        end_date="20230131",
        asset="E",
        freq="D",
    )
