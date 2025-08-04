import pytest

from downloader.utils import normalize_stock_code


def test_normalize_stock_code_sh():
    """测试上海证券交易所代码"""
    assert normalize_stock_code("600519") == "600519.SH"
    assert normalize_stock_code("SH600519") == "600519.SH"
    assert normalize_stock_code("600519SH") == "600519.SH"
    assert normalize_stock_code("sh600519") == "600519.SH"
    assert normalize_stock_code("600519.sh") == "600519.SH"
    assert normalize_stock_code("600519.SH") == "600519.SH"


def test_normalize_stock_code_sz():
    """测试深圳证券交易所代码"""
    assert normalize_stock_code("000001") == "000001.SZ"
    assert normalize_stock_code("SZ000001") == "000001.SZ"
    assert normalize_stock_code("000001SZ") == "000001.SZ"
    assert normalize_stock_code("sz000001") == "000001.SZ"
    assert normalize_stock_code("000001.sz") == "000001.SZ"
    assert normalize_stock_code("000001.SZ") == "000001.SZ"


def test_normalize_stock_code_gem():
    """测试创业板代码"""
    assert normalize_stock_code("300750") == "300750.SZ"


def test_invalid_code():
    """测试无效代码"""
    with pytest.raises(ValueError, match="无法从 '12345' 中提取有效的6位股票代码"):
        normalize_stock_code("12345")

    with pytest.raises(ValueError, match="无法识别的股票代码前缀: 123456"):
        normalize_stock_code("123456")

    with pytest.raises(ValueError, match="无法从 'invalid' 中提取有效的6位股票代码"):
        normalize_stock_code("invalid")


def test_non_string_input():
    """测试非字符串输入"""
    with pytest.raises(TypeError, match="股票代码必须是字符串，而不是 <class 'int'>"):
        normalize_stock_code(600519)
