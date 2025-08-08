import pytest

from downloader.utils import normalize_stock_code, is_interval_greater_than_7_days, get_table_name


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


def test_normalize_stock_code_bj():
    """测试北交所代码"""
    assert normalize_stock_code("800001") == "800001.BJ"
    assert normalize_stock_code("900001") == "900001.BJ"


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


def test_interval_greater_than_7_days():
    # 测试间隔大于 7 天的情况
    assert is_interval_greater_than_7_days("20250101", "20250110")
    # 测试间隔小于 7 天的情况
    assert not is_interval_greater_than_7_days("20250101", "20250105")
    # 测试间隔等于 7 天的情况
    assert not is_interval_greater_than_7_days("20250101", "20250108")
    # 测试无效日期格式的情况
    with pytest.raises(ValueError):
        is_interval_greater_than_7_days("invalid", "20250110")


def test_get_table_name_stock_data():
    """测试股票数据表名生成"""
    # 测试标准股票代码
    assert get_table_name("daily", "600519.SH") == "daily_600519_SH"
    assert get_table_name("daily_basic", "000001.SZ") == "daily_basic_000001_SZ"
    assert get_table_name("financials", "300750.SZ") == "financials_300750_SZ"
    
    # 测试需要标准化的股票代码
    assert get_table_name("daily", "600519") == "daily_600519_SH"
    assert get_table_name("daily", "SH600519") == "daily_600519_SH"
    assert get_table_name("daily_basic", "000001") == "daily_basic_000001_SZ"


def test_get_table_name_system_tables():
    """测试系统表名生成"""
    # 测试系统表
    assert get_table_name("system", "stock_list_system") == "sys_stock_list"
    assert get_table_name("system", "system") == "sys_stock_list"
    assert get_table_name("any_type", "list_system") == "sys_stock_list"
    
    # 测试包含_system的实体ID
    assert get_table_name("daily", "test_system") == "sys_test"
    assert get_table_name("basic", "config_system") == "sys_config"


def test_get_table_name_stock_list():
    """测试股票列表相关表名生成"""
    # 测试stock_list_开头的数据类型
    assert get_table_name("stock_list_basic", "any_entity") == "sys_stock_list"
    assert get_table_name("stock_list_info", "600519.SH") == "sys_stock_list"
    
    # 测试list_system实体ID
    assert get_table_name("basic_info", "list_system") == "sys_stock_list"
