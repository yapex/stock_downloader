"""配置管理和工具函数的综合测试"""

import pytest
from unittest.mock import patch, mock_open
from box import Box
import sys
from pathlib import Path

from neo.helpers.utils import (
    normalize_stock_code,
    is_interval_greater_than_7_days,
)

# 添加 src 目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


class TestConfig:
    """测试简化的配置管理"""

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=b'[tushare]\ntoken = "test_token"\n[database]\ntype = "duckdb"\npath = "./test.db"\n[api]\ntimeout = 30\nretry_count = 3',
    )
    @patch("pathlib.Path.open")
    def test_config_loading(self, mock_path_open, mock_file):
        """测试配置加载"""
        mock_path_open.return_value = mock_file.return_value

        # 重新导入配置模块以触发配置加载
        import importlib

        if "neo.configs.app_config" in sys.modules:
            importlib.reload(sys.modules["neo.configs.app_config"])
        else:
            pass

        from neo.configs import get_config

        # 测试点号访问
        config = get_config()
        assert config.tushare.token == "test_token"
        assert config.database.type == "duckdb"
        assert config.database.path == "./test.db"
        assert config.api.timeout == 30
        assert config.api.retry_count == 3

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=b'[tushare]\ntoken = "test_token"\n[database]\ntype = "duckdb"\npath = "./test.db"',
    )
    @patch("pathlib.Path.open")
    def test_box_features(self, mock_path_open, mock_file):
        """测试 Box 特有功能"""
        mock_path_open.return_value = mock_file.return_value

        # 重新导入配置模块
        import importlib

        if "neo.configs.app_config" in sys.modules:
            importlib.reload(sys.modules["neo.configs.app_config"])
        else:
            pass

        from neo.configs import get_config

        # 测试 Box 特有功能
        config = get_config()

        # 测试字典式访问
        assert config["tushare"]["token"] == "test_token"
        assert config["database"]["type"] == "duckdb"

        # 测试 get 方法
        assert config.get("tushare").get("token") == "test_token"
        assert config.get("nonexistent", "default") == "default"

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=b'[stocks]\nsymbols = ["000001.SZ", "000002.SZ", "600000.SH"]\n[exchanges]\nlist = ["SZ", "SH", "BJ"]',
    )
    @patch("pathlib.Path.open")
    def test_list_access(self, mock_path_open, mock_file):
        """测试列表访问"""
        mock_path_open.return_value = mock_file.return_value

        # 重新导入配置模块
        import importlib

        if "neo.configs.app_config" in sys.modules:
            importlib.reload(sys.modules["neo.configs.app_config"])
        else:
            pass

        from neo.configs import get_config

        # 测试列表访问
        config = get_config()
        assert len(config.stocks.symbols) == 3
        assert config.stocks.symbols[0] == "000001.SZ"
        assert "SZ" in config.exchanges.list

        # 测试列表操作
        symbols = config.stocks.symbols
        assert isinstance(symbols, list)
        assert "600000.SH" in symbols

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=b'[app]\nname = "stock_downloader"\ndebug = true\n[nested]\n[nested.deep]\nvalue = 42\n[nested.deep.deeper]\nfinal = "success"',
    )
    @patch("pathlib.Path.open")
    def test_deep_nested_access(self, mock_path_open, mock_file):
        """测试深层嵌套访问"""
        mock_path_open.return_value = mock_file.return_value

        # 重新导入配置模块
        import importlib

        if "neo.configs.app_config" in sys.modules:
            importlib.reload(sys.modules["neo.configs.app_config"])
        else:
            pass

        from neo.configs import get_config

        # 测试深层嵌套访问
        config = get_config()
        assert config.app.name == "stock_downloader"
        assert config.app.debug is True
        assert config.nested.deep.value == 42
        assert config.nested.deep.deeper.final == "success"

    def test_box_object_type(self):
        """测试配置对象类型"""
        # 重新导入配置模块
        import importlib

        if "neo.configs.app_config" in sys.modules:
            importlib.reload(sys.modules["neo.configs.app_config"])
        else:
            pass

        from neo.configs import get_config

        # 验证 config 是 Box 对象
        config = get_config()
        assert isinstance(config, Box)
        assert hasattr(config, "get")
        assert hasattr(config, "to_dict")


class TestStockCodeNormalization:
    """股票代码标准化测试类"""

    def test_normalize_stock_code_sh(self):
        """测试上海证券交易所代码"""
        assert normalize_stock_code("600519") == "600519.SH"
        assert normalize_stock_code("SH600519") == "600519.SH"
        assert normalize_stock_code("600519SH") == "600519.SH"
        assert normalize_stock_code("sh600519") == "600519.SH"
        assert normalize_stock_code("600519.sh") == "600519.SH"
        assert normalize_stock_code("600519.SH") == "600519.SH"

    def test_normalize_stock_code_sz(self):
        """测试深圳证券交易所代码"""
        assert normalize_stock_code("000001") == "000001.SZ"
        assert normalize_stock_code("SZ000001") == "000001.SZ"
        assert normalize_stock_code("000001SZ") == "000001.SZ"
        assert normalize_stock_code("sz000001") == "000001.SZ"
        assert normalize_stock_code("000001.sz") == "000001.SZ"
        assert normalize_stock_code("000001.SZ") == "000001.SZ"

    def test_normalize_stock_code_gem(self):
        """测试创业板代码"""
        assert normalize_stock_code("300750") == "300750.SZ"

    def test_normalize_stock_code_bj(self):
        """测试北交所代码"""
        assert normalize_stock_code("800001") == "800001.BJ"
        assert normalize_stock_code("900001") == "900001.BJ"

    def test_invalid_code(self):
        """测试无效代码"""
        with pytest.raises(ValueError, match="无法从 '12345' 中提取有效的6位股票代码"):
            normalize_stock_code("12345")

        with pytest.raises(ValueError, match="无法识别的股票代码前缀: 123456"):
            normalize_stock_code("123456")

        with pytest.raises(
            ValueError, match="无法从 'invalid' 中提取有效的6位股票代码"
        ):
            normalize_stock_code("invalid")

    def test_non_string_input(self):
        """测试非字符串输入"""
        with pytest.raises(
            TypeError, match="股票代码必须是字符串，而不是 <class 'int'>"
        ):
            normalize_stock_code(600519)


class TestDateUtils:
    """日期工具函数测试类"""

    def test_interval_greater_than_7_days(self):
        """测试日期间隔判断"""
        # 测试间隔大于 7 天的情况
        assert is_interval_greater_than_7_days("20250101", "20250110")

        # 测试间隔小于 7 天的情况
        assert not is_interval_greater_than_7_days("20250101", "20250105")

        # 测试间隔等于 7 天的情况
        assert not is_interval_greater_than_7_days("20250101", "20250108")

        # 测试无效日期格式的情况
        with pytest.raises(ValueError):
            is_interval_greater_than_7_days("invalid", "20250110")
