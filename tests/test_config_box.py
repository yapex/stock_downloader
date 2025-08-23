"""测试基于 python-box 的配置管理器"""

from unittest.mock import patch, mock_open
from box import Box
import sys
from pathlib import Path

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

        # 测试 get 方法
        config = get_config()
        assert config.get("tushare").get("token") == "test_token"
        assert config.get("nonexistent", "default") == "default"

        # 测试 to_dict 方法
        config_dict = config.to_dict()
        assert isinstance(config_dict, dict)
        assert config_dict["tushare"]["token"] == "test_token"

        # 测试字典式访问
        assert config["tushare"]["token"] == "test_token"
        assert config["database"]["type"] == "duckdb"

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
