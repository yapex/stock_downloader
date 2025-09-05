import pytest
from unittest.mock import Mock, patch
from neo.helpers.group_handler import GroupHandler
from neo.database.interfaces import IDBOperator


class TestGroupHandler:
    """GroupHandler 测试类"""

    def test_init_with_db_operator(self):
        """测试使用 db_operator 初始化"""
        mock_db_operator = Mock(spec=IDBOperator)
        handler = GroupHandler(db_operator=mock_db_operator)
        assert handler.db_operator is mock_db_operator

    def test_init_without_db_operator(self):
        """测试不使用 db_operator 初始化"""
        handler = GroupHandler()
        assert handler.db_operator is None

    @patch("neo.database.operator.ParquetDBQueryer")
    def test_create_default(self, mock_db_operator_class):
        """测试 create_default 工厂方法"""
        mock_db_operator_instance = Mock(spec=IDBOperator)
        mock_db_operator_class.create_default.return_value = mock_db_operator_instance

        handler = GroupHandler.create_default()

        assert isinstance(handler, GroupHandler)
        assert handler.db_operator is mock_db_operator_instance
        mock_db_operator_class.create_default.assert_called_once()

    @patch("neo.helpers.group_handler.get_config")
    def test_get_task_types_for_group_valid_group(self, mock_get_config):
        """测试获取有效组的任务类型"""
        mock_config = Mock()
        mock_config.task_groups = {"test_group": ["stock_basic", "stock_daily"]}
        mock_get_config.return_value = mock_config

        handler = GroupHandler()
        task_types = handler.get_task_types_for_group("test_group")

        assert task_types == ["stock_basic", "stock_daily"]

    @patch("neo.helpers.group_handler.get_config")
    def test_get_task_types_for_group_invalid_group(self, mock_get_config):
        """测试获取无效组的任务类型"""
        mock_config = Mock()
        mock_config.task_groups = {}
        mock_get_config.return_value = mock_config

        handler = GroupHandler()

        with pytest.raises(ValueError, match="未找到组配置: invalid_group"):
            handler.get_task_types_for_group("invalid_group")

    @patch("neo.helpers.group_handler.get_config")
    def test_get_symbols_for_group_with_db_operator(self, mock_get_config):
        """测试使用 db_operator 获取组的股票代码"""
        mock_config = Mock()
        mock_config.task_groups = {"test_group": ["stock_daily"]}
        mock_get_config.return_value = mock_config

        mock_db_operator = Mock(spec=IDBOperator)
        mock_db_operator.get_all_symbols.return_value = ["000001.SZ", "000002.SZ"]

        handler = GroupHandler(db_operator=mock_db_operator)
        symbols = handler.get_symbols_for_group("test_group")

        assert symbols == ["000001.SZ", "000002.SZ"]
        mock_db_operator.get_all_symbols.assert_called_once()

    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.helpers.group_handler.get_config")
    def test_get_symbols_for_group_without_db_operator(
        self, mock_get_config, mock_parquet_queryer_class
    ):
        """测试没有 db_operator 时获取组的股票代码"""
        mock_config = Mock()
        mock_config.task_groups = {"test_group": ["stock_daily"]}
        mock_get_config.return_value = mock_config

        mock_parquet_queryer = Mock()
        mock_parquet_queryer.get_all_symbols.return_value = ["000001.SZ", "000002.SZ"]
        mock_parquet_queryer_class.create_default.return_value = mock_parquet_queryer

        handler = GroupHandler()
        result = handler.get_symbols_for_group("test_group")

        assert result == ["000001.SZ", "000002.SZ"]
        mock_parquet_queryer_class.create_default.assert_called_once()
        mock_parquet_queryer.get_all_symbols.assert_called_once()

    @patch("neo.helpers.group_handler.get_config")
    def test_get_symbols_for_group_system_task(self, mock_get_config):
        """测试 stock_basic 或 trade_cal 组不需要具体股票代码"""
        mock_config = Mock()
        mock_config.task_groups = {"sys_group": ["stock_basic", "trade_cal"]}
        mock_get_config.return_value = mock_config

        handler = GroupHandler()
        symbols = handler.get_symbols_for_group("sys_group")

        assert symbols == [""]
