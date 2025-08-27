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
        assert handler._db_operator is mock_db_operator

    def test_init_without_db_operator(self):
        """测试不使用 db_operator 初始化"""
        handler = GroupHandler()
        assert handler._db_operator is None

    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")
    def test_create_default(self, mock_schema_loader_class, mock_db_operator_class):
        """测试 create_default 工厂方法"""
        # 设置 mock
        mock_schema_loader_instance = Mock()
        mock_schema_loader_class.return_value = mock_schema_loader_instance
        mock_db_operator_instance = Mock(spec=IDBOperator)
        mock_db_operator_class.return_value = mock_db_operator_instance

        # 调用工厂方法
        handler = GroupHandler.create_default()

        # 验证结果
        assert isinstance(handler, GroupHandler)
        assert handler._db_operator is mock_db_operator_instance
        mock_schema_loader_class.assert_called_once_with()
        mock_db_operator_class.assert_called_once_with(schema_loader=mock_schema_loader_instance)

    @patch("neo.helpers.group_handler.get_config")
    def test_get_task_types_for_group_valid_group(self, mock_get_config):
        """测试获取有效组的任务类型"""
        # 设置 mock 配置
        mock_config = Mock()
        mock_config.task_groups = {"test_group": ["stock_basic", "stock_daily"]}
        mock_get_config.return_value = mock_config

        handler = GroupHandler()
        task_types = handler.get_task_types_for_group("test_group")

        # 验证结果
        assert len(task_types) == 2
        from neo.task_bus.types import TaskType

        assert TaskType.stock_basic in task_types
        assert TaskType.stock_daily in task_types

    @patch("neo.helpers.group_handler.get_config")
    def test_get_task_types_for_group_invalid_group(self, mock_get_config):
        """测试获取无效组的任务类型"""
        # 设置 mock 配置
        mock_config = Mock()
        mock_config.task_groups = {}
        mock_get_config.return_value = mock_config

        handler = GroupHandler()

        with pytest.raises(ValueError, match="未找到组配置: invalid_group"):
            handler.get_task_types_for_group("invalid_group")

    @patch("neo.helpers.group_handler.get_config")
    def test_get_task_types_for_group_invalid_task_type(self, mock_get_config):
        """测试获取包含无效任务类型的组"""
        # 设置 mock 配置
        mock_config = Mock()
        mock_config.task_groups = {"test_group": ["invalid_task_type"]}
        mock_get_config.return_value = mock_config

        handler = GroupHandler()

        with pytest.raises(ValueError, match="未知的任务类型: invalid_task_type"):
            handler.get_task_types_for_group("test_group")

    @patch("neo.helpers.group_handler.get_config")
    def test_get_symbols_for_group_with_db_operator(self, mock_get_config):
        """测试使用 db_operator 获取组的股票代码"""
        # 设置 mock 配置
        mock_config = Mock()
        mock_config.task_groups = {
            "test_group": ["stock_daily"]  # 不包含 stock_basic
        }
        mock_get_config.return_value = mock_config

        mock_db_operator = Mock(spec=IDBOperator)
        mock_db_operator.get_all_symbols.return_value = ["000001.SZ", "000002.SZ"]

        handler = GroupHandler(db_operator=mock_db_operator)
        symbols = handler.get_symbols_for_group("test_group")

        assert symbols == ["000001.SZ", "000002.SZ"]
        mock_db_operator.get_all_symbols.assert_called_once()

    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")
    @patch("neo.helpers.group_handler.get_config")
    def test_get_symbols_for_group_without_db_operator(self, mock_get_config, mock_schema_loader_class, mock_parquet_queryer_class):
        """测试没有 db_operator 时获取组的股票代码"""
        # 设置 mock 配置
        mock_config = Mock()
        mock_config.task_groups = {
            "test_group": ["stock_daily"]  # 不包含 stock_basic
        }
        mock_get_config.return_value = mock_config

        # 设置 mock ParquetDBQueryer
        mock_parquet_queryer = Mock()
        mock_parquet_queryer.get_all_symbols.return_value = ["000001.SZ", "000002.SZ"]
        mock_parquet_queryer_class.return_value = mock_parquet_queryer

        handler = GroupHandler()
        result = handler.get_symbols_for_group("test_group")

        # 验证结果
        assert result == ["000001.SZ", "000002.SZ"]
        # 验证 ParquetDBQueryer 被正确创建和调用
        mock_schema_loader_class.assert_called_once()
        mock_parquet_queryer_class.assert_called_once_with(schema_loader=mock_schema_loader_class.return_value)
        mock_parquet_queryer.get_all_symbols.assert_called_once()

    @patch("neo.helpers.group_handler.get_config")
    def test_get_symbols_for_group_stock_basic(self, mock_get_config):
        """测试 stock_basic 组不需要具体股票代码"""
        # 设置 mock 配置
        mock_config = Mock()
        mock_config.task_groups = {
            "stock_basic_group": ["stock_basic", "other_task"]
        }
        mock_get_config.return_value = mock_config

        handler = GroupHandler()
        symbols = handler.get_symbols_for_group("stock_basic_group")

        # stock_basic 组应该返回空列表
        assert symbols == []

    @patch("neo.helpers.group_handler.get_config")
    def test_get_symbols_for_group_invalid_group(self, mock_get_config):
        """测试获取无效组的股票代码"""
        # 设置 mock 配置
        mock_config = Mock()
        mock_config.task_groups = {}
        mock_get_config.return_value = mock_config

        handler = GroupHandler()

        with pytest.raises(ValueError, match="未找到组配置: invalid_group"):
            handler.get_symbols_for_group("invalid_group")

    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")
    @patch("neo.helpers.group_handler.get_config")
    def test_get_symbols_for_group_db_exception(self, mock_get_config, mock_schema_loader_class, mock_parquet_queryer_class):
        """测试数据库异常情况"""
        # 设置 mock 配置
        mock_config = Mock()
        mock_config.task_groups = {
            "test_group": ["stock_daily"]
        }
        mock_get_config.return_value = mock_config

        # 设置 mock ParquetDBQueryer 抛出异常
        mock_parquet_queryer = Mock()
        mock_parquet_queryer.get_all_symbols.side_effect = Exception("数据库连接失败")
        mock_parquet_queryer_class.return_value = mock_parquet_queryer

        handler = GroupHandler()

        with pytest.raises(ValueError, match="从数据库获取股票代码失败"):
            handler.get_symbols_for_group("test_group")

    def test_get_all_symbols_from_db_no_symbols(self):
        """测试数据库中没有股票代码的情况"""
        mock_db_operator = Mock(spec=IDBOperator)
        mock_db_operator.get_all_symbols.return_value = []

        handler = GroupHandler(db_operator=mock_db_operator)

        with pytest.raises(ValueError, match="数据库中没有找到股票代码"):
            handler._get_all_symbols_from_db()

    def test_get_all_symbols_from_db_exception(self):
        """测试从数据库获取股票代码时发生异常"""
        mock_db_operator = Mock(spec=IDBOperator)
        mock_db_operator.get_all_symbols.side_effect = Exception("数据库连接失败")

        handler = GroupHandler(db_operator=mock_db_operator)

        with pytest.raises(ValueError, match="从数据库获取股票代码失败"):
            handler._get_all_symbols_from_db()
