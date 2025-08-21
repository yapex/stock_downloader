import pytest
from unittest.mock import MagicMock, patch
from neo.helpers.group_handler import GroupHandler
from neo.task_bus.types import TaskType
from box import Box


class TestGroupHandler:
    """GroupHandler 类的测试用例"""
    
    def setup_method(self):
        """每个测试方法执行前的设置"""
        self.group_handler = GroupHandler()
    
    @patch('neo.helpers.group_handler.get_config')
    def test_get_symbols_for_group_test(self, mock_get_config):
        """测试获取 test 组的股票代码"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'test': ['stock_daily']
            }
        })
        mock_get_config.return_value = mock_config
        
        symbols = self.group_handler.get_symbols_for_group('test')
        
        # test 组应该返回默认的测试股票代码
        expected_symbols = ['000001.SZ', '600519.SH']
        assert symbols == expected_symbols
    
    @patch('neo.helpers.group_handler.get_config')
    @patch('neo.database.operator.DBOperator')
    def test_get_symbols_for_group_stock_basic(self, mock_db_operator_class, mock_get_config):
        """测试获取 stock_basic 组的股票代码（从数据库）"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'stock_basic': ['stock_basic']
            }
        })
        mock_get_config.return_value = mock_config
        
        # 模拟数据库操作器
        mock_db_operator = MagicMock()
        mock_db_operator.get_all_stock_codes.return_value = ['000001.SZ', '600000.SH']
        mock_db_operator_class.return_value = mock_db_operator
        
        symbols = self.group_handler.get_symbols_for_group('stock_basic')
        
        # stock_basic 组应该返回空列表（不需要具体股票代码）
        expected_symbols = []
        assert symbols == expected_symbols
    
    @patch('neo.helpers.group_handler.get_config')
    @patch('neo.database.operator.DBOperator')
    def test_get_symbols_for_group_stock_basic_no_db_operator(self, mock_db_operator_class, mock_get_config):
        """测试 stock_basic 组无数据库操作器时的处理"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'stock_basic': ['stock_basic']
            }
        })
        mock_get_config.return_value = mock_config
        
        # 模拟数据库操作器创建失败
        mock_db_operator_class.side_effect = Exception("Database connection failed")
        
        symbols = self.group_handler.get_symbols_for_group('stock_basic')
        
        # stock_basic 组应该返回空列表（不需要具体股票代码）
        expected_symbols = []
        assert symbols == expected_symbols
    
    @patch('neo.helpers.group_handler.get_config')
    def test_get_symbols_for_group_unknown_group(self, mock_get_config):
        """测试未知组名的股票代码获取"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'test': ['stock_daily']
            }
        })
        mock_get_config.return_value = mock_config
        
        # 未知组应该抛出 ValueError
        with pytest.raises(ValueError, match="未找到组配置: unknown_group"):
            self.group_handler.get_symbols_for_group('unknown_group')
    
    @patch('neo.helpers.group_handler.get_config')
    def test_get_task_types_for_group_test(self, mock_get_config):
        """测试获取 test 组的任务类型"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'test': ['stock_daily']
            }
        })
        mock_get_config.return_value = mock_config
        
        task_types = self.group_handler.get_task_types_for_group('test')
        
        assert len(task_types) == 1
        assert task_types[0] == TaskType.STOCK_DAILY
    
    @patch('neo.helpers.group_handler.get_config')
    def test_get_task_types_for_group_multiple_types(self, mock_get_config):
        """测试获取包含多个任务类型的组"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'all': ['stock_basic', 'stock_daily']
            }
        })
        mock_get_config.return_value = mock_config
        
        task_types = self.group_handler.get_task_types_for_group('all')
        
        assert len(task_types) == 2
        assert TaskType.STOCK_BASIC in task_types
        assert TaskType.STOCK_DAILY in task_types
    
    @patch('neo.helpers.group_handler.get_config')
    def test_get_task_types_for_group_unknown_group(self, mock_get_config):
        """测试未知组名的任务类型获取"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'test': ['stock_daily']
            }
        })
        mock_get_config.return_value = mock_config
        
        # 未知组应该抛出 ValueError
        with pytest.raises(ValueError, match="未找到组配置: unknown_group"):
            self.group_handler.get_task_types_for_group('unknown_group')
    
    @patch('neo.helpers.group_handler.get_config')
    def test_get_task_types_for_group_invalid_task_type(self, mock_get_config):
        """测试无效任务类型的处理"""
        # 模拟配置
        mock_config = Box({
            'task_groups': {
                'invalid': ['invalid_task_type']
            }
        })
        mock_get_config.return_value = mock_config
        
        with pytest.raises(ValueError, match="未知的任务类型: invalid_task_type"):
            self.group_handler.get_task_types_for_group('invalid')