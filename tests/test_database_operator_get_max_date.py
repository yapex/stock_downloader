"""DatabaseOperator get_max_date 方法单元测试"""

from unittest.mock import Mock, patch

import pytest

from src.neo.database.operator import DatabaseOperator
from src.neo.database.schema_loader import SchemaLoader


class TestDatabaseOperatorGetMaxDate:
    """DatabaseOperator get_max_date 方法测试类"""

    @pytest.fixture
    def mock_schema_loader(self):
        """模拟 Schema 加载器"""
        loader = Mock(spec=SchemaLoader)
        return loader

    @pytest.fixture
    def operator(self, mock_schema_loader):
        """创建 DatabaseOperator 实例"""
        with patch('src.neo.database.operator.duckdb.connect'):
            return DatabaseOperator(
                db_path=":memory:",
                schema_loader=mock_schema_loader
            )

    def test_get_max_date_with_ts_code_primary_key(self, operator, mock_schema_loader):
        """测试包含 ts_code 主键的表查询最大日期"""
        # 模拟 schema 配置
        mock_schema_loader.get_table_config.return_value = {
            'primary_key': ['ts_code', 'trade_date'],
            'date_col': 'trade_date'
        }
        
        # 模拟数据库查询结果
        mock_result = [('000001.SZ', '20240101'), ('000002.SZ', '20240102')]
        
        with patch.object(operator, '_execute_query', return_value=mock_result):
            result = operator.get_max_date('stock_daily', ['000001.SZ', '000002.SZ'])
            
        # 验证结果
        expected = {'000001.SZ': '20240101', '000002.SZ': '20240102'}
        assert result == expected
        
        # 验证 SQL 查询包含 ts_code 分组
        operator._execute_query.assert_called_once()
        sql_query = operator._execute_query.call_args[0][0]
        assert 'ts_code' in sql_query
        assert 'GROUP BY ts_code' in sql_query
        assert 'WHERE ts_code IN' in sql_query

    def test_get_max_date_without_ts_code_primary_key(self, operator, mock_schema_loader):
        """测试不包含 ts_code 主键的表查询最大日期（如 trade_cal）"""
        # 模拟 schema 配置
        mock_schema_loader.get_table_config.return_value = {
            'primary_key': ['exchange', 'cal_date'],
            'date_col': 'cal_date'
        }
        
        # 模拟数据库查询结果
        mock_result = [('20240105',)]  # 只返回最大日期
        
        with patch.object(operator, '_execute_query', return_value=mock_result):
            result = operator.get_max_date('trade_cal', ['SSE'])
            
        # 验证结果
        expected = {'SSE': '20240105'}
        assert result == expected
        
        # 验证 SQL 查询不包含 ts_code 分组
        operator._execute_query.assert_called_once()
        sql_query = operator._execute_query.call_args[0][0]
        assert 'ts_code' not in sql_query
        assert 'GROUP BY' not in sql_query
        assert 'WHERE' not in sql_query  # 不需要 WHERE 条件

    def test_get_max_date_single_primary_key_with_ts_code(self, operator, mock_schema_loader):
        """测试单一主键包含 ts_code 的表"""
        # 模拟 schema 配置
        mock_schema_loader.get_table_config.return_value = {
            'primary_key': ['ts_code'],
            'date_col': 'list_date'
        }
        
        # 模拟数据库查询结果
        mock_result = [('000001.SZ', '19910403')]
        
        with patch.object(operator, '_execute_query', return_value=mock_result):
            result = operator.get_max_date('stock_basic', ['000001.SZ'])
            
        # 验证结果
        expected = {'000001.SZ': '19910403'}
        assert result == expected
        
        # 验证 SQL 查询包含 ts_code
        operator._execute_query.assert_called_once()
        sql_query = operator._execute_query.call_args[0][0]
        assert 'ts_code' in sql_query
        assert 'GROUP BY ts_code' in sql_query

    def test_get_max_date_empty_symbols_list(self, operator, mock_schema_loader):
        """测试空的 symbols 列表"""
        # 模拟 schema 配置
        mock_schema_loader.get_table_config.return_value = {
            'primary_key': ['ts_code', 'trade_date'],
            'date_col': 'trade_date'
        }
        
        result = operator.get_max_date('stock_daily', [])
        
        # 验证结果为空字典
        assert result == {}

    def test_get_max_date_no_data_found(self, operator, mock_schema_loader):
        """测试查询无数据的情况"""
        # 模拟 schema 配置
        mock_schema_loader.get_table_config.return_value = {
            'primary_key': ['ts_code', 'trade_date'],
            'date_col': 'trade_date'
        }
        
        # 模拟数据库查询返回空结果
        with patch.object(operator, '_execute_query', return_value=[]):
            result = operator.get_max_date('stock_daily', ['000001.SZ'])
            
        # 验证结果为空字典
        assert result == {}

    def test_get_max_date_database_error(self, operator, mock_schema_loader):
        """测试数据库查询错误的情况"""
        # 模拟 schema 配置
        mock_schema_loader.get_table_config.return_value = {
            'primary_key': ['ts_code', 'trade_date'],
            'date_col': 'trade_date'
        }
        
        # 模拟数据库查询抛出异常
        with patch.object(operator, '_execute_query', side_effect=Exception("数据库连接失败")):
            with pytest.raises(Exception, match="数据库连接失败"):
                operator.get_max_date('stock_daily', ['000001.SZ'])

    def test_get_max_date_multiple_symbols_with_ts_code(self, operator, mock_schema_loader):
        """测试多个股票代码的查询"""
        # 模拟 schema 配置
        mock_schema_loader.get_table_config.return_value = {
            'primary_key': ['ts_code', 'trade_date'],
            'date_col': 'trade_date'
        }
        
        # 模拟数据库查询结果
        mock_result = [
            ('000001.SZ', '20240101'),
            ('000002.SZ', '20240102'),
            ('600519.SH', '20240103')
        ]
        
        with patch.object(operator, '_execute_query', return_value=mock_result):
            result = operator.get_max_date('stock_daily', ['000001.SZ', '000002.SZ', '600519.SH'])
            
        # 验证结果
        expected = {
            '000001.SZ': '20240101',
            '000002.SZ': '20240102',
            '600519.SH': '20240103'
        }
        assert result == expected

    def test_get_max_date_sql_injection_protection(self, operator, mock_schema_loader):
        """测试 SQL 注入防护"""
        # 模拟 schema 配置
        mock_schema_loader.get_table_config.return_value = {
            'primary_key': ['ts_code', 'trade_date'],
            'date_col': 'trade_date'
        }
        
        # 尝试 SQL 注入
        malicious_symbols = ["'; DROP TABLE stock_daily; --"]
        
        with patch.object(operator, '_execute_query', return_value=[]):
            result = operator.get_max_date('stock_daily', malicious_symbols)
            
        # 验证查询仍然正常执行（参数化查询应该防止注入）
        assert result == {}
        operator._execute_query.assert_called_once()

    @pytest.mark.parametrize("table_name,primary_key,date_col,has_ts_code", [
        ('stock_daily', ['ts_code', 'trade_date'], 'trade_date', True),
        ('daily_basic', ['ts_code', 'trade_date'], 'trade_date', True),
        ('stock_basic', ['ts_code'], 'list_date', True),
        ('trade_cal', ['exchange', 'cal_date'], 'cal_date', False),
        ('income', ['ts_code', 'end_date'], 'end_date', True),
    ])
    def test_get_max_date_different_table_types(self, operator, mock_schema_loader, 
                                               table_name, primary_key, date_col, has_ts_code):
        """参数化测试不同表类型的查询逻辑"""
        # 模拟 schema 配置
        mock_schema_loader.get_table_config.return_value = {
            'primary_key': primary_key,
            'date_col': date_col
        }
        
        # 模拟查询结果
        if has_ts_code:
            mock_result = [('000001.SZ', '20240101')]
        else:
            mock_result = [('20240101',)]
        
        with patch.object(operator, '_execute_query', return_value=mock_result):
            result = operator.get_max_date(table_name, ['000001.SZ'])
            
        # 验证结果格式
        assert isinstance(result, dict)
        assert len(result) <= 1
        
        # 验证 SQL 查询逻辑
        operator._execute_query.assert_called_once()
        sql_query = operator._execute_query.call_args[0][0]
        
        if has_ts_code:
            assert 'ts_code' in sql_query
            assert 'GROUP BY ts_code' in sql_query
        else:
            assert 'ts_code' not in sql_query
            assert 'GROUP BY' not in sql_query