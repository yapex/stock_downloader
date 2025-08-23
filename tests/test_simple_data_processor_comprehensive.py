"""SimpleDataProcessor 综合测试

测试数据清洗、转换、批量处理等核心功能
"""

import pandas as pd
import time
from unittest.mock import Mock, patch
import pytest

from neo.data_processor.simple_data_processor import SimpleDataProcessor
from neo.database.operator import DBOperator
from neo.database.interfaces import ISchemaLoader


class TestSimpleDataProcessorComprehensive:
    """SimpleDataProcessor 综合测试类"""

    def setup_method(self):
        """测试前设置"""
        self.mock_db_operator = Mock(spec=DBOperator)
        self.mock_schema_loader = Mock(spec=ISchemaLoader)
        
        # 设置schema_loader的默认返回值
        mock_schema = Mock()
        mock_schema.table_name = "test_table"
        mock_schema.columns = [
            {"name": "ts_code", "type": "TEXT"},
            {"name": "symbol", "type": "TEXT"},
            {"name": "name", "type": "TEXT"}
        ]
        self.mock_schema_loader.load_schema.return_value = mock_schema
        
        self.processor = SimpleDataProcessor(
            db_operator=self.mock_db_operator,
            schema_loader=self.mock_schema_loader,
            enable_batch=False
        )

    def test_create_default_factory_method(self):
        """测试create_default工厂方法"""
        with patch('neo.data_processor.simple_data_processor.DBOperator') as mock_db_op, \
             patch('neo.data_processor.simple_data_processor.SchemaLoader') as mock_schema:
            
            processor = SimpleDataProcessor.create_default()
            
            assert isinstance(processor, SimpleDataProcessor)
            assert processor.enable_batch is True
            mock_db_op.create_default.assert_called_once()
            mock_schema.assert_called_once()

    def test_get_table_name_with_string(self):
        """测试_get_table_name方法处理字符串参数"""
        result = self.processor._get_table_name("stock_basic")
        
        assert result == "test_table"
        self.mock_schema_loader.load_schema.assert_called_once_with("stock_basic")

    def test_get_table_name_with_enum(self):
        """测试_get_table_name方法处理枚举参数"""
        mock_enum = Mock()
        mock_enum.name = "stock_basic"
        
        result = self.processor._get_table_name(mock_enum)
        
        assert result == "test_table"
        self.mock_schema_loader.load_schema.assert_called_once_with("stock_basic")

    def test_get_table_name_key_error(self):
        """测试_get_table_name方法处理KeyError"""
        self.mock_schema_loader.load_schema.side_effect = KeyError("Unknown task type")
        
        result = self.processor._get_table_name("unknown_type")
        
        assert result is None



    def test_save_data_success(self):
        """测试数据保存成功"""
        test_data = pd.DataFrame({"col1": [1, 2, 3]})
        
        result = self.processor._save_data(test_data, "stock_basic")
        
        assert result is True
        self.mock_db_operator.upsert.assert_called_once_with("test_table", test_data)

    def test_save_data_unknown_task_type(self):
        """测试保存数据 - 未知任务类型"""
        test_data = pd.DataFrame({"col1": [1, 2, 3]})
        self.mock_schema_loader.load_schema.side_effect = KeyError("Unknown")
        
        result = self.processor._save_data(test_data, "unknown_type")
        
        assert result is False

    def test_save_data_db_exception(self):
        """测试保存数据 - 数据库异常"""
        test_data = pd.DataFrame({"col1": [1, 2, 3]})
        self.mock_db_operator.upsert.side_effect = Exception("DB error")
        
        result = self.processor._save_data(test_data, "stock_basic")
        
        assert result is False

    def test_process_with_batch_mode(self):
        """测试批量模式处理"""
        # 启用批量模式
        self.processor.enable_batch = True
        test_data = pd.DataFrame({"col1": [1, 2, 3]})
        
        with patch.object(self.processor, '_add_to_buffer', return_value=True) as mock_add:
            result = self.processor.process("stock_basic", test_data)
            
        assert result is True
        mock_add.assert_called_once_with(test_data, "stock_basic")

    def test_process_empty_data(self):
        """测试处理空数据"""
        empty_data = pd.DataFrame()
        
        result = self.processor.process("stock_basic", empty_data)
        
        assert result is False

    def test_process_none_data(self):
        """测试处理None数据"""
        result = self.processor.process("stock_basic", None)
        
        assert result is False

    def test_process_exception_handling(self):
        """测试处理过程中的异常处理"""
        test_data = pd.DataFrame({"col1": [1, 2, 3]})
        self.mock_db_operator.upsert.side_effect = Exception("Processing error")
        
        result = self.processor.process("stock_basic", test_data)
        
        assert result is False

    def test_flush_all_empty_buffers(self):
        """测试刷新空缓冲区"""
        result = self.processor.flush_all()
        
        assert result is True

    def test_flush_all_with_data(self):
        """测试刷新有数据的缓冲区"""
        # 启用批量模式并添加数据到缓冲区
        self.processor.enable_batch = True
        test_data = pd.DataFrame({"col1": [1, 2, 3]})
        
        # 手动添加数据到缓冲区
        self.processor.batch_buffers["stock_basic"].append(test_data)
        
        result = self.processor.flush_all()
        
        assert result is True
        self.mock_db_operator.upsert.assert_called_once()

    def test_individual_flush_updates_last_flush_time(self):
        """测试单独刷新成功后更新最后刷新时间"""
        import time
        from unittest.mock import patch
        
        # 启用批量模式
        self.processor.enable_batch = True
        self.processor.batch_size = 2  # 设置较小的批量大小
        
        # 记录初始刷新时间
        initial_flush_time = self.processor.last_flush_time
        
        # 创建足够触发刷新的数据（行数达到batch_size）
        test_data = pd.DataFrame({"col1": [1, 2]})
        
        # 模拟时间流逝
        with patch('time.time', return_value=initial_flush_time + 10):
            result = self.processor.process("stock_basic", test_data)
        
        # 验证处理成功
        assert result is True
        
        # 验证最后刷新时间被更新
        assert self.processor.last_flush_time == initial_flush_time + 10
        
        # 验证数据库操作被调用
        self.mock_db_operator.upsert.assert_called_once()

    def test_individual_flush_failure_handling(self):
        """测试单独刷新失败的处理"""
        # 启用批量模式
        self.processor.enable_batch = True
        self.processor.batch_size = 2  # 设置较小的批量大小
        
        # 创建足够触发刷新的数据（行数达到batch_size）
        test_data = pd.DataFrame({"col1": [1, 2]})
        
        # 模拟数据库操作失败
        self.mock_db_operator.upsert.side_effect = Exception("DB flush error")
        
        result = self.processor.process("stock_basic", test_data)
        
        # 验证处理失败
        assert result is False
        
        # 验证数据库操作被调用
        self.mock_db_operator.upsert.assert_called_once()