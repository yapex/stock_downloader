"""SimpleDataProcessor 综合测试

测试数据清洗、转换、批量处理等核心功能
"""

import pandas as pd
import time
from unittest.mock import Mock, patch

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
        self.processor.batch_size = 10  # 设置较大的批量大小，避免自动刷新
        test_data = pd.DataFrame({"col1": [1, 2, 3]})
        
        result = self.processor.process("stock_basic", test_data)
        
        assert result is True
        
        # 验证数据被添加到缓冲区
        buffer_status = self.processor.get_buffer_status()
        assert buffer_status.get("stock_basic", 0) == 3  # 3行数据

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
        self.processor.batch_size = 10  # 设置较大的批量大小，避免自动刷新
        test_data = pd.DataFrame({"col1": [1, 2, 3]})
        
        # 通过process方法添加数据到缓冲区（不会触发自动刷新）
        self.processor.process("stock_basic", test_data)
        
        # 验证数据在缓冲区中
        buffer_status = self.processor.get_buffer_status()
        assert buffer_status.get("stock_basic", 0) > 0
        
        result = self.processor.flush_all()
        
        assert result is True
        self.mock_db_operator.upsert.assert_called_once()

    def test_batch_processing_with_auto_flush(self):
        """测试批量处理和自动刷新"""
        # 启用批量模式
        self.processor.enable_batch = True
        self.processor.batch_size = 2  # 设置较小的批量大小
        
        # 第一次处理 - 数据应该被缓存
        test_data1 = pd.DataFrame({"col1": [1]})
        result1 = self.processor.process("stock_basic", test_data1)
        
        # 验证处理成功
        assert result1 is True
        
        # 第二次处理 - 应该触发批量保存
        test_data2 = pd.DataFrame({"col1": [2]})
        result2 = self.processor.process("stock_basic", test_data2)
        
        # 验证处理成功
        assert result2 is True
        
        # 等待一小段时间让异步刷新完成
        time.sleep(0.1)
        
        # 验证数据库最终被调用
        assert self.mock_db_operator.upsert.call_count >= 1

    def test_batch_flush_failure_handling(self):
        """测试批量刷新失败的处理"""
        # 启用批量模式
        self.processor.enable_batch = True
        self.processor.batch_size = 1  # 设置为1，确保立即触发刷新
    
        # 创建测试数据
        test_data = pd.DataFrame({"col1": [1, 2]})
    
        # 模拟数据库操作失败
        self.mock_db_operator.upsert.side_effect = Exception("DB flush error")
    
        result = self.processor.process("stock_basic", test_data)
    
        # 验证处理成功（数据已添加到缓冲区）
        assert result is True
        
        # 等待一小段时间让异步刷新完成
        time.sleep(0.1)
        
        # 验证数据库操作被调用
        assert self.mock_db_operator.upsert.call_count >= 1