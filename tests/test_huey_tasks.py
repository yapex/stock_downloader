import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock


class TestHueyTasks:
    """测试Huey任务"""
    
    def test_process_task_result_success(self):
        """测试成功处理TaskResult"""
        from downloader.task.types import TaskPriority, TaskType
        
        # 创建mock对象
        mock_data_processor = Mock()
        
        # 准备测试数据
        test_data = {
            'config': {
                'symbol': 'AAPL',
                'task_type': 'STOCK_DAILY',
                'priority': 'MEDIUM',
                'max_retries': 2
            },
            'success': True,
            'data': {'symbol': ['AAPL'], 'price': [150.0]},  # 可以转换为DataFrame的数据
            'error': None,
            'retry_count': 0
        }
        
        # 重置全局变量并mock get_data_processor函数
        import downloader.producer.huey_tasks as huey_tasks_module
        original_data_processor = huey_tasks_module._data_processor
        
        try:
            # 重置全局变量
            huey_tasks_module._data_processor = None
            
            with patch('downloader.producer.huey_tasks.get_data_processor') as mock_get_processor:
                mock_get_processor.return_value = mock_data_processor
                
                # 导入函数
                from downloader.producer.huey_tasks import process_task_result
                
                # 执行函数 - 调用原始函数而不是Huey任务
                process_task_result.func(test_data)
                
                # 验证get_data_processor被调用
                mock_get_processor.assert_called_once()
                
                # 验证process_task_result被调用
                mock_data_processor.process_task_result.assert_called_once()
                
                # 验证调用参数
                call_args = mock_data_processor.process_task_result.call_args[0][0]
                
                assert call_args.config.symbol == 'AAPL'
                assert call_args.config.task_type == TaskType.STOCK_DAILY
                assert call_args.success is True
                assert call_args.data is not None
                
                # 验证传入的TaskResult对象类型
                from downloader.task.types import TaskResult
                assert isinstance(call_args, TaskResult)
        finally:
            # 恢复原始状态
            huey_tasks_module._data_processor = original_data_processor
    
    def test_process_task_result_with_string_task_type(self):
        """测试字符串TaskType的处理"""
        from downloader.task.types import TaskPriority
        
        test_data = {
            'config': {
                'symbol': 'AAPL',
                'task_type': 'STOCK_BASIC',  # 字符串形式的TaskType
                'priority': 'MEDIUM',
                'max_retries': 3
            },
            'success': True,
            'data': {'symbol': ['AAPL'], 'price': [150.0]},
            'error': None,
            'retry_count': 0
        }
        
        # 设置mock
        mock_processor = Mock()
        
        import downloader.producer.huey_tasks as huey_tasks_module
        original_data_processor = huey_tasks_module._data_processor
        
        try:
            # 重置全局变量以确保get_data_processor被调用
            huey_tasks_module._data_processor = None
            
            with patch('downloader.producer.huey_tasks.get_data_processor', return_value=mock_processor):
                from downloader.producer.huey_tasks import process_task_result
                
                # 执行任务
                process_task_result.func(test_data)
                
                # 验证TaskType正确转换
                from downloader.task.types import TaskType
                call_args = mock_processor.process_task_result.call_args[0][0]
                assert call_args.config.symbol == ''  # STOCK_BASIC任务类型会自动将symbol设为空字符串
                assert call_args.config.task_type == TaskType.STOCK_BASIC
        finally:
            # 恢复原始状态
            huey_tasks_module._data_processor = original_data_processor
    
    def test_process_task_result_failure(self):
        """测试处理失败的TaskResult"""
        from downloader.task.types import TaskPriority
        
        test_data = {
            'config': {
                'symbol': 'AAPL',
                'task_type': 'STOCK_BASIC',
                'priority': 'LOW',
                'max_retries': 3
            },
            'success': False,
            'data': None,
            'error': 'Test error message',
            'retry_count': 1
        }
        
        # 设置mock
        mock_processor = Mock()
        
        import downloader.producer.huey_tasks as huey_tasks_module
        original_data_processor = huey_tasks_module._data_processor
        
        try:
            # 重置全局变量以确保get_data_processor被调用
            huey_tasks_module._data_processor = None
            
            with patch('downloader.producer.huey_tasks.get_data_processor', return_value=mock_processor):
                from downloader.producer.huey_tasks import process_task_result
                
                # 执行任务
                process_task_result.func(test_data)
                
                # 验证处理器被调用
                mock_processor.process_task_result.assert_called_once()
                
                # 验证传入的TaskResult
                from downloader.task.types import TaskResult
                from downloader.task.types import TaskType
                
                call_args = mock_processor.process_task_result.call_args[0][0]
                assert isinstance(call_args, TaskResult)
                assert call_args.config.symbol == ''  # STOCK_BASIC任务类型会自动将symbol设为空字符串
                assert call_args.config.task_type == TaskType.STOCK_BASIC
                assert call_args.success is False
                assert call_args.error == 'Test error message'
                assert call_args.retry_count == 1
        finally:
            # 恢复原始状态
            huey_tasks_module._data_processor = original_data_processor
    
    def test_process_task_result_with_none_data(self):
        """测试处理空数据的TaskResult"""
        from downloader.task.types import TaskPriority
        
        test_data = {
            'config': {
                'symbol': 'AAPL',
                'task_type': 'STOCK_BASIC',
                'priority': 'MEDIUM',
                'max_retries': 3
            },
            'success': True,
            'data': None,
            'error': None,
            'retry_count': 0
        }
        
        # 设置mock
        mock_processor = Mock()
        
        import downloader.producer.huey_tasks as huey_tasks_module
        original_data_processor = huey_tasks_module._data_processor
        
        try:
            # 重置全局变量以确保get_data_processor被调用
            huey_tasks_module._data_processor = None
            
            with patch('downloader.producer.huey_tasks.get_data_processor', return_value=mock_processor):
                from downloader.producer.huey_tasks import process_task_result
                process_task_result.func(test_data)
                
                # 验证处理器被调用
                mock_processor.process_task_result.assert_called_once()
                
                # 验证传入的TaskResult
                from downloader.task.types import TaskResult
                from downloader.task.types import TaskType
                
                call_args = mock_processor.process_task_result.call_args[0][0]
                assert isinstance(call_args, TaskResult)
                assert call_args.config.symbol == ''  # STOCK_BASIC任务类型会自动将symbol设为空字符串
                assert call_args.config.task_type == TaskType.STOCK_BASIC
                assert call_args.success is True
                assert call_args.data is None
        finally:
            # 恢复原始状态
            huey_tasks_module._data_processor = original_data_processor
    
    def test_get_data_processor_singleton(self):
        """测试DataProcessor单例模式"""
        with patch('downloader.producer.huey_tasks.DataProcessor') as mock_dp_class, \
             patch('downloader.producer.huey_tasks.BatchSaver') as mock_bs_class, \
             patch('downloader.producer.huey_tasks.DBOperator') as mock_db_class, \
             patch('downloader.producer.huey_tasks.get_config') as mock_config:
            
            # 配置mock
            mock_config.return_value.consumer.batch_size = 100
            mock_db_instance = Mock()
            mock_db_class.return_value = mock_db_instance
            mock_bs_instance = Mock()
            mock_bs_class.return_value = mock_bs_instance
            mock_dp_instance = Mock()
            mock_dp_class.return_value = mock_dp_instance
            
            # 清除全局变量
            import downloader.producer.huey_tasks
            downloader.producer.huey_tasks._data_processor = None
            
            from downloader.producer.huey_tasks import get_data_processor
            
            # 第一次调用
            processor1 = get_data_processor()
            
            # 第二次调用
            processor2 = get_data_processor()
            
            # 验证返回同一个实例
            assert processor1 is processor2
            
            # 验证只创建了一次
            mock_dp_class.assert_called_once()
    
    def test_process_task_result_error_handling(self):
        """测试错误处理"""
        from downloader.task.types import TaskPriority
        
        test_data = {
            'config': {
                'symbol': 'AAPL',
                'task_type': 'INVALID_TYPE',  # 无效的TaskType
                'priority': 'MEDIUM',
                'max_retries': 3
            },
            'success': True,
            'data': None,
            'error': None,
            'retry_count': 0
        }
        
        # 应该抛出异常（因为INVALID_TYPE不是有效的TaskType）
        with pytest.raises(KeyError):
            from downloader.producer.huey_tasks import process_task_result
            process_task_result.func(test_data)