#!/usr/bin/env python3
"""Huey 任务消费端集成测试

测试 Huey Worker 消费和处理任务的完整流程。
"""

import pytest
import pandas as pd
import sqlite3
import time
from unittest.mock import patch, Mock, MagicMock

from neo.task_bus.huey_task_bus import process_task_result, get_huey
from neo.task_bus.types import TaskResult, DownloadTaskConfig, TaskType, TaskPriority
from neo.data_processor.simple_data_processor import SimpleDataProcessor


class TestHueyConsumerIntegration:
    """Huey 消费端集成测试类"""
    
    def setup_method(self):
        """每个测试方法执行前的设置"""
        self.huey = get_huey()
        # 确保使用即时模式进行测试
        self.huey.immediate = True
    
    def create_test_task_data(self, symbol="000001.SZ", task_type=TaskType.STOCK_BASIC, success=True, data_rows=1):
        """创建测试用的任务数据"""
        config = DownloadTaskConfig(
            symbol=symbol,
            task_type=task_type,
            priority=TaskPriority.HIGH,
            max_retries=3
        )
        
        # 创建符合股票数据格式的测试数据
        test_data = None
        if success and data_rows > 0:
            test_data = pd.DataFrame({
                'ts_code': [f'{symbol}'] * data_rows,
                'symbol': [symbol.split('.')[0]] * data_rows,
                'name': ['测试股票'] * data_rows,
                'area': ['深圳'] * data_rows,
                'industry': ['测试行业'] * data_rows,
                'cnspell': ['test'] * data_rows,
                'market': ['主板'] * data_rows,
                'list_date': ['20200101'] * data_rows,
                'act_name': ['测试控制人'] * data_rows,
                'act_ent_type': ['企业'] * data_rows
            })
        
        task_result = TaskResult(
            config=config,
            success=success,
            data=test_data,
            error=None if success else Exception("测试错误"),
            retry_count=0
        )
        
        # 序列化为字典格式（模拟 HueyTaskBus._serialize_task_result 的输出）
        task_result_data = {
            'config': {
                'symbol': config.symbol,
                'task_type': config.task_type.value,
                'priority': config.priority.value,
                'max_retries': config.max_retries
            },
            'success': task_result.success,
            'data': test_data.to_dict('records') if test_data is not None else None,
            'error': str(task_result.error) if task_result.error else None,
            'retry_count': task_result.retry_count
        }
        
        return task_result_data
    
    @patch('neo.data_processor.simple_data_processor.SimpleDataProcessor.process')
    def test_process_task_result_success(self, mock_process, capfd):
        """测试成功处理任务结果"""
        # 设置模拟返回值
        mock_process.return_value = True
        
        # 创建测试数据
        task_data = self.create_test_task_data()
        
        # 执行任务处理
        process_task_result(task_data)
        
        # 验证数据处理器被调用
        mock_process.assert_called_once()
        
        # 验证调用参数
        processed_task_result = mock_process.call_args[0][0]
        assert processed_task_result.config.symbol == "000001.SZ"
        assert processed_task_result.config.task_type == TaskType.STOCK_BASIC
        assert processed_task_result.success is True
        assert processed_task_result.data is not None
        assert len(processed_task_result.data) == 1
        
        # 验证控制台输出
        captured = capfd.readouterr()
        assert "🚀 队列任务开始: 000001.SZ_TaskTemplate" in captured.out
        assert "✅ 队列任务完成: 000001.SZ_TaskTemplate" in captured.out
    
    @patch('neo.data_processor.simple_data_processor.SimpleDataProcessor.process')
    def test_process_task_result_failure(self, mock_process, capfd):
        """测试处理任务结果失败的情况"""
        # 设置模拟返回值为失败
        mock_process.return_value = False
        
        # 创建测试数据
        task_data = self.create_test_task_data()
        
        # 执行任务处理
        process_task_result(task_data)
        
        # 验证数据处理器被调用
        mock_process.assert_called_once()
        
        # 验证控制台输出
        captured = capfd.readouterr()
        assert "🚀 队列任务开始: 000001.SZ_TaskTemplate" in captured.out
        assert "❌ 队列任务失败: 000001.SZ_TaskTemplate" in captured.out
    
    @patch('neo.data_processor.simple_data_processor.SimpleDataProcessor.process')
    def test_process_task_result_with_exception(self, mock_process, capfd):
        """测试处理任务时发生异常"""
        # 设置模拟抛出异常
        mock_process.side_effect = Exception("数据处理异常")
        
        # 创建测试数据
        task_data = self.create_test_task_data()
        
        # 测试异常处理 - 在 Huey 上下文中异常会被捕获并记录
        # 我们主要验证异常处理的行为和日志输出
        try:
            process_task_result(task_data)
        except Exception:
            pass  # 异常可能被 Huey 处理
        
        # 验证数据处理器被调用
        mock_process.assert_called_once()
        
        # 验证控制台输出包含异常信息
        captured = capfd.readouterr()
        assert "🚀 队列任务开始: 000001.SZ_TaskTemplate" in captured.out
        assert "💥 队列任务异常:" in captured.out
        assert "数据处理异常" in captured.out
    
    def test_process_task_result_data_deserialization(self, capfd):
        """测试任务数据的反序列化过程"""
        with patch('neo.data_processor.simple_data_processor.SimpleDataProcessor.process') as mock_process:
            mock_process.return_value = True
            
            # 创建包含多行数据的测试
            task_data = self.create_test_task_data(data_rows=3)
            
            # 执行任务处理
            process_task_result(task_data)
            
            # 验证反序列化的数据
            processed_task_result = mock_process.call_args[0][0]
            assert isinstance(processed_task_result.data, pd.DataFrame)
            assert len(processed_task_result.data) == 3
            assert 'ts_code' in processed_task_result.data.columns
            assert processed_task_result.data.iloc[0]['ts_code'] == '000001.SZ'
    
    def test_process_task_result_different_task_types(self, capfd):
        """测试不同任务类型的处理"""
        task_types = [
            TaskType.STOCK_BASIC,
            TaskType.STOCK_DAILY,
            TaskType.DAILY_BASIC
        ]
        
        with patch('neo.data_processor.simple_data_processor.SimpleDataProcessor.process') as mock_process:
            mock_process.return_value = True
            
            for task_type in task_types:
                # 创建不同任务类型的测试数据
                task_data = self.create_test_task_data(task_type=task_type)
                
                # 执行任务处理
                process_task_result(task_data)
                
                # 验证任务类型正确传递
                processed_task_result = mock_process.call_args[0][0]
                assert processed_task_result.config.task_type == task_type
        
        # 验证所有任务类型都被处理
        assert mock_process.call_count == len(task_types)
    
    def test_process_task_result_priority_handling(self):
        """测试任务优先级的处理"""
        priorities = [TaskPriority.LOW, TaskPriority.MEDIUM, TaskPriority.HIGH]
        
        with patch('neo.data_processor.simple_data_processor.SimpleDataProcessor.process') as mock_process:
            mock_process.return_value = True
            
            for priority in priorities:
                # 创建不同优先级的任务配置
                config = DownloadTaskConfig(
                    symbol="000001.SZ",
                    task_type=TaskType.STOCK_BASIC,
                    priority=priority,
                    max_retries=3
                )
                
                task_data = {
                    'config': {
                        'symbol': config.symbol,
                        'task_type': config.task_type.value,
                        'priority': config.priority.value,
                        'max_retries': config.max_retries
                    },
                    'success': True,
                    'data': [{'ts_code': '000001.SZ', 'name': '测试'}],
                    'error': None,
                    'retry_count': 0
                }
                
                # 执行任务处理
                process_task_result(task_data)
                
                # 验证优先级正确传递
                processed_task_result = mock_process.call_args[0][0]
                assert processed_task_result.config.priority == priority


class TestSimpleDataProcessorConsumer:
    """SimpleDataProcessor 消费端功能测试"""
    
    def setup_method(self):
        """每个测试方法执行前的设置"""
        self.data_processor = SimpleDataProcessor()
    
    def create_task_result(self, success=True, data_rows=1, symbol="000001.SZ"):
        """创建 TaskResult 对象"""
        config = DownloadTaskConfig(
            symbol=symbol,
            task_type=TaskType.STOCK_BASIC,
            priority=TaskPriority.HIGH
        )
        
        test_data = None
        if success and data_rows > 0:
            test_data = pd.DataFrame({
                'ts_code': [f'{symbol}'] * data_rows,
                'symbol': [symbol.split('.')[0]] * data_rows,
                'name': ['测试股票'] * data_rows,
                'area': ['深圳'] * data_rows,
                'industry': ['测试行业'] * data_rows,
                'cnspell': ['test'] * data_rows,
                'market': ['主板'] * data_rows,
                'list_date': ['20200101'] * data_rows,
                'act_name': ['测试控制人'] * data_rows,
                'act_ent_type': ['企业'] * data_rows
            })
        
        return TaskResult(
            config=config,
            success=success,
            data=test_data,
            error=None if success else Exception("测试错误")
        )
    
    @patch('neo.data_processor.simple_data_processor.SimpleDataProcessor._save_data')
    def test_process_successful_task(self, mock_save_data, capfd):
        """测试成功处理任务"""
        mock_save_data.return_value = True
        
        task_result = self.create_task_result()
        result = self.data_processor.process(task_result)
        
        assert result is True
        mock_save_data.assert_called_once()
        
        # 验证控制台输出
        captured = capfd.readouterr()
        assert "📊 开始处理: 000001.SZ_STOCK_BASIC" in captured.out
        assert "🎉 数据处理完成: 000001.SZ_STOCK_BASIC，成功保存 1 行数据" in captured.out
    
    def test_process_failed_task(self, capfd):
        """测试处理失败的任务"""
        task_result = self.create_task_result(success=False)
        result = self.data_processor.process(task_result)
        
        assert result is False
        
        # 验证控制台输出
        captured = capfd.readouterr()
        assert "❌ 任务执行失败，跳过处理: 000001.SZ_STOCK_BASIC - 测试错误" in captured.out
    
    def test_process_empty_data_task(self, capfd):
        """测试处理空数据的任务"""
        task_result = self.create_task_result(data_rows=0)
        result = self.data_processor.process(task_result)
        
        assert result is False
        
        # 验证控制台输出
        captured = capfd.readouterr()
        assert "⚠️  数据为空，跳过处理: 000001.SZ_STOCK_BASIC" in captured.out
    
    @patch('neo.data_processor.simple_data_processor.SimpleDataProcessor._save_data')
    def test_process_multiple_rows(self, mock_save_data, capfd):
        """测试处理多行数据"""
        mock_save_data.return_value = True
        
        task_result = self.create_task_result(data_rows=5)
        result = self.data_processor.process(task_result)
        
        assert result is True
        mock_save_data.assert_called_once()
        
        # 验证控制台输出
        captured = capfd.readouterr()
        assert "📈 数据行数: 5 行，列数: 10 列" in captured.out
        assert "🎉 数据处理完成: 000001.SZ_STOCK_BASIC，成功保存 5 行数据" in captured.out
    
    @patch('neo.data_processor.simple_data_processor.SimpleDataProcessor._save_data')
    def test_process_save_failure(self, mock_save_data, capfd):
        """测试数据保存失败的情况"""
        mock_save_data.return_value = False
        
        task_result = self.create_task_result()
        result = self.data_processor.process(task_result)
        
        assert result is False
        mock_save_data.assert_called_once()
        
        # 验证控制台输出
        captured = capfd.readouterr()
        assert "❌ 数据保存失败: 000001.SZ_STOCK_BASIC" in captured.out
    
    @patch('neo.data_processor.simple_data_processor.SimpleDataProcessor._save_data')
    def test_process_save_exception(self, mock_save_data, capfd):
        """测试数据保存时发生异常"""
        mock_save_data.side_effect = Exception("数据库连接失败")
        
        task_result = self.create_task_result()
        result = self.data_processor.process(task_result)
        
        assert result is False
        
        # 验证控制台输出
        captured = capfd.readouterr()
        assert "💥 处理异常: 000001.SZ_STOCK_BASIC - 数据库连接失败" in captured.out


class TestConsumerPerformance:
    """消费端性能测试"""
    
    def setup_method(self):
        """每个测试方法执行前的设置"""
        self.huey = get_huey()
        self.huey.immediate = True
    
    @patch('neo.data_processor.simple_data_processor.SimpleDataProcessor.process')
    def test_batch_task_processing(self, mock_process):
        """测试批量任务处理性能"""
        mock_process.return_value = True
        
        # 创建多个任务
        task_count = 10
        symbols = [f"00000{i}.SZ" for i in range(1, task_count + 1)]
        
        start_time = time.time()
        
        for symbol in symbols:
            task_data = {
                'config': {
                    'symbol': symbol,
                    'task_type': TaskType.STOCK_BASIC.value,
                    'priority': TaskPriority.MEDIUM.value,
                    'max_retries': 3
                },
                'success': True,
                'data': [{'ts_code': symbol, 'name': f'股票{symbol}'}],
                'error': None,
                'retry_count': 0
            }
            
            process_task_result(task_data)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # 验证所有任务都被处理
        assert mock_process.call_count == task_count
        
        # 验证处理时间合理（每个任务平均不超过 0.1 秒）
        assert processing_time < task_count * 0.1
        
        print(f"处理 {task_count} 个任务耗时: {processing_time:.3f} 秒")
    
    def test_consumer_memory_usage(self):
        """测试消费端内存使用情况"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        with patch('neo.data_processor.simple_data_processor.SimpleDataProcessor.process') as mock_process:
            mock_process.return_value = True
            
            # 处理大量任务
            for i in range(100):
                task_data = {
                    'config': {
                        'symbol': f"00000{i % 10}.SZ",
                        'task_type': TaskType.STOCK_BASIC.value,
                        'priority': TaskPriority.MEDIUM.value,
                        'max_retries': 3
                    },
                    'success': True,
                    'data': [{'ts_code': f"00000{i % 10}.SZ", 'name': f'股票{i}'}] * 10,  # 每个任务10行数据
                    'error': None,
                    'retry_count': 0
                }
                
                process_task_result(task_data)
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        print(f"初始内存: {initial_memory:.2f} MB")
        print(f"最终内存: {final_memory:.2f} MB")
        print(f"内存增长: {memory_increase:.2f} MB")
        
        # 验证内存增长在合理范围内（不超过 50MB）
        assert memory_increase < 50