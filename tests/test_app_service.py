"""AppService 测试

测试 AppService 类的功能。
"""

from unittest.mock import Mock, patch

import pytest

from neo.helpers.app_service import AppService, DataProcessorRunner
from neo.task_bus.types import DownloadTaskConfig, TaskType


class TestAppService:
    """AppService 类测试"""

    def test_run_data_processor(self):
        """测试 run_data_processor 方法"""
        app_service = AppService()
        with patch.object(DataProcessorRunner, 'run_data_processor') as mock_run:
            app_service.run_data_processor()
            mock_run.assert_called_once()

    def test_run_downloader_dry_run(self):
        """测试试运行模式"""
        app_service = AppService()
        tasks = [DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001")]
        with patch.object(app_service, '_print_dry_run_info') as mock_print:
            app_service.run_downloader(tasks, dry_run=True)
            mock_print.assert_called_once_with(tasks)

    @patch('neo.tasks.huey_tasks.process_data_task')
    @patch('neo.tasks.huey_tasks.download_task')
    @patch('neo.configs.huey_config.huey')
    def test_execute_download_task_with_submission_success(self, mock_huey, mock_download_task, mock_process_task):
        """测试成功提交下载任务"""
        app_service = AppService()
        
        # 模拟 download_task.s() 返回的对象
        mock_download_signature = Mock()
        mock_download_task.s.return_value = mock_download_signature
        
        # 模拟 .then() 返回的 pipeline
        mock_pipeline = Mock()
        mock_download_signature.then.return_value = mock_pipeline
        
        # 模拟 huey.enqueue() 返回的结果
        mock_result = Mock()
        mock_huey.enqueue.return_value = mock_result
        
        task = DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001")
        
        result = app_service._execute_download_task_with_submission(task)
        
        assert result == mock_result
        mock_download_task.s.assert_called_once_with(TaskType.stock_basic, "000001")
        mock_download_signature.then.assert_called_once_with(mock_process_task)
        mock_huey.enqueue.assert_called_once_with(mock_pipeline)

    @patch('neo.tasks.huey_tasks.download_task')
    def test_execute_download_task_with_submission_failure(self, mock_download_task):
        """测试提交下载任务失败"""
        app_service = AppService()
        mock_download_task.side_effect = Exception("Task submission failed")
        task = DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001")
        
        result = app_service._execute_download_task_with_submission(task)
        
        assert result is None

    def test_run_downloader_synchronous(self):
        """测试同步下载器逻辑"""
        app_service = AppService()
        tasks = [
            DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001"),
            DownloadTaskConfig(task_type=TaskType.stock_daily, symbol="000002"),
        ]

        # 模拟 _execute_download_task_with_submission 的返回值
        mock_result = Mock()

        with patch.object(app_service, '_execute_download_task_with_submission', return_value=mock_result) as mock_execute:
            result = app_service.run_downloader(tasks, dry_run=False)

            # 验证任务被提交
            assert mock_execute.call_count == 2

            # 验证返回了任务结果列表
            assert result == [mock_result, mock_result]


class TestDataProcessorRunner:
    """测试 DataProcessorRunner 类"""
    
    def test_run_data_processor(self):
        """测试运行数据处理器"""
        with patch('huey.consumer.Consumer') as mock_consumer_class, \
             patch('neo.helpers.app_service.get_config') as mock_get_config:
            
            # 模拟配置
            mock_config = Mock()
            mock_config.huey.max_workers = 4
            mock_get_config.return_value = mock_config
            
            # 模拟 Consumer 实例
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            
            # 模拟 KeyboardInterrupt 来结束运行
            mock_consumer.run.side_effect = KeyboardInterrupt()
            
            DataProcessorRunner.run_data_processor()
            
            # 验证 Consumer 被正确创建和调用
            mock_consumer_class.assert_called_once()
            mock_consumer.run.assert_called_once()