"""Huey 任务测试

测试带 @huey_task 装饰器的下载任务函数和Huey集成功能。
"""

from unittest.mock import Mock, patch
import pandas as pd
import pytest

from neo.app import container
from neo.configs.huey_config import huey_fast, huey_slow
from neo.tasks.huey_tasks import download_task, process_data_task
from neo.task_bus.types import TaskType


class TestDownloadTask:
    """测试 download_task 函数"""

    @patch("neo.tasks.huey_tasks.process_data_task")
    def test_download_task_success_and_chains(self, mock_process_data_task):
        """测试下载任务成功，并正确调用后续处理任务"""
        mock_downloader = Mock()
        mock_downloader.download.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})

        with container.downloader.override(mock_downloader):
            # 直接调用任务的函数体进行测试
            download_task.func(TaskType.stock_basic, "000001.SZ")

        # 验证下载器被调用
        mock_downloader.download.assert_called_once_with(
            TaskType.stock_basic, "000001.SZ"
        )

        # 验证后续任务被调用
        mock_process_data_task.assert_called_once()

    @patch("neo.tasks.huey_tasks.process_data_task")
    def test_download_task_empty_data_does_not_chain(self, mock_process_data_task):
        """测试下载任务返回空数据时，不调用后续任务"""
        mock_downloader = Mock()
        mock_downloader.download.return_value = pd.DataFrame()  # Empty dataframe

        with container.downloader.override(mock_downloader):
            # 直接调用任务的函数体进行测试
            download_task.func(TaskType.stock_basic, "000001.SZ")

        # 验证下载器被调用
        mock_downloader.download.assert_called_once_with(
            TaskType.stock_basic, "000001.SZ"
        )

        # 验证后续任务未被调用
        mock_process_data_task.assert_not_called()


class TestProcessDataTask:
    """测试 process_data_task 函数"""

    @patch('neo.app.container.data_processor')
    def test_process_data_task_with_data(self, mock_data_processor_factory):
        """测试当有数据时，处理任务能正确调用下游"""
        mock_processor = Mock()
        mock_processor.process.return_value = True
        mock_data_processor_factory.return_value = mock_processor
        
        data = [{'ts_code': '000001.SZ'}]
        
        # 直接调用任务的函数体进行测试
        result = process_data_task.func(TaskType.stock_basic, "000001.SZ", data)

        mock_processor.process.assert_called_once()
        assert result is True

    @patch('neo.app.container.data_processor')
    def test_process_data_task_with_no_data(self, mock_data_processor_factory):
        """测试当数据为空时，处理任务不调用下游"""
        mock_processor = Mock()
        mock_data_processor_factory.return_value = mock_processor
        
        # Test with empty list
        process_data_task.func(TaskType.stock_basic, "000001.SZ", [])
        mock_processor.process.assert_not_called()

        # Test with None
        process_data_task.func(TaskType.stock_basic, "000001.SZ", None)
        mock_processor.process.assert_not_called()


class TestHueyIntegration:
    """Huey 集成测试类"""

    def test_huey_configuration(self):
        """测试 Huey 双队列配置是否正确"""
        assert huey_fast.name == "fast_queue"
        assert not huey_fast.immediate
        assert not huey_fast.utc

        assert huey_slow.name == "slow_queue"
        assert not huey_slow.immediate
        assert not huey_slow.utc
