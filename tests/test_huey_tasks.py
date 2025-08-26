"""Huey 任务测试

测试带 @huey_task 装饰器的下载任务函数和Huey集成功能。
"""

from unittest.mock import Mock, patch
import pandas as pd
import pytest
from huey import MemoryHuey

# 在导入任何 neo 模块之前先 patch huey_config
pytestmark = pytest.mark.usefixtures("mock_huey_config")

from neo.task_bus.types import TaskType


class TestDownloadTask:
    """测试 download_task 函数"""

    @patch("neo.tasks.huey_tasks.process_data_task")
    def test_download_task_success_and_chains(self, mock_process_data_task):
        """测试下载任务成功，并正确调用后续处理任务"""
        from neo.app import container
        from neo.tasks.huey_tasks import download_task
        
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
        from neo.app import container
        from neo.tasks.huey_tasks import download_task
        
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
        from neo.tasks.huey_tasks import process_data_task
        
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
        from neo.tasks.huey_tasks import process_data_task
        
        mock_processor = Mock()
        mock_data_processor_factory.return_value = mock_processor
        
        # Test with empty list
        process_data_task.func(TaskType.stock_basic, "000001.SZ", [])
        mock_processor.process.assert_not_called()

        # Test with None
        process_data_task.func(TaskType.stock_basic, "000001.SZ", None)
        mock_processor.process.assert_not_called()


class TestHueyIntegration:
    """测试 Huey 集成功能"""

    def test_huey_fast_queue_configured(self):
        """测试快速队列配置正确"""
        from neo.configs.huey_config import huey_fast
        
        # 验证队列名称（现在是内存模式）
        assert huey_fast.name == 'test_fast'
        
        # 验证队列可以正常工作
        @huey_fast.task()
        def test_task():
            return "fast_result"
        
        # 执行任务（immediate=True 会立即执行）
        result = test_task()
        assert result() == "fast_result"

    def test_huey_slow_queue_configured(self):
        """测试慢速队列配置正确"""
        from neo.configs.huey_config import huey_slow
        
        # 验证队列名称（现在是内存模式）
        assert huey_slow.name == 'test_slow'
        
        # 验证队列可以正常工作
        @huey_slow.task()
        def test_task():
            return "slow_result"
        
        # 执行任务（immediate=True 会立即执行）
        result = test_task()
        assert result() == "slow_result"


class TestBuildAndEnqueueTask:
    """测试 build_and_enqueue_downloads_task 任务"""

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.database.parquet_operator.ParquetDBOperator")
    @patch("neo.app.container.group_handler")
    @patch("neo.app.container.db_operator") # 我们将模拟旧的DBOperator
    @patch("neo.app.container.config")
    def test_build_and_enqueue_logic(self, mock_config, mock_db_operator, mock_group_handler, mock_parquet_operator, mock_download_task):
        """测试构建和派发任务的核心逻辑"""
        from neo.tasks.huey_tasks import build_and_enqueue_downloads_task
        from neo.utils.date_utils import get_next_day_str

        # 1. 配置 Mocks
        # a. GroupHandler 返回两个任务
        mock_group_handler.return_value.get_members.return_value = [
            ("stock_daily", "000001.SZ"), # 已有数据
            ("stock_daily", "000002.SZ"), # 新股票
        ]

        # b. DBOperator 返回一个股票的最新日期
        mock_db_operator.return_value.get_max_date.return_value = {
            "000001.SZ": "20240110"
        }

        # c. ParquetDBOperator 返回最新日期
        mock_parquet_operator.return_value.get_max_date.return_value = {
            "000001.SZ": "20240110"
        }

        # d. Config 返回默认起始日期和存储路径
        mock_config.return_value.downloader.default_start_date = "19900101"
        mock_config.return_value.storage.parquet_base_path = "/tmp/test_parquet"

        # 2. 执行任务
        build_and_enqueue_downloads_task.func("all_stocks")

        # 3. 验证
        # a. 验证 GroupHandler 被正确调用
        mock_group_handler.return_value.get_members.assert_called_once_with("all_stocks")

        # b. 验证 ParquetDBOperator 被正确调用
        mock_parquet_operator.assert_called_once_with("/tmp/test_parquet")
        mock_parquet_operator.return_value.get_max_date.assert_called_once_with(
            "stock_daily", ["000001.SZ", "000002.SZ"]
        )

        # c. 验证 download_task 的派发
        assert mock_download_task.call.call_count == 2
        calls = mock_download_task.call.call_args_list

        # 验证第一个任务（已有数据）
        call_1_args = calls[0].kwargs
        assert call_1_args["task_type"] == "stock_daily"
        assert call_1_args["symbol"] == "000001.SZ"
        assert call_1_args["start_date"] == get_next_day_str("20240110") # 20240111

        # 验证第二个任务（新股票）
        call_2_args = calls[1].kwargs
        assert call_2_args["task_type"] == "stock_daily"
        assert call_2_args["symbol"] == "000002.SZ"
        assert call_2_args["start_date"] == "19900101"
