"""测试 TushareDownloader 任务执行器

验证 TushareDownloader 作为任务执行器的功能。
"""

import pytest
from unittest.mock import patch, Mock
import pandas as pd

from downloader.producer.tushare_downloader import TushareDownloader
from downloader.producer.fetcher_builder import TaskType
from downloader.task.download_task import DownloadTaskConfig, TaskResult
from downloader.task.types import TaskPriority
from downloader.producer.huey_tasks import process_fetched_data

# 测试数据
SUCCESS_SYM = "000001.SZ"
FAIL_SYM = "666666.SH"
EMPTY_SYM = "000002.SZ"
SUCCESS_DATA = pd.DataFrame({"ts_code": [SUCCESS_SYM], "close": [10.0]})
EMPTY_DATA = pd.DataFrame()


class TestTushareDownloader:
    """测试 TushareDownloader 任务执行器"""

    @pytest.fixture
    def downloader(self, huey_immediate):
        """创建任务执行器实例"""
        downloader = TushareDownloader()
        
        # Mock FetcherBuilder 的 build_by_task 方法
        with patch.object(
            downloader.download_task.fetcher_builder, "build_by_task"
        ) as mock_build:
            downloader._mock_build = mock_build
            yield downloader

    def test_successful_execution_with_huey_task(self, downloader, huey_immediate):
        """测试成功执行任务并触发 Huey 任务"""
        # 设置 mock 返回成功数据
        mock_fetcher = Mock(return_value=SUCCESS_DATA)
        downloader._mock_build.return_value = mock_fetcher

        # 创建任务配置
        config = DownloadTaskConfig(
            symbol=SUCCESS_SYM,
            task_type=TaskType.STOCK_DAILY,
            priority=TaskPriority.MEDIUM,
            max_retries=3
        )

        # 执行任务
        with patch('downloader.producer.tushare_downloader.process_fetched_data') as mock_huey:
            result = downloader.execute(config)

        # 验证结果
        assert result.success is True
        assert result.data is not None
        assert not result.data.empty
        assert result.error is None

        # 验证 Huey 任务被触发
        mock_huey.assert_called_once_with(
            SUCCESS_SYM, TaskType.STOCK_DAILY.name, SUCCESS_DATA.to_dict()
        )

    def test_successful_execution_with_empty_data(self, downloader, huey_immediate):
        """测试成功执行但数据为空的情况"""
        # 设置 mock 返回空数据
        mock_fetcher = Mock(return_value=EMPTY_DATA)
        downloader._mock_build.return_value = mock_fetcher

        # 创建任务配置
        config = DownloadTaskConfig(
            symbol=EMPTY_SYM,
            task_type=TaskType.STOCK_DAILY,
            priority=TaskPriority.MEDIUM,
            max_retries=3
        )

        # 执行任务
        with patch('downloader.producer.tushare_downloader.process_fetched_data') as mock_huey:
            result = downloader.execute(config)

        # 验证结果
        assert result.success is True
        assert result.data is not None
        assert result.data.empty
        assert result.error is None

        # 验证 Huey 任务未被触发（因为数据为空）
        mock_huey.assert_not_called()

    def test_failed_execution(self, downloader, huey_immediate):
        """测试执行失败的情况"""
        # 设置 mock 抛出异常
        mock_fetcher = Mock(side_effect=Exception("网络错误"))
        downloader._mock_build.return_value = mock_fetcher

        # 创建任务配置
        config = DownloadTaskConfig(
            symbol=FAIL_SYM,
            task_type=TaskType.STOCK_DAILY,
            priority=TaskPriority.MEDIUM,
            max_retries=3
        )

        # 执行任务
        with patch('downloader.producer.tushare_downloader.process_fetched_data') as mock_huey:
            result = downloader.execute(config)

        # 验证结果
        assert result.success is False
        assert result.data is None
        assert result.error is not None
        assert "网络错误" in str(result.error)

        # 验证 Huey 任务未被触发
        mock_huey.assert_not_called()

    def test_huey_task_failure_does_not_affect_main_task(self, downloader, huey_immediate):
        """测试 Huey 任务失败不影响主任务成功状态"""
        # 设置 mock 返回成功数据
        mock_fetcher = Mock(return_value=SUCCESS_DATA)
        downloader._mock_build.return_value = mock_fetcher

        # 创建任务配置
        config = DownloadTaskConfig(
            symbol=SUCCESS_SYM,
            task_type=TaskType.STOCK_DAILY,
            priority=TaskPriority.MEDIUM,
            max_retries=3
        )

        # 执行任务，模拟 Huey 任务失败
        with patch('downloader.producer.tushare_downloader.process_fetched_data') as mock_huey:
            mock_huey.side_effect = Exception("Huey 任务失败")
            result = downloader.execute(config)

        # 验证主任务仍然成功
        assert result.success is True
        assert result.data is not None
        assert not result.data.empty
        assert result.error is None

        # 验证 Huey 任务被调用了
        mock_huey.assert_called_once()
