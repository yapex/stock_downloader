#!/usr/bin/env python3
"""任务提交集成测试

测试SimpleDownloader将TaskResult提交到TaskBus的完整流程。"""

import pytest
import pandas as pd
from unittest.mock import Mock

from neo.downloader.simple_downloader import SimpleDownloader
from neo.task_bus.types import TaskType, DownloadTaskConfig
from tests.mocks.mock_task_bus import MockTaskBus


@pytest.fixture
def mock_task_bus():
    """创建Mock TaskBus用于测试"""
    return MockTaskBus()


class TestTaskSubmissionIntegration:
    """任务提交集成测试类"""

    def test_task_submission_to_task_bus(self, mock_task_bus):
        """测试任务成功提交到TaskBus"""
        # 准备测试数据
        test_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20230101"],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "vol": [1000000],
            }
        )

        # 创建SimpleDownloader实例，注入MockTaskBus
        downloader = SimpleDownloader(task_bus=mock_task_bus)

        # Mock FetcherBuilder的build_by_task方法
        mock_fetcher = Mock(return_value=test_data)
        downloader.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher)

        # 获取初始提交任务数量
        initial_count = mock_task_bus.get_submitted_count()

        # 创建下载任务配置
        config = DownloadTaskConfig(symbol="000001.SZ", task_type=TaskType.stock_basic)

        # 执行下载
        result = downloader.download(config)

        # 验证下载成功
        assert result.success is True
        assert result.data is not None
        assert len(result.data) == 1

        # 验证任务已提交到TaskBus
        final_count = mock_task_bus.get_submitted_count()
        assert final_count == initial_count + 1

        # 验证提交的任务内容
        submitted_tasks = mock_task_bus.get_submitted_tasks()
        assert len(submitted_tasks) == 1

        submitted_task = submitted_tasks[0]
        assert submitted_task.config.symbol == "000001.SZ"
        assert submitted_task.config.task_type == TaskType.stock_basic
        assert submitted_task.success is True
        assert submitted_task.data is not None
        assert len(submitted_task.data) == 1

    def test_multiple_task_submissions(self, mock_task_bus):
        """测试多个任务提交到TaskBus"""
        # 准备测试数据
        test_data_1 = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20230101"],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "vol": [1000000],
            }
        )

        test_data_2 = pd.DataFrame(
            {
                "ts_code": ["000002.SZ"],
                "trade_date": ["20230101"],
                "open": [20.0],
                "high": [21.0],
                "low": [19.0],
                "close": [20.5],
                "vol": [2000000],
            }
        )

        # 创建SimpleDownloader实例，注入MockTaskBus
        downloader = SimpleDownloader(task_bus=mock_task_bus)

        # Mock FetcherBuilder的build_by_task方法
        mock_fetcher_1 = Mock(return_value=test_data_1)
        mock_fetcher_2 = Mock(return_value=test_data_2)

        # 获取初始提交任务数量
        initial_count = mock_task_bus.get_submitted_count()

        # 创建第一个下载任务配置
        config_1 = DownloadTaskConfig(
            symbol="000001.SZ", task_type=TaskType.stock_basic
        )

        # Mock第一次调用
        downloader.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher_1)
        result_1 = downloader.download(config_1)

        # 创建第二个下载任务配置
        config_2 = DownloadTaskConfig(
            symbol="000002.SZ", task_type=TaskType.stock_basic
        )

        # Mock第二次调用
        downloader.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher_2)
        result_2 = downloader.download(config_2)

        # 验证两次下载都成功
        assert result_1.success is True
        assert result_1.data is not None
        assert len(result_1.data) == 1

        assert result_2.success is True
        assert result_2.data is not None
        assert len(result_2.data) == 1

        # 验证两个任务都已提交到TaskBus
        final_count = mock_task_bus.get_submitted_count()
        assert final_count == initial_count + 2

        # 验证提交的任务内容
        submitted_tasks = mock_task_bus.get_submitted_tasks()
        assert len(submitted_tasks) == 2

        # 验证第一个任务
        task_1 = submitted_tasks[0]
        assert task_1.config.symbol == "000001.SZ"
        assert task_1.config.task_type == TaskType.stock_basic
        assert task_1.success is True

        # 验证第二个任务
        task_2 = submitted_tasks[1]
        assert task_2.config.symbol == "000002.SZ"
        assert task_2.config.task_type == TaskType.stock_basic
        assert task_2.success is True

    def test_failed_download_no_task_submission(self, mock_task_bus):
        """测试下载失败时不应提交任务到TaskBus"""
        # 创建SimpleDownloader实例，注入MockTaskBus
        downloader = SimpleDownloader(task_bus=mock_task_bus)

        # Mock FetcherBuilder的build_by_task方法抛出异常
        downloader.fetcher_builder.build_by_task = Mock(
            side_effect=Exception("下载失败")
        )

        # 获取初始提交任务数量
        initial_count = mock_task_bus.get_submitted_count()

        # 创建下载任务配置
        config = DownloadTaskConfig(symbol="INVALID.SZ", task_type=TaskType.stock_basic)

        # 执行下载任务（应该失败）
        result = downloader.download(config)

        # 验证下载失败
        assert result.success is False

        # 验证没有新任务被提交到TaskBus
        final_count = mock_task_bus.get_submitted_count()
        assert final_count == initial_count

        # 验证提交的任务列表没有变化
        submitted_tasks = mock_task_bus.get_submitted_tasks()
        assert len(submitted_tasks) == initial_count
