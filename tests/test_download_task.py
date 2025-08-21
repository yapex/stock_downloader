"""DownloadTask 单元测试"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from pyrate_limiter import Limiter

from downloader.task.download_task import (
    DownloadTask,
    DownloadTaskConfig,
    TaskResult,
    TaskPriority
)
from downloader.producer.fetcher_builder import TaskType


class TestDownloadTaskConfig:
    """测试 DownloadTaskConfig"""
    
    def test_normal_task_config(self):
        """测试普通任务配置"""
        config = DownloadTaskConfig(
            symbol="600519",
            task_type=TaskType.STOCK_DAILY,
            priority=TaskPriority.HIGH,
            max_retries=3
        )
        
        assert config.symbol == "600519"
        assert config.task_type == TaskType.STOCK_DAILY
        assert config.priority == TaskPriority.HIGH
        assert config.max_retries == 3
    
    def test_stock_basic_task_config(self):
        """测试 STOCK_BASIC 任务配置自动修正 symbol"""
        config = DownloadTaskConfig(
            symbol="600519",  # 应该被自动修正为空字符串
            task_type=TaskType.STOCK_BASIC
        )
        
        assert config.symbol == ""
        assert config.task_type == TaskType.STOCK_BASIC
    
    def test_default_values(self):
        """测试默认值"""
        config = DownloadTaskConfig(
            symbol="600519",
            task_type=TaskType.STOCK_DAILY
        )
        
        assert config.priority == TaskPriority.MEDIUM
        assert config.max_retries == 2


class TestTaskResult:
    """测试 TaskResult"""
    
    def test_successful_result(self):
        """测试成功结果"""
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        data = pd.DataFrame({"ts_code": ["600519.SH"], "close": [100.0]})
        
        result = TaskResult(
            config=config,
            success=True,
            data=data
        )
        
        assert result.success is True
        assert result.error is None
        assert result.retry_count == 0
        assert len(result.data) == 1
    
    def test_failed_result(self):
        """测试失败结果"""
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        error = Exception("Network error")
        
        result = TaskResult(
            config=config,
            success=False,
            error=error,
            retry_count=1
        )
        
        assert result.success is False
        assert result.data is None
        assert result.error == error
        assert result.retry_count == 1


class TestDownloadTask:
    """测试 DownloadTask"""
    
    @pytest.fixture
    def mock_rate_limiter(self):
        """模拟速率限制器"""
        limiter = Mock(spec=Limiter)
        limiter.try_acquire = Mock()
        return limiter
    
    @pytest.fixture
    def download_task(self, mock_rate_limiter):
        """创建 DownloadTask 实例"""
        return DownloadTask(rate_limiter=mock_rate_limiter)
    
    def test_init_with_custom_rate_limiter(self, mock_rate_limiter):
        """测试使用自定义速率限制器初始化"""
        task = DownloadTask(rate_limiter=mock_rate_limiter)
        assert task.rate_limiter == mock_rate_limiter
    
    def test_init_with_default_rate_limiter(self):
        """测试使用默认速率限制器初始化"""
        task = DownloadTask()
        assert task.rate_limiter is not None
        assert isinstance(task.rate_limiter, Limiter)
    
    @patch('downloader.task.download_task.process_fetched_data')
    def test_execute_successful_normal_task(self, mock_process_data, download_task, mock_rate_limiter):
        """测试成功执行普通任务"""
        # 准备测试数据
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        mock_data = pd.DataFrame({"ts_code": ["600519.SH"], "close": [100.0]})
        
        # 模拟 fetcher_builder
        mock_fetcher = Mock(return_value=mock_data)
        download_task.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher)
        
        # 执行任务
        result = download_task.execute(config)
        
        # 验证结果
        assert result.success is True
        assert result.error is None
        assert result.data.equals(mock_data)
        
        # 验证调用
        mock_rate_limiter.try_acquire.assert_called_once_with(TaskType.STOCK_DAILY.name, 1)
        download_task.fetcher_builder.build_by_task.assert_called_once_with(TaskType.STOCK_DAILY, "600519")
        mock_fetcher.assert_called_once()
        mock_process_data.assert_called_once_with("600519", TaskType.STOCK_DAILY.name, mock_data.to_dict())
    
    @patch('downloader.task.download_task.process_fetched_data')
    def test_execute_successful_stock_basic_task(self, mock_process_data, download_task, mock_rate_limiter):
        """测试成功执行 STOCK_BASIC 任务"""
        # 准备测试数据
        config = DownloadTaskConfig("", TaskType.STOCK_BASIC)
        mock_data = pd.DataFrame({"ts_code": ["600519.SH", "000001.SZ"], "name": ["贵州茅台", "平安银行"]})
        
        # 模拟 fetcher_builder
        mock_fetcher = Mock(return_value=mock_data)
        download_task.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher)
        
        # 执行任务
        result = download_task.execute(config)
        
        # 验证结果
        assert result.success is True
        assert result.error is None
        assert result.data.equals(mock_data)
        
        # 验证调用 - STOCK_BASIC 不传入 symbol 参数
        download_task.fetcher_builder.build_by_task.assert_called_once_with(TaskType.STOCK_BASIC)
        mock_process_data.assert_called_once_with("", TaskType.STOCK_BASIC.name, mock_data.to_dict())
    
    def test_execute_failed_task(self, download_task, mock_rate_limiter):
        """测试任务执行失败"""
        # 准备测试数据
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        error = Exception("Network error")
        
        # 模拟 fetcher_builder 抛出异常
        mock_fetcher = Mock(side_effect=error)
        download_task.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher)
        
        # 执行任务
        result = download_task.execute(config)
        
        # 验证结果
        assert result.success is False
        assert result.error == error
        assert result.data is None
        
        # 验证速率限制仍然被调用
        mock_rate_limiter.try_acquire.assert_called_once_with(TaskType.STOCK_DAILY.name, 1)
    
    @patch('downloader.task.download_task.process_fetched_data')
    def test_execute_empty_dataframe(self, mock_process_data, download_task, mock_rate_limiter):
        """测试处理空 DataFrame"""
        # 准备测试数据
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        empty_data = pd.DataFrame()
        
        # 模拟 fetcher_builder
        mock_fetcher = Mock(return_value=empty_data)
        download_task.fetcher_builder.build_by_task = Mock(return_value=mock_fetcher)
        
        # 执行任务
        result = download_task.execute(config)
        
        # 验证结果
        assert result.success is True
        assert result.error is None
        assert result.data.empty
        
        # 验证空数据不会被处理
        mock_process_data.assert_not_called()
    
    def test_rate_limiting_called(self, download_task, mock_rate_limiter):
        """测试速率限制被正确调用"""
        config = DownloadTaskConfig("600519", TaskType.STOCK_DAILY)
        
        # 模拟 fetcher_builder 抛出异常以快速结束测试
        download_task.fetcher_builder.build_by_task = Mock(side_effect=Exception("Test"))
        
        # 执行任务
        download_task.execute(config)
        
        # 验证速率限制被调用
        mock_rate_limiter.try_acquire.assert_called_once_with(TaskType.STOCK_DAILY.name, 1)