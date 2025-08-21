import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from downloader.task.download_task import DownloadTask
from downloader.task.types import DownloadTaskConfig, TaskResult, TaskPriority
from downloader.producer.fetcher_builder import TaskType


class TestDownloadTaskIntegration:
    """测试DownloadTask与Huey的集成"""
    
    @pytest.fixture
    def mock_fetcher_builder(self):
        """Mock FetcherBuilder"""
        mock_builder = Mock()
        mock_fetcher = Mock()
        mock_builder.build.return_value = mock_fetcher
        return mock_builder, mock_fetcher
    
    @pytest.fixture
    def sample_config(self):
        """示例配置"""
        return DownloadTaskConfig(
            symbol='AAPL',
            task_type=TaskType.STOCK_DAILY,
            priority=TaskPriority.MEDIUM,
            max_retries=3
        )
    
    @pytest.fixture
    def sample_data(self):
        """示例数据"""
        return pd.DataFrame({
            'ts_code': ['AAPL'],
            'trade_date': ['20231201'],
            'close': [150.0]
        })
    
    def test_successful_task_sends_to_huey(self, mock_fetcher_builder, sample_config, sample_data):
        """测试成功任务发送到Huey队列"""
        mock_builder, mock_fetcher = mock_fetcher_builder
        mock_fetcher.fetch.return_value = sample_data
        
        with patch('downloader.task.download_task.FetcherBuilder', return_value=mock_builder), \
             patch('downloader.task.download_task.process_task_result') as mock_process_task:
            
            # 创建并执行任务
            task = DownloadTask()
            result = task.execute(sample_config)
            
            # 验证返回结果
            assert result.success is True
            assert result.config == sample_config
            assert isinstance(result.data, pd.DataFrame)
            
            # 验证Huey任务被调用
            mock_process_task.assert_called_once()
            
            # 验证传入的数据格式
            call_args = mock_process_task.call_args[0][0]
            assert isinstance(call_args, dict)
            assert call_args['success'] is True
            assert call_args['config']['symbol'] == 'AAPL'
            assert call_args['config']['task_type'] == 'STOCK_DAILY'  # 应该是字符串
            assert isinstance(call_args['data'], dict)  # DataFrame应该被序列化为字典
    
    def test_failed_task_sends_to_huey(self, mock_fetcher_builder, sample_config):
        """测试失败任务也发送到Huey队列"""
        mock_builder, mock_fetcher = mock_fetcher_builder
        mock_fetcher.fetch.side_effect = Exception("Network error")
        
        with patch('downloader.task.download_task.FetcherBuilder', return_value=mock_builder), \
             patch('downloader.task.download_task.process_task_result') as mock_process_task:
            
            # 创建并执行任务
            task = DownloadTask()
            result = task.execute(sample_config)
            
            # 验证返回结果
            assert result.success is False
            assert result.error is not None
            
            # 验证Huey任务被调用
            mock_process_task.assert_called_once()
            
            # 验证传入的数据格式
            call_args = mock_process_task.call_args[0][0]
            assert call_args['success'] is False
            assert call_args['error'] == 'Network error'
            assert call_args['data'] is None
    
    def test_empty_data_not_sent_to_huey(self, mock_fetcher_builder, sample_config):
        """测试空数据不发送到Huey队列"""
        mock_builder, mock_fetcher = mock_fetcher_builder
        empty_data = pd.DataFrame()  # 空DataFrame
        mock_fetcher.fetch.return_value = empty_data
        
        with patch('downloader.task.download_task.FetcherBuilder', return_value=mock_builder), \
             patch('downloader.task.download_task.process_task_result') as mock_process_task:
            
            # 创建并执行任务
            task = DownloadTask()
            result = task.execute(sample_config)
            
            # 验证返回结果
            assert result.success is True
            assert result.data.empty
            
            # 验证Huey任务没有被调用（因为数据为空）
            mock_process_task.assert_not_called()
    
    def test_task_result_serialization_format(self, mock_fetcher_builder, sample_config, sample_data):
        """测试TaskResult序列化格式"""
        mock_builder, mock_fetcher = mock_fetcher_builder
        mock_fetcher.fetch.return_value = sample_data
        
        with patch('downloader.task.download_task.FetcherBuilder', return_value=mock_builder), \
             patch('downloader.task.download_task.process_task_result') as mock_process_task:
            
            # 创建并执行任务
            task = DownloadTask()
            task.execute(sample_config)
            
            # 获取传入Huey的数据
            call_args = mock_process_task.call_args[0][0]
            
            # 验证数据结构
            expected_keys = {'config', 'success', 'data', 'error', 'retry_count'}
            assert set(call_args.keys()) == expected_keys
            
            # 验证config结构
            config_data = call_args['config']
            expected_config_keys = {'symbol', 'task_type', 'priority', 'max_retries'}
            assert set(config_data.keys()) == expected_config_keys
            
            # 验证TaskType被序列化为字符串
            assert isinstance(config_data['task_type'], str)
            assert config_data['task_type'] == 'STOCK_DAILY'
            
            # 验证DataFrame被序列化为字典
            assert isinstance(call_args['data'], dict)
            assert 'ts_code' in call_args['data']
            assert 'trade_date' in call_args['data']
            assert 'close' in call_args['data']
    
    def test_rate_limiting_still_works(self, mock_fetcher_builder, sample_config, sample_data):
        """测试速率限制仍然有效"""
        mock_builder, mock_fetcher = mock_fetcher_builder
        mock_fetcher.fetch.return_value = sample_data
        
        with patch('downloader.task.download_task.FetcherBuilder', return_value=mock_builder), \
             patch('downloader.task.download_task.process_task_result'), \
             patch('downloader.task.download_task.time.sleep') as mock_sleep:
            
            # 创建并执行任务
            task = DownloadTask()
            task.execute(sample_config)
            
            # 验证速率限制被应用（sleep被调用）
            mock_sleep.assert_called_once()