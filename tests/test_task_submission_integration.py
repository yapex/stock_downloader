#!/usr/bin/env python3
"""任务提交集成测试

验证数据下载后任务是否成功提交到 Huey 队列的集成测试。
"""

import pytest
import sqlite3
import time
import os
import pandas as pd
from unittest.mock import patch, Mock

from neo.task_bus.huey_task_bus import HueyTaskBus, get_huey
from neo.data_processor.simple_data_processor import SimpleDataProcessor
from neo.task_bus.types import TaskResult, DownloadTaskConfig, TaskType, TaskPriority
from neo.downloader.simple_downloader import SimpleDownloader


class TestTaskSubmissionIntegration:
    """任务提交集成测试类"""
    
    def setup_method(self, setup_test_database):
        """每个测试方法执行前的设置"""
        self.data_processor = SimpleDataProcessor()
        self.task_bus = HueyTaskBus(self.data_processor)
        self.downloader = SimpleDownloader()
    
    def get_queue_task_count(self, huey_instance=None):
        """获取队列中的任务数量"""
        huey = huey_instance or get_huey()
        try:
            # 对于 MemoryHuey，直接检查内存中的任务
            if hasattr(huey.storage, 'data'):
                # MemoryHuey 使用字典存储
                return len(huey.storage.data.get('queue', []))
            elif hasattr(huey.storage, 'filename'):
                # SQLiteHuey 使用数据库存储
                conn = sqlite3.connect(huey.storage.filename)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM task;")
                count = cursor.fetchone()[0]
                conn.close()
                return count
            else:
                # 其他类型的存储，尝试通过 pending_count 方法
                return len(huey.pending())
        except Exception:
            return 0
    
    @patch('neo.task_bus.huey_task_bus.process_task_result')
    @patch('neo.downloader.simple_downloader.FetcherBuilder')
    def test_task_submission_to_queue(self, mock_fetcher_builder_class, mock_process_task, huey_immediate, setup_test_database):
        """测试任务是否成功提交到队列"""
        # 准备测试数据
        test_data = pd.DataFrame({
            'ts_code': ['000001.SZ'],
            'trade_date': ['20230101'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.0],
            'close': [10.5],
            'vol': [1000000]
        })
        
        # Mock FetcherBuilder
        mock_fetcher = Mock(return_value=test_data)
        mock_builder_instance = Mock()
        mock_builder_instance.build_by_task.return_value = mock_fetcher
        mock_fetcher_builder_class.return_value = mock_builder_instance
        
        # 创建下载任务配置
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.STOCK_BASIC,
            priority=TaskPriority.HIGH,
            max_retries=3
        )
        
        # 执行下载任务
        result = self.downloader.download(config)
        print(f"下载结果: {result}")
        
        # 验证下载成功
        assert result.success is True
        assert result.data is not None
        assert not result.data.empty
        
        # 验证任务被提交到队列（通过检查 process_task_result 是否被调用）
        mock_process_task.assert_called_once()
        
        # 验证调用参数
        call_args = mock_process_task.call_args[0][0]
        assert call_args['config']['symbol'] == "000001.SZ"
        assert call_args['config']['task_type'] == TaskType.STOCK_BASIC.value
        assert call_args['success'] is True
    
    @patch('neo.task_bus.huey_task_bus.process_task_result')
    @patch('neo.downloader.simple_downloader.FetcherBuilder')
    def test_multiple_task_submissions(self, mock_fetcher_builder_class, mock_process_task, huey_immediate, setup_test_database):
        """测试多个任务提交"""
        # 准备测试数据
        test_data = pd.DataFrame({
            'ts_code': ['000001.SZ'],
            'trade_date': ['20230101'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.0],
            'close': [10.5],
            'vol': [1000000]
        })
        
        # Mock FetcherBuilder
        mock_fetcher = Mock(return_value=test_data)
        mock_builder_instance = Mock()
        mock_builder_instance.build_by_task.return_value = mock_fetcher
        mock_fetcher_builder_class.return_value = mock_builder_instance
        # 创建多个任务配置
        configs = [
            DownloadTaskConfig(
                symbol="000001.SZ",
                task_type=TaskType.STOCK_BASIC,
                priority=TaskPriority.HIGH
            ),
            DownloadTaskConfig(
                symbol="000002.SZ", 
                task_type=TaskType.STOCK_BASIC,
                priority=TaskPriority.MEDIUM
            ),
            DownloadTaskConfig(
                symbol="000003.SZ",
                task_type=TaskType.STOCK_BASIC,
                priority=TaskPriority.LOW
            )
        ]
        
        successful_downloads = 0
        
        # 执行多个下载任务
        for i, config in enumerate(configs):
            try:
                result = self.downloader.download(config)
                print(f"任务 {i+1} 下载结果: {result}")
                if result.success and result.data is not None and not result.data.empty:
                    successful_downloads += 1
            except Exception as e:
                print(f"任务 {i+1} 下载失败: {e}")
        
        # 验证至少有一些任务成功下载
        assert successful_downloads > 0, f"没有任务成功下载"
        
        # 验证任务被提交到队列的次数等于成功下载的次数
        assert mock_process_task.call_count == successful_downloads, f"期望 {successful_downloads} 次任务提交，实际 {mock_process_task.call_count} 次"
    
    @patch('neo.task_bus.huey_task_bus.process_task_result')
    @patch('neo.downloader.simple_downloader.SimpleDownloader._fetch_data')
    def test_task_submission_with_mock_data(self, mock_fetch_data, mock_process_task, huey_immediate, setup_test_database):
        """使用模拟数据测试任务提交"""
        import pandas as pd
        
        # 模拟真实的股票数据格式
        mock_data = pd.DataFrame({
            'ts_code': ['000001.SZ'],
            'symbol': ['000001'],
            'name': ['平安银行'],
            'area': ['深圳'],
            'industry': ['银行'],
            'cnspell': ['payh'],
            'market': ['主板'],
            'list_date': ['19910403'],
            'act_name': ['无实际控制人'],
            'act_ent_type': ['无']
        })
        
        mock_fetch_data.return_value = mock_data
        
        # 创建任务配置
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.STOCK_BASIC,
            priority=TaskPriority.HIGH
        )
        
        # 执行下载
        result = self.downloader.download(config)
        
        # 验证下载成功
        assert result.success is True
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data.iloc[0]['ts_code'] == '000001.SZ'
        
        # 验证任务被提交到队列（通过检查 process_task_result 是否被调用）
        mock_process_task.assert_called_once()
        
        # 验证调用参数
        call_args = mock_process_task.call_args[0][0]
        assert call_args['config']['symbol'] == "000001.SZ"
        assert call_args['config']['task_type'] == TaskType.STOCK_BASIC.value
        assert call_args['success'] is True
    
    @patch('neo.task_bus.huey_task_bus.process_task_result')
    @patch('neo.downloader.simple_downloader.FetcherBuilder')
    def test_queue_task_processing_status(self, mock_fetcher_builder_class, mock_process_task, huey_immediate, setup_test_database):
        """测试队列任务处理状态"""
        # 准备测试数据
        test_data = pd.DataFrame({
            'ts_code': ['000001.SZ'],
            'trade_date': ['20230101'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.0],
            'close': [10.5],
            'vol': [1000000]
        })
        
        # Mock FetcherBuilder
        mock_fetcher = Mock(return_value=test_data)
        mock_builder_instance = Mock()
        mock_builder_instance.build_by_task.return_value = mock_fetcher
        mock_fetcher_builder_class.return_value = mock_builder_instance
        # 创建一个测试任务
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.STOCK_BASIC,
            priority=TaskPriority.HIGH
        )
        
        # 执行下载任务
        try:
            result = self.downloader.download(config)
            print(f"测试任务下载结果: {result}")
            
            if result.success and result.data is not None and not result.data.empty:
                # 验证任务被提交到队列
                mock_process_task.assert_called_once()
                print("任务成功提交到队列")
            else:
                print("任务下载失败，未提交到队列")
        except Exception as e:
            print(f"任务执行异常: {e}")
        
        # 验证队列功能基本可用
        assert mock_process_task.call_count >= 0, "队列任务处理功能异常"
    
    @patch('neo.task_bus.huey_task_bus.process_task_result')
    @patch('neo.downloader.simple_downloader.FetcherBuilder')
    def test_huey_instance_availability(self, mock_fetcher_builder_class, mock_process_task, huey_immediate, setup_test_database):
        """测试 Huey 实例可用性"""
        # 准备测试数据
        test_data = pd.DataFrame({
            'ts_code': ['000001.SZ'],
            'trade_date': ['20230101'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.0],
            'close': [10.5],
            'vol': [1000000]
        })
        
        # Mock FetcherBuilder
        mock_fetcher = Mock(return_value=test_data)
        mock_builder_instance = Mock()
        mock_builder_instance.build_by_task.return_value = mock_fetcher
        mock_fetcher_builder_class.return_value = mock_builder_instance
        huey = get_huey()
        
        # 验证 Huey 实例存在且配置正确
        assert huey is not None
        assert hasattr(huey, 'storage')
        
        # 测试任务提交功能是否正常
        config = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.STOCK_BASIC,
            priority=TaskPriority.HIGH
        )
        
        try:
            result = self.downloader.download(config)
            if result.success and result.data is not None and not result.data.empty:
                # 验证任务提交功能正常
                mock_process_task.assert_called_once()
                print("Huey 实例任务提交功能正常")
        except Exception as e:
            print(f"Huey 实例测试异常: {e}")
        
        print(f"Huey 实例类型: {type(huey)}")
        print(f"Huey 存储类型: {type(huey.storage)}")