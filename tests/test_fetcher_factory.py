"""测试 TushareFetcherFactory 工厂类"""

import os
import pytest
from unittest.mock import patch, MagicMock
from downloader.fetcher_factory import (
    TushareFetcherFactory,
    TushareFetcherTestHelper,
    get_singleton,
    get_fetcher,
    _reset_singleton,
    _get_instance_info,
    create_singleton_factory,
    create_thread_local_factory
)
from downloader.fetcher import TushareFetcher
from downloader.config_impl import create_config_manager
from tests.test_implementations import MockFetcher, MockStrategy, SingletonMockStrategy


class TestTushareFetcherFactory:
    """测试 TushareFetcherFactory 类"""
    
    def setup_method(self):
        """每个测试前重置状态"""
        TushareFetcherTestHelper.reset_singleton()
        TushareFetcherTestHelper.reset_thread_local()
    
    def teardown_method(self):
        """每个测试方法后重置单例"""
        TushareFetcherTestHelper.reset_singleton()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    def test_factory_with_custom_strategy(self):
        """测试工厂使用自定义策略"""
        test_fetcher = MockFetcher()
        strategy = MockStrategy(test_fetcher)
        
        # 创建配置对象
        config = create_config_manager("config.yaml")
        
        factory = TushareFetcherFactory(config, strategy)
        result = factory.get_instance()
        
        assert result == test_fetcher
        assert strategy.call_count == 1
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    def test_factory_calls_strategy_each_time(self):
        """测试工厂每次都调用策略函数"""
        test_fetcher1 = MockFetcher()
        test_fetcher2 = MockFetcher()
        
        call_count = 0
        def multi_strategy(token: str):
            nonlocal call_count
            call_count += 1
            return test_fetcher1 if call_count == 1 else test_fetcher2
        
        # 创建配置对象
        config = create_config_manager("config.yaml")
        
        factory = TushareFetcherFactory(config, multi_strategy)
        result1 = factory.get_instance()
        result2 = factory.get_instance()
        
        assert result1 == test_fetcher1
        assert result2 == test_fetcher2
        assert call_count == 2
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('downloader.fetcher_factory.TushareFetcher')
    def test_create_singleton_factory(self, mock_fetcher_class):
        """测试创建单例工厂"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        # 创建配置对象
        config = create_config_manager("config.yaml")
        
        factory = create_singleton_factory(config)
        result1 = factory.get_instance()
        result2 = factory.get_instance()
        
        # 应该返回同一个实例（单例行为）
        assert result1 == mock_instance
        assert result2 == mock_instance
        assert result1 is result2
        # 构造函数只应该被调用一次
        mock_fetcher_class.assert_called_once_with('test_token')
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('downloader.fetcher_factory.TushareFetcher')
    def test_create_thread_local_factory(self, mock_fetcher_class):
        """测试创建线程本地工厂"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        # 创建配置对象
        config = create_config_manager("config.yaml")
        
        factory = create_thread_local_factory(config)
        result1 = factory.get_instance()
        result2 = factory.get_instance()
        
        # 在同一线程内应该返回同一个实例
        assert result1 == mock_instance
        assert result2 == mock_instance
        assert result1 is result2
        # 构造函数只应该被调用一次
        mock_fetcher_class.assert_called_once_with('test_token')
class TestModuleFunctions:
    """测试模块级函数"""
    
    def setup_method(self):
        """每个测试前重置状态"""
        TushareFetcherTestHelper.reset_singleton()
        TushareFetcherTestHelper.reset_thread_local()
    
    def teardown_method(self):
        """每个测试方法后重置单例"""
        TushareFetcherTestHelper.reset_singleton()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('downloader.fetcher_factory.TushareFetcher')
    def test_get_singleton(self, mock_fetcher_class):
        """测试 get_singleton 函数"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        result = get_singleton()
        assert result == mock_instance
        mock_fetcher_class.assert_called_once_with('test_token')
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('downloader.fetcher_factory.TushareFetcher')
    def test_get_fetcher_singleton_mode(self, mock_fetcher_class):
        """测试 get_fetcher 函数（单例模式）"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        result = get_fetcher(use_singleton=True)
        assert result == mock_instance
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('downloader.fetcher_factory.TushareFetcher')
    def test_get_fetcher_non_singleton_mode_deprecated(self, mock_fetcher_class):
        """测试 get_fetcher 函数（非单例模式已废弃）"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        with patch('downloader.fetcher_factory.logger') as mock_logger:
            result = get_fetcher(use_singleton=False)
            assert result == mock_instance
            mock_logger.warning.assert_called_once_with("非单例模式已废弃，将使用单例模式")
    
    def test_reset_singleton(self):
        """测试 _reset_singleton 函数"""
        with patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'}):
            with patch('src.downloader.fetcher_factory.TushareFetcher'):
                # 创建实例
                get_singleton()
                assert TushareFetcherTestHelper.is_singleton_initialized()
                
                # 重置
                _reset_singleton()
                assert not TushareFetcherTestHelper.is_singleton_initialized()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('downloader.fetcher_factory.TushareFetcher')
    def test_get_instance_info(self, mock_fetcher_class):
        """测试 _get_instance_info 函数"""
        # 未初始化时
        info = _get_instance_info()
        assert info['singleton_instance_exists'] is False
        assert info['singleton_instance_id'] is None
        assert info['thread_local_instance_exists'] is False
        assert info['thread_local_instance_id'] is None
        
        # 创建实例后
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        get_singleton()
        info = _get_instance_info()
        assert info['singleton_instance_exists'] is True
        assert info['singleton_instance_id'] == id(mock_instance)
        assert info['thread_local_instance_exists'] is False
        assert info['thread_local_instance_id'] is None


class TestThreadSafety:
    """测试线程安全性"""
    
    def setup_method(self):
        """每个测试方法前重置单例"""
        TushareFetcherTestHelper.reset_singleton()
    
    def teardown_method(self):
        """每个测试方法后重置单例"""
        TushareFetcherTestHelper.reset_singleton()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('downloader.fetcher_factory.TushareFetcher')
    def test_concurrent_access(self, mock_fetcher_class):
        """测试并发访问时的线程安全性"""
        import threading
        import time
        
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        results = []
        
        def get_instance_worker():
            # 添加小延迟模拟并发竞争
            time.sleep(0.01)
            instance = get_singleton()
            results.append(instance)
        
        # 创建多个线程同时获取实例
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=get_instance_worker)
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证所有线程获取的是同一个实例
        assert len(results) == 5
        for result in results:
            assert result is mock_instance
        
        # 验证构造函数只被调用一次
        mock_fetcher_class.assert_called_once()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('downloader.fetcher_factory.TushareFetcher')
    def test_concurrent_access_with_lock_contention(self, mock_fetcher_class):
        """测试锁竞争情况下的线程安全性"""
        import threading
        import time
        
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        # 模拟慢速初始化来增加锁竞争
        def slow_init(*args, **kwargs):
            time.sleep(0.1)  # 模拟慢速初始化
            return mock_instance
        
        mock_fetcher_class.side_effect = slow_init
        
        results = []
        
        def get_instance_worker():
            instance = get_singleton()
            results.append(instance)
        
        # 创建多个线程同时访问
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=get_instance_worker)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证所有线程都获得了同一个实例
        assert len(results) == 3
        for result in results:
            assert result is mock_instance
        
        # 验证构造函数只被调用一次
        mock_fetcher_class.assert_called_once()