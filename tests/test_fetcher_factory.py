"""测试 TushareFetcherFactory 工厂类"""

import os
import pytest
from unittest.mock import patch, MagicMock
from src.downloader.fetcher_factory import (
    TushareFetcherFactory,
    get_singleton,
    get_fetcher,
    _reset_singleton,
    _get_instance_info
)
from src.downloader.fetcher import TushareFetcher


class TestTushareFetcherFactory:
    """测试 TushareFetcherFactory 类"""
    
    def setup_method(self):
        """每个测试方法前重置单例"""
        TushareFetcherFactory.reset()
    
    def teardown_method(self):
        """每个测试方法后重置单例"""
        TushareFetcherFactory.reset()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token_123'})
    @patch('src.downloader.fetcher_factory.TushareFetcher')
    def test_get_instance_success(self, mock_fetcher_class):
        """测试成功获取单例实例"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        # 第一次调用
        result1 = TushareFetcherFactory.get_instance()
        assert result1 == mock_instance
        mock_fetcher_class.assert_called_once_with('test_token_123')
        
        # 第二次调用应该返回同一个实例
        result2 = TushareFetcherFactory.get_instance()
        assert result2 == mock_instance
        assert result1 is result2
        # 构造函数只应该被调用一次
        mock_fetcher_class.assert_called_once()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher_factory.TushareFetcher')
    def test_get_instance_already_exists(self, mock_fetcher_class):
        """测试当实例已存在时直接返回"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        # 手动设置实例
        TushareFetcherFactory._instance = mock_instance
        TushareFetcherFactory._initialized = True
        
        # 调用 get_instance 应该直接返回已存在的实例
        result = TushareFetcherFactory.get_instance()
        assert result == mock_instance
        # 不应该调用构造函数
        mock_fetcher_class.assert_not_called()
    
    @patch.dict('os.environ', {}, clear=True)
    def test_get_instance_no_token(self):
        """测试没有设置 TUSHARE_TOKEN 时抛出异常"""
        with pytest.raises(ValueError, match="错误：未设置 TUSHARE_TOKEN 环境变量"):
            TushareFetcherFactory.get_instance()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': ''})
    def test_get_instance_empty_token(self):
        """测试空 token 时抛出异常"""
        with pytest.raises(ValueError, match="错误：未设置 TUSHARE_TOKEN 环境变量"):
            TushareFetcherFactory.get_instance()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher_factory.TushareFetcher')
    def test_reset(self, mock_fetcher_class):
        """测试重置单例"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        # 创建实例
        instance1 = TushareFetcherFactory.get_instance()
        assert TushareFetcherFactory.is_initialized()
        
        # 重置
        TushareFetcherFactory.reset()
        assert not TushareFetcherFactory.is_initialized()
        assert TushareFetcherFactory._instance is None
        
        # 重新创建应该是新实例
        instance2 = TushareFetcherFactory.get_instance()
        assert mock_fetcher_class.call_count == 2
    
    def test_is_initialized(self):
        """测试初始化状态检查"""
        # 初始状态
        assert not TushareFetcherFactory.is_initialized()
        
        # 创建实例后
        with patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'}):
            with patch('src.downloader.fetcher_factory.TushareFetcher'):
                TushareFetcherFactory.get_instance()
                assert TushareFetcherFactory.is_initialized()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher_factory.TushareFetcher')
    def test_get_instance_id(self, mock_fetcher_class):
        """测试获取实例ID"""
        # 未初始化时
        assert TushareFetcherFactory.get_instance_id() is None
        
        # 创建实例后
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        TushareFetcherFactory.get_instance()
        instance_id = TushareFetcherFactory.get_instance_id()
        assert instance_id == id(mock_instance)


class TestModuleFunctions:
    """测试模块级函数"""
    
    def setup_method(self):
        """每个测试方法前重置单例"""
        _reset_singleton()
    
    def teardown_method(self):
        """每个测试方法后重置单例"""
        _reset_singleton()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher_factory.TushareFetcher')
    def test_get_singleton(self, mock_fetcher_class):
        """测试 get_singleton 函数"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        result = get_singleton()
        assert result == mock_instance
        mock_fetcher_class.assert_called_once_with('test_token')
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher_factory.TushareFetcher')
    def test_get_fetcher_singleton_mode(self, mock_fetcher_class):
        """测试 get_fetcher 函数（单例模式）"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        result = get_fetcher(use_singleton=True)
        assert result == mock_instance
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher_factory.TushareFetcher')
    def test_get_fetcher_non_singleton_mode_deprecated(self, mock_fetcher_class):
        """测试 get_fetcher 函数（非单例模式已废弃）"""
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        with patch('src.downloader.fetcher_factory.logger') as mock_logger:
            result = get_fetcher(use_singleton=False)
            assert result == mock_instance
            mock_logger.warning.assert_called_once_with("非单例模式已废弃，将使用单例模式")
    
    def test_reset_singleton(self):
        """测试 _reset_singleton 函数"""
        with patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'}):
            with patch('src.downloader.fetcher_factory.TushareFetcher'):
                # 创建实例
                get_singleton()
                assert TushareFetcherFactory.is_initialized()
                
                # 重置
                _reset_singleton()
                assert not TushareFetcherFactory.is_initialized()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher_factory.TushareFetcher')
    def test_get_instance_info(self, mock_fetcher_class):
        """测试 _get_instance_info 函数"""
        # 未初始化时
        info = _get_instance_info()
        assert info['is_singleton_initialized'] is False
        assert info['singleton_instance_id'] is None
        assert info['singleton_exists'] is False
        
        # 创建实例后
        mock_instance = MagicMock()
        mock_fetcher_class.return_value = mock_instance
        
        get_singleton()
        info = _get_instance_info()
        assert info['is_singleton_initialized'] is True
        assert info['singleton_instance_id'] == id(mock_instance)
        assert info['singleton_exists'] is True


class TestThreadSafety:
    """测试线程安全性"""
    
    def setup_method(self):
        """每个测试方法前重置单例"""
        TushareFetcherFactory.reset()
    
    def teardown_method(self):
        """每个测试方法后重置单例"""
        TushareFetcherFactory.reset()
    
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    @patch('src.downloader.fetcher_factory.TushareFetcher')
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
            instance = TushareFetcherFactory.get_instance()
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
    @patch('src.downloader.fetcher_factory.TushareFetcher')
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
            instance = TushareFetcherFactory.get_instance()
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