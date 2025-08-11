import os
import threading
import time
import pytest
from unittest.mock import patch, MagicMock

from downloader.fetcher_factory import (
    TushareFetcherFactory,
    get_thread_local_fetcher
)
from downloader.fetcher import TushareFetcher


class TestThreadLocalFetcher:
    """测试线程本地fetcher功能"""
    
    def setup_method(self):
        """每个测试前重置状态"""
        TushareFetcherFactory.reset_thread_local()
    
    def teardown_method(self):
        """每个测试后清理状态"""
        TushareFetcherFactory.reset_thread_local()
    
    @patch.dict(os.environ, {'TUSHARE_TOKEN': 'test_token'})
    def test_get_thread_local_instance_creates_new_instance(self):
        """测试获取线程本地实例会创建新实例"""
        # 初始状态下没有实例
        assert not TushareFetcherFactory.has_thread_local_instance()
        assert TushareFetcherFactory.get_thread_local_instance_id() is None
        
        # 获取实例
        fetcher = TushareFetcherFactory.get_thread_local_instance()
        
        # 验证实例已创建
        assert isinstance(fetcher, TushareFetcher)
        assert TushareFetcherFactory.has_thread_local_instance()
        assert TushareFetcherFactory.get_thread_local_instance_id() is not None
    
    @patch.dict(os.environ, {'TUSHARE_TOKEN': 'test_token'})
    def test_get_thread_local_instance_returns_same_instance(self):
        """测试在同一线程内多次获取返回相同实例"""
        fetcher1 = TushareFetcherFactory.get_thread_local_instance()
        fetcher2 = TushareFetcherFactory.get_thread_local_instance()
        
        # 验证是同一个实例
        assert fetcher1 is fetcher2
        assert id(fetcher1) == id(fetcher2)
        assert TushareFetcherFactory.get_thread_local_instance_id() == id(fetcher1)
    
    def test_get_thread_local_instance_without_token_raises_error(self):
        """测试没有token时抛出异常"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="错误：未设置 TUSHARE_TOKEN 环境变量"):
                TushareFetcherFactory.get_thread_local_instance()
    
    @patch.dict(os.environ, {'TUSHARE_TOKEN': 'test_token'})
    def test_reset_thread_local_clears_instance(self):
        """测试重置线程本地实例"""
        # 创建实例
        fetcher = TushareFetcherFactory.get_thread_local_instance()
        instance_id = id(fetcher)
        
        # 验证实例存在
        assert TushareFetcherFactory.has_thread_local_instance()
        assert TushareFetcherFactory.get_thread_local_instance_id() == instance_id
        
        # 重置实例
        TushareFetcherFactory.reset_thread_local()
        
        # 验证实例已清除
        assert not TushareFetcherFactory.has_thread_local_instance()
        assert TushareFetcherFactory.get_thread_local_instance_id() is None
    
    @patch.dict(os.environ, {'TUSHARE_TOKEN': 'test_token'})
    def test_different_threads_get_different_instances(self):
        """测试不同线程获取不同的实例"""
        results = {}
        
        def thread_worker(thread_name):
            """线程工作函数"""
            fetcher = TushareFetcherFactory.get_thread_local_instance()
            results[thread_name] = {
                'instance_id': id(fetcher),
                'thread_id': threading.get_ident(),
                'has_instance': TushareFetcherFactory.has_thread_local_instance(),
                'factory_instance_id': TushareFetcherFactory.get_thread_local_instance_id()
            }
        
        # 创建多个线程
        threads = []
        for i in range(3):
            thread = threading.Thread(target=thread_worker, args=[f'thread_{i}'])
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证每个线程都有自己的实例
        assert len(results) == 3
        instance_ids = [result['instance_id'] for result in results.values()]
        thread_ids = [result['thread_id'] for result in results.values()]
        
        # 所有实例ID都不同
        assert len(set(instance_ids)) == 3
        # 所有线程ID都不同
        assert len(set(thread_ids)) == 3
        
        # 每个线程都有实例
        for result in results.values():
            assert result['has_instance'] is True
            assert result['factory_instance_id'] == result['instance_id']
    
    @patch.dict(os.environ, {'TUSHARE_TOKEN': 'test_token'})
    def test_thread_local_vs_global_singleton(self):
        """测试线程本地实例与全局单例的独立性"""
        # 获取全局单例
        global_fetcher = TushareFetcherFactory.get_instance()
        global_id = id(global_fetcher)
        
        # 获取线程本地实例
        thread_local_fetcher = TushareFetcherFactory.get_thread_local_instance()
        thread_local_id = id(thread_local_fetcher)
        
        # 验证是不同的实例
        assert global_fetcher is not thread_local_fetcher
        assert global_id != thread_local_id
        
        # 验证全局单例状态不受影响
        assert TushareFetcherFactory.is_initialized() is True
        assert TushareFetcherFactory.get_instance_id() == global_id
        
        # 验证线程本地状态独立
        assert TushareFetcherFactory.has_thread_local_instance() is True
        assert TushareFetcherFactory.get_thread_local_instance_id() == thread_local_id
    
    @patch.dict(os.environ, {'TUSHARE_TOKEN': 'test_token'})
    def test_get_thread_local_fetcher_function(self):
        """测试对外接口函数"""
        fetcher1 = get_thread_local_fetcher()
        fetcher2 = get_thread_local_fetcher()
        
        # 验证返回相同实例
        assert fetcher1 is fetcher2
        assert isinstance(fetcher1, TushareFetcher)
    
    @patch.dict(os.environ, {'TUSHARE_TOKEN': 'test_token'})
    def test_concurrent_access_same_thread(self):
        """测试同一线程内并发访问的安全性"""
        import concurrent.futures
        
        def get_fetcher():
            return TushareFetcherFactory.get_thread_local_instance()
        
        # 在同一线程内使用线程池执行器（实际上还是同一线程）
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            futures = [executor.submit(get_fetcher) for _ in range(5)]
            fetchers = [future.result() for future in futures]
        
        # 验证所有返回的都是同一个实例
        first_fetcher = fetchers[0]
        for fetcher in fetchers[1:]:
            assert fetcher is first_fetcher
    
    @patch.dict(os.environ, {'TUSHARE_TOKEN': 'test_token'})
    def test_thread_local_instance_isolation(self):
        """测试线程本地实例的隔离性"""
        main_thread_fetcher = TushareFetcherFactory.get_thread_local_instance()
        main_thread_id = id(main_thread_fetcher)
        
        other_thread_result = {}
        
        def other_thread_work():
            # 在另一个线程中获取实例
            fetcher = TushareFetcherFactory.get_thread_local_instance()
            other_thread_result['fetcher_id'] = id(fetcher)
            other_thread_result['has_instance'] = TushareFetcherFactory.has_thread_local_instance()
            
            # 重置另一个线程的实例
            TushareFetcherFactory.reset_thread_local()
            other_thread_result['has_instance_after_reset'] = TushareFetcherFactory.has_thread_local_instance()
        
        thread = threading.Thread(target=other_thread_work)
        thread.start()
        thread.join()
        
        # 验证另一个线程有自己的实例
        assert other_thread_result['fetcher_id'] != main_thread_id
        assert other_thread_result['has_instance'] is True
        assert other_thread_result['has_instance_after_reset'] is False
        
        # 验证主线程的实例不受影响
        assert TushareFetcherFactory.has_thread_local_instance() is True
        assert TushareFetcherFactory.get_thread_local_instance_id() == main_thread_id
        assert TushareFetcherFactory.get_thread_local_instance() is main_thread_fetcher