"""测试StockBasic的ThreadLocal单例模式"""

import threading
import time
from unittest.mock import patch
from src.downloader2.models.stock_basic import StockBasic


class TestStockBasicThreadLocal:
    """测试StockBasic的ThreadLocal单例功能"""
    
    def setup_method(self):
        """每个测试方法前清理ThreadLocal实例"""
        StockBasic.clear_instance()
    
    def test_single_thread_singleton(self):
        """测试单线程下的单例模式"""
        with patch('tushare.set_token'):
            instance1 = StockBasic.get_instance("test_token")
            instance2 = StockBasic.get_instance("test_token")
            
            # 同一线程中应该返回相同的实例
            assert instance1 is instance2
            assert instance1._token == "test_token"
    
    def test_different_threads_different_instances(self):
        """测试不同线程有不同的实例"""
        instances = {}
        
        def create_instance(thread_id: int, token: str):
            with patch('tushare.set_token'):
                instance = StockBasic.get_instance(token)
                instances[thread_id] = instance
        
        # 创建两个线程
        thread1 = threading.Thread(target=create_instance, args=(1, "token1"))
        thread2 = threading.Thread(target=create_instance, args=(2, "token2"))
        
        thread1.start()
        thread2.start()
        
        thread1.join()
        thread2.join()
        
        # 不同线程应该有不同的实例
        assert instances[1] is not instances[2]
        assert instances[1]._token == "token1"
        assert instances[2]._token == "token2"
    
    def test_same_thread_multiple_calls(self):
        """测试同一线程多次调用返回相同实例"""
        instances = []
        
        def create_multiple_instances():
            with patch('tushare.set_token'):
                for _ in range(5):
                    instance = StockBasic.get_instance("test_token")
                    instances.append(instance)
        
        thread = threading.Thread(target=create_multiple_instances)
        thread.start()
        thread.join()
        
        # 所有实例应该是同一个
        first_instance = instances[0]
        for instance in instances[1:]:
            assert instance is first_instance
    
    def test_clear_instance(self):
        """测试清除实例功能"""
        with patch('tushare.set_token'):
            # 创建实例
            instance1 = StockBasic.get_instance("test_token")
            
            # 清除实例
            StockBasic.clear_instance()
            
            # 再次获取应该是新实例
            instance2 = StockBasic.get_instance("test_token")
            
            assert instance1 is not instance2
    
    def test_clear_instance_different_threads(self):
        """测试清除实例不影响其他线程"""
        instances = {}
        
        def thread1_operations():
            with patch('tushare.set_token'):
                instances['thread1_before'] = StockBasic.get_instance("token1")
                time.sleep(0.1)  # 等待thread2创建实例
                StockBasic.clear_instance()  # 只清除当前线程的实例
                instances['thread1_after'] = StockBasic.get_instance("token1")
        
        def thread2_operations():
            with patch('tushare.set_token'):
                instances['thread2_before'] = StockBasic.get_instance("token2")
                time.sleep(0.2)  # 等待thread1清除实例
                instances['thread2_after'] = StockBasic.get_instance("token2")
        
        thread1 = threading.Thread(target=thread1_operations)
        thread2 = threading.Thread(target=thread2_operations)
        
        thread1.start()
        thread2.start()
        
        thread1.join()
        thread2.join()
        
        # thread1清除实例后应该创建新实例
        assert instances['thread1_before'] is not instances['thread1_after']
        
        # thread2的实例不应该受影响
        assert instances['thread2_before'] is instances['thread2_after']
        
        # 不同线程的实例应该不同
        assert instances['thread1_before'] is not instances['thread2_before']
        assert instances['thread1_after'] is not instances['thread2_after']
    
    def test_constructor_still_works(self):
        """测试直接构造函数仍然可用"""
        with patch('tushare.set_token'):
            # 直接构造
            direct_instance = StockBasic("direct_token")
            
            # 通过单例获取
            singleton_instance = StockBasic.get_instance("singleton_token")
            
            # 应该是不同的实例
            assert direct_instance is not singleton_instance
            assert direct_instance._token == "direct_token"
            assert singleton_instance._token == "singleton_token"