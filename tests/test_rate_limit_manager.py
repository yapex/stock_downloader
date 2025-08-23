"""测试 RateLimitManager 类"""

from unittest.mock import Mock, patch
import pytest

from neo.helpers.rate_limit_manager import RateLimitManager, get_rate_limit_manager
from neo.helpers.interfaces import IRateLimitManager
from neo.task_bus.types import TaskType
from pyrate_limiter import Limiter


class TestRateLimitManager:
    """测试 RateLimitManager 类"""

    def setup_method(self):
        """每个测试方法前的设置"""
        # 在每个测试中单独创建 manager
        pass

    def teardown_method(self):
        """每个测试方法后的清理"""
        # 清理单例实例，避免测试间相互影响
        RateLimitManager._instance = None
        import neo.helpers.rate_limit_manager
        neo.helpers.rate_limit_manager._global_rate_limit_manager = None

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_implements_interface(self, mock_get_config):
        """测试实现了IRateLimitManager接口"""
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config
        
        manager = RateLimitManager()
        assert isinstance(manager, IRateLimitManager)

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_get_rate_limit_config_default(self, mock_get_config):
        """测试获取默认速率限制配置"""
        # 模拟配置返回空的download_tasks
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config
        
        manager = RateLimitManager()
        rate_limit = manager.get_rate_limit_config(TaskType.stock_basic)
        assert rate_limit == 190  # 默认值

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_get_rate_limit_config_custom(self, mock_get_config):
        """测试获取自定义速率限制配置"""
        # 模拟配置返回自定义的rate_limit_per_minute
        mock_config = Mock()
        # 使用 api_method 作为配置键
        mock_config.download_tasks = {
            TaskType.stock_basic.value.api_method: {'rate_limit_per_minute': 100}
        }
        mock_get_config.return_value = mock_config
        
        manager = RateLimitManager()
        rate_limit = manager.get_rate_limit_config(TaskType.stock_basic)
        assert rate_limit == 100

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_get_limiter_creates_new(self, mock_get_config):
        """测试获取限制器时创建新实例"""
        mock_config = Mock()
        mock_config.download_tasks = {
            TaskType.stock_basic.value.api_method: {'rate_limit_per_minute': 150}
        }
        mock_get_config.return_value = mock_config
        
        manager = RateLimitManager()
        limiter = manager.get_limiter(TaskType.stock_basic)
        
        assert isinstance(limiter, Limiter)
        assert str(TaskType.stock_basic.value) in manager.rate_limiters

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_get_limiter_reuses_existing(self, mock_get_config):
        """测试获取限制器时复用已存在的实例"""
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config
        
        manager = RateLimitManager()
        # 第一次获取
        limiter1 = manager.get_limiter(TaskType.stock_basic)
        # 第二次获取
        limiter2 = manager.get_limiter(TaskType.stock_basic)
        
        # 应该是同一个实例
        assert limiter1 is limiter2
        assert len(manager.rate_limiters) == 1

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_apply_rate_limiting_success(self, mock_get_config):
        """测试成功应用速率限制"""
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config
        
        manager = RateLimitManager()
        # 应该不抛出异常
        manager.apply_rate_limiting(TaskType.stock_basic)

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_cleanup_removes_all_limiters(self, mock_get_config):
        """测试清理移除所有限制器"""
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config
        
        manager = RateLimitManager()
        # 创建一些限制器
        manager.get_limiter(TaskType.stock_basic)
        assert len(manager.rate_limiters) == 1

        # 清理
        manager.cleanup()
        assert len(manager.rate_limiters) == 0

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_different_task_types_get_different_limiters(self, mock_get_config):
        """测试不同任务类型获取不同的限制器"""
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config
        
        manager = RateLimitManager()
        limiter1 = manager.get_limiter(TaskType.stock_basic)
        limiter2 = manager.get_limiter(TaskType.daily_basic)
        
        # 应该是不同的实例
        assert limiter1 is not limiter2
        assert len(manager.rate_limiters) == 2


class TestRateLimitManagerSingleton:
    """测试 RateLimitManager 单例模式"""

    def teardown_method(self):
        """每个测试方法后的清理"""
        # 清理单例实例，避免测试间相互影响
        RateLimitManager._instance = None
        import neo.helpers.rate_limit_manager
        neo.helpers.rate_limit_manager._global_rate_limit_manager = None

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_singleton_same_instance(self, mock_get_config):
        """测试单例模式返回相同实例"""
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config

        # 创建两个实例
        manager1 = RateLimitManager()
        manager2 = RateLimitManager()

        # 应该是同一个实例
        assert manager1 is manager2

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_get_rate_limit_manager_singleton(self, mock_get_config):
        """测试 get_rate_limit_manager 函数返回单例"""
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config

        # 多次调用应该返回相同实例
        manager1 = get_rate_limit_manager()
        manager2 = get_rate_limit_manager()
        manager3 = RateLimitManager()

        # 应该都是同一个实例
        assert manager1 is manager2
        assert manager1 is manager3
        assert isinstance(manager1, RateLimitManager)

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_singleton_thread_safety(self, mock_get_config):
        """测试单例模式的线程安全性"""
        import threading
        import time
        
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config

        instances = []
        
        def create_instance():
            # 添加小延迟模拟并发场景
            time.sleep(0.01)
            instances.append(RateLimitManager())
        
        # 创建多个线程同时创建实例
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=create_instance)
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 所有实例应该都是同一个
        first_instance = instances[0]
        for instance in instances[1:]:
            assert instance is first_instance

    @patch('neo.helpers.rate_limit_manager.get_config')
    def test_singleton_no_duplicate_initialization(self, mock_get_config):
        """测试单例模式避免重复初始化"""
        mock_config = Mock()
        mock_config.download_tasks = {}
        mock_get_config.return_value = mock_config

        # 第一次创建
        manager1 = RateLimitManager()
        original_rate_limiters = manager1.rate_limiters
        
        # 添加一些数据
        manager1.get_limiter(TaskType.stock_basic)
        assert len(manager1.rate_limiters) == 1
        
        # 第二次创建应该不会重新初始化
        manager2 = RateLimitManager()
        
        # 应该是同一个实例，且数据保持不变
        assert manager1 is manager2
        assert manager2.rate_limiters is original_rate_limiters
        assert len(manager2.rate_limiters) == 1