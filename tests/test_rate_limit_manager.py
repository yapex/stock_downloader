"""测试 RateLimitManager 类"""

from unittest.mock import Mock, patch

from neo.helpers.rate_limit_manager import RateLimitManager, get_rate_limit_manager
from neo.helpers.interfaces import IRateLimitManager

# TaskType 现在使用字符串，TaskTemplateRegistry 已移除
from pyrate_limiter import Limiter
from neo.containers import AppContainer
class TestRateLimitManager:
    """测试 RateLimitManager 类"""

    def setup_method(self):
        """每个测试方法前的设置"""
        from tests.fixtures.mock_factory import MockFactory
        self.mock_factory = MockFactory()

    def teardown_method(self):
        """每个测试方法后的清理"""
        # 清理单例实例，避免测试间相互影响
        RateLimitManager._instance = None
        import neo.helpers.rate_limit_manager

        neo.helpers.rate_limit_manager._global_rate_limit_manager = None

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_implements_interface(self, mock_get_config):
        """测试实现了IRateLimitManager接口"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]

        manager = RateLimitManager()
        assert isinstance(manager, IRateLimitManager)

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_get_rate_limit_config_default(self, mock_get_config):
        """测试获取默认速率限制配置"""
        # 使用 MockFactory 创建空的下载任务配置
        mocks = self.mock_factory.create_complete_rate_limit_mocks({})
        mock_get_config.return_value = mocks["config"]

        manager = RateLimitManager()
        rate_limit = manager.get_rate_limit_config("stock_basic")
        assert rate_limit == 190  # 默认值

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_get_rate_limit_config_custom(self, mock_get_config):
        """测试获取自定义速率限制配置"""
        # 使用 MockFactory 创建包含自定义速率限制的配置
        download_tasks = {"stock_basic": {"rate_limit_per_minute": 100}}
        mocks = self.mock_factory.create_complete_rate_limit_mocks(download_tasks)
        mock_get_config.return_value = mocks["config"]

        manager = RateLimitManager()
        rate_limit = manager.get_rate_limit_config("stock_basic")
        assert rate_limit == 100

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_get_limiter_creates_new(self, mock_get_config):
        """测试获取限制器时创建新实例"""
        # 使用 MockFactory 创建包含速率限制配置的 mock
        download_tasks = {"stock_basic": {"rate_limit_per_minute": 150}}
        mocks = self.mock_factory.create_complete_rate_limit_mocks(download_tasks)
        mock_get_config.return_value = mocks["config"]

        manager = RateLimitManager()
        limiter = manager.get_limiter("stock_basic")

        assert isinstance(limiter, Limiter)
        assert "stock_basic" in manager.rate_limiters

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_get_limiter_reuses_existing(self, mock_get_config):
        """测试获取限制器时复用已存在的实例"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]

        manager = RateLimitManager()
        # 第一次获取
        limiter1 = manager.get_limiter("stock_basic")
        # 第二次获取
        limiter2 = manager.get_limiter("stock_basic")

        # 应该是同一个实例
        assert limiter1 is limiter2
        assert len(manager.rate_limiters) == 1

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_apply_rate_limiting_success(self, mock_get_config):
        """测试成功应用速率限制"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]

        manager = RateLimitManager()
        # 应该不抛出异常
        manager.apply_rate_limiting("stock_basic")

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_cleanup_removes_all_limiters(self, mock_get_config):
        """测试清理移除所有限制器"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]

        manager = RateLimitManager()
        # 创建一些限制器
        manager.get_limiter("stock_basic")
        assert len(manager.rate_limiters) == 1

        # 清理
        manager.cleanup()
        assert len(manager.rate_limiters) == 0

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_different_task_types_get_different_limiters(self, mock_get_config):
        """测试不同任务类型获取不同的限制器"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]

        manager = RateLimitManager()
        limiter1 = manager.get_limiter("stock_basic")
        limiter2 = manager.get_limiter("daily_basic")

        # 应该是不同的实例
        assert limiter1 is not limiter2
        assert len(manager.rate_limiters) == 2
class TestRateLimitManagerSingleton:
    """测试 RateLimitManager 单例模式"""
    
    def setup_method(self):
        """每个测试方法前的设置"""
        from tests.fixtures.mock_factory import MockFactory
        self.mock_factory = MockFactory()

    def teardown_method(self):
        """每个测试方法后的清理"""
        # 清理单例实例，避免测试间相互影响
        RateLimitManager._singleton_instance = None

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_singleton_same_instance(self, mock_get_config):
        """测试单例模式返回相同实例"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]

        # 创建两个单例实例
        manager1 = RateLimitManager.singleton()
        manager2 = RateLimitManager.singleton()

        # 应该是同一个实例
        assert manager1 is manager2

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_get_rate_limit_manager_singleton(self, mock_get_config):
        """测试 get_rate_limit_manager 函数返回单例"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]
        # 多次调用应该返回相同实例
        manager1 = get_rate_limit_manager()
        manager2 = get_rate_limit_manager()
        manager3 = RateLimitManager.singleton()

        # 应该都是同一个实例
        assert manager1 is manager2
        assert manager1 is manager3
        assert isinstance(manager1, RateLimitManager)

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_singleton_thread_safety(self, mock_get_config):
        """测试单例模式的线程安全性"""
        import threading
        import time

        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]
        instances = []

        def create_instance():
            # 添加小延迟模拟并发场景
            time.sleep(0.01)
            instances.append(RateLimitManager.singleton())

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

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_singleton_preserves_state(self, mock_get_config):
        """测试单例模式保持状态"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]
        # 第一次创建单例
        manager1 = RateLimitManager.singleton()
        original_rate_limiters = manager1.rate_limiters

        # 添加一些数据
        manager1.get_limiter("stock_basic")
        assert len(manager1.rate_limiters) == 1

        # 第二次获取单例应该保持状态
        manager2 = RateLimitManager.singleton()

        # 应该是同一个实例，且数据保持不变
        assert manager1 is manager2
        assert manager2.rate_limiters is original_rate_limiters
        assert len(manager2.rate_limiters) == 1

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_multiple_instances_are_different(self, mock_get_config):
        """测试直接实例化可以创建多个不同的实例"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]
        # 直接实例化创建多个实例
        manager1 = RateLimitManager()
        manager2 = RateLimitManager()

        # 应该是不同的实例
        assert manager1 is not manager2

        # 每个实例都有自己的状态
        manager1.get_limiter("stock_basic")
        assert len(manager1.rate_limiters) == 1
        assert len(manager2.rate_limiters) == 0

        # 单例实例应该与直接实例化的实例不同
        singleton_manager = RateLimitManager.singleton()
        assert singleton_manager is not manager1
        assert singleton_manager is not manager2
class TestRateLimitManagerContainer:
    """测试从 Container 中获取 RateLimitManager"""
    
    def setup_method(self):
        """每个测试方法前的设置"""
        from tests.fixtures.mock_factory import MockFactory
        self.mock_factory = MockFactory()

    def teardown_method(self):
        """每个测试方法后的清理"""
        # 清理单例实例，避免测试间相互影响
        RateLimitManager._singleton_instance = None

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_get_rate_limit_manager_from_container(self, mock_get_config):
        """测试从容器中获取 RateLimitManager 实例"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]
        container = AppContainer()
        rate_limit_manager = container.rate_limit_manager()

        assert isinstance(rate_limit_manager, RateLimitManager)
        assert isinstance(rate_limit_manager, IRateLimitManager)

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_container_provides_singleton_instance(self, mock_get_config):
        """测试容器提供单例 RateLimitManager 实例（Singleton 模式）"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]
        container = AppContainer()
        rate_limit_manager1 = container.rate_limit_manager()
        rate_limit_manager2 = container.rate_limit_manager()

        # Singleton 模式应该提供相同的实例
        assert rate_limit_manager1 is rate_limit_manager2
        assert isinstance(rate_limit_manager1, RateLimitManager)
        assert isinstance(rate_limit_manager2, RateLimitManager)

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_container_rate_limit_manager_functionality(self, mock_get_config):
        """测试从容器获取的 RateLimitManager 功能正常"""
        # 使用 MockFactory 创建包含特定配置的 mock
        download_tasks = {"stock_basic": {"rate_limit_per_minute": 120}}
        mocks = self.mock_factory.create_complete_rate_limit_mocks(download_tasks)
        mock_get_config.return_value = mocks["config"]
        container = AppContainer()
        rate_limit_manager = container.rate_limit_manager()

        # 测试获取速率限制配置
        rate_limit = rate_limit_manager.get_rate_limit_config("stock_basic")
        assert rate_limit == 120

        # 测试获取限制器
        limiter = rate_limit_manager.get_limiter("stock_basic")
        assert isinstance(limiter, Limiter)

        # 测试应用速率限制
        rate_limit_manager.apply_rate_limiting("stock_basic")

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_different_containers_share_same_global_singleton(self, mock_get_config):
        """测试不同容器共享同一个全局 RateLimitManager 单例"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]
        container1 = AppContainer()
        container2 = AppContainer()

        rate_limit_manager1 = container1.rate_limit_manager()
        rate_limit_manager2 = container2.rate_limit_manager()

        # 因为 RateLimitManager 使用了全局单例模式，所以不同容器也应该返回同一个实例
        assert rate_limit_manager1 is rate_limit_manager2
        assert isinstance(rate_limit_manager1, RateLimitManager)

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_container_singleton_preserves_state(self, mock_get_config):
        """测试容器单例保持状态"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]
        container = AppContainer()
        rate_limit_manager1 = container.rate_limit_manager()

        # 添加一些数据
        rate_limit_manager1.get_limiter("stock_basic")
        assert len(rate_limit_manager1.rate_limiters) == 1

        # 再次获取应该保持状态
        rate_limit_manager2 = container.rate_limit_manager()

        # 应该是同一个实例，且数据保持不变
        assert rate_limit_manager1 is rate_limit_manager2
        assert len(rate_limit_manager2.rate_limiters) == 1

    @patch("neo.helpers.rate_limit_manager.get_config")
    def test_container_integration_with_downloader(self, mock_get_config):
        """测试容器中 RateLimitManager 与 SimpleDownloader 的集成"""
        # 使用 MockFactory 创建配置 mock
        mocks = self.mock_factory.create_complete_rate_limit_mocks()
        mock_get_config.return_value = mocks["config"]
        container = AppContainer()
        downloader = container.downloader()
        rate_limit_manager = container.rate_limit_manager()

        # downloader 应该使用容器中的 rate_limit_manager
        assert downloader.rate_limit_manager is rate_limit_manager
