"""测试 SimpleDownloader 与容器的集成功能"""

from unittest.mock import Mock

from neo.downloader.simple_downloader import SimpleDownloader
from neo.helpers.interfaces import IRateLimitManager
from neo.containers import AppContainer


class TestSimpleDownloaderContainer:
    """测试从 AppContainer 获取 SimpleDownloader 的相关行为"""

    def test_get_downloader_from_container(self):
        """测试从容器中获取 SimpleDownloader 实例"""
        container = AppContainer()
        downloader = container.downloader()

        # 验证实例类型
        assert isinstance(downloader, SimpleDownloader)

        # 验证依赖注入正确
        assert downloader.rate_limit_manager is not None
        assert downloader.fetcher_builder is not None

        # 验证具有预期的方法
        assert hasattr(downloader, "download")

    def test_container_provides_same_instances(self):
        """测试容器以单例模式提供相同的 SimpleDownloader 实例"""
        container = AppContainer()
        downloader1 = container.downloader()
        downloader2 = container.downloader()

        # 验证是相同的实例（单例模式）
        assert downloader1 is downloader2
        assert isinstance(downloader1, SimpleDownloader)
        assert isinstance(downloader2, SimpleDownloader)

    def test_container_downloader_functionality(self):
        """测试从容器获取的 SimpleDownloader 功能是否正常"""
        container = AppContainer()
        downloader = container.downloader()

        # 验证可以调用 download 方法（即使可能因为网络或配置问题失败）
        # 这里主要验证方法存在且可调用，不验证具体结果
        assert callable(downloader.download)

        # 验证依赖的组件都已正确注入
        assert downloader.rate_limit_manager is not None
        assert downloader.fetcher_builder is not None

    def test_different_containers_have_different_singletons(self):
        """测试不同容器实例会创建不同的单例组件"""
        container1 = AppContainer()
        container2 = AppContainer()

        downloader1 = container1.downloader()
        downloader2 = container2.downloader()

        # 验证不同容器的下载器是不同实例
        assert downloader1 is not downloader2

        # 验证不同容器的速率限制管理器是相同实例
        # （因为 RateLimitManager 是全局单例）
        rate_manager1 = container1.rate_limit_manager()
        rate_manager2 = container2.rate_limit_manager()
        assert rate_manager1 is rate_manager2

    def test_same_container_shares_singleton_components(self):
        """测试同一容器内的单例组件共享"""
        container = AppContainer()

        # 获取两个下载器实例
        downloader1 = container.downloader()
        downloader2 = container.downloader()

        # 验证下载器是相同实例（单例模式）
        assert downloader1 is downloader2

        # 但它们共享同一个速率限制管理器（单例）
        assert downloader1.rate_limit_manager is downloader2.rate_limit_manager

        # 直接从容器获取的速率限制管理器也应该是同一个实例
        rate_manager = container.rate_limit_manager()
        assert downloader1.rate_limit_manager is rate_manager
        assert downloader2.rate_limit_manager is rate_manager

    def test_container_downloader_with_mocked_dependencies(self):
        """测试容器中下载器与模拟依赖的集成"""
        container = AppContainer()

        # 模拟速率限制管理器
        mock_rate_manager = Mock(spec=IRateLimitManager)
        container.rate_limit_manager.override(mock_rate_manager)

        try:
            downloader = container.downloader()

            # 验证使用了模拟的速率限制管理器
            assert downloader.rate_limit_manager is mock_rate_manager

            # 验证其他依赖仍然正常
            assert downloader.fetcher_builder is not None

        finally:
            # 清理覆盖
            container.rate_limit_manager.reset_override()
