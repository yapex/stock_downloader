"""AppService 测试

测试 AppService 类的功能，包括工厂方法。
"""

from unittest.mock import patch

from neo.helpers.app_service import AppService, ServiceFactory

from neo.container import AppContainer


class TestAppService:
    """AppService 类测试"""

    def test_init(self):
        """测试 AppService 初始化"""
        # 创建 AppService 实例
        app_service = AppService()

        # 验证实例创建成功
        assert isinstance(app_service, AppService)

    def test_create_default(self):
        """测试 create_default 工厂方法"""
        # 调用工厂方法
        app_service = AppService.create_default()

        # 验证返回的实例
        assert isinstance(app_service, AppService)

    def test_run_data_processor(self):
        """测试 run_data_processor 方法"""
        app_service = AppService()

        # 模拟 DataProcessorRunner 的方法
        with patch("neo.helpers.app_service.DataProcessorRunner") as mock_runner:
            app_service.run_data_processor()

            # 验证调用了正确的方法
            mock_runner.setup_signal_handlers.assert_called_once()
            mock_runner.setup_huey_logging.assert_called_once()
            mock_runner.run_consumer.assert_called_once()


class TestServiceFactory:
    """ServiceFactory 类测试"""

    def test_create_app_service_with_defaults(self):
        """测试使用默认参数创建 AppService"""
        # 调用工厂方法
        app_service = ServiceFactory.create_app_service()

        # 验证返回的实例
        assert isinstance(app_service, AppService)

    def test_create_app_service_with_custom_params(self):
        """测试使用自定义参数创建 AppService"""
        # 调用工厂方法
        app_service = ServiceFactory.create_app_service(with_progress=False)

        # 验证返回的实例
        assert isinstance(app_service, AppService)


class TestAppServiceContainer:
    """测试从 AppContainer 获取 AppService 的相关行为"""

    def test_get_app_service_from_container(self):
        """测试能从容器获取 AppService 实例"""
        container = AppContainer()

        app_service = container.app_service()

        assert isinstance(app_service, AppService)
        assert hasattr(app_service, "tasks_progress_tracker")

    def test_container_provides_different_service_instances(self):
        """测试容器每次提供不同的 AppService 实例（工厂模式）"""
        container = AppContainer()

        service1 = container.app_service()
        service2 = container.app_service()

        assert service1 is not service2
        assert isinstance(service1, AppService)
        assert isinstance(service2, AppService)

    def test_container_service_functionality(self):
        """测试容器获取的 AppService 功能正常"""
        container = AppContainer()

        app_service = container.app_service()

        # 验证基本功能可用
        assert hasattr(app_service, "run_data_processor")
        assert callable(app_service.run_data_processor)

        # 验证依赖注入正确
        assert app_service.tasks_progress_tracker is not None

    def test_container_service_dependencies_injection(self):
        """测试容器正确注入了依赖"""
        container = AppContainer()

        app_service = container.app_service()

        # 验证依赖类型正确
        from neo.tqmd import TasksProgressTracker

        assert isinstance(app_service.tasks_progress_tracker, TasksProgressTracker)

    def test_different_containers_have_different_services(self):
        """测试不同容器实例有不同的服务"""
        container1 = AppContainer()
        container2 = AppContainer()

        service1 = container1.app_service()
        service2 = container2.app_service()

        # 不同容器的实例应该不同
        assert service1 is not service2
        assert isinstance(service1, AppService)
        assert isinstance(service2, AppService)

    def test_container_service_with_mocked_dependencies(self):
        """测试容器服务与模拟依赖的集成"""
        container = AppContainer()

        app_service = container.app_service()

        # 验证实例创建成功
        assert isinstance(app_service, AppService)
