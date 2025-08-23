"""AppService 测试

测试 AppService 类的功能，包括工厂方法。
"""

from unittest.mock import Mock, patch

from neo.helpers.app_service import AppService, ServiceFactory
from neo.database.interfaces import IDBOperator
from neo.downloader.interfaces import IDownloader


class TestAppService:
    """AppService 类测试"""

    def test_init(self):
        """测试 AppService 初始化"""
        # 创建模拟对象
        mock_db_operator = Mock(spec=IDBOperator)
        mock_downloader = Mock(spec=IDownloader)

        # 创建 AppService 实例
        app_service = AppService(
            db_operator=mock_db_operator, downloader=mock_downloader
        )

        # 验证属性设置正确
        assert app_service.db_operator is mock_db_operator
        assert app_service.downloader is mock_downloader

    @patch("neo.helpers.app_service.get_config")
    @patch("neo.helpers.app_service.DBOperator")
    @patch("neo.downloader.simple_downloader.SimpleDownloader")
    def test_create_default(
        self, mock_simple_downloader, mock_db_operator, mock_get_config
    ):
        """测试 create_default 工厂方法"""
        # 设置模拟配置
        mock_config = Mock()
        mock_config.database.path = ":memory:"
        mock_get_config.return_value = mock_config

        # 设置模拟实例
        mock_db_instance = Mock(spec=IDBOperator)
        mock_downloader_instance = Mock(spec=IDownloader)
        mock_db_operator.create_default.return_value = mock_db_instance
        mock_simple_downloader.create_default.return_value = mock_downloader_instance

        # 调用工厂方法
        app_service = AppService.create_default()

        # 验证配置被正确调用
        mock_get_config.assert_called_once()
        mock_db_operator.create_default.assert_called_once()
        mock_simple_downloader.create_default.assert_called_once()

        # 验证返回的实例
        assert isinstance(app_service, AppService)
        assert app_service.db_operator is mock_db_instance
        assert app_service.downloader is mock_downloader_instance

    def test_run_data_processor(self):
        """测试 run_data_processor 方法"""
        # 创建模拟对象
        mock_db_operator = Mock(spec=IDBOperator)
        mock_downloader = Mock(spec=IDownloader)

        app_service = AppService(
            db_operator=mock_db_operator, downloader=mock_downloader
        )

        # 模拟 DataProcessorRunner 的方法
        with patch("neo.helpers.app_service.DataProcessorRunner") as mock_runner:
            app_service.run_data_processor()

            # 验证调用了正确的方法
            mock_runner.setup_signal_handlers.assert_called_once()
            mock_runner.setup_huey_logging.assert_called_once()
            mock_runner.run_consumer.assert_called_once()


class TestServiceFactory:
    """ServiceFactory 类测试"""

    @patch("neo.helpers.app_service.get_config")
    @patch("neo.helpers.app_service.DBOperator")
    def test_create_app_service_with_defaults(self, mock_db_operator, mock_get_config):
        """测试使用默认参数创建 AppService"""
        # 设置模拟配置
        mock_config = Mock()
        mock_get_config.return_value = mock_config

        # 设置模拟实例
        mock_db_instance = Mock(spec=IDBOperator)
        mock_db_operator.create_default.return_value = mock_db_instance

        with patch(
            "neo.downloader.simple_downloader.SimpleDownloader"
        ) as mock_simple_downloader:
            mock_downloader_instance = Mock(spec=IDownloader)
            mock_simple_downloader.create_default.return_value = (
                mock_downloader_instance
            )

            # 调用工厂方法
            app_service = ServiceFactory.create_app_service()

            # 验证调用了正确的方法
            mock_db_operator.create_default.assert_called_once()
            mock_simple_downloader.create_default.assert_called_once()

            # 验证返回的实例
            assert isinstance(app_service, AppService)
            assert app_service.db_operator is mock_db_instance
            assert app_service.downloader is mock_downloader_instance

    def test_create_app_service_with_custom_params(self):
        """测试使用自定义参数创建 AppService"""
        # 创建模拟对象
        mock_db_operator = Mock(spec=IDBOperator)
        mock_downloader = Mock(spec=IDownloader)

        with patch("neo.helpers.app_service.get_config"):
            # 调用工厂方法
            app_service = ServiceFactory.create_app_service(
                db_operator=mock_db_operator, downloader=mock_downloader
            )

            # 验证返回的实例
            assert isinstance(app_service, AppService)
            assert app_service.db_operator is mock_db_operator
            assert app_service.downloader is mock_downloader
