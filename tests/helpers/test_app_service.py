"""测试 AppService Facade (纯单元测试)"""

from unittest.mock import Mock, patch

from neo.task_bus.types import DownloadTaskConfig, TaskType


class TestAppServiceFacade:
    """测试 AppService Facade - 纯单元测试，无容器依赖"""

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.configs.huey_config.huey_fast")
    @patch("neo.configs.huey_config.huey_slow")
    def test_run_data_processor(
        self, mock_huey_slow, mock_huey_fast, mock_download_task
    ):
        """测试 run_data_processor 是否正确委托给 consumer_runner"""
        # 导入放在 patch 内部，避免模块级别的依赖加载
        from neo.helpers.app_service import AppService

        # 1. 创建依赖的 Mock 对象
        mock_consumer_runner = Mock()
        mock_downloader_service = Mock()

        # 2. 实例化被测对象，并注入 Mock 依赖
        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        # 3. 调用被测方法
        app_service.run_data_processor("fast")

        # 4. 断言 Mock 对象的方法被正确调用
        mock_consumer_runner.run.assert_called_once_with("fast")

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.configs.huey_config.huey_fast")
    @patch("neo.configs.huey_config.huey_slow")
    def test_run_downloader(self, mock_huey_slow, mock_huey_fast, mock_download_task):
        """测试 run_downloader 是否正确委托给 downloader_service"""
        # 导入放在 patch 内部，避免模块级别的依赖加载
        from neo.helpers.app_service import AppService

        # 1. 创建依赖的 Mock 对象
        mock_consumer_runner = Mock()
        mock_downloader_service = Mock()

        # 2. 实例化被测对象，并注入 Mock 依赖
        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        # 3. 调用被测方法
        tasks = [DownloadTaskConfig(task_type=TaskType.stock_basic, symbol="000001")]
        app_service.run_downloader(tasks, dry_run=True)

        # 4. 断言 Mock 对象的方法被正确调用
        mock_downloader_service.run.assert_called_once()
        args, kwargs = mock_downloader_service.run.call_args
        assert args[0] == tasks
        assert args[1] is True
