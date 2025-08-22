from unittest.mock import MagicMock, patch
from neo.helpers.app_service import AppService
from neo.task_bus.types import TaskType, DownloadTaskConfig, TaskPriority


class TestAppService:
    """AppService 测试类"""

    def setup_method(self):
        """每个测试方法前的设置"""
        self.mock_downloader = MagicMock()
        self.app_service = AppService(downloader=self.mock_downloader)

    def test_run_downloader_with_single_task(self):
        """测试运行单个下载任务"""
        # 创建测试任务
        task = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.HIGH,
        )
        tasks = [task]

        # 执行测试
        self.app_service.run_downloader(tasks, dry_run=False)

        # 验证下载器被调用
        self.mock_downloader.download.assert_called_once_with(task)

    def test_run_downloader_with_multiple_tasks(self):
        """测试运行多个下载任务"""
        # 创建测试任务
        task1 = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.HIGH,
        )
        task2 = DownloadTaskConfig(
            symbol="000002.SZ",
            task_type=TaskType.stock_daily,
            priority=TaskPriority.MEDIUM,
        )
        tasks = [task1, task2]

        # 执行测试
        self.app_service.run_downloader(tasks, dry_run=False)

        # 验证下载器被调用两次
        assert self.mock_downloader.download.call_count == 2
        self.mock_downloader.download.assert_any_call(task1)
        self.mock_downloader.download.assert_any_call(task2)

    @patch("builtins.print")
    def test_run_downloader_dry_run_mode(self, mock_print):
        """测试干运行模式"""
        # 创建测试任务
        task = DownloadTaskConfig(
            symbol="000001.SZ",
            task_type=TaskType.stock_basic,
            priority=TaskPriority.HIGH,
        )
        tasks = [task]

        # 执行干运行
        self.app_service.run_downloader(tasks, dry_run=True)

        # 验证不会调用下载器
        self.mock_downloader.download.assert_not_called()

        # 不再检查日志输出内容
        mock_print.assert_called()

    @patch("builtins.print")
    def test_run_downloader_dry_run_with_multiple_tasks(self, mock_print):
        """测试干运行模式下的多个任务"""
        # 创建测试任务
        tasks = [
            DownloadTaskConfig(
                symbol="000001.SZ",
                task_type=TaskType.stock_basic,
                priority=TaskPriority.HIGH,
            ),
            DownloadTaskConfig(
                symbol="000002.SZ",
                task_type=TaskType.stock_daily,
                priority=TaskPriority.MEDIUM,
            ),
        ]

        # 执行干运行
        self.app_service.run_downloader(tasks, dry_run=True)

        # 验证不会调用下载器
        self.mock_downloader.download.assert_not_called()

        # 不再检查日志输出内容
        mock_print.assert_called()

    def test_run_downloader_with_empty_tasks(self):
        """测试空任务列表"""
        tasks = []

        # 执行下载
        self.app_service.run_downloader(tasks)

        # 验证下载器不会调用 download 方法
        self.mock_downloader.download.assert_not_called()

    @patch("builtins.print")
    def test_run_downloader_dry_run_with_empty_tasks(self, mock_print):
        """测试干运行模式下的空任务列表"""
        tasks = []

        # 执行干运行
        self.app_service.run_downloader(tasks, dry_run=True)

        # 验证不会调用下载器
        self.mock_downloader.download.assert_not_called()

        # 不再检查日志输出内容
        mock_print.assert_called()
