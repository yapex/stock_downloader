"""
测试 DownloaderService 类 (纯单元测试)
"""

from unittest.mock import patch

from neo.services.downloader_service import DownloaderService
from neo.helpers.task_builder import DownloadTaskConfig


class TestDownloaderService:
    """DownloaderService - 纯单元测试"""

    @patch("neo.services.downloader_service.download_task")
    def test_run_submits_tasks(self, mock_download_task):
        """测试 run 方法是否为每个任务都调用了 download_task"""
        # 1. 准备 mock 和被测对象
        mock_download_task.return_value = "mocked_result"
        service = DownloaderService()
        # 使用 DownloadTaskConfig 对象
        tasks = [
            DownloadTaskConfig(task_type="stock_basic", symbol="000001"),
            DownloadTaskConfig(task_type="stock_daily", symbol="000002"),
        ]

        # 2. 调用被测方法
        results = service.run(tasks, dry_run=False)

        # 3. 断言
        assert mock_download_task.call_count == 2
        mock_download_task.assert_any_call("stock_basic", "000001")
        mock_download_task.assert_any_call("stock_daily", "000002")
        assert results == ["mocked_result", "mocked_result"]

    @patch("neo.services.downloader_service.download_task")
    def test_run_dry_run_does_not_submit(self, mock_download_task):
        """测试 dry_run=True 时，不调用 download_task"""
        # 1. 准备 mock 和被测对象
        service = DownloaderService()
        tasks = [DownloadTaskConfig(task_type="stock_basic", symbol="000001")]

        # 2. 调用被测方法
        with patch("builtins.print") as mock_print:
            service.run(tasks, dry_run=True)

        # 3. 断言
        mock_download_task.assert_not_called()
        # 验证打印了试运行信息
        assert mock_print.call_count > 0

    @patch("neo.services.downloader_service.download_task")
    def test_run_handles_submission_failure(self, mock_download_task):
        """测试当 download_task 抛出异常时，run 方法能优雅处理"""
        # 1. 准备 mock 和被测对象
        mock_download_task.side_effect = [Exception("Boom!"), "mocked_result"]
        service = DownloaderService()
        tasks = [
            DownloadTaskConfig(task_type="stock_basic", symbol="FAIL"),
            DownloadTaskConfig(task_type="stock_daily", symbol="OK"),
        ]

        # 2. 调用被测方法
        results = service.run(tasks, dry_run=False)

        # 3. 断言
        assert mock_download_task.call_count == 2
        # 验证结果只包含成功的任务
        assert results == ["mocked_result"]
