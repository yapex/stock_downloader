"""测试 sys 组全局任务的处理逻辑

主要测试场景：
1. sys 组应该能够正确创建任务映射，即使它只包含全局任务
2. 全局任务应该使用空字符串作为 symbol 标识符
3. AppService 应该正确处理全局任务组
"""

from unittest.mock import Mock, patch

from neo.helpers.app_service import AppService
from neo.helpers.group_handler import GroupHandler
from neo.services.consumer_runner import ConsumerRunner
from neo.services.downloader_service import DownloaderService


class TestSysGroupFix:
    """测试 sys 组修复"""

    def test_sys_group_should_return_global_tasks_mapping(self):
        """测试：sys 组应该返回全局任务映射"""
        # Arrange
        mock_consumer_runner = Mock(spec=ConsumerRunner)
        mock_downloader_service = Mock(spec=DownloaderService)

        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        # Mock container 来返回预期的结果
        with patch("neo.app.container") as mock_container:
            mock_group_handler = Mock(spec=GroupHandler)
            mock_container.group_handler.return_value = mock_group_handler

            # sys 组包含的任务类型
            mock_group_handler.get_task_types_for_group.return_value = [
                "stock_basic",
                "trade_cal",
            ]
            # 对于全局任务组，应该返回包含空字符串的列表，而不是空列表
            mock_group_handler.get_symbols_for_group.return_value = [""]

            # Act
            result = app_service.build_task_stock_mapping_from_group("sys")

            # Assert
            expected = {"stock_basic": [""], "trade_cal": [""]}
            assert result == expected
            assert result  # 确保结果不为空

    def test_group_handler_should_return_empty_string_for_global_tasks(self):
        """测试：GroupHandler 应该为全局任务返回空字符串列表"""
        # 这个测试验证我们的修复方案

        # TODO: 这个测试目前会失败，因为当前的实现返回空列表
        # 修复后这个测试应该通过
        pass

    def test_mixed_group_should_return_actual_symbols(self):
        """测试：混合组应该返回实际的股票代码"""
        # Arrange
        mock_consumer_runner = Mock(spec=ConsumerRunner)
        mock_downloader_service = Mock(spec=DownloaderService)

        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        with patch("neo.app.container") as mock_container:
            mock_group_handler = Mock(spec=GroupHandler)
            mock_container.group_handler.return_value = mock_group_handler

            # daily 组包含需要股票代码的任务
            mock_group_handler.get_task_types_for_group.return_value = [
                "stock_daily",
                "daily_basic",
            ]
            mock_group_handler.get_symbols_for_group.return_value = [
                "000001.SZ",
                "000002.SZ",
            ]

            # Act
            result = app_service.build_task_stock_mapping_from_group("daily")

            # Assert
            expected = {
                "stock_daily": ["000001.SZ", "000002.SZ"],
                "daily_basic": ["000001.SZ", "000002.SZ"],
            }
            assert result == expected

    def test_current_implementation_problem(self):
        """测试：当前实现的问题 - sys 组返回空映射"""
        # Arrange
        mock_consumer_runner = Mock(spec=ConsumerRunner)
        mock_downloader_service = Mock(spec=DownloaderService)

        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        with patch("neo.app.container") as mock_container:
            mock_group_handler = Mock(spec=GroupHandler)
            mock_container.group_handler.return_value = mock_group_handler

            # 模拟当前的实现：全局任务组返回空列表
            mock_group_handler.get_task_types_for_group.return_value = [
                "stock_basic",
                "trade_cal",
            ]
            mock_group_handler.get_symbols_for_group.return_value = []  # 当前实现

            # Act
            result = app_service.build_task_stock_mapping_from_group("sys")

            # Assert - 这展示了当前的问题
            assert result == {}  # 当前实现返回空字典，这是问题所在
