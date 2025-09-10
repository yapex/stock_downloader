"""集成测试：验证 sys 组的实际行为

测试真实的 GroupHandler 和 AppService 交互
"""

from unittest.mock import Mock, patch

from neo.helpers.app_service import AppService
from neo.helpers.group_handler import GroupHandler
from neo.services.consumer_runner import ConsumerRunner
from neo.services.downloader_service import DownloaderService


class TestSysGroupIntegration:
    """sys 组集成测试"""

    def test_group_handler_returns_empty_string_for_sys_group(self):
        """测试：GroupHandler 对 sys 组返回空字符串列表"""
        # 创建一个真实的 GroupHandler 实例（但 mock 数据库依赖）
        with patch("neo.database.operator.ParquetDBQueryer") as mock_db_class:
            # Mock 数据库操作
            mock_db_instance = Mock()
            mock_db_instance.get_all_symbols.return_value = ["000001.SZ", "000002.SZ"]
            mock_db_class.create_default.return_value = mock_db_instance

            # Mock TaskFilter
            with patch("neo.helpers.group_handler.TaskFilter") as mock_filter_class:
                mock_filter_instance = Mock()
                mock_filter_instance.filter_symbols.return_value = [
                    "000001.SZ",
                    "000002.SZ",
                ]
                mock_filter_class.return_value = mock_filter_instance

                group_handler = GroupHandler.create_default()

                # 测试 sys 组
                symbols = group_handler.get_symbols_for_group("sys")
                assert symbols == [""], f"Expected [''] for sys group, got {symbols}"

                # 测试任务类型
                task_types = group_handler.get_task_types_for_group("sys")
                assert "stock_basic" in task_types
                assert "trade_cal" in task_types

    def test_app_service_with_real_group_handler(self):
        """测试：AppService 与真实 GroupHandler 的集成"""
        # 准备 AppService
        mock_consumer_runner = Mock(spec=ConsumerRunner)
        mock_downloader_service = Mock(spec=DownloaderService)

        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        # Mock container 返回真实的 GroupHandler（但 mock 其依赖）
        with patch("neo.app.container") as mock_container:
            with patch("neo.database.operator.ParquetDBQueryer") as mock_db_class:
                # Mock 数据库操作
                mock_db_instance = Mock()
                mock_db_instance.get_all_symbols.return_value = [
                    "000001.SZ",
                    "000002.SZ",
                ]
                mock_db_class.create_default.return_value = mock_db_instance

                # Mock TaskFilter
                with patch("neo.helpers.group_handler.TaskFilter") as mock_filter_class:
                    mock_filter_instance = Mock()
                    mock_filter_instance.filter_symbols.return_value = [
                        "000001.SZ",
                        "000002.SZ",
                    ]
                    mock_filter_class.return_value = mock_filter_instance

                    # 返回真实的 GroupHandler
                    mock_container.group_handler.return_value = (
                        GroupHandler.create_default()
                    )

                    # Act
                    result = app_service.build_task_stock_mapping_from_group("sys")

                    # Assert
                    expected = {"stock_basic": [""], "trade_cal": [""]}
                    assert result == expected
                    assert result  # 确保不为空

    def test_non_global_group_still_works(self):
        """测试：非全局组仍然正常工作"""
        with patch("neo.database.operator.ParquetDBQueryer") as mock_db_class:
            # Mock 数据库操作
            mock_db_instance = Mock()
            mock_db_instance.get_all_symbols.return_value = ["000001.SZ", "000002.SZ"]
            mock_db_class.create_default.return_value = mock_db_instance

            # Mock TaskFilter
            with patch("neo.helpers.group_handler.TaskFilter") as mock_filter_class:
                mock_filter_instance = Mock()
                mock_filter_instance.filter_symbols.return_value = [
                    "000001.SZ",
                    "000002.SZ",
                ]
                mock_filter_class.return_value = mock_filter_instance

                group_handler = GroupHandler.create_default()

                # 测试 daily 组（需要股票代码的组）
                symbols = group_handler.get_symbols_for_group("daily")
                assert symbols == ["000001.SZ", "000002.SZ"]
                assert len(symbols) > 1 or (len(symbols) == 1 and symbols[0] != "")
