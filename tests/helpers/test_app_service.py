"""测试 AppService Facade (纯单元测试)"""

from unittest.mock import Mock, patch

# TaskType 和 DownloadTaskConfig 已被移除，现在使用字符串表示任务类型
# from neo.task_bus.types import DownloadTaskConfig, TaskType


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
        # 使用字典替代 DownloadTaskConfig，因为它已被移除
        tasks = [{"task_type": "stock_basic", "symbol": "000001"}]
        app_service.run_downloader(tasks, dry_run=True)

        # 4. 断言 Mock 对象的方法被正确调用
        mock_downloader_service.run.assert_called_once()
        args, kwargs = mock_downloader_service.run.call_args
        assert args[0] == tasks
        assert args[1] is True

    @patch("neo.helpers.app_service.build_and_enqueue_downloads_task")
    def test_build_and_submit_downloads(self, mock_build_and_enqueue):
        """测试 build_and_submit_downloads 是否正确调用 build_and_enqueue_downloads_task"""
        from neo.helpers.app_service import AppService

        # 1. 创建依赖的 Mock 对象
        mock_consumer_runner = Mock()
        mock_downloader_service = Mock()

        # 2. 实例化被测对象，并注入 Mock 依赖
        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        # 3. 准备测试数据
        task_stock_mapping = {
            "stock_basic": ["000001.SZ", "000002.SZ"],
            "daily": ["000001.SZ"],
        }

        # 4. 调用被测方法
        app_service.build_and_submit_downloads(task_stock_mapping)

        # 5. 断言 Mock 对象的方法被正确调用
        mock_build_and_enqueue.assert_called_once_with(task_stock_mapping)

    @patch("neo.app.container")
    def test_build_task_stock_mapping_from_group(self, mock_container):
        """测试 build_task_stock_mapping_from_group 是否正确构建任务映射"""
        from neo.helpers.app_service import AppService

        # 1. 创建依赖的 Mock 对象
        mock_consumer_runner = Mock()
        mock_downloader_service = Mock()

        # 2. 设置 container mock
        mock_group_handler = Mock()
        mock_group_handler.get_task_types_for_group.return_value = [
            "stock_basic",
            "daily",
        ]
        mock_group_handler.get_symbols_for_group.return_value = [
            "000001.SZ",
            "000002.SZ",
        ]
        mock_container.group_handler.return_value = mock_group_handler

        # 3. 实例化被测对象，并注入 Mock 依赖
        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        # 4. 调用被测方法
        result = app_service.build_task_stock_mapping_from_group("all_stocks")

        # 5. 断言结果
        expected_mapping = {
            "stock_basic": ["000001.SZ", "000002.SZ"],
            "daily": ["000001.SZ", "000002.SZ"],
        }
        assert result == expected_mapping

        # 6. 断言 Mock 对象的方法被正确调用
        mock_container.group_handler.assert_called_once()
        mock_group_handler.get_task_types_for_group.assert_called_once_with(
            "all_stocks"
        )
        mock_group_handler.get_symbols_for_group.assert_called_once_with("all_stocks")

    @patch("neo.app.container")
    def test_build_task_stock_mapping_from_group_with_stock_codes(self, mock_container):
        """测试 build_task_stock_mapping_from_group 使用指定股票代码的情况"""
        from neo.helpers.app_service import AppService

        # 1. 创建依赖的 Mock 对象
        mock_consumer_runner = Mock()
        mock_downloader_service = Mock()

        # 2. 设置 container mock
        mock_group_handler = Mock()
        mock_group_handler.get_task_types_for_group.return_value = [
            "stock_basic",
            "daily",
        ]
        mock_container.group_handler.return_value = mock_group_handler

        # 3. 实例化被测对象，并注入 Mock 依赖
        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        # 4. 调用被测方法，指定股票代码
        stock_codes = ["600519.SH"]
        result = app_service.build_task_stock_mapping_from_group(
            "all_stocks", stock_codes
        )

        # 5. 断言结果
        expected_mapping = {"stock_basic": ["600519.SH"], "daily": ["600519.SH"]}
        assert result == expected_mapping

        # 6. 断言 Mock 对象的方法被正确调用
        mock_container.group_handler.assert_called_once()
        mock_group_handler.get_task_types_for_group.assert_called_once_with(
            "all_stocks"
        )
        # get_symbols_for_group 不应该被调用，因为使用了指定的股票代码
        mock_group_handler.get_symbols_for_group.assert_not_called()

    @patch("neo.app.container")
    def test_build_task_stock_mapping_from_group_sys_group_should_work(
        self, mock_container
    ):
        """测试 sys 组（包含 stock_basic 和 trade_cal）应该正常工作"""
        from neo.helpers.app_service import AppService

        # 1. 创建依赖的 Mock 对象
        mock_consumer_runner = Mock()
        mock_downloader_service = Mock()

        # 2. 设置 container mock - 模拟 sys 组的修复后行为
        mock_group_handler = Mock()
        mock_group_handler.get_task_types_for_group.return_value = [
            "stock_basic",
            "trade_cal",  # sys 组包含这两个任务
        ]
        # 模拟 GroupHandler 的修复后逻辑：对于全局任务返回 [""] 作为占位符
        mock_group_handler.get_symbols_for_group.return_value = [""]
        mock_container.group_handler.return_value = mock_group_handler

        # 3. 实例化被测对象
        app_service = AppService(
            consumer_runner=mock_consumer_runner,
            downloader_service=mock_downloader_service,
        )

        # 4. 调用被测方法
        result = app_service.build_task_stock_mapping_from_group("sys")

        # 5. 断言结果 - 修复后应该返回正确的映射
        expected_mapping = {
            "stock_basic": [""],  # 不需要股票代码的任务使用空字符串
            "trade_cal": [""],  # 不需要股票代码的任务使用空字符串
        }
        assert result == expected_mapping

        # 6. 断言 Mock 对象的方法被正确调用
        mock_container.group_handler.assert_called_once()
        mock_group_handler.get_task_types_for_group.assert_called_once_with("sys")
        mock_group_handler.get_symbols_for_group.assert_called_once_with("sys")
