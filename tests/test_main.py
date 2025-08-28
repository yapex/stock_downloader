"""测试 main.py 模块的单元测试"""

from unittest.mock import Mock, patch
from typer.testing import CliRunner
from neo.main import app, main
from neo.helpers.app_service import AppService


class TestDlCommand:
    """测试 dl 命令"""

    def setup_method(self):
        """每个测试方法前的设置"""
        self.runner = CliRunner()

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    @patch("neo.tasks.huey_tasks.build_and_enqueue_downloads_task")
    def test_dl_with_group_parameter(self, mock_task, mock_container, mock_logging):
        """测试带有 group 参数的 dl 命令"""
        # 设置 mock
        mock_container.task_builder.return_value = Mock()
        mock_container.group_handler.return_value = Mock()

        # 执行命令
        result = self.runner.invoke(app, ["dl", "--group", "test_group"])

        # 验证结果
        assert result.exit_code == 0
        mock_logging.assert_called_once_with("download", "info")
        mock_task.assert_called_once_with(group_name="test_group", stock_codes=None)
        assert "正在提交任务组" in result.stdout
        assert "✅ 任务已成功提交到后台处理" in result.stdout

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    @patch("neo.tasks.huey_tasks.build_and_enqueue_downloads_task")
    def test_dl_with_debug_flag(self, mock_task, mock_container, mock_logging):
        """测试带有 debug 标志的 dl 命令"""
        # 设置 mock
        mock_container.task_builder.return_value = Mock()
        mock_container.group_handler.return_value = Mock()

        # 执行命令
        result = self.runner.invoke(app, ["dl", "--debug", "--group", "test_group"])

        # 验证结果
        assert result.exit_code == 0
        mock_logging.assert_called_once_with("download", "debug")
        mock_task.assert_called_once_with(group_name="test_group", stock_codes=None)

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    @patch("neo.tasks.huey_tasks.build_and_enqueue_downloads_task")
    def test_dl_with_symbols_parameter(self, mock_task, mock_container, mock_logging):
        """测试带有 symbols 参数的 dl 命令"""
        # 设置 mock
        mock_container.task_builder.return_value = Mock()
        mock_container.group_handler.return_value = Mock()

        # 执行命令
        result = self.runner.invoke(
            app,
            [
                "dl",
                "--symbols",
                "000001.SZ",
                "--symbols",
                "000002.SZ",
                "--group",
                "test_group",
            ],
        )

        # 验证结果
        assert result.exit_code == 0
        mock_task.assert_called_once_with(
            group_name="test_group", stock_codes=["000001.SZ", "000002.SZ"]
        )

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    @patch("neo.tasks.huey_tasks.build_and_enqueue_downloads_task")
    def test_dl_with_dry_run_flag(self, mock_task, mock_container, mock_logging):
        """测试带有 dry-run 标志的 dl 命令"""
        # 设置 mock
        mock_container.task_builder.return_value = Mock()
        mock_container.group_handler.return_value = Mock()

        # 执行命令
        result = self.runner.invoke(app, ["dl", "--dry-run", "--group", "test_group"])

        # 验证结果
        assert result.exit_code == 0
        mock_task.assert_called_once_with(group_name="test_group", stock_codes=None)

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    @patch("neo.tasks.huey_tasks.build_and_enqueue_downloads_task")
    def test_dl_without_group_parameter(self, mock_task, mock_container, mock_logging):
        """测试不带 group 参数的 dl 命令"""
        # 设置 mock
        mock_container.task_builder.return_value = Mock()
        mock_container.group_handler.return_value = Mock()

        # 执行命令
        result = self.runner.invoke(app, ["dl"])

        # 验证结果
        assert result.exit_code == 0
        mock_task.assert_called_once_with(group_name=None, stock_codes=None)

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    @patch("neo.tasks.huey_tasks.build_and_enqueue_downloads_task")
    def test_dl_task_exception_handling(self, mock_task, mock_container, mock_logging):
        """测试 dl 命令中任务执行异常的处理"""
        # 设置 mock - 让任务抛出异常
        mock_container.task_builder.return_value = Mock()
        mock_container.group_handler.return_value = Mock()
        mock_task.side_effect = Exception("任务执行失败")

        # 执行命令
        result = self.runner.invoke(app, ["dl", "--group", "test_group"])

        # 验证异常被抛出
        assert result.exit_code != 0


class TestDpCommand:
    """测试 dp 命令"""

    def setup_method(self):
        """每个测试方法前的设置"""
        self.runner = CliRunner()

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    def test_dp_with_fast_queue(self, mock_container, mock_logging):
        """测试启动 fast 队列的 dp 命令"""
        # 设置 mock
        mock_app_service = Mock()
        mock_container.app_service.return_value = mock_app_service

        # 执行命令
        result = self.runner.invoke(app, ["dp", "fast"])

        # 验证结果
        assert result.exit_code == 0
        mock_logging.assert_called_once_with("consumer_fast", "info")
        mock_app_service.run_data_processor.assert_called_once_with("fast")

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    def test_dp_with_slow_queue(self, mock_container, mock_logging):
        """测试启动 slow 队列的 dp 命令"""
        # 设置 mock
        mock_app_service = Mock()
        mock_container.app_service.return_value = mock_app_service

        # 执行命令
        result = self.runner.invoke(app, ["dp", "slow"])

        # 验证结果
        assert result.exit_code == 0
        mock_logging.assert_called_once_with("consumer_slow", "info")
        mock_app_service.run_data_processor.assert_called_once_with("slow")

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    def test_dp_with_maint_queue(self, mock_container, mock_logging):
        """测试启动 maint 队列的 dp 命令"""
        # 设置 mock
        mock_app_service = Mock()
        mock_container.app_service.return_value = mock_app_service

        # 执行命令
        result = self.runner.invoke(app, ["dp", "maint"])

        # 验证结果
        assert result.exit_code == 0
        mock_logging.assert_called_once_with("consumer_maint", "info")
        mock_app_service.run_data_processor.assert_called_once_with("maint")

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    def test_dp_with_debug_flag(self, mock_container, mock_logging):
        """测试带有 debug 标志的 dp 命令"""
        # 设置 mock
        mock_app_service = Mock()
        mock_container.app_service.return_value = mock_app_service

        # 执行命令
        result = self.runner.invoke(app, ["dp", "fast", "--debug"])

        # 验证结果
        assert result.exit_code == 0
        mock_logging.assert_called_once_with("consumer_fast", "debug")

    def test_dp_without_queue_name(self):
        """测试不提供队列名称的 dp 命令"""
        # 执行命令
        result = self.runner.invoke(app, ["dp"])

        # 验证结果 - 应该失败，因为 queue_name 是必需参数
        assert result.exit_code != 0

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    def test_dp_app_service_exception_handling(self, mock_container, mock_logging):
        """测试 dp 命令中 app_service 异常的处理"""
        # 设置 mock - 让 app_service 抛出异常
        mock_app_service = Mock()
        mock_app_service.run_data_processor.side_effect = Exception(
            "数据处理器启动失败"
        )
        mock_container.app_service.return_value = mock_app_service

        # 执行命令
        result = self.runner.invoke(app, ["dp", "fast"])

        # 验证异常被抛出
        assert result.exit_code != 0


class TestMainFunction:
    """测试 main 函数"""

    @patch("neo.main.app")
    def test_main_function_calls_app(self, mock_app):
        """测试 main 函数调用 typer app"""
        # 创建 mock app_service
        mock_app_service = Mock()

        # 调用 main 函数
        main(mock_app_service)

        # 验证 app() 被调用
        mock_app.assert_called_once()

    @patch("neo.main.app")
    def test_main_function_with_dependency_injection(self, mock_app):
        """测试 main 函数的依赖注入"""
        # 创建 mock app_service
        mock_app_service = Mock(spec=AppService)

        # 调用 main 函数
        main(mock_app_service)

        # 验证 app() 被调用
        mock_app.assert_called_once()


class TestEdgeCases:
    """测试边界条件和异常情况"""

    def setup_method(self):
        """每个测试方法前的设置"""
        self.runner = CliRunner()

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    @patch("neo.tasks.huey_tasks.build_and_enqueue_downloads_task")
    def test_dl_container_exception(self, mock_task, mock_container, mock_logging):
        """测试容器获取组件时的异常处理"""
        # 设置 mock - 让容器抛出异常
        mock_container.task_builder.side_effect = Exception("容器初始化失败")
        mock_container.group_handler.return_value = Mock()

        # 执行命令
        result = self.runner.invoke(app, ["dl", "--group", "test_group"])

        # 验证异常被抛出
        assert result.exit_code != 0

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    def test_dp_container_exception(self, mock_container, mock_logging):
        """测试 dp 命令容器获取异常"""
        # 设置 mock - 让容器抛出异常
        mock_container.app_service.side_effect = Exception("容器初始化失败")

        # 执行命令
        result = self.runner.invoke(app, ["dp", "fast"])

        # 验证异常被抛出
        assert result.exit_code != 0

    @patch("neo.helpers.utils.setup_logging")
    def test_dl_logging_exception(self, mock_logging):
        """测试日志设置异常"""
        # 设置 mock - 让日志设置抛出异常
        mock_logging.side_effect = Exception("日志初始化失败")

        # 执行命令
        result = self.runner.invoke(app, ["dl", "--group", "test_group"])

        # 验证异常被抛出
        assert result.exit_code != 0

    @patch("neo.helpers.utils.setup_logging")
    def test_dp_logging_exception(self, mock_logging):
        """测试 dp 命令日志设置异常"""
        # 设置 mock - 让日志设置抛出异常
        mock_logging.side_effect = Exception("日志初始化失败")

        # 执行命令
        result = self.runner.invoke(app, ["dp", "fast"])

        # 验证异常被抛出
        assert result.exit_code != 0

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    @patch("neo.tasks.huey_tasks.build_and_enqueue_downloads_task")
    def test_dl_with_empty_group_name(self, mock_task, mock_container, mock_logging):
        """测试空的组名参数"""
        # 设置 mock
        mock_container.task_builder.return_value = Mock()
        mock_container.group_handler.return_value = Mock()

        # 执行命令
        result = self.runner.invoke(app, ["dl", "--group", ""])

        # 验证结果
        assert result.exit_code == 0
        mock_task.assert_called_once_with(group_name="", stock_codes=None)

    @patch("neo.helpers.utils.setup_logging")
    @patch("neo.main.container")
    @patch("neo.tasks.huey_tasks.build_and_enqueue_downloads_task")
    def test_dl_with_multiple_flags(self, mock_task, mock_container, mock_logging):
        """测试同时使用多个标志"""
        # 设置 mock
        mock_container.task_builder.return_value = Mock()
        mock_container.group_handler.return_value = Mock()

        # 执行命令
        result = self.runner.invoke(
            app,
            [
                "dl",
                "--group",
                "test_group",
                "--debug",
                "--dry-run",
                "--symbols",
                "000001.SZ",
                "--symbols",
                "000002.SZ",
            ],
        )

        # 验证结果
        assert result.exit_code == 0
        mock_logging.assert_called_once_with("download", "debug")
        mock_task.assert_called_once_with(
            group_name="test_group", stock_codes=["000001.SZ", "000002.SZ"]
        )

    def test_dp_with_invalid_queue_name(self):
        """测试无效的队列名称"""
        # 注意：typer 不会验证参数值，只会传递给函数
        # 这个测试主要验证命令能正常接收参数
        runner = CliRunner()

        with (
            patch("neo.helpers.utils.setup_logging"),
            patch("neo.main.container") as mock_container,
        ):
            mock_app_service = Mock()
            mock_container.app_service.return_value = mock_app_service

            result = runner.invoke(app, ["dp", "invalid_queue"])

            assert result.exit_code == 0
            mock_app_service.run_data_processor.assert_called_once_with("invalid_queue")


class TestAppConfiguration:
    """测试应用配置"""

    def test_app_help_message(self):
        """测试应用的帮助信息"""
        runner = CliRunner()
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "Neo 股票数据处理系统命令行工具" in result.stdout

    def test_dl_command_help(self):
        """测试 dl 命令的帮助信息"""
        runner = CliRunner()
        result = runner.invoke(app, ["dl", "--help"])

        assert result.exit_code == 0
        assert "下载股票数据" in result.stdout
        assert "--group" in result.stdout
        assert "--symbols" in result.stdout
        assert "--debug" in result.stdout
        assert "--dry-run" in result.stdout

    def test_dp_command_help(self):
        """测试 dp 命令的帮助信息"""
        runner = CliRunner()
        result = runner.invoke(app, ["dp", "--help"])

        assert result.exit_code == 0
        assert "启动指定队列的数据处理器消费者" in result.stdout
        assert "--debug" in result.stdout

    def test_invalid_command(self):
        """测试无效的命令"""
        runner = CliRunner()
        result = runner.invoke(app, ["invalid_command"])

        assert result.exit_code != 0
        # typer 的错误信息可能在 stderr 中，或者格式不同
        # 主要验证命令执行失败即可
        assert result.exit_code == 2  # typer 通常返回 2 表示命令行参数错误
