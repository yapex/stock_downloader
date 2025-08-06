import pytest
from unittest.mock import patch, MagicMock, mock_open
from typer.testing import CliRunner
from downloader.main import app, setup_logging, TqdmLoggingHandler


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def sample_config_content():
    return """
downloader:
  symbols:
    - "600519.SH"
    - "000001.SZ"
  tasks:
    daily:
      enabled: true
storage:
  base_path: "test_data"
"""


class TestCLIIntegration:
    """CLI 集成测试"""

    def test_cli_help_output(self, cli_runner):
        """测试CLI帮助信息输出"""
        result = cli_runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "基于 Tushare Pro" in result.stdout

    @patch('downloader.main.setup_logging')
    @patch('downloader.main.DownloaderApp')
    def test_cli_with_default_config(
        self, mock_app_class, mock_setup_logging, cli_runner, sample_config_content
    ):
        """测试使用默认配置文件的CLI调用"""
        mock_app = MagicMock()
        mock_app_class.return_value = mock_app
        mock_app.run_download.return_value = True

        with patch("builtins.open", mock_open(read_data=sample_config_content)), \
             patch("pathlib.Path.exists", return_value=True):

            result = cli_runner.invoke(app, [])
            
            assert result.exit_code == 0
            mock_setup_logging.assert_called_once()
            mock_app.run_download.assert_called_once_with(
                config_path="config.yaml", 
                group_name="default",
                symbols=None, 
                force=False
            )

    @patch('downloader.main.setup_logging')
    @patch('downloader.main.DownloaderApp')
    def test_cli_with_custom_config(
        self, mock_app_class, mock_setup_logging, cli_runner, sample_config_content
    ):
        """测试使用自定义配置文件的CLI调用"""
        mock_app = MagicMock()
        mock_app_class.return_value = mock_app
        mock_app.run_download.return_value = True

        with patch("builtins.open", mock_open(read_data=sample_config_content)), \
             patch("pathlib.Path.exists", return_value=True):

            result = cli_runner.invoke(app, ["--config", "custom.yaml"])
            
            assert result.exit_code == 0
            mock_app.run_download.assert_called_once_with(
                config_path="custom.yaml", 
                group_name="default",
                symbols=None, 
                force=False
            )

    @patch('downloader.main.setup_logging')
    @patch('downloader.main.DownloaderApp')
    def test_cli_with_symbols(
        self, mock_app_class, mock_setup_logging, cli_runner, sample_config_content
    ):
        """测试指定股票符号的CLI调用"""
        mock_app = MagicMock()
        mock_app_class.return_value = mock_app
        mock_app.run_download.return_value = True

        with patch("builtins.open", mock_open(read_data=sample_config_content)), \
             patch("pathlib.Path.exists", return_value=True):

            result = cli_runner.invoke(app, ["600519.SH", "000001.SZ"])
            
            assert result.exit_code == 0
            mock_app.run_download.assert_called_once_with(
                config_path="config.yaml", 
                group_name="default",
                symbols=["600519.SH", "000001.SZ"], 
                force=False
            )

    @patch('downloader.main.setup_logging')
    @patch('downloader.main.DownloaderApp')
    def test_cli_with_all_symbols(
        self, mock_app_class, mock_setup_logging, cli_runner, sample_config_content
    ):
        """测试下载所有股票的CLI调用"""
        mock_app = MagicMock()
        mock_app_class.return_value = mock_app
        mock_app.run_download.return_value = True

        with patch("builtins.open", mock_open(read_data=sample_config_content)), \
             patch("pathlib.Path.exists", return_value=True):

            result = cli_runner.invoke(app, ["all"])
            
            assert result.exit_code == 0
            mock_app.run_download.assert_called_once_with(
                config_path="config.yaml", 
                group_name="default",
                symbols=["all"], 
                force=False
            )

    @patch('downloader.main.setup_logging')
    @patch('downloader.main.DownloaderApp')
    def test_cli_with_force_flag(
        self, mock_app_class, mock_setup_logging, cli_runner, sample_config_content
    ):
        """测试强制执行标志的CLI调用"""
        mock_app = MagicMock()
        mock_app_class.return_value = mock_app
        mock_app.run_download.return_value = True

        with patch("builtins.open", mock_open(read_data=sample_config_content)), \
             patch("pathlib.Path.exists", return_value=True):

            result = cli_runner.invoke(app, ["--force"])
            
            assert result.exit_code == 0
            mock_app.run_download.assert_called_once_with(
                config_path="config.yaml", 
                group_name="default",
                symbols=None, 
                force=True
            )

    @patch('downloader.main.setup_logging')
    @patch('downloader.main.DownloaderApp')
    def test_cli_with_combined_options(
        self, mock_app_class, mock_setup_logging, cli_runner, sample_config_content
    ):
        """测试组合选项的CLI调用"""
        mock_app = MagicMock()
        mock_app_class.return_value = mock_app
        mock_app.run_download.return_value = True

        with patch("builtins.open", mock_open(read_data=sample_config_content)), \
             patch("pathlib.Path.exists", return_value=True):

            result = cli_runner.invoke(app, [
                "--config", "custom.yaml", 
                "--force", 
                "600519.SH"
            ])
            
            assert result.exit_code == 0
            mock_app.run_download.assert_called_once_with(
                config_path="custom.yaml", 
                group_name="default",
                symbols=["600519.SH"], 
                force=True
            )

    @patch('downloader.main.setup_logging')
    @patch('downloader.main.DownloaderApp')
    def test_cli_file_not_found_error(
        self, mock_app_class, mock_setup_logging, cli_runner
    ):
        """测试配置文件不存在时的错误处理"""
        mock_app = MagicMock()
        mock_app_class.return_value = mock_app
        mock_app.run_download.side_effect = FileNotFoundError("配置文件不存在")

        result = cli_runner.invoke(app, [])
        
        # CLI 应该正常退出，错误会被记录在日志中
        assert result.exit_code == 0
        mock_app.run_download.assert_called_once()

    @patch('downloader.main.setup_logging')
    @patch('downloader.main.DownloaderApp')
    def test_cli_general_exception(
        self, mock_app_class, mock_setup_logging, cli_runner, sample_config_content
    ):
        """测试一般异常的错误处理"""
        mock_app = MagicMock()
        mock_app_class.return_value = mock_app
        mock_app.run_download.side_effect = Exception("未知错误")

        with patch("builtins.open", mock_open(read_data=sample_config_content)), \
             patch("pathlib.Path.exists", return_value=True):

            result = cli_runner.invoke(app, [])
            
            assert result.exit_code == 0
            mock_app.run_download.assert_called_once()


class TestSetupLogging:
    """测试日志设置功能"""

    def test_setup_logging_creates_handlers(self):
        """测试setup_logging是否正确创建处理器"""
        with patch('logging.FileHandler') as mock_file_handler, \
             patch('logging.getLogger') as mock_get_logger:

            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_logger.handlers = []

            setup_logging()

            # 验证文件处理器被创建
            mock_file_handler.assert_called_once()
            
            # 验证日志级别被设置
            mock_logger.setLevel.assert_called_with(20)  # logging.INFO = 20
            
            # 验证处理器被添加
            assert mock_logger.addHandler.call_count == 2

    def test_setup_logging_clears_existing_handlers(self):
        """测试setup_logging是否清理现有处理器"""
        with patch('logging.FileHandler'), \
             patch('logging.getLogger') as mock_get_logger:

            mock_logger = MagicMock()
            mock_handler = MagicMock()
            mock_logger.handlers = [mock_handler]
            mock_get_logger.return_value = mock_logger

            setup_logging()

            # 验证现有处理器被移除
            mock_logger.removeHandler.assert_called_once_with(mock_handler)


class TestTqdmLoggingHandler:
    """测试与tqdm兼容的日志处理器"""

    def test_tqdm_handler_initialization(self):
        """测试TqdmLoggingHandler初始化"""
        handler = TqdmLoggingHandler()
        assert handler is not None

    @patch('downloader.main.tqdm.write')
    def test_tqdm_handler_emit(self, mock_tqdm_write):
        """测试TqdmLoggingHandler的emit方法"""
        handler = TqdmLoggingHandler()
        
        # 创建一个模拟的日志记录
        import logging
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # 设置一个简单的格式器
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        
        handler.emit(record)
        
        # 验证tqdm.write被调用
        mock_tqdm_write.assert_called_once()
        args, kwargs = mock_tqdm_write.call_args
        assert "Test message" in args[0]

    @patch('downloader.main.tqdm.write')
    def test_tqdm_handler_emit_exception_handling(self, mock_tqdm_write):
        """测试TqdmLoggingHandler异常处理"""
        mock_tqdm_write.side_effect = Exception("Write error")
        
        handler = TqdmLoggingHandler()
        
        with patch.object(handler, 'handleError') as mock_handle_error:
            import logging
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="test.py",
                lineno=1,
                msg="Test message",
                args=(),
                exc_info=None
            )
            
            handler.emit(record)
            
            # 验证错误处理被调用
            mock_handle_error.assert_called_once_with(record)
