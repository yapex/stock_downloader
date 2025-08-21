"""测试 downloader_app.py 模块"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from typer.testing import CliRunner

from downloader.downloader_app import app, download_command
from downloader.producer.downloader_manager import DownloaderManager
from downloader.producer.tushare_downloader import TushareDownloader


class TestDownloaderApp:
    """测试下载器应用"""

    def setup_method(self):
        """设置测试环境"""
        self.runner = CliRunner()

    @patch('downloader.downloader_app.DownloaderManager.create_from_config')
    @patch('downloader.downloader_app.TushareDownloader')
    def test_download_command_success(self, mock_tushare, mock_create_manager):
        """测试成功执行下载命令"""
        # 模拟管理器和统计信息
        mock_stats = MagicMock()
        mock_stats.total_tasks = 10
        mock_stats.successful_tasks = 8
        mock_stats.failed_tasks = 2
        mock_stats.success_rate = 0.8
        
        mock_manager = MagicMock()
        mock_manager.download_group.return_value = mock_stats
        mock_create_manager.return_value = mock_manager
        
        # 执行命令
        result = self.runner.invoke(app, [
            "--group", "test",
            "--symbols", "000001,600519"
        ])
        
        # 验证结果
        assert result.exit_code == 0
        assert "任务执行完成" in result.stdout
        assert "总任务数: 10" in result.stdout
        assert "成功任务数: 8" in result.stdout
        assert "失败任务数: 2" in result.stdout
        assert "成功率: 80.00%" in result.stdout
        
        # 验证调用
        mock_create_manager.assert_called_once()
        mock_manager.download_group.assert_called_once_with(
            group="test",
            symbols=["000001", "600519"],
            max_retries=3
        )

    @patch('downloader.downloader_app.DownloaderManager.create_from_config')
    @patch('downloader.downloader_app.TushareDownloader')
    def test_download_command_with_config_file(self, mock_tushare, mock_create_manager):
        """测试使用配置文件的下载命令"""
        # 创建临时配置文件
        config_file = Path("test_config.toml")
        config_file.write_text("[test]\nkey = 'value'")
        
        try:
            mock_stats = MagicMock()
            mock_stats.total_tasks = 5
            mock_stats.successful_tasks = 5
            mock_stats.failed_tasks = 0
            mock_stats.success_rate = 1.0
            
            mock_manager = MagicMock()
            mock_manager.download_group.return_value = mock_stats
            mock_create_manager.return_value = mock_manager
            
            # 执行命令
            result = self.runner.invoke(app, [
                "--group", "test",
                "--config", str(config_file)
            ])
            
            # 验证结果
            assert result.exit_code == 0
            assert "任务执行完成" in result.stdout
            
            # 验证配置文件路径被传递
            mock_create_manager.assert_called_once()
            call_args = mock_create_manager.call_args
            assert call_args.kwargs['config_path'] == config_file
            
        finally:
            # 清理临时文件
            if config_file.exists():
                config_file.unlink()

    def test_download_command_config_file_not_exists(self):
        """测试配置文件不存在的情况"""
        result = self.runner.invoke(app, [
            "--group", "test",
            "--config", "nonexistent_config.toml"
        ])
        
        assert result.exit_code == 1
        # 检查是否有异常被抛出
        assert isinstance(result.exception, SystemExit)

    def test_download_command_dry_run(self):
        """测试 dry-run 模式"""
        result = self.runner.invoke(app, [
            "--group", "test",
            "--symbols", "000001",
            "--dry-run"
        ])
        
        assert result.exit_code == 0
        assert "参数验证成功，dry-run 模式不启动下载器" in result.stdout

    def test_download_command_dry_run_with_config(self):
        """测试带配置文件的 dry-run 模式"""
        # 创建临时配置文件
        config_file = Path("test_config.toml")
        config_file.write_text("[test]\nkey = 'value'")
        
        try:
            result = self.runner.invoke(app, [
                "--group", "test",
                "--config", str(config_file),
                "--dry-run"
            ])
            
            assert result.exit_code == 0
            assert "参数验证成功，dry-run 模式不启动下载器" in result.stdout
            assert f"将使用配置文件: {config_file}" in result.stdout
            
        finally:
            # 清理临时文件
            if config_file.exists():
                config_file.unlink()

    def test_download_command_symbols_parsing(self):
        """测试股票代码解析"""
        with patch('downloader.downloader_app.DownloaderManager.create_from_config') as mock_create, \
             patch('downloader.downloader_app.TushareDownloader'):
            
            mock_stats = MagicMock()
            mock_stats.total_tasks = 1
            mock_stats.successful_tasks = 1
            mock_stats.failed_tasks = 0
            mock_stats.success_rate = 1.0
            
            mock_manager = MagicMock()
            mock_manager.download_group.return_value = mock_stats
            mock_create.return_value = mock_manager
            
            # 测试带空格的股票代码
            result = self.runner.invoke(app, [
                "--group", "test",
                "--symbols", " 000001 , 600519 , 000002 "
            ])
            
            assert result.exit_code == 0
            
            # 验证股票代码被正确解析
            mock_manager.download_group.assert_called_once_with(
                group="test",
                symbols=["000001", "600519", "000002"],
                max_retries=3
            )

    def test_download_command_no_symbols(self):
        """测试不提供股票代码的情况"""
        with patch('downloader.downloader_app.DownloaderManager.create_from_config') as mock_create, \
             patch('downloader.downloader_app.TushareDownloader'):
            
            mock_stats = MagicMock()
            mock_stats.total_tasks = 1
            mock_stats.successful_tasks = 1
            mock_stats.failed_tasks = 0
            mock_stats.success_rate = 1.0
            
            mock_manager = MagicMock()
            mock_manager.download_group.return_value = mock_stats
            mock_create.return_value = mock_manager
            
            result = self.runner.invoke(app, [
                "--group", "test"
            ])
            
            assert result.exit_code == 0
            
            # 验证 symbols 参数为 None
            mock_manager.download_group.assert_called_once_with(
                group="test",
                symbols=None,
                max_retries=3
            )

    @patch('downloader.downloader_app.DownloaderManager.create_from_config')
    @patch('downloader.downloader_app.TushareDownloader')
    def test_download_command_value_error(self, mock_tushare, mock_create_manager):
        """测试 ValueError 异常处理"""
        mock_create_manager.side_effect = ValueError("测试错误")
        
        result = self.runner.invoke(app, [
            "--group", "test"
        ])
        
        assert result.exit_code == 1
        # 检查是否有异常被抛出
        assert isinstance(result.exception, SystemExit)

    @patch('downloader.downloader_app.DownloaderManager.create_from_config')
    @patch('downloader.downloader_app.TushareDownloader')
    def test_download_command_unexpected_error(self, mock_tushare, mock_create_manager):
        """测试意外异常处理"""
        mock_create_manager.side_effect = RuntimeError("意外错误")
        
        result = self.runner.invoke(app, [
            "--group", "test"
        ])
        
        assert result.exit_code == 1
        # 检查是否有异常被抛出
        assert isinstance(result.exception, SystemExit)

    def test_download_command_empty_symbols(self):
        """测试空的股票代码字符串"""
        with patch('downloader.downloader_app.DownloaderManager.create_from_config') as mock_create, \
             patch('downloader.downloader_app.TushareDownloader'):
            
            mock_stats = MagicMock()
            mock_stats.total_tasks = 1
            mock_stats.successful_tasks = 1
            mock_stats.failed_tasks = 0
            mock_stats.success_rate = 1.0
            
            mock_manager = MagicMock()
            mock_manager.download_group.return_value = mock_stats
            mock_create.return_value = mock_manager
            
            # 测试空字符串和只有逗号的情况
            result = self.runner.invoke(app, [
                "--group", "test",
                "--symbols", " , , "
            ])
            
            assert result.exit_code == 0
            
            # 验证空的股票代码列表被过滤
            mock_manager.download_group.assert_called_once_with(
                group="test",
                symbols=[],
                max_retries=3
            )

    def test_app_creation(self):
        """测试应用创建"""
        # 测试应用对象存在
        from downloader.downloader_app import app
        assert app is not None
        # Typer 应用有 registered_commands 属性
        assert hasattr(app, 'registered_commands')
        
    def test_logger_creation(self):
        """测试日志记录器创建"""
        from downloader.downloader_app import logger
        assert logger is not None
        assert logger.name == 'downloader.downloader_app'
        
    def test_main_execution(self):
        """测试主函数执行路径"""
        # 测试 __name__ == "__main__" 的代码路径
        with patch('downloader.downloader_app.app') as mock_app:
            # 模拟主函数执行
            exec("if __name__ == '__main__': app()")
            # 由于条件不满足，app() 不会被调用