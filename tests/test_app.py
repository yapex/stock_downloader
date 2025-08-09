import pytest
from unittest.mock import patch, MagicMock, mock_open
import logging
from downloader.app import DownloaderApp
from downloader.config import load_config


@pytest.fixture
def mock_logger():
    logger = MagicMock(spec=logging.Logger)
    return logger


@pytest.fixture
def downloader_app(mock_logger):
    return DownloaderApp(logger=mock_logger)


@pytest.fixture
def sample_config():
    return {
        "storage": {
            "db_path": "test_data/stock.db"
        },
        "tasks": {
            "test_daily": {
                "name": "测试日线任务",
                "type": "daily",
                "adjust": "qfq"
            }
        },
        "groups": {
            "default": {
                "description": "测试配置",
                "symbols": ["600519.SH", "000001.SZ"],
                "max_concurrent_tasks": 1,
                "tasks": ["test_daily"]
            }
        }
    }


class TestDownloaderApp:
    def test_init_with_logger(self, mock_logger):
        app = DownloaderApp(logger=mock_logger)
        assert app.logger is mock_logger

    def test_init_without_logger(self):
        app = DownloaderApp()
        assert app.logger is not None

    def test_process_symbols_config_no_symbols(self, downloader_app, sample_config):
        original_config = sample_config.copy()
        result, overridden = downloader_app.process_symbols_config(sample_config, None)
        assert result == original_config
        assert overridden is False

    def test_process_symbols_config_empty_symbols(self, downloader_app, sample_config):
        original_config = sample_config.copy()
        result, overridden = downloader_app.process_symbols_config(sample_config, [])
        assert result == original_config
        assert overridden is False

    def test_process_symbols_config_specific_symbols(self, downloader_app, mock_logger):
        config = {}
        symbols = ["600519.SH", "000001.SZ"]

        result, overridden = downloader_app.process_symbols_config(config, symbols)

        assert result["downloader"]["symbols"] == symbols
        assert overridden is True

    def test_process_symbols_config_all_symbol(self, downloader_app, mock_logger):
        config = {}
        symbols = ["all"]

        result, overridden = downloader_app.process_symbols_config(config, symbols)

        assert result["downloader"]["symbols"] == "all"
        assert overridden is True

    def test_process_symbols_config_all_symbol_case_insensitive(self, downloader_app):
        config = {}
        symbols = ["ALL"]

        result, overridden = downloader_app.process_symbols_config(config, symbols)

        assert result["downloader"]["symbols"] == "all"
        assert overridden is True

    def test_process_symbols_config_existing_downloader_section(self, downloader_app):
        config = {
            "downloader": {
                "existing_key": "existing_value"
            }
        }
        symbols = ["600519.SH"]

        result, overridden = downloader_app.process_symbols_config(config, symbols)

        assert result["downloader"]["symbols"] == symbols
        assert result["downloader"]["existing_key"] == "existing_value"
        assert overridden is True

    def test_create_components(self, downloader_app, sample_config):
        with patch('downloader.app.get_fetcher') as mock_get_fetcher, \
             patch('downloader.app.get_storage') as mock_get_storage:
            mock_get_fetcher.return_value = "fetcher_instance"
            mock_get_storage.return_value = "storage_instance"
            
            fetcher, storage = downloader_app.create_components(sample_config)
            
            assert fetcher == "fetcher_instance"
            assert storage == "storage_instance"
            
            # 验证调用参数
            mock_get_fetcher.assert_called_once_with(use_singleton=True)
            mock_get_storage.assert_called_once_with(db_path=sample_config["storage"]["db_path"])

    def test_create_components_default_storage_path(self, downloader_app):
        config = {}

        with patch('downloader.app.get_fetcher') as mock_get_fetcher, \
             patch('downloader.app.get_storage') as mock_get_storage:

            downloader_app.create_components(config)

            # 验证调用参数
            mock_get_fetcher.assert_called_once_with(use_singleton=True)
            mock_get_storage.assert_called_once_with(db_path="data/stock.db")

    @patch('downloader.app.DownloadEngine')
    @patch('downloader.app.load_config')
    def test_run_download_success(
        self, mock_load_config, mock_engine_class, downloader_app, sample_config
    ):
        mock_load_config.return_value = sample_config
        mock_engine = MagicMock()
        mock_engine_class.return_value = mock_engine

        with patch.object(downloader_app, 'create_components') as mock_create:
            mock_fetcher, mock_storage = MagicMock(), MagicMock()
            mock_create.return_value = (mock_fetcher, mock_storage)

            result = downloader_app.run_download()

            assert result is True

    @patch('downloader.app.load_config')
    def test_run_download_with_custom_params(
        self, mock_load_config, downloader_app, sample_config
    ):
        mock_load_config.return_value = sample_config

        with patch.object(downloader_app, 'create_components') as mock_create, \
             patch('downloader.app.DownloadEngine') as mock_engine_class:

            mock_create.return_value = (MagicMock(), MagicMock())
            mock_engine = MagicMock()
            mock_engine_class.return_value = mock_engine

            downloader_app.run_download(
                config_path="custom_config.yaml",
                symbols=["600519.SH"],
                force=True
            )

    @patch('downloader.app.load_config')
    def test_run_download_file_not_found_error(
        self, mock_load_config, downloader_app, mock_logger
    ):
        mock_load_config.side_effect = FileNotFoundError("配置文件不存在")

        with pytest.raises(FileNotFoundError):
            downloader_app.run_download()

    @patch('downloader.app.load_config')
    def test_run_download_value_error(
        self, mock_load_config, downloader_app, mock_logger
    ):
        mock_load_config.side_effect = ValueError("配置参数无效")

        with pytest.raises(ValueError):
            downloader_app.run_download()

    @patch('downloader.app.load_config')
    def test_run_download_general_exception(
        self, mock_load_config, downloader_app, mock_logger
    ):
        mock_load_config.side_effect = Exception("未知错误")

        with pytest.raises(Exception):
            downloader_app.run_download()

    @patch('downloader.app.load_config')
    def test_run_download_with_symbols_processing(
        self, mock_load_config, downloader_app, sample_config
    ):
        mock_load_config.return_value = sample_config.copy()

        with patch.object(downloader_app, 'create_components') as mock_create, \
             patch('downloader.app.DownloadEngine') as mock_engine_class:

            mock_create.return_value = (MagicMock(), MagicMock())
            mock_engine = MagicMock()
            mock_engine_class.return_value = mock_engine

            symbols = ["300001.SZ"]
            downloader_app.run_download(symbols=symbols)


class TestLoadConfig:
    def test_load_config_success(self):
        yaml_content = """
storage:
  db_path: "data/stock.db"

groups:
  default:
    description: "测试配置"
    symbols:
      - "600519.SH"
      - "000001.SZ"
    max_concurrent_tasks: 1
    tasks: []
        """

        with patch("builtins.open", mock_open(read_data=yaml_content)), \
             patch("pathlib.Path.exists", return_value=True):

            config = load_config("test_config.yaml")

            assert config["groups"]["default"]["symbols"] == ["600519.SH", "000001.SZ"]

    def test_load_config_file_not_found(self):
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError) as exc_info:
                load_config("nonexistent.yaml")

            assert "配置文件 nonexistent.yaml 不存在" in str(exc_info.value)

    def test_load_config_default_path(self):
        with patch("builtins.open", mock_open(read_data="test: value")), \
             patch("pathlib.Path.exists", return_value=True):

            config = load_config()

            assert config["test"] == "value"



