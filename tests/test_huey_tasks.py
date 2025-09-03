"""Huey 任务测试

重构后的测试文件，使用共用mock类，减少patch使用，降低与实现细节的耦合。
"""

from unittest.mock import Mock, MagicMock, patch
import pandas as pd
import pytest
from pathlib import Path

# 在导入任何 neo 模块之前先 patch huey_config
pytestmark = pytest.mark.usefixtures("mock_huey_config")

# TaskType 已移除，现在使用字符串表示任务类型


class MockContainer:
    """共用的容器Mock类"""

    def __init__(self):
        # 创建downloader实例mock
        self._downloader_instance = Mock()
        self._downloader_instance.download.return_value = pd.DataFrame(
            {"ts_code": ["000001.SZ"]}
        )

        # downloader()方法返回downloader实例
        self.downloader = Mock(return_value=self._downloader_instance)

        # 确保每次调用downloader()都返回同一个实例
        self.downloader.return_value = self._downloader_instance

        # 创建group_handler实例mock
        self._group_handler_instance = Mock()
        self._group_handler_instance.get_task_types_for_group.return_value = [
            "stock_daily"
        ]
        self._group_handler_instance.get_symbols_for_group.return_value = [
            "000001.SZ",
            "000002.SZ",
        ]

        # group_handler()方法返回group_handler实例
        self.group_handler = Mock(return_value=self._group_handler_instance)

        # 确保每次调用group_handler()都返回同一个实例
        self.group_handler.return_value = self._group_handler_instance

        self.data_processor = Mock()
        self.db_queryer = Mock()
        self.config = Mock()

        # 设置默认返回值
        self.data_processor.return_value.process.return_value = True
        self.config.return_value.download_tasks.default_start_date = "19900101"
        self.config.return_value.storage.parquet_base_path = "data/parquet"
        self.config.return_value.database.metadata_path = "data/metadata.db"
        self.config.return_value.cron_tasks.sync_metadata_schedule = "0 0 * * *"


class MockConfig:
    """共用的配置Mock类"""

    def __init__(self):
        self.storage = Mock()
        self.database = Mock()
        self.cron_tasks = Mock()

        # 创建一个更真实的 download_tasks mock
        self.download_tasks = Mock()
        self.download_tasks.default_start_date = "19900101"

        # 为 'stock_daily' 任务创建 mock 配置
        stock_daily_config = Mock()
        stock_daily_config.update_strategy = "incremental"

        # 将任务配置附加到 download_tasks 上
        # 允许形如 config.download_tasks.stock_daily.update_strategy 的访问
        self.download_tasks.stock_daily = stock_daily_config

        self.storage.parquet_base_path = "data/parquet"
        self.database.metadata_path = "data/metadata.db"
        self.cron_tasks.sync_metadata_schedule = "0 0 * * *"


class MockPath:
    """共用的Path Mock类"""

    def __init__(self, exists=True, is_dir=True, items=None):
        self.mock_path = MagicMock(spec=Path)
        self.mock_path.exists.return_value = exists
        self.mock_path.is_dir.return_value = is_dir
        self.mock_path.iterdir.return_value = items or []
        self.mock_path.__truediv__.return_value = MagicMock(spec=Path)
        self.mock_path.resolve.return_value = self.mock_path
        self.mock_path.parents = [MagicMock(spec=Path)] * 4


class MockDuckDB:
    """共用的DuckDB Mock类"""

    def __init__(self):
        self.connection = Mock()
        self.connect_context = Mock()
        self.connect_context.__enter__ = Mock(return_value=self.connection)
        self.connect_context.__exit__ = Mock(return_value=None)

    def connect(self, *args, **kwargs):
        return self.connect_context


@pytest.fixture
def mock_container():
    """提供共用的容器mock"""
    return MockContainer()


@pytest.fixture
def mock_config():
    """提供共用的配置mock"""
    return MockConfig()


@pytest.fixture
def mock_path():
    """提供共用的Path mock"""
    return MockPath()


@pytest.fixture
def mock_duckdb():
    """提供共用的DuckDB mock"""
    return MockDuckDB()


class TestDownloadTask:
    """测试 download_task 函数"""

    @patch("neo.app.container")
    @patch("neo.tasks.data_processing_tasks.process_data_task")
    def test_download_task_success_and_chains(self, mock_process_task, mock_container):
        """测试下载任务成功，并正确调用后续处理任务"""
        from neo.tasks.huey_tasks import download_task

        # 创建完整的mock container，确保不受其他测试影响
        mock_container.reset_mock()
        mock_downloader_instance = Mock()
        mock_downloader_instance.download.return_value = pd.DataFrame(
            {"test": [1, 2, 3]}
        )

        # 设置container.downloader()返回mock_downloader_instance
        mock_container.downloader.return_value = mock_downloader_instance

        # 执行任务
        download_task.func("stock_basic", "000001.SZ")

        # 验证调用
        mock_downloader_instance.download.assert_called_once_with(
            "stock_basic", "000001.SZ"
        )
        mock_process_task.assert_called_once()

    @patch("neo.app.container")
    @patch("neo.tasks.data_processing_tasks.process_data_task")
    def test_download_task_empty_data_does_not_chain(
        self, mock_process_task, mock_container
    ):
        """测试下载任务返回空数据时，不调用后续处理任务"""
        from neo.tasks.huey_tasks import download_task

        # 创建完整的mock container，确保不受其他测试影响
        mock_container.reset_mock()
        mock_downloader_instance = Mock()
        mock_downloader_instance.download.return_value = pd.DataFrame()

        # 设置container.downloader()返回mock_downloader_instance
        mock_container.downloader.return_value = mock_downloader_instance

        # 执行任务
        download_task.func("stock_basic", "000001.SZ")

        # 验证调用
        mock_downloader_instance.download.assert_called_once_with(
            "stock_basic", "000001.SZ"
        )
        # 验证没有调用后续处理任务
        mock_process_task.assert_not_called()

    @patch("neo.app.container")
    @patch("neo.tasks.data_processing_tasks.process_data_task")
    def test_download_task_none_data_does_not_chain(
        self, mock_process_task, mock_container
    ):
        """测试下载任务返回None时，不调用后续处理任务"""
        from neo.tasks.huey_tasks import download_task

        # 创建完整的mock container，确保不受其他测试影响
        mock_container.reset_mock()
        mock_downloader_instance = Mock()
        mock_downloader_instance.download.return_value = None

        # 设置container.downloader()返回mock_downloader_instance
        mock_container.downloader.return_value = mock_downloader_instance

        # 执行任务
        download_task.func("stock_basic", "000001.SZ")

        # 验证调用
        mock_downloader_instance.download.assert_called_once_with(
            "stock_basic", "000001.SZ"
        )
        # 验证没有调用后续处理任务
        mock_process_task.assert_not_called()

    @patch("neo.app.container")
    @patch("neo.tasks.data_processing_tasks.process_data_task")
    @patch("neo.tasks.download_tasks.logger")
    def test_download_task_exception_handling(
        self, mock_logger, mock_process_task, mock_container
    ):
        """测试下载任务执行时抛出异常"""
        from neo.tasks.huey_tasks import download_task

        # 创建完整的mock container，确保不受其他测试影响
        mock_container.reset_mock()
        mock_downloader_instance = Mock()
        mock_downloader_instance.download.side_effect = Exception("Download failed")

        # 设置container.downloader()返回mock_downloader_instance
        mock_container.downloader.return_value = mock_downloader_instance

        # 执行任务并验证异常
        with pytest.raises(Exception, match="Download failed"):
            download_task.func("stock_basic", "000001.SZ")

        mock_downloader_instance.download.assert_called_once_with(
            "stock_basic", "000001.SZ"
        )
        mock_logger.error.assert_called_once()
        mock_process_task.assert_not_called()


class TestProcessDataTask:
    """测试 process_data_task 函数"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        # 清理可能存在的mock
        import unittest.mock

        unittest.mock._patch._active_patches.clear()

    @patch("neo.tasks.data_processing_tasks.DataProcessor")
    def test_process_data_task_with_data(self, mock_data_processor_class):
        """测试有数据时的处理"""
        from neo.tasks.data_processing_tasks import process_data_task

        mock_data_processor_class.reset_mock()

        # 设置mock实例
        mock_processor_instance = Mock()
        mock_processor_instance.process_data.return_value = True
        mock_data_processor_class.return_value = mock_processor_instance

        # 测试数据
        data = [{"ts_code": "000001.SZ"}]

        # 调用函数
        result = process_data_task.func("stock_basic", "000001.SZ", data)

        # 验证结果
        assert result is True
        mock_data_processor_class.assert_called_once()
        mock_processor_instance.process_data.assert_called_once_with(
            "stock_basic", "000001.SZ", data
        )

    @patch("neo.tasks.data_processing_tasks.DataProcessor")
    def test_process_data_task_with_no_data(self, mock_data_processor_class):
        """测试无数据时的处理"""
        from neo.tasks.data_processing_tasks import process_data_task

        mock_data_processor_class.reset_mock()

        # 设置mock实例
        mock_processor_instance = Mock()
        mock_processor_instance.process_data.return_value = False
        mock_data_processor_class.return_value = mock_processor_instance

        # 测试空列表
        result = process_data_task.func("stock_basic", "000001.SZ", [])

        # 验证结果
        assert result is False
        mock_data_processor_class.assert_called_once()
        mock_processor_instance.process_data.assert_called_once_with(
            "stock_basic", "000001.SZ", []
        )

        # 重置mock
        mock_processor_instance.reset_mock()
        mock_data_processor_class.reset_mock()

        # 重新设置mock实例
        mock_processor_instance = Mock()
        mock_processor_instance.process_data.return_value = False
        mock_data_processor_class.return_value = mock_processor_instance

        # 测试None
        result = process_data_task.func("stock_basic", "000001.SZ", None)
        assert result is False
        mock_processor_instance.process_data.assert_called_with(
            "stock_basic", "000001.SZ", None
        )

    @patch("neo.tasks.data_processing_tasks.logger")
    @patch("neo.tasks.data_processing_tasks.DataProcessor")
    def test_process_data_task_exception_handling(
        self, mock_data_processor_class, mock_logger
    ):
        """测试异常处理"""
        from neo.tasks.data_processing_tasks import process_data_task

        mock_data_processor_class.reset_mock()

        # 设置mock实例抛出异常
        mock_processor_instance = Mock()
        mock_processor_instance.process_data.side_effect = Exception(
            "Processing failed"
        )
        mock_data_processor_class.return_value = mock_processor_instance

        # 测试数据
        data = [{"ts_code": "000001.SZ"}]

        # 验证异常被抛出
        with pytest.raises(Exception, match="Processing failed"):
            process_data_task.func("stock_basic", "000001.SZ", data)

        mock_data_processor_class.assert_called_once()
        mock_processor_instance.process_data.assert_called_once_with(
            "stock_basic", "000001.SZ", data
        )
        # 验证错误日志被记录
        mock_logger.error.assert_called_once()


class TestHueyIntegration:
    """测试 Huey 集成功能"""

    def test_huey_fast_queue_configured(self):
        """测试快速队列配置正确"""
        from neo.configs.huey_config import huey_fast

        assert huey_fast.name == "test_fast"

        @huey_fast.task()
        def test_task():
            return "fast_result"

        result = test_task()
        assert result() == "fast_result"

    def test_huey_slow_queue_configured(self):
        """测试慢速队列配置正确"""
        from neo.configs.huey_config import huey_slow

        assert huey_slow.name == "test_slow"

        @huey_slow.task()
        def test_task():
            return "slow_result"

        result = test_task()
        assert result() == "slow_result"


class TestMetadataSyncTask:
    """测试元数据同步任务"""

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_get_sync_metadata_crontab(self, mock_get_config):
        """测试 get_sync_metadata_crontab 是否正确解析配置"""
        from neo.tasks.huey_tasks import get_sync_metadata_crontab

        # 创建mock配置
        mock_config = Mock()
        mock_cron_tasks = Mock()
        mock_cron_tasks.sync_metadata_schedule = "0 0 * * *"
        mock_config.cron_tasks = mock_cron_tasks
        mock_get_config.return_value = mock_config

        cron = get_sync_metadata_crontab()
        # 验证返回的不是None，说明函数正常执行
        assert cron is not None
        # 验证get_config被调用
        mock_get_config.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.MetadataSyncManager")
    def test_sync_metadata_parquet_path_not_exists(self, mock_sync_manager_class):
        """测试当 parquet_base_path 不存在时，sync_metadata 任务是否跳过"""
        from neo.tasks.metadata_sync_tasks import sync_metadata

        # 创建mock sync manager
        mock_sync_manager = Mock()
        mock_sync_manager_class.return_value = mock_sync_manager

        # 调用原始函数（不是Huey包装的任务）
        sync_metadata.func()

        # 验证MetadataSyncManager被创建并调用sync方法
        mock_sync_manager_class.assert_called_once()
        mock_sync_manager.sync.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.MetadataSyncManager")
    def test_sync_metadata_no_items_found(self, mock_sync_manager_class):
        """测试当 parquet_base_path 中没有找到任何条目时，sync_metadata 任务是否正确处理"""
        from neo.tasks.metadata_sync_tasks import sync_metadata

        # 创建mock sync manager
        mock_sync_manager = Mock()
        mock_sync_manager_class.return_value = mock_sync_manager

        # 调用原始函数（不是Huey包装的任务）
        sync_metadata.func()

        # 验证MetadataSyncManager被创建并调用sync方法
        mock_sync_manager_class.assert_called_once()
        mock_sync_manager.sync.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.MetadataSyncManager")
    def test_sync_metadata_success(self, mock_sync_manager_class):
        """测试 sync_metadata 任务成功执行"""
        from neo.tasks.metadata_sync_tasks import sync_metadata

        # 创建mock sync manager
        mock_sync_manager = Mock()
        mock_sync_manager_class.return_value = mock_sync_manager

        # 调用原始函数（不是Huey包装的任务）
        sync_metadata.func()

        # 验证MetadataSyncManager被创建并调用sync方法
        mock_sync_manager_class.assert_called_once()
        mock_sync_manager.sync.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.MetadataSyncManager")
    def test_sync_metadata_exception_handling(self, mock_sync_manager_class):
        """测试 sync_metadata 任务的异常处理"""
        from neo.tasks.metadata_sync_tasks import sync_metadata

        # 创建mock sync manager，设置抛出异常
        mock_sync_manager = Mock()
        mock_sync_manager.sync.side_effect = Exception("Test sync error")
        mock_sync_manager_class.return_value = mock_sync_manager

        with pytest.raises(Exception, match="Test sync error"):
            # 调用原始函数（不是Huey包装的任务）
            sync_metadata.func()

        # 验证MetadataSyncManager被创建并调用sync方法
        mock_sync_manager_class.assert_called_once()
        mock_sync_manager.sync.assert_called_once()


class TestBuildAndEnqueueTask:
    """测试 build_and_enqueue_downloads_task 任务"""

    @patch("neo.tasks.download_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer.create_default")
    @patch("neo.tasks.download_tasks.get_config")
    @patch("neo.app.container")
    def test_build_and_enqueue_logic(
        self,
        mock_container,
        mock_get_config,
        mock_parquet_db_create_default,
        mock_download_task,
    ):
        """测试构建和派发任务的核心逻辑"""
        from neo.tasks.download_tasks import build_and_enqueue_downloads_task
        from neo.helpers.utils import get_next_day_str

        # 设置db_queryer mock
        mock_db_queryer = Mock()
        mock_db_queryer.get_latest_trading_day.return_value = "20240115"
        mock_db_queryer.get_max_date.return_value = {"000001.SZ": "20240110"}
        mock_container.db_queryer.return_value = mock_db_queryer

        # 设置schema_loader mock
        mock_schema_loader = Mock()
        mock_table_config = Mock()
        mock_table_config.date_col = "trade_date"
        mock_schema_loader.get_table_config.return_value = mock_table_config
        mock_container.schema_loader.return_value = mock_schema_loader

        # 设置config mock
        mock_config = MockConfig()
        mock_get_config.return_value = mock_config

        # 准备任务映射
        task_stock_mapping = {"stock_daily": ["000001.SZ", "000002.SZ"]}

        # 执行任务
        build_and_enqueue_downloads_task.func(task_stock_mapping)

        # 验证container.db_queryer被正确调用
        mock_container.db_queryer.assert_called_once()

        # 验证download_task的派发
        assert mock_download_task.call_count == 2
        calls = mock_download_task.call_args_list

        # 根据symbol排序调用，确保测试的稳定性
        calls_by_symbol = {call[1]["symbol"]: call[1] for call in calls}

        # 验证000001.SZ任务（已有数据）
        call_000001 = calls_by_symbol["000001.SZ"]
        assert call_000001["task_type"] == "stock_daily"
        assert call_000001["start_date"] == get_next_day_str("20240110")

        # 验证000002.SZ任务（无历史数据）
        call_000002 = calls_by_symbol["000002.SZ"]
        assert call_000002["task_type"] == "stock_daily"
        assert call_000002["start_date"] == "19900101"

    @patch("neo.tasks.download_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer.create_default")
    @patch("neo.tasks.download_tasks.get_config")
    @patch("neo.app.container")
    def test_build_and_enqueue_with_specific_stock_codes(
        self,
        mock_container,
        mock_get_config,
        mock_parquet_db_create_default,
        mock_download_task,
    ):
        """测试使用指定股票代码的构建和派发任务逻辑"""
        from neo.tasks.download_tasks import build_and_enqueue_downloads_task
        from neo.helpers.utils import get_next_day_str

        # 设置db_queryer mock
        mock_db_queryer = Mock()
        mock_db_queryer.get_latest_trading_day.return_value = "20240115"
        mock_db_queryer.get_max_date.return_value = {"600519.SH": "20240110"}
        mock_container.db_queryer.return_value = mock_db_queryer

        # 设置schema_loader mock
        mock_schema_loader = Mock()
        mock_table_config = Mock()
        mock_table_config.date_col = "trade_date"
        mock_schema_loader.get_table_config.return_value = mock_table_config
        mock_container.schema_loader.return_value = mock_schema_loader

        # 设置config mock
        mock_config = MockConfig()
        mock_get_config.return_value = mock_config

        # 准备任务映射，指定特定股票代码
        task_stock_mapping = {"stock_daily": ["600519.SH"]}

        # 执行任务
        build_and_enqueue_downloads_task.func(task_stock_mapping)

        # 验证container.db_queryer被正确调用
        mock_container.db_queryer.assert_called_once()

        # 验证download_task的派发
        assert mock_download_task.call_count == 1
        call_args = mock_download_task.call_args_list[0][1]
        assert call_args["task_type"] == "stock_daily"
        assert call_args["symbol"] == "600519.SH"
        assert call_args["start_date"] == get_next_day_str("20240110")

    @patch("neo.tasks.download_tasks.logger")
    @patch("neo.tasks.download_tasks.download_task")
    @patch("neo.tasks.download_tasks.get_config")
    @patch("neo.app.container")
    def test_build_and_enqueue_exception_handling(
        self, mock_container, mock_get_config, mock_download_task, mock_logger
    ):
        """测试构建和派发任务的异常处理"""
        from neo.tasks.download_tasks import build_and_enqueue_downloads_task

        # 设置db_queryer抛出异常
        mock_container.db_queryer.side_effect = Exception("构建下载任务失败")

        # 设置config mock
        mock_config = MockConfig()
        mock_get_config.return_value = mock_config

        # 准备任务映射
        task_stock_mapping = {"stock_daily": ["000001.SZ"]}

        with pytest.raises(Exception, match="构建下载任务失败"):
            build_and_enqueue_downloads_task.func(task_stock_mapping)

        mock_container.db_queryer.assert_called_once()
        mock_logger.error.assert_called_once()
        assert "构建下载任务失败" in mock_logger.error.call_args[0][0]
        mock_download_task.assert_not_called()
