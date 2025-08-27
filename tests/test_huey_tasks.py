"""Huey 任务测试

测试带 @huey_task 装饰器的下载任务函数和Huey集成功能。
"""

from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import pytest
from huey import MemoryHuey, crontab
from huey.crontab import Crontab
from datetime import time
from pathlib import Path  # Added for Path mocking

# 在导入任何 neo 模块之前先 patch huey_config
pytestmark = pytest.mark.usefixtures("mock_huey_config")

from neo.task_bus.types import TaskType


class TestDownloadTask:
    """测试 download_task 函数"""

    @patch("neo.tasks.huey_tasks.process_data_task")
    def test_download_task_success_and_chains(self, mock_process_data_task):
        """测试下载任务成功，并正确调用后续处理任务"""
        from neo.app import container
        from neo.tasks.huey_tasks import download_task

        mock_downloader = Mock()
        mock_downloader.download.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})

        with container.downloader.override(mock_downloader):
            # 直接调用任务的函数体进行测试
            download_task.func(TaskType.stock_basic, "000001.SZ")

        # 验证下载器被调用
        mock_downloader.download.assert_called_once_with(
            TaskType.stock_basic, "000001.SZ"
        )

        # 验证后续任务被调用
        mock_process_data_task.assert_called_once()

    @patch("neo.tasks.huey_tasks.process_data_task")
    def test_download_task_empty_data_does_not_chain(self, mock_process_data_task):
        """测试下载任务返回空数据时，不调用后续任务"""
        from neo.app import container
        from neo.tasks.huey_tasks import download_task

        mock_downloader = Mock()
        mock_downloader.download.return_value = pd.DataFrame()  # Empty dataframe

        with container.downloader.override(mock_downloader):
            # 直接调用任务的函数体进行测试
            download_task.func(TaskType.stock_basic, "000001.SZ")

        # 验证下载器被调用
        mock_downloader.download.assert_called_once_with(
            TaskType.stock_basic, "000001.SZ"
        )

        # 验证后续任务未被调用
        mock_process_data_task.assert_not_called()

    @patch("neo.tasks.huey_tasks.process_data_task")
    def test_download_task_none_data_does_not_chain(self, mock_process_data_task):
        """测试下载任务返回 None 时，不调用后续任务"""
        from neo.app import container
        from neo.tasks.huey_tasks import download_task

        mock_downloader = Mock()
        mock_downloader.download.return_value = None  # None data

        with container.downloader.override(mock_downloader):
            # 直接调用任务的函数体进行测试
            download_task.func(TaskType.stock_basic, "000001.SZ")

        # 验证下载器被调用
        mock_downloader.download.assert_called_once_with(
            TaskType.stock_basic, "000001.SZ"
        )

        # 验证后续任务未被调用
        mock_process_data_task.assert_not_called()

    @patch("neo.tasks.huey_tasks.process_data_task")
    def test_download_task_exception_handling(self, mock_process_data_task):
        """测试下载任务执行时抛出异常"""
        from neo.app import container
        from neo.tasks.huey_tasks import download_task

        mock_downloader = Mock()
        mock_downloader.download.side_effect = Exception("Download failed")

        with container.downloader.override(mock_downloader):
            with patch("neo.tasks.huey_tasks.logger") as mock_logger:
                with pytest.raises(Exception, match="Download failed"):
                    download_task.func(TaskType.stock_basic, "000001.SZ")

                mock_logger.error.assert_called_once()
                assert "下载任务执行失败" in mock_logger.error.call_args[0][0]

        mock_downloader.download.assert_called_once_with(
            TaskType.stock_basic, "000001.SZ"
        )
        mock_process_data_task.assert_not_called()


class TestProcessDataTask:
    """测试 process_data_task 函数"""

    @patch("neo.app.container.data_processor")
    def test_process_data_task_with_data(self, mock_data_processor_factory):
        """测试当有数据时，处理任务能正确调用下游"""
        from neo.tasks.huey_tasks import process_data_task

        mock_processor = Mock()
        mock_processor.process.return_value = True
        mock_data_processor_factory.return_value = mock_processor

        data = [{"ts_code": "000001.SZ"}]

        # 直接调用任务的函数体进行测试
        result = process_data_task.func(TaskType.stock_basic, "000001.SZ", data)

        mock_processor.process.assert_called_once()
        assert result is True

    @patch("neo.app.container.data_processor")
    def test_process_data_task_with_no_data(self, mock_data_processor_factory):
        """测试当数据为空时，处理任务不调用下游"""
        from neo.tasks.huey_tasks import process_data_task

        mock_processor = Mock()
        mock_data_processor_factory.return_value = mock_processor

        # Test with empty list
        process_data_task.func(TaskType.stock_basic, "000001.SZ", [])
        mock_processor.process.assert_not_called()

        # Test with None
        process_data_task.func(TaskType.stock_basic, "000001.SZ", None)
        mock_processor.process.assert_not_called()

    @patch("neo.app.container.data_processor")
    def test_process_data_task_exception_handling(self, mock_data_processor_factory):
        """测试 process_data_task 内部的异常处理"""
        from neo.tasks.huey_tasks import process_data_task

        mock_processor = Mock()
        mock_processor.process.side_effect = Exception("Processing failed")
        mock_data_processor_factory.return_value = mock_processor

        data = [{"ts_code": "000001.SZ"}]

        with patch("neo.tasks.huey_tasks.logger") as mock_logger:
            with pytest.raises(Exception):
                process_data_task.func(TaskType.stock_basic, "000001.SZ", data)

            mock_logger.error.assert_called_once()
            assert "数据处理任务执行失败" in mock_logger.error.call_args[0][0]


class TestHueyIntegration:
    """测试 Huey 集成功能"""

    def test_huey_fast_queue_configured(self):
        """测试快速队列配置正确"""
        from neo.configs.huey_config import huey_fast

        # 验证队列名称（现在是内存模式）
        assert huey_fast.name == "test_fast"

        # 验证队列可以正常工作
        @huey_fast.task()
        def test_task():
            return "fast_result"

        # 执行任务（immediate=True 会立即执行）
        result = test_task()
        assert result() == "fast_result"

    def test_huey_slow_queue_configured(self):
        """测试慢速队列配置正确"""
        from neo.configs.huey_config import huey_slow

        # 验证队列名称（现在是内存模式）
        assert huey_slow.name == "test_slow"

        # 验证队列可以正常工作
        @huey_slow.task()
        def test_task():
            return "slow_result"

        # 执行任务（immediate=True 会立即执行）
        result = test_task()
        assert result() == "slow_result"


class TestMetadataSyncTask:
    """测试元数据同步任务"""

    @patch("neo.tasks.huey_tasks.get_config")
    def test_get_sync_metadata_crontab(self, mock_get_config):
        """测试 get_sync_metadata_crontab 是否正确解析配置"""
        from neo.tasks.huey_tasks import get_sync_metadata_crontab
        from huey import crontab

        mock_config_instance = Mock()
        mock_config_instance.cron_tasks.sync_metadata_schedule = "0 0 * * *"
        mock_get_config.return_value = mock_config_instance

        cron = get_sync_metadata_crontab()
        assert isinstance(cron, Crontab)
        assert cron.minute == "0"
        assert cron.hour == "0"
        assert cron.day_of_month == "*"
        assert cron.month_of_year == "*"
        assert cron.day_of_week == "*"

    @patch("neo.tasks.huey_tasks.Path")
    @patch("neo.tasks.huey_tasks.duckdb")
    @patch("neo.tasks.huey_tasks.get_config")
    def test_sync_metadata_parquet_path_not_exists(
        self, mock_get_config, mock_duckdb, mock_Path
    ):
        """测试当 parquet_base_path 不存在时，sync_metadata 任务是否跳过"""
        from neo.tasks.huey_tasks import sync_metadata

        mock_config_instance = Mock()
        mock_config_instance.storage.parquet_base_path = "data/parquet"
        mock_config_instance.database.metadata_path = "data/metadata.db"
        mock_config_instance.cron_tasks.sync_metadata_schedule = "0 0 * * *"
        mock_get_config.return_value = mock_config_instance

        mock_path_instance = MagicMock(spec=Path)
        mock_path_instance.is_dir.return_value = False
        mock_path_instance.__truediv__.return_value = MagicMock(
            spec=Path
        )  # Mock for / operator
        mock_Path.return_value = mock_path_instance

        with patch("neo.tasks.huey_tasks.logger") as mock_logger:
            sync_metadata()

            mock_logger.warning.assert_called_once()
            assert "Parquet 根目录" in mock_logger.warning.call_args[0][0]
            mock_duckdb.connect.assert_not_called()

    @patch("neo.tasks.huey_tasks.Path")
    @patch("neo.tasks.huey_tasks.duckdb")
    @patch("neo.tasks.huey_tasks.get_config")
    def test_sync_metadata_no_items_found(
        self, mock_get_config, mock_duckdb, mock_Path
    ):
        """测试当 parquet_base_path 中没有找到任何条目时，sync_metadata 任务是否正确处理"""
        from neo.tasks.huey_tasks import sync_metadata

        mock_config_instance = Mock()
        mock_config_instance.storage.parquet_base_path = "data/parquet"
        mock_config_instance.database.metadata_path = "data/metadata.db"
        mock_config_instance.cron_tasks.sync_metadata_schedule = "0 0 * * *"
        mock_get_config.return_value = mock_config_instance

        mock_path_instance = MagicMock(spec=Path)
        mock_path_instance.is_dir.return_value = True
        mock_path_instance.iterdir.return_value = []  # No items found
        mock_path_instance.__truediv__.return_value = MagicMock(spec=Path)
        mock_Path.return_value = mock_path_instance

        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value.__enter__.return_value = mock_duckdb_conn

        with patch("neo.tasks.huey_tasks.logger") as mock_logger:
            sync_metadata()

            mock_logger.warning.assert_called_once()
            assert "没有找到任何条目" in mock_logger.warning.call_args[0][0]
            mock_duckdb_conn.execute.assert_not_called()

    @patch("neo.tasks.huey_tasks.Path")
    @patch("neo.tasks.huey_tasks.duckdb")
    @patch("neo.tasks.huey_tasks.get_config")
    def test_sync_metadata_success(self, mock_get_config, mock_duckdb, mock_Path):
        """测试 sync_metadata 任务成功执行"""
        from neo.tasks.huey_tasks import sync_metadata

        mock_config_instance = Mock()
        mock_config_instance.storage.parquet_base_path = "data/parquet"
        mock_config_instance.database.metadata_path = "data/metadata.db"
        mock_config_instance.cron_tasks.sync_metadata_schedule = "0 0 * * *"
        mock_get_config.return_value = mock_config_instance

        # Mock Path objects
        mock_parquet_base_path = MagicMock(spec=Path)
        mock_parquet_base_path.is_dir.return_value = True
        mock_table_dir_1 = MagicMock(spec=Path)
        mock_table_dir_1.name = "table1"
        mock_table_dir_1.is_dir.return_value = True
        mock_table_dir_1.__truediv__.return_value = MagicMock(
            spec=Path
        )  # Mock for / operator
        mock_table_dir_2 = MagicMock(spec=Path)
        mock_table_dir_2.name = "table2"
        mock_table_dir_2.is_dir.return_value = True
        mock_table_dir_2.__truediv__.return_value = MagicMock(spec=Path)
        mock_parquet_base_path.iterdir.return_value = [
            mock_table_dir_1,
            mock_table_dir_2,
        ]

        mock_Path.return_value = (
            mock_parquet_base_path  # For project_root and metadata_db_path
        )
        mock_Path.return_value.parents = [
            MagicMock(spec=Path),
            MagicMock(spec=Path),
            MagicMock(spec=Path),
            mock_parquet_base_path,
        ]  # Mock parents for project_root
        mock_Path.return_value.resolve.return_value = mock_parquet_base_path

        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value.__enter__.return_value = mock_duckdb_conn

        with patch("neo.tasks.huey_tasks.logger") as mock_logger:
            sync_metadata()

            mock_duckdb.connect.assert_called_once_with(str(mock_Path.return_value))
            assert mock_duckdb_conn.execute.call_count == 2
            mock_logger.info.assert_called()
            assert "元数据同步任务成功完成" in mock_logger.info.call_args_list[-1][0][0]

    @patch("neo.tasks.huey_tasks.Path")
    @patch("neo.tasks.huey_tasks.duckdb")
    @patch("neo.tasks.huey_tasks.get_config")
    def test_sync_metadata_exception_handling(
        self, mock_get_config, mock_duckdb, mock_Path
    ):
        """测试 sync_metadata 任务的异常处理"""
        from neo.tasks.huey_tasks import sync_metadata

        mock_config_instance = Mock()
        mock_config_instance.storage.parquet_base_path = "data/parquet"
        mock_config_instance.database.metadata_path = "data/metadata.db"
        mock_config_instance.cron_tasks.sync_metadata_schedule = "0 0 * * *"
        mock_get_config.return_value = mock_config_instance

        mock_path_instance = MagicMock(spec=Path)
        mock_path_instance.is_dir.return_value = True
        mock_path_instance.iterdir.side_effect = Exception("Test iterdir error")
        mock_path_instance.__truediv__.return_value = MagicMock(spec=Path)
        mock_Path.return_value = mock_path_instance
        mock_Path.return_value.parents = [
            MagicMock(),
            MagicMock(),
            MagicMock(),
            mock_path_instance,
        ]  # Mock parents for project_root
        mock_Path.return_value.resolve.return_value = mock_path_instance

        mock_duckdb_conn = Mock()
        mock_duckdb.connect.return_value.__enter__.return_value = mock_duckdb_conn

        with patch("neo.tasks.huey_tasks.logger") as mock_logger:
            with pytest.raises(Exception, match="Test iterdir error"):
                sync_metadata()

            mock_logger.error.assert_called_once()
            assert "元数据同步任务失败" in mock_logger.error.call_args[0][0]


class TestBuildAndEnqueueTask:
    """测试 build_and_enqueue_downloads_task 任务"""

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")  # Added patch for SchemaLoader
    @patch("neo.app.container.group_handler")
    @patch("neo.app.container.db_queryer")  # 使用新的 db_queryer
    @patch("neo.app.container.config")
    def test_build_and_enqueue_logic(
        self,
        mock_config,
        mock_db_queryer,
        mock_group_handler,
        mock_parquet_operator,
        mock_schema_loader,  # Added mock_schema_loader
        mock_download_task,
    ):
        """测试构建和派发任务的核心逻辑"""
        from neo.tasks.huey_tasks import build_and_enqueue_downloads_task
        from neo.helpers.utils import get_next_day_str

        # 1. 配置 Mocks
        # a. GroupHandler 返回任务类型和股票代码
        mock_group_handler.return_value.get_task_types_for_group.return_value = [
            "stock_daily"
        ]
        mock_group_handler.return_value.get_symbols_for_group.return_value = [
            "000001.SZ",
            "000002.SZ",
        ]

        # b. DBQueryer 返回一个股票的最新日期
        mock_db_queryer.return_value.get_max_date.return_value = {
            "000001.SZ": "20240110"
        }

        # c. ParquetDBQueryer 返回最新日期
        mock_parquet_operator.return_value.get_max_date.return_value = {
            "000001.SZ": "20240110"
        }

        # d. Config 返回默认起始日期和存储路径
        mock_config.return_value.download_tasks.default_start_date = "19900101"
        mock_config.return_value.storage.parquet_base_path = "/tmp/test_parquet"

        # 2. 执行任务
        build_and_enqueue_downloads_task.func("all_stocks", stock_codes=None)

        # 3. 验证
        # a. 验证 GroupHandler 被正确调用
        mock_group_handler.return_value.get_task_types_for_group.assert_called_once_with(
            "all_stocks"
        )
        mock_group_handler.return_value.get_symbols_for_group.assert_called_once_with(
            "all_stocks"
        )

        # b. 验证 ParquetDBQueryer 被正确调用
        # 注意：现在 ParquetDBQueryer 需要 schema_loader 和 parquet_base_path 参数
        mock_parquet_operator.assert_called_once_with(
            schema_loader=mock_schema_loader.return_value,  # Assert SchemaLoader is passed
            parquet_base_path=mock_config.return_value.storage.parquet_base_path,
        )
        mock_parquet_operator.return_value.get_max_date.assert_called_once_with(
            "stock_daily", ["000001.SZ", "000002.SZ"]
        )

        # c. 验证 download_task 的派发
        assert mock_download_task.call_count == 2
        calls = mock_download_task.call_args_list

        # 验证第一个任务（已有数据）
        call_1_args = calls[0][1]  # kwargs
        assert call_1_args["task_type"] == "stock_daily"
        assert call_1_args["symbol"] == "000001.SZ"
        assert call_1_args["start_date"] == get_next_day_str("20240110")  # 20240111

        # 验证第二个任务（无历史数据）
        call_2_args = calls[1][1]  # kwargs
        assert call_2_args["task_type"] == "stock_daily"
        assert call_2_args["symbol"] == "000002.SZ"
        assert call_2_args["start_date"] == "19900101"  # 使用默认起始日期

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")  # Added patch for SchemaLoader
    @patch("neo.app.container.group_handler")
    @patch("neo.app.container.db_queryer")
    @patch("neo.app.container.config")
    def test_build_and_enqueue_with_specific_stock_codes(
        self,
        mock_config,
        mock_db_queryer,
        mock_group_handler,
        mock_parquet_operator,
        mock_schema_loader,  # Added mock_schema_loader
        mock_download_task,
    ):
        """测试使用指定股票代码的构建和派发任务逻辑"""
        from neo.tasks.huey_tasks import build_and_enqueue_downloads_task
        from neo.helpers.utils import get_next_day_str

        # 1. 配置 Mocks
        # a. GroupHandler 返回任务类型（但股票代码会被忽略）
        mock_group_handler.return_value.get_task_types_for_group.return_value = [
            "stock_daily"
        ]
        mock_group_handler.return_value.get_symbols_for_group.return_value = [
            "000001.SZ",
            "000002.SZ",
        ]  # 这个会被忽略

        # b. ParquetDBQueryer 返回最新日期
        mock_parquet_operator.return_value.get_max_date.return_value = {
            "600519.SH": "20240110"
        }

        # c. Config 返回默认起始日期和存储路径
        mock_config.return_value.download_tasks.default_start_date = "19900101"
        mock_config.return_value.storage.parquet_base_path = "/tmp/test_parquet"

        # 2. 执行任务，指定特定股票代码
        build_and_enqueue_downloads_task.func("all_stocks", stock_codes=["600519.SH"])

        # 3. 验证
        # a. 验证 GroupHandler 被正确调用
        mock_group_handler.return_value.get_task_types_for_group.assert_called_once_with(
            "all_stocks"
        )
        # 注意：get_symbols_for_group 不应该被调用，因为使用了指定的股票代码
        mock_group_handler.return_value.get_symbols_for_group.assert_not_called()

        # b. 验证 ParquetDBQueryer 被正确调用，使用指定的股票代码
        mock_parquet_operator.assert_called_once_with(
            schema_loader=mock_schema_loader.return_value,  # Assert SchemaLoader is passed
            parquet_base_path=mock_config.return_value.storage.parquet_base_path,
        )
        mock_parquet_operator.return_value.get_max_date.assert_called_once_with(
            "stock_daily", ["600519.SH"]
        )

        # c. 验证只派发了一个任务（指定的股票）
        assert mock_download_task.call_count == 1
        call_args = mock_download_task.call_args[1]  # kwargs
        assert call_args["task_type"] == "stock_daily"
        assert call_args["symbol"] == "600519.SH"
        assert call_args["start_date"] == get_next_day_str("20240110")  # 20240111

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")
    @patch("neo.app.container")
    @patch("neo.configs.app_config.get_config")
    @patch("neo.tasks.huey_tasks.datetime")
    def test_build_and_enqueue_skip_today_before_market_close(
        self,
        mock_datetime,
        mock_config,
        mock_container,
        mock_schema_loader,
        mock_parquet_operator,
        mock_download_task,
    ):
        """测试在收盘前跳过今日数据下载的逻辑"""
        from neo.tasks.huey_tasks import build_and_enqueue_downloads_task
        from datetime import datetime, time

        # 1. 配置时间 Mock - 模拟当前时间为今天下午3点
        today_str = "20240115"  # 修改为YYYYMMDD格式
        mock_now = datetime(2024, 1, 15, 15, 0)  # 使用真实的datetime对象
        mock_datetime.now.return_value = mock_now

        # 2. 配置容器 Mocks
        mock_group_handler = Mock()
        mock_group_handler.get_task_types_for_group.return_value = ["stock_daily"]
        mock_group_handler.get_symbols_for_group.return_value = ["600519.SH"]
        mock_container.group_handler.return_value = mock_group_handler
        mock_container.db_queryer.return_value = Mock()

        # 3. 配置 ParquetDBQueryer Mock
        mock_parquet_instance = Mock()
        mock_parquet_instance.get_max_date.return_value = {"600519.SH": today_str}
        mock_parquet_operator.return_value = mock_parquet_instance

        # 4. 配置 Config Mock
        mock_config_instance = Mock()
        mock_config_instance.download_tasks.default_start_date = "19900101"
        mock_config_instance.storage.parquet_base_path = "/tmp/test_parquet"
        mock_config.return_value = mock_config_instance

        # 5. 执行任务
        build_and_enqueue_downloads_task.func("test_group")

        # 6. 验证没有派发任务（因为当前时间未到收盘时间）
        mock_parquet_operator.assert_called_once_with(
            schema_loader=mock_schema_loader.return_value,
            parquet_base_path=mock_config.return_value.storage.parquet_base_path,
        )
        assert mock_download_task.call_count == 0

        # 6. 验证没有派发任务（因为当前时间未到收盘时间）
        assert mock_download_task.call_count == 0

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")
    @patch("neo.app.container")
    @patch("neo.configs.app_config.get_config")
    @patch("neo.tasks.huey_tasks.datetime")
    def test_build_and_enqueue_allow_today_after_market_close(
        self,
        mock_datetime,
        mock_config,
        mock_container,
        mock_schema_loader,
        mock_parquet_operator,
        mock_download_task,
    ):
        """测试在收盘后允许下载今日数据的逻辑"""
        from neo.tasks.huey_tasks import build_and_enqueue_downloads_task
        from neo.helpers.utils import get_next_day_str
        from datetime import datetime, time

        # 1. 配置时间 Mock - 模拟当前时间为今天晚上7点
        today_str = "20240115"  # 修改为YYYYMMDD格式
        mock_now = datetime(2024, 1, 15, 19, 0)  # 使用真实的datetime对象
        mock_datetime.now.return_value = mock_now

        # 2. 配置容器 Mocks
        mock_group_handler = Mock()
        mock_group_handler.get_task_types_for_group.return_value = ["stock_daily"]
        mock_group_handler.get_symbols_for_group.return_value = ["600519.SH"]
        mock_container.group_handler.return_value = mock_group_handler
        mock_container.db_queryer.return_value = Mock()

        # 3. 配置 ParquetDBQueryer Mock
        mock_parquet_instance = Mock()
        mock_parquet_instance.get_max_date.return_value = {"600519.SH": today_str}
        mock_parquet_operator.return_value = mock_parquet_instance

        # 4. 配置 Config Mock
        mock_config_instance = Mock()
        mock_config_instance.download_tasks.default_start_date = "19900101"
        mock_config_instance.storage.parquet_base_path = "/tmp/test_parquet"
        mock_config.return_value = mock_config_instance

        # 5. 执行任务
        build_and_enqueue_downloads_task.func("test_group")

        # 6. 验证派发了任务（因为当前时间已过收盘时间）
        mock_parquet_operator.assert_called_once_with(
            schema_loader=mock_schema_loader.return_value,
            parquet_base_path=mock_config.return_value.storage.parquet_base_path,
        )
        assert mock_download_task.call_count == 1
        call_args = mock_download_task.call_args[1]
        assert call_args["task_type"] == "stock_daily"
        assert call_args["symbol"] == "600519.SH"
        assert call_args["start_date"] == get_next_day_str(today_str)

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")
    @patch("neo.app.container")
    @patch("neo.configs.app_config.get_config")
    @patch("neo.tasks.huey_tasks.datetime")
    def test_build_and_enqueue_exception_handling(
        self,
        mock_datetime,
        mock_config,
        mock_container,
        mock_schema_loader,
        mock_parquet_operator,
        mock_download_task,
    ):
        """测试 build_and_enqueue_downloads_task 的异常处理"""
        from neo.tasks.huey_tasks import build_and_enqueue_downloads_task

        mock_container.group_handler.return_value.get_task_types_for_group.side_effect = Exception(
            "Test Exception"
        )

        with patch("neo.tasks.huey_tasks.logger") as mock_logger:
            with pytest.raises(Exception, match="Test Exception"):
                build_and_enqueue_downloads_task.func("test_group")

            mock_logger.error.assert_called_once()
            assert "构建下载任务失败" in mock_logger.error.call_args[0][0]

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")  # Added patch for SchemaLoader
    @patch("neo.app.container.group_handler")
    @patch("neo.app.container.db_queryer")
    @patch("neo.app.container.config")
    def test_build_and_enqueue_no_task_types(
        self,
        mock_config,
        mock_db_queryer,
        mock_group_handler,
        mock_parquet_operator,
        mock_schema_loader,  # Added mock_schema_loader
        mock_download_task,
    ):
        """测试当任务组没有任务类型时，任务是否正确结束"""
        from neo.tasks.huey_tasks import build_and_enqueue_downloads_task

        mock_group_handler.return_value.get_task_types_for_group.return_value = []
        mock_group_handler.return_value.get_symbols_for_group.return_value = [
            "000001.SZ"
        ]

        with patch("neo.tasks.huey_tasks.logger") as mock_logger:
            build_and_enqueue_downloads_task.func("empty_group")

            mock_group_handler.return_value.get_task_types_for_group.assert_called_once_with(
                "empty_group"
            )
            mock_logger.warning.assert_called_once_with(
                "任务组 'empty_group' 中没有找到任何任务类型，任务结束。"
            )
            mock_download_task.assert_not_called()

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")  # Added patch for SchemaLoader
    @patch("neo.app.container.group_handler")
    @patch("neo.app.container.db_queryer")
    @patch("neo.app.container.config")
    def test_build_and_enqueue_stock_basic_task(
        self,
        mock_config,
        mock_db_queryer,
        mock_group_handler,
        mock_parquet_operator,
        mock_schema_loader,  # Added mock_schema_loader
        mock_download_task,
    ):
        """测试构建和派发 stock_basic 类型的任务（不需要股票代码）"""
        from neo.tasks.huey_tasks import build_and_enqueue_downloads_task

        mock_group_handler.return_value.get_task_types_for_group.return_value = [
            "stock_basic"
        ]
        mock_group_handler.return_value.get_symbols_for_group.return_value = []

        mock_parquet_operator.return_value.get_max_date.return_value = {}

        mock_config.return_value.download_tasks.default_start_date = "19900101"
        mock_config.return_value.storage.parquet_base_path = "/tmp/test_parquet"

        build_and_enqueue_downloads_task.func("basic_data")

        mock_group_handler.return_value.get_task_types_for_group.assert_called_once_with(
            "basic_data"
        )
        mock_group_handler.return_value.get_symbols_for_group.assert_called_once_with(
            "basic_data"
        )
        mock_parquet_operator.assert_called_once_with(
            schema_loader=mock_schema_loader.return_value,
            parquet_base_path=mock_config.return_value.storage.parquet_base_path,
        )
        mock_parquet_operator.return_value.get_max_date.assert_called_once_with(
            "stock_basic", []
        )
        mock_download_task.assert_called_once_with(
            task_type="stock_basic", symbol="", start_date="19900101"
        )

    @patch("neo.tasks.huey_tasks.download_task")
    @patch("neo.database.operator.ParquetDBQueryer")
    @patch("neo.database.schema_loader.SchemaLoader")
    @patch("neo.app.container")
    @patch("neo.configs.app_config.get_config")
    @patch("neo.tasks.huey_tasks.datetime")
    def test_build_and_enqueue_skip_yesterday_before_market_close(
        self,
        mock_datetime,
        mock_config,
        mock_container,
        mock_schema_loader,
        mock_parquet_operator,
        mock_download_task,
    ):
        """测试在收盘前跳过昨日数据下载的逻辑"""
        from neo.tasks.huey_tasks import build_and_enqueue_downloads_task
        from datetime import datetime, time, timedelta

        # 1. 配置时间 Mock - 模拟当前时间为今天下午3点
        today_str = (datetime.now()).strftime("%Y%m%d")
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        mock_now = datetime(2024, 1, 16, 15, 0)  # 使用真实的datetime对象
        mock_datetime.now.return_value = mock_now

        # 2. 配置容器 Mocks
        mock_group_handler = Mock()
        mock_group_handler.get_task_types_for_group.return_value = ["stock_daily"]
        mock_group_handler.get_symbols_for_group.return_value = ["600519.SH"]
        mock_container.group_handler.return_value = mock_group_handler
        mock_container.db_queryer.return_value = Mock()

        # 3. 配置 ParquetDBQueryer Mock
        mock_parquet_instance = Mock()
        mock_parquet_instance.get_max_date.return_value = {"600519.SH": yesterday_str}
        mock_parquet_operator.return_value = mock_parquet_instance

        # 4. 配置 Config Mock
        mock_config_instance = Mock()
        mock_config_instance.download_tasks.default_start_date = "19900101"
        mock_config_instance.storage.parquet_base_path = "/tmp/test_parquet"
        mock_config.return_value = mock_config_instance

        # 5. 执行任务
        build_and_enqueue_downloads_task.func("test_group")

        # 6. 验证没有派发任务（因为当前时间未到收盘时间）
        mock_parquet_operator.assert_called_once_with(
            schema_loader=mock_schema_loader.return_value,
            parquet_base_path=mock_config.return_value.storage.parquet_base_path,
        )
        assert mock_download_task.call_count == 0
