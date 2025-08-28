"""元数据同步任务测试模块

全面测试 metadata_sync_tasks.py 中的所有功能，包括 MetadataSyncManager 类和相关函数。
"""

import pytest
from unittest.mock import Mock, patch

from neo.tasks.metadata_sync_tasks import (
    MetadataSyncManager,
    sync_metadata,
    get_sync_metadata_crontab,
)


class TestMetadataSyncManager:
    """测试 MetadataSyncManager 类"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        self.manager = MetadataSyncManager()

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_init(self, mock_get_config):
        """测试 MetadataSyncManager 初始化"""
        mock_config = Mock()
        mock_get_config.return_value = mock_config

        manager = MetadataSyncManager()

        assert manager.config == mock_config
        assert manager._project_root is None
        assert manager._parquet_base_path is None
        assert manager._metadata_db_path is None
        mock_get_config.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    @patch("neo.tasks.metadata_sync_tasks.Path")
    def test_get_project_paths_first_call(self, mock_path_class, mock_get_config):
        """测试首次调用 _get_project_paths 方法"""
        # 设置配置mock
        mock_config = Mock()
        mock_config.storage.parquet_base_path = "data/parquet"
        mock_config.database.metadata_path = "data/metadata.db"
        mock_get_config.return_value = mock_config

        # 设置Path mock
        mock_file_path = Mock()
        mock_project_root = Mock()
        mock_file_path.resolve.return_value.parents = [
            Mock(),
            Mock(),
            Mock(),
            mock_project_root,
        ]

        # Mock Path(__file__)
        mock_path_class.return_value = mock_file_path

        # Mock Path / 操作
        mock_parquet_path = Mock()
        mock_metadata_path = Mock()
        mock_project_root.__truediv__ = Mock(
            side_effect=[mock_parquet_path, mock_metadata_path]
        )

        manager = MetadataSyncManager()

        # 调用方法
        project_root, parquet_base_path, metadata_db_path = manager._get_project_paths()

        # 验证结果
        assert project_root == mock_project_root
        assert manager._project_root == mock_project_root
        assert manager._parquet_base_path == mock_parquet_path
        assert manager._metadata_db_path == mock_metadata_path

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_get_project_paths_cached(self, mock_get_config):
        """测试 _get_project_paths 方法的缓存功能"""
        mock_config = Mock()
        mock_get_config.return_value = mock_config

        manager = MetadataSyncManager()

        # 设置缓存值
        mock_project_root = Mock()
        mock_parquet_base_path = Mock()
        mock_metadata_db_path = Mock()

        manager._project_root = mock_project_root
        manager._parquet_base_path = mock_parquet_base_path
        manager._metadata_db_path = mock_metadata_db_path

        # 调用方法
        project_root, parquet_base_path, metadata_db_path = manager._get_project_paths()

        # 验证返回缓存值
        assert project_root == mock_project_root
        assert parquet_base_path == mock_parquet_base_path
        assert metadata_db_path == mock_metadata_db_path

    def test_validate_parquet_directory_exists(self):
        """测试验证存在的 Parquet 目录"""
        mock_path = Mock()
        mock_path.is_dir.return_value = True

        result = self.manager._validate_parquet_directory(mock_path)

        assert result is True
        mock_path.is_dir.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.logger")
    def test_validate_parquet_directory_not_exists(self, mock_logger):
        """测试验证不存在的 Parquet 目录"""
        mock_path = Mock()
        mock_path.is_dir.return_value = False

        result = self.manager._validate_parquet_directory(mock_path)

        assert result is False
        mock_path.is_dir.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.duckdb")
    @patch("neo.tasks.metadata_sync_tasks.logger")
    def test_setup_duckdb_connection(self, mock_logger, mock_duckdb):
        """测试设置 DuckDB 连接"""
        mock_connection = Mock()
        mock_duckdb.connect.return_value = mock_connection
        mock_path = Mock()

        result = self.manager._setup_duckdb_connection(mock_path)

        assert result == mock_connection
        mock_duckdb.connect.assert_called_once_with(str(mock_path))

        # 验证内存限制设置
        from unittest.mock import call

        expected_calls = [call("SET memory_limit='2GB'"), call("SET max_memory='2GB'")]
        mock_connection.execute.assert_has_calls(expected_calls)

    def test_scan_parquet_directories_empty(self):
        """测试扫描空的 Parquet 目录"""
        mock_path = Mock()
        mock_path.iterdir.return_value = []

        result = self.manager._scan_parquet_directories(mock_path)

        assert result == []
        mock_path.iterdir.assert_called_once()

    def test_scan_parquet_directories_with_files_and_dirs(self):
        """测试扫描包含文件和目录的 Parquet 目录"""
        # 创建mock文件和目录
        mock_file = Mock()
        mock_file.is_dir.return_value = False
        mock_file.name = "file.txt"

        mock_dir1 = Mock()
        mock_dir1.is_dir.return_value = True
        mock_dir1.name = "table1"

        mock_dir2 = Mock()
        mock_dir2.is_dir.return_value = True
        mock_dir2.name = "table2"

        mock_path = Mock()
        mock_path.iterdir.return_value = [mock_file, mock_dir1, mock_dir2]

        result = self.manager._scan_parquet_directories(mock_path)

        assert len(result) == 2
        assert mock_dir1 in result
        assert mock_dir2 in result
        assert mock_file not in result

        mock_path.iterdir.assert_called_once()

    def test_drop_existing_table_or_view_success(self):
        """测试成功删除已存在的表或视图"""
        mock_connection = Mock()
        table_name = "test_table"

        self.manager._drop_existing_table_or_view(mock_connection, table_name)

        from unittest.mock import call

        expected_calls = [
            call(f"DROP TABLE IF EXISTS {table_name}"),
            call(f"DROP VIEW IF EXISTS {table_name}"),
        ]
        mock_connection.execute.assert_has_calls(expected_calls)

    @patch("neo.tasks.metadata_sync_tasks.logger")
    def test_drop_existing_table_or_view_exception(self, mock_logger):
        """测试删除表或视图时发生异常"""
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Drop failed")
        table_name = "test_table"

        # 不应该抛出异常
        self.manager._drop_existing_table_or_view(mock_connection, table_name)

    def test_create_metadata_view(self):
        """测试创建元数据视图"""
        mock_connection = Mock()
        table_name = "test_table"
        mock_table_dir = Mock()
        valid_files = ["/path/to/file1.parquet", "/path/to/file2.parquet"]

        with patch.object(
            self.manager, "_validate_parquet_files", return_value=valid_files
        ):
            self.manager._create_metadata_view(
                mock_connection, table_name, mock_table_dir
            )

            # 验证SQL执行
            mock_connection.execute.assert_called_once()
            actual_call = mock_connection.execute.call_args[0][0]

            # 验证SQL内容包含预期的部分
            assert f"CREATE OR REPLACE VIEW {table_name}" in actual_call
            assert "read_parquet" in actual_call

    def test_create_metadata_view_handles_existing_view(self):
        """测试创建元数据视图时处理已存在的视图"""
        mock_connection = Mock()
        table_name = "existing_table"
        mock_table_dir = Mock()
        valid_files = ["/path/to/existing/file1.parquet"]

        with patch.object(
            self.manager, "_validate_parquet_files", return_value=valid_files
        ):
            # 模拟视图已存在的情况，CREATE OR REPLACE VIEW 应该成功执行
            self.manager._create_metadata_view(
                mock_connection, table_name, mock_table_dir
            )

            # 验证使用了 CREATE OR REPLACE VIEW
            mock_connection.execute.assert_called_once()
            actual_call = mock_connection.execute.call_args[0][0]
            assert "CREATE OR REPLACE VIEW" in actual_call
            assert "existing_table" in actual_call

    def test_validate_parquet_files_valid_files(self):
        """测试验证有效的 Parquet 文件"""
        mock_table_dir = Mock()
        mock_file1 = Mock()
        mock_file1.stat.return_value.st_size = 1000
        mock_file2 = Mock()
        mock_file2.stat.return_value.st_size = 2000

        mock_table_dir.rglob.return_value = [mock_file1, mock_file2]

        with patch("neo.tasks.metadata_sync_tasks.duckdb.connect") as mock_connect:
            mock_con = Mock()
            mock_connect.return_value.__enter__.return_value = mock_con

            result = self.manager._validate_parquet_files(mock_table_dir)

            assert len(result) == 2
            assert str(mock_file1) in result
            assert str(mock_file2) in result

    def test_validate_parquet_files_skip_small_files(self):
        """测试跳过过小的 Parquet 文件"""
        mock_table_dir = Mock()
        mock_small_file = Mock()
        mock_small_file.stat.return_value.st_size = 50  # 小于100字节
        mock_valid_file = Mock()
        mock_valid_file.stat.return_value.st_size = 1000

        mock_table_dir.rglob.return_value = [mock_small_file, mock_valid_file]

        with patch("neo.tasks.metadata_sync_tasks.duckdb.connect") as mock_connect:
            mock_con = Mock()
            mock_connect.return_value.__enter__.return_value = mock_con

            result = self.manager._validate_parquet_files(mock_table_dir)

            assert len(result) == 1
            assert str(mock_valid_file) in result
            assert str(mock_small_file) not in result

    def test_validate_parquet_files_skip_corrupted_files(self):
        """测试跳过损坏的 Parquet 文件"""
        mock_table_dir = Mock()
        mock_corrupted_file = Mock()
        mock_corrupted_file.stat.return_value.st_size = 1000
        mock_valid_file = Mock()
        mock_valid_file.stat.return_value.st_size = 1000

        mock_table_dir.rglob.return_value = [mock_corrupted_file, mock_valid_file]

        with patch("neo.tasks.metadata_sync_tasks.duckdb.connect") as mock_connect:
            mock_con = Mock()
            mock_connect.return_value.__enter__.return_value = mock_con

            # 模拟第一个文件读取失败，第二个文件成功
            def side_effect(sql):
                if str(mock_corrupted_file) in sql:
                    raise Exception("Invalid Input Error: No magic bytes found")
                return None

            mock_con.execute.side_effect = side_effect

            result = self.manager._validate_parquet_files(mock_table_dir)

            assert len(result) == 1
            assert str(mock_valid_file) in result
            assert str(mock_corrupted_file) not in result

    def test_create_metadata_view_no_valid_files(self):
        """测试没有有效文件时跳过视图创建"""
        mock_connection = Mock()
        mock_table_dir = Mock()

        with patch.object(self.manager, "_validate_parquet_files", return_value=[]):
            self.manager._create_metadata_view(
                mock_connection, "test_table", mock_table_dir
            )

            # 验证没有执行 SQL
            mock_connection.execute.assert_not_called()

    def test_create_metadata_view_with_valid_files(self):
        """测试有有效文件时创建视图"""
        mock_connection = Mock()
        mock_table_dir = Mock()
        valid_files = ["/path/to/file1.parquet", "/path/to/file2.parquet"]

        with patch.object(
            self.manager, "_validate_parquet_files", return_value=valid_files
        ):
            self.manager._create_metadata_view(
                mock_connection, "test_table", mock_table_dir
            )

            # 验证执行了 SQL
            mock_connection.execute.assert_called_once()
            sql_call = mock_connection.execute.call_args[0][0]
            assert "CREATE OR REPLACE VIEW test_table" in sql_call
            assert "['/path/to/file1.parquet', '/path/to/file2.parquet']" in sql_call

    def test_sync_table_metadata(self):
        """测试同步表元数据"""
        mock_connection = Mock()
        mock_table_dir = Mock()
        mock_table_dir.name = "test_table"

        with (
            patch.object(self.manager, "_drop_existing_table_or_view") as mock_drop,
            patch.object(self.manager, "_create_metadata_view") as mock_create,
        ):
            self.manager._sync_table_metadata(mock_connection, mock_table_dir)

            mock_drop.assert_called_once_with(mock_connection, "test_table")
            mock_create.assert_called_once_with(
                mock_connection, "test_table", mock_table_dir
            )


class TestMetadataSyncIntegration:
    """测试 MetadataSyncManager 的集成功能"""

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_sync_metadata_success(self, mock_get_config):
        """测试成功的元数据同步流程"""
        # 设置配置mock
        mock_config = Mock()
        mock_config.storage.parquet_base_path = "data/parquet"
        mock_config.database.metadata_path = "data/metadata.db"
        mock_get_config.return_value = mock_config

        manager = MetadataSyncManager()

        # Mock所有依赖方法
        with (
            patch.object(manager, "_get_project_paths") as mock_get_paths,
            patch.object(manager, "_validate_parquet_directory") as mock_validate,
            patch.object(manager, "_setup_duckdb_connection") as mock_setup_db,
            patch.object(manager, "_scan_parquet_directories") as mock_scan,
            patch.object(manager, "_sync_table_metadata") as mock_sync_table,
        ):
            # 设置返回值
            mock_project_root = Mock()
            mock_parquet_base_path = Mock()
            mock_metadata_db_path = Mock()
            mock_get_paths.return_value = (
                mock_project_root,
                mock_parquet_base_path,
                mock_metadata_db_path,
            )

            mock_validate.return_value = True

            mock_connection = Mock()
            mock_context_manager = Mock()
            mock_context_manager.__enter__ = Mock(return_value=mock_connection)
            mock_context_manager.__exit__ = Mock(return_value=None)
            mock_setup_db.return_value = mock_context_manager

            mock_table_dir1 = Mock()
            mock_table_dir2 = Mock()
            mock_scan.return_value = [mock_table_dir1, mock_table_dir2]

            # 执行同步
            manager.sync_metadata()

            # 验证调用
            mock_get_paths.assert_called_once()
            mock_validate.assert_called_once_with(mock_parquet_base_path)
            mock_setup_db.assert_called_once_with(mock_metadata_db_path)
            mock_scan.assert_called_once_with(mock_parquet_base_path)

            # 验证每个表都被同步
            assert mock_sync_table.call_count == 2
            mock_sync_table.assert_any_call(mock_connection, mock_table_dir1)
            mock_sync_table.assert_any_call(mock_connection, mock_table_dir2)

            # 验证日志

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_sync_metadata_parquet_directory_invalid(self, mock_get_config):
        """测试 Parquet 目录无效时的处理"""
        mock_config = Mock()
        mock_get_config.return_value = mock_config

        manager = MetadataSyncManager()

        with (
            patch.object(manager, "_get_project_paths") as mock_get_paths,
            patch.object(manager, "_validate_parquet_directory") as mock_validate,
        ):
            mock_get_paths.return_value = (Mock(), Mock(), Mock())
            mock_validate.return_value = False

            # 执行同步
            manager.sync_metadata()

            # 验证只调用了路径获取和验证
            mock_get_paths.assert_called_once()
            mock_validate.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_sync_metadata_no_table_directories(self, mock_get_config):
        """测试没有表目录时的处理"""
        mock_config = Mock()
        mock_get_config.return_value = mock_config

        manager = MetadataSyncManager()

        with (
            patch.object(manager, "_get_project_paths") as mock_get_paths,
            patch.object(manager, "_validate_parquet_directory") as mock_validate,
            patch.object(manager, "_setup_duckdb_connection") as mock_setup_db,
            patch.object(manager, "_scan_parquet_directories") as mock_scan,
        ):
            mock_get_paths.return_value = (Mock(), Mock(), Mock())
            mock_validate.return_value = True

            mock_connection = Mock()
            mock_context_manager = Mock()
            mock_context_manager.__enter__ = Mock(return_value=mock_connection)
            mock_context_manager.__exit__ = Mock(return_value=None)
            mock_setup_db.return_value = mock_context_manager

            mock_scan.return_value = []  # 没有表目录

            # 执行同步
            manager.sync_metadata()

            # 验证调用
            mock_get_paths.assert_called_once()
            mock_validate.assert_called_once()
            mock_setup_db.assert_called_once()
            mock_scan.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_sync_metadata_exception_handling(self, mock_get_config):
        """测试元数据同步过程中的异常处理"""
        mock_config = Mock()
        mock_get_config.return_value = mock_config

        manager = MetadataSyncManager()

        with patch.object(manager, "_get_project_paths") as mock_get_paths:
            mock_get_paths.side_effect = Exception("Path error")

            # 验证异常被重新抛出
            with pytest.raises(Exception, match="Path error"):
                manager.sync_metadata()

            # 验证错误日志


class TestSyncMetadataFunction:
    """测试 sync_metadata 函数"""

    @patch("neo.tasks.metadata_sync_tasks.MetadataSyncManager")
    def test_sync_metadata_function(self, mock_manager_class):
        """测试 sync_metadata 函数调用 MetadataSyncManager"""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager

        # 调用函数
        sync_metadata.func()

        # 验证调用
        mock_manager_class.assert_called_once()
        mock_manager.sync_metadata.assert_called_once()


class TestGetSyncMetadataCrontab:
    """测试 get_sync_metadata_crontab 函数"""

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_get_sync_metadata_crontab_valid_schedule(self, mock_get_config):
        """测试有效的 cron 调度配置"""
        mock_config = Mock()
        mock_config.cron_tasks.sync_metadata_schedule = "0 2 * * *"
        mock_get_config.return_value = mock_config

        result = get_sync_metadata_crontab()

        # 验证返回的不是 None
        assert result is not None
        mock_get_config.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_get_sync_metadata_crontab_default_schedule(self, mock_get_config):
        """测试默认的 cron 调度配置"""
        mock_config = Mock()
        mock_config.cron_tasks.sync_metadata_schedule = "0 0 * * *"
        mock_get_config.return_value = mock_config

        result = get_sync_metadata_crontab()

        # 验证返回的不是 None
        assert result is not None
        mock_get_config.assert_called_once()

    @patch("neo.tasks.metadata_sync_tasks.get_config")
    def test_get_sync_metadata_crontab_complex_schedule(self, mock_get_config):
        """测试复杂的 cron 调度配置"""
        mock_config = Mock()
        mock_config.cron_tasks.sync_metadata_schedule = "30 1 */2 * 1-5"
        mock_get_config.return_value = mock_config

        result = get_sync_metadata_crontab()

        # 验证返回的不是 None
        assert result is not None
        mock_get_config.assert_called_once()
