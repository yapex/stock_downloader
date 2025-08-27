"""元数据同步任务模块

包含 Parquet 文件元数据同步相关的 Huey 任务。
"""

import logging
from pathlib import Path
from typing import List, Optional
import os

import duckdb
from huey import crontab
from ..configs.huey_config import huey_maint
from ..configs.app_config import get_config

logger = logging.getLogger(__name__)


class MetadataSyncManager:
    """元数据同步管理器，负责管理 Parquet 文件的元数据同步"""

    def __init__(self):
        self.config = get_config()
        self._project_root = None
        self._parquet_base_path = None
        self._metadata_db_path = None

    def _get_project_paths(self) -> tuple[Path, Path, Path]:
        """获取项目相关路径

        Returns:
            tuple[项目根目录, Parquet基础路径, 元数据DB路径]
        """
        if self._project_root is None:
            # 获取当前文件所在目录的绝对路径，并找到项目根目录
            # neo/tasks/metadata_sync_tasks.py -> neo/tasks -> neo -> src -> project_root
            self._project_root = Path(__file__).resolve().parents[3]
            self._parquet_base_path = (
                self._project_root / self.config.storage.parquet_base_path
            )
            self._metadata_db_path = (
                self._project_root / self.config.database.metadata_path
            )

            logger.debug(f"诊断: 项目根目录: {self._project_root}")
            logger.debug(f"诊断: Parquet 根目录: {self._parquet_base_path}")
            logger.debug(f"诊断: 元数据DB路径: {self._metadata_db_path}")

        return self._project_root, self._parquet_base_path, self._metadata_db_path

    def _validate_parquet_directory(self, parquet_base_path: Path) -> bool:
        """验证 Parquet 目录是否存在

        Args:
            parquet_base_path: Parquet 基础路径

        Returns:
            bool: 目录是否存在且有效
        """
        if not parquet_base_path.is_dir():
            logger.warning(f"Parquet 根目录 {parquet_base_path} 不存在，跳过同步。")
            return False
        return True

    def _setup_duckdb_connection(
        self, metadata_db_path: Path
    ) -> duckdb.DuckDBPyConnection:
        """设置 DuckDB 连接和内存限制

        Args:
            metadata_db_path: 元数据数据库路径

        Returns:
            DuckDB 连接对象
        """
        con = duckdb.connect(str(metadata_db_path))
        # 设置DuckDB内存限制，防止内存溢出
        con.execute("SET memory_limit='2GB'")
        con.execute("SET max_memory='2GB'")
        logger.info("诊断: 成功连接到元数据DB，已设置内存限制为2GB。")
        return con

    def _scan_parquet_directories(self, parquet_base_path: Path) -> List[Path]:
        """扫描 Parquet 根目录下的所有子目录

        Args:
            parquet_base_path: Parquet 基础路径

        Returns:
            List[Path]: 有效的表目录列表
        """
        found_items = list(parquet_base_path.iterdir())
        if not found_items:
            logger.warning(f"警告: 在 {parquet_base_path} 中没有找到任何条目。")
            return []

        logger.debug(
            f"诊断: 在 {parquet_base_path} 中找到以下条目: {[p.name for p in found_items]}"
        )

        # 只返回目录
        table_dirs = [item for item in found_items if item.is_dir()]
        logger.debug(f"诊断: 找到 {len(table_dirs)} 个表目录")

        return table_dirs

    def _drop_existing_table_or_view(
        self, con: duckdb.DuckDBPyConnection, table_name: str
    ) -> None:
        """删除已存在的表或视图

        Args:
            con: DuckDB 连接
            table_name: 表名
        """
        try:
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"DROP VIEW IF EXISTS {table_name}")
        except Exception as e:
            logger.debug(f"删除表/视图时出现异常（可忽略）: {e}")

    def _validate_parquet_files(self, table_dir: Path) -> List[str]:
        """验证 Parquet 文件的有效性

        Args:
            table_dir: 表目录路径

        Returns:
            有效的 Parquet 文件路径列表
        """
        valid_files = []
        parquet_files = list(table_dir.rglob("*.parquet"))
        
        for file_path in parquet_files:
            try:
                # 检查文件大小，空文件或过小的文件可能损坏
                if file_path.stat().st_size < 100:  # 至少100字节
                    logger.warning(f"跳过过小的文件: {file_path}")
                    continue
                
                # 尝试用 DuckDB 读取文件头部验证格式
                with duckdb.connect(":memory:") as test_con:
                    test_con.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path}') LIMIT 1")
                    valid_files.append(str(file_path))
                    
            except Exception as e:
                logger.warning(f"跳过损坏的 Parquet 文件 {file_path}: {e}")
                continue
        
        return valid_files

    def _create_metadata_view(
        self, con: duckdb.DuckDBPyConnection, table_name: str, table_dir: Path
    ) -> None:
        """创建元数据视图

        Args:
            con: DuckDB 连接
            table_name: 表名
            table_dir: 表目录路径
        """
        logger.debug(f"正在为表 {table_name} 从目录 {table_dir} 同步元数据...")

        # 验证 Parquet 文件
        valid_files = self._validate_parquet_files(table_dir)
        
        if not valid_files:
            logger.warning(f"表 {table_name} 没有有效的 Parquet 文件，跳过视图创建")
            return
        
        # 构建文件路径列表用于 read_parquet
        files_str = "[" + ", ".join(f"'{f}'" for f in valid_files) + "]"
        
        # 使用VIEW而不是TABLE，避免将所有数据加载到内存
        # VIEW只存储查询定义，不存储实际数据
        # 使用 CREATE OR REPLACE VIEW 避免视图已存在的错误
        sql = f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT * FROM read_parquet({files_str}, hive_partitioning=1, union_by_name=True);
        """
        con.execute(sql)
        logger.info(f"✅ 表 {table_name} 元数据视图同步完成，包含 {len(valid_files)} 个有效文件。")

    def _sync_table_metadata(
        self, con: duckdb.DuckDBPyConnection, table_dir: Path
    ) -> None:
        """同步单个表的元数据

        Args:
            con: DuckDB 连接
            table_dir: 表目录路径
        """
        table_name = table_dir.name

        # 先删除可能存在的TABLE或VIEW，避免类型冲突
        self._drop_existing_table_or_view(con, table_name)

        # 创建新的视图
        self._create_metadata_view(con, table_name, table_dir)

    def sync_metadata(self) -> None:
        """执行元数据同步的主要方法"""
        logger.info("🛠️ [HUEY_MAINT] 开始执行元数据同步任务...")

        try:
            # 1. 获取项目路径
            project_root, parquet_base_path, metadata_db_path = (
                self._get_project_paths()
            )

            # 2. 验证 Parquet 目录
            if not self._validate_parquet_directory(parquet_base_path):
                return

            # 3. 设置数据库连接
            with self._setup_duckdb_connection(metadata_db_path) as con:
                # 4. 扫描表目录
                table_dirs = self._scan_parquet_directories(parquet_base_path)
                if not table_dirs:
                    return

                # 5. 同步每个表的元数据
                for table_dir in table_dirs:
                    self._sync_table_metadata(con, table_dir)

            logger.info("🛠️ [HUEY_MAINT] 元数据同步任务成功完成。")

        except Exception as e:
            logger.error(f"❌ [HUEY_MAINT] 元数据同步任务失败: {e}")
            raise e


def get_sync_metadata_crontab():
    """从配置中读取 cron 表达式"""
    config = get_config()
    schedule = config.cron_tasks.sync_metadata_schedule
    minute, hour, day, month, day_of_week = schedule.split()
    return crontab(minute, hour, day, month, day_of_week)


@huey_maint.periodic_task(get_sync_metadata_crontab(), name="sync_metadata")
def sync_metadata():
    """
    周期性任务：扫描 Parquet 文件目录，并更新 DuckDB 元数据文件。
    """
    sync_manager = MetadataSyncManager()
    sync_manager.sync_metadata()
