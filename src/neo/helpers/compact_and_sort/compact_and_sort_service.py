"""
数据压缩和排序服务实现
"""

import os
import shutil
from pathlib import Path
from typing import Dict, Any
import ibis
import duckdb
import pandas as pd

from .interfaces import (
    CompactAndSortService,
    DataDeduplicationStrategy,
    FilenameGenerator,
)
from .deduplication_strategies import HybridDeduplicationStrategy
from .filename_generator import UUIDFilenameGenerator


class CompactAndSortServiceImpl(CompactAndSortService):
    """数据压缩和排序服务实现"""

    def __init__(
        self,
        data_dir: Path,
        temp_dir: Path,
        deduplication_strategy: DataDeduplicationStrategy = None,
        filename_generator: FilenameGenerator = None,
    ):
        """
        初始化服务

        Args:
            data_dir: 数据目录
            temp_dir: 临时目录
            deduplication_strategy: 数据去重策略
            filename_generator: 文件名生成器
        """
        self.data_dir = data_dir
        self.temp_dir = temp_dir

        # 使用默认策略如果未提供
        self.deduplication_strategy = (
            deduplication_strategy or HybridDeduplicationStrategy()
        )
        self.filename_generator = filename_generator or UUIDFilenameGenerator()

    def compact_and_sort_table(
        self, table_name: str, table_config: Dict[str, Any]
    ) -> None:
        """
        对单个表进行压缩和排序

        Args:
            table_name: 表名
            table_config: 表配置信息
        """
        source_path = self.data_dir / table_name
        target_path = self.temp_dir / table_name

        is_partitioned = "date_col" in table_config

        # 1. 前置校验
        self._validate_source_directory_structure(source_path, is_partitioned)

        # 2. 使用DuckDB连接处理数据
        with duckdb.connect(database=":memory:") as con:
            # 3. 读取数据并应用去重策略
            table = self._load_and_deduplicate_data(
                con, table_name, table_config, is_partitioned
            )

            # 4. 准备SQL并执行优化
            self._execute_optimization(
                con, table, table_name, table_config, target_path, is_partitioned
            )

            # 5. 数据一致性校验
            is_valid, needs_confirmation = self._validate_data_consistency(
                con, table_name, source_path, target_path, is_partitioned
            )

            if not is_valid:
                raise ValueError(
                    f"数据验证失败，优化中断。临时数据保留在 {target_path}"
                )

        # 6. 替换旧数据
        self._replace_old_data(source_path, target_path, needs_confirmation)

    def _validate_source_directory_structure(
        self, source_path: Path, is_partitioned: bool
    ):
        """前置校验：检查源目录结构是否符合规范"""
        print(f"  校验目录结构: {source_path}")
        if not source_path.exists() or not source_path.is_dir():
            raise FileNotFoundError(f"源目录不存在或不是一个目录: {source_path}")

        # 系统文件白名单
        system_files_whitelist = {
            ".DS_Store",
            "Thumbs.db",
            ".gitignore",
            "desktop.ini",
            ".localized",
            "._.DS_Store",
            "Icon\r",
        }

        ignored_files_count = 0

        if is_partitioned:
            # 分区表：目录下只允许存在 year=... 的子目录
            for item in source_path.iterdir():
                if item.name in system_files_whitelist:
                    ignored_files_count += 1
                    continue

                if item.is_file():
                    raise ValueError(
                        f"校验失败！分区表 {source_path.name} 的根目录不应包含任何文件，发现: {item.name}"
                    )
                if not item.name.startswith("year="):
                    raise ValueError(
                        f"校验失败！分区表 {source_path.name} 的子目录必须以 'year=' 开头，发现: {item.name}"
                    )
        else:
            # 非分区表：目录下只允许存在 .parquet 文件
            for item in source_path.iterdir():
                if item.name in system_files_whitelist:
                    ignored_files_count += 1
                    continue

                if item.is_dir():
                    raise ValueError(
                        f"校验失败！非分区表 {source_path.name} 的根目录不应包含任何子目录，发现: {item.name}"
                    )
                if item.suffix != ".parquet":
                    raise ValueError(
                        f"校验失败！非分区表 {source_path.name} 的根目录只应包含 .parquet 文件，发现: {item.name}"
                    )

        if ignored_files_count > 0:
            print(f"  📋 已忽略 {ignored_files_count} 个系统文件")
        print("  ✅ 目录结构校验通过。")

    def _load_and_deduplicate_data(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        table_config: Dict[str, Any],
        is_partitioned: bool,
    ) -> ibis.Table:
        """加载数据并应用去重策略"""
        source_path = self.data_dir / table_name
        source_pattern = f"'{source_path}/**/*.parquet'"

        if is_partitioned:
            # 分区表使用目录模式
            df = con.execute(
                f"SELECT * FROM read_parquet({source_pattern}, hive_partitioning=1, union_by_name=true)"
            ).df()
        else:
            # 非分区表
            df = con.execute(f"SELECT * FROM read_parquet({source_pattern})").df()

        # 转换为ibis表
        table = ibis.memtable(df)

        # 应用去重策略
        deduplicated_table = self.deduplication_strategy.deduplicate(
            table, table_config
        )

        return deduplicated_table

    def _execute_optimization(
        self,
        con: duckdb.DuckDBPyConnection,
        table: ibis.Table,
        table_name: str,
        table_config: Dict[str, Any],
        target_path: Path,
        is_partitioned: bool,
    ):
        """执行优化操作"""
        # 获取排序列
        sort_columns = self._get_sort_columns(table_config)

        # 清理目标路径
        if target_path.exists():
            if target_path.is_dir():
                shutil.rmtree(target_path)
            else:
                target_path.unlink()

        # 为非分区表创建目录
        if not is_partitioned:
            target_path.mkdir(parents=True, exist_ok=True)

        # 将去重后的表保存为临时parquet文件
        temp_table_path = self.temp_dir / "temp_table.parquet"
        table.to_parquet(temp_table_path)

        # 构建SQL并执行
        if is_partitioned:
            copy_statement = f"""
            COPY (
                SELECT * FROM read_parquet('{self.temp_dir}/temp_table.parquet')
                ORDER BY {sort_columns}
            )
            TO '{target_path}'
            (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE 1);
            """
        else:
            # 非分区表：生成唯一文件名
            filename = self.filename_generator.generate_filename(table_name)
            target_file_path = target_path / filename
            copy_statement = f"""
            COPY (
                SELECT * FROM read_parquet('{self.temp_dir}/temp_table.parquet')
                ORDER BY {sort_columns}
            )
            TO '{target_file_path}'
            (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);
            """

        print("  准备执行优化 SQL...")
        print(copy_statement)

        con.execute(copy_statement)
        print(f"  ✅ 表 {table_name} 已成功优化到临时目录: {target_path}")

    def _get_sort_columns(self, table_config: Dict[str, Any]) -> str:
        """根据表配置获取排序列"""
        primary_key = table_config.get("primary_key", [])
        if not primary_key:
            raise ValueError(f"表 {table_config.get('table_name')} 没有定义主键")
        return ", ".join(primary_key)

    def _validate_data_consistency(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        source_path: Path,
        target_path: Path,
        is_partitioned: bool,
    ) -> tuple[bool, bool]:
        """验证源数据和优化后数据的一致性"""
        print("　开始数据一致性验证...")

        try:
            if is_partitioned:
                # 分区表使用目录模式
                source_pattern = f"'{source_path}/**/*.parquet'"
                target_pattern = f"'{target_path}/**/*.parquet'"
                source_count = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet({source_pattern}, hive_partitioning=1)"
                ).fetchone()[0]
                target_count = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet({target_pattern}, hive_partitioning=1)"
                ).fetchone()[0]
            else:
                # 非分区表可能是单个文件或目录
                if target_path.is_file():
                    source_pattern = f"'{source_path}/**/*.parquet'"
                    target_pattern = f"'{target_path}'"
                    source_count = con.execute(
                        f"SELECT COUNT(*) FROM read_parquet({source_pattern})"
                    ).fetchone()[0]
                    target_count = con.execute(
                        f"SELECT COUNT(*) FROM read_parquet({target_pattern})"
                    ).fetchone()[0]
                else:
                    source_pattern = f"'{source_path}/**/*.parquet'"
                    target_pattern = f"'{target_path}/**/*.parquet'"
                    source_count = con.execute(
                        f"SELECT COUNT(*) FROM read_parquet({source_pattern})"
                    ).fetchone()[0]
                    target_count = con.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{target_path}/**/*.parquet')"
                    ).fetchone()[0]

            print(f"　　记录数验证: 源={source_count:,}, 优化后={target_count:,}")

            if source_count != target_count:
                # 有去重，需要用户确认
                timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
                import uuid
                unique_id = str(uuid.uuid4())[:8]
                backup_name = f"{source_path.name}.backup_{timestamp}_{unique_id}"
                
                # 一次性确认：去重和删除原数据
                if not self._ask_user_combined_confirmation(
                    table_name, source_count, target_count, backup_name
                ):
                    return False, False  # 用户拒绝，不进行任何操作
                print("　　✅ 记录数校验通过（用户确认去重结果）。")
                return True, False  # 不需要额外确认，直接删除备份
            else:
                # 无去重，直接继续，不需要确认
                print("　　✅ 记录数校验通过，无重复数据，直接继续优化。")
                return True, False  # 无去重，直接删除备份

        except Exception as e:
            print(f"　❌ 验证过程中发生错误: {e}")
            if not self._ask_user_confirmation(
                "验证失败，是否强行继续？", default=False
            ):
                return False, False
            print("　　✅ 用户选择强行继续。")
            return True, False  # 强行继续，直接删除备份

    def _ask_user_confirmation(self, message: str, default: bool = False) -> bool:
        """简单的用户确认函数"""
        default_text = "[Y/n]" if default else "[y/N]"
        try:
            response = input(f"⚠️ {message} {default_text}: ").strip().lower()
            if not response:
                return default
            return response in ["y", "yes"]
        except (KeyboardInterrupt, EOFError):
            print("\n用户中断操作。")
            return False

    def _ask_user_combined_confirmation(
        self, table_name: str, source_count: int, target_count: int, backup_name: str
    ) -> bool:
        """合并确认：去重和删除备份的一次性确认"""
        print("\n" + "="*60)
        print(f"  📊 数据优化结果 - 表: {table_name}")
        print("="*60)
        print(f"  原始记录数: {source_count:,}")
        print(f"  优化后记录数: {target_count:,}")
        removed_count = source_count - target_count
        if removed_count > 0:
            print(f"  🔄 去重移除: {removed_count:,} 条记录")
            removal_percentage = (removed_count / source_count) * 100
            print(f"  📊 去重比例: {removal_percentage:.2f}%")
        else:
            print("  ✅ 无重复数据")
        print("-" * 60)
        print("　📦 将创建备份: " + backup_name)
        print("　🗑️ 优化后将自动删除备份")
        print("="*60)
        
        try:
            response = input(
                "⚠️ 是否接受去重结果并继续（包括删除原数据备份）？ [y/N]: "
            ).strip().lower()
            
            if response in ["y", "yes"]:
                print("　✅ 用户确认继续优化操作")
                return True
            else:
                print("　❌ 用户取消操作，保留原数据")
                return False
        except (KeyboardInterrupt, EOFError):
            print("\n用户中断操作。")
            return False

    def _replace_old_data(
        self, source_path: Path, target_path: Path, needs_confirmation: bool
    ):
        """替换旧数据"""
        # 使用UUID确保备份目录名唯一
        timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
        import uuid

        unique_id = str(uuid.uuid4())[:8]
        backup_path = source_path.with_suffix(f".backup_{timestamp}_{unique_id}")

        print(f"  正在用优化后的数据替换旧数据...备份至: {backup_path}")
        source_path.rename(backup_path)
        target_path.rename(source_path)
        print("  ✅ 数据替换成功。")

        # 清理备份
        self._cleanup_backup(backup_path, needs_confirmation)

    def _cleanup_backup(self, backup_path: Path, needs_confirmation: bool):
        """清理备份目录 - 已经通过合并确认，直接删除备份"""
        # 由于已经通过合并确认，不需要额外确认，直接删除备份
        print(f"　✅ 根据用户确认，删除备份目录: {backup_path}")
        self._safe_remove_backup(backup_path)

    def _safe_remove_backup(self, backup_path: Path) -> bool:
        """安全删除备份目录，处理权限问题"""
        try:
            if backup_path.exists():
                # 先修复权限
                os.system(f"chmod -R u+w '{backup_path}'")
                shutil.rmtree(backup_path)
                print(f"  ✅ 备份已删除: {backup_path}")
                return True
        except Exception as e:
            print(f"  ❌ 删除备份失败 {backup_path}: {e}")
            return False
        return False
