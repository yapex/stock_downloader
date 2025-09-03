import duckdb
from pathlib import Path
import re
import time
from typing import Set


class MetadataSyncManager:
    """
    管理 DuckDB 元数据与 Parquet 数据湖之间的同步。
    支持混合模式：快速 mtime 检查 + 可靠的状态比较。
    """

    def __init__(self, metadata_db_path: str, parquet_base_path: str):
        """
        初始化同步管理器。

        Args:
            metadata_db_path (str): DuckDB 元数据文件的路径 (e.g., 'data/metadata.db').
            parquet_base_path (str): Parquet 文件存储的根目录路径 (e.g., 'data/parquet').
        """
        self.db_path = Path(metadata_db_path)
        self.parquet_path = Path(parquet_base_path)
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        self.parquet_path.mkdir(exist_ok=True, parents=True)

    def _has_recent_files(self, table_name: str, minutes: int) -> bool:
        """检查表目录是否有最近几分钟内的文件变化。"""
        table_dir = self.parquet_path / table_name
        if not table_dir.is_dir():
            return False

        cutoff_time = time.time() - (minutes * 60)

        # 使用 rglob 递归检查所有文件和目录
        for path_object in table_dir.rglob("*"):
            try:
                if path_object.stat().st_mtime > cutoff_time:
                    return True
            except FileNotFoundError:
                # 文件在检查期间可能被删除，安全地忽略
                continue

        return False

    def _get_physical_partitions(self, table_name: str) -> Set[str]:
        """扫描物理目录，获取一个表实际存在的、且包含数据的分区。"""
        table_dir = self.parquet_path / table_name
        if not table_dir.is_dir():
            return set()

        valid_partitions = set()
        for partition_dir in table_dir.iterdir():
            if partition_dir.is_dir() and re.match(r"year=\d{4}", partition_dir.name):
                if any(partition_dir.glob("*.parquet*")):
                    valid_partitions.add(partition_dir.name)

        return valid_partitions

    def _get_view_partitions(
        self, con: duckdb.DuckDBPyConnection, table_name: str
    ) -> Set[str]:
        """从 DuckDB 的元数据中，查询一个视图当前指向了哪些分区。"""
        try:
            result_df = con.execute(
                f"SELECT DISTINCT file_name FROM duckdb_scanned_files('{table_name}')"
            ).fetchdf()
            if result_df.empty:
                return set()

            partitions = set()
            for file_path in result_df["file_name"]:
                match = re.search(r"(year=\d{4})", file_path)
                if match:
                    partitions.add(match.group(1))
            return partitions
        except duckdb.Error:
            return set()

    def _view_exists(self, con: duckdb.DuckDBPyConnection, view_name: str) -> bool:
        """检查视图是否存在。"""
        try:
            res = con.execute(
                "SELECT 1 FROM duckdb_views() WHERE view_name = ?", [view_name]
            ).fetchone()
            return res is not None
        except duckdb.Error:
            return False

    def sync(self, force_full_scan: bool = False, mtime_check_minutes: int = 60):
        """
        执行混合模型的同步。

        Args:
            force_full_scan (bool): 强制执行完整的状态比较。
            mtime_check_minutes (int): mtime 检查窗口，0 表示禁用。
        """
        table_names = [p.name for p in self.parquet_path.iterdir() if p.is_dir()]
        if not table_names:
            print(f"警告：在 '{self.parquet_path}' 目录下未找到任何数据表子目录。")
            return

        print(f"开始对 {len(table_names)} 个表进行元数据同步...")
        updated_count = 0
        skipped_count = 0

        try:
            with duckdb.connect(database=str(self.db_path)) as con:
                for table_name in table_names:
                    # --- 快速扫描模式 ---
                    if not force_full_scan:
                        # 默认快速模式：只检查最近的文件变动
                        if mtime_check_minutes > 0 and not self._has_recent_files(
                            table_name, minutes=mtime_check_minutes
                        ):
                            print(f"  -> 表 '{table_name}' 最近 {mtime_check_minutes} 分钟内无变动，跳过。")
                            skipped_count += 1
                            continue
                        elif mtime_check_minutes == 0:
                            # mtime_check_minutes = 0 表示检查所有文件，不做时间限制
                            pass
                    
                    # --- 完整扫描模式 ---
                    physical_partitions = self._get_physical_partitions(table_name)

                    if not physical_partitions:
                        table_dir = self.parquet_path / table_name
                        if any(table_dir.glob("*.parquet*")):
                            glob_pattern = f"{table_dir}/*.parquet"
                            sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{glob_pattern}');"
                            print(
                                f"  -> 正在为未分区表 '{table_name}' 创建/更新视图..."
                            )
                            con.execute(sql)
                            updated_count += 1
                        else:
                            print(
                                f"  -> 表 '{table_name}' 没有任何有效数据文件，跳过。"
                            )
                        continue

                    view_partitions = self._get_view_partitions(con, table_name)

                    if physical_partitions != view_partitions or not self._view_exists(
                        con, table_name
                    ):
                        print(
                            f"  -> 检测到表 '{table_name}' 的分区状态不一致，正在更新视图..."
                        )

                        partition_paths_str = ", ".join(
                            [
                                f"'{self.parquet_path / table_name / p}/*.parquet'"
                                for p in sorted(list(physical_partitions))
                            ]
                        )

                        sql = f"""
                        CREATE OR REPLACE VIEW {table_name} AS
                        SELECT * FROM read_parquet([{partition_paths_str}], union_by_name=true);
                        """
                        con.execute(sql)
                        updated_count += 1
                    else:
                        print(f"  -> 表 '{table_name}' 的状态已是最新，跳过。")
                        skipped_count += 1

            print(f"\n元数据同步完成！更新: {updated_count}, 跳过: {skipped_count}。")

        except duckdb.Error as e:
            print(f"\n元数据同步过程中发生错误: {e}")
            raise
