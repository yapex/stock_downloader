import duckdb
from pathlib import Path
import re
import time
import json
import hashlib
from typing import Set, Dict, Optional


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
        # 缓存文件路径
        self.cache_path = self.db_path.parent / ".metadata_sync_cache.json"
        self.cache = {}
        self._load_cache()
        
    def _load_cache(self):
        """加载缓存数据。"""
        try:
            if self.cache_path.exists():
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    self.cache = json.load(f)
            else:
                self.cache = {}
        except (json.JSONDecodeError, IOError):
            self.cache = {}
    
    def _save_cache(self):
        """保存缓存数据。"""
        try:
            with open(self.cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
        except IOError:
            pass  # 忽略缓存保存失败
    
    def _quick_table_check(self, table_name: str, minutes: int) -> Dict[str, any]:
        """快速检查表目录是否有变化，使用改进的缓存机制。"""
        table_dir = self.parquet_path / table_name
        if not table_dir.exists():
            return {"has_changes": False, "last_modified": None, "file_count": 0, "method": "dir_not_exists"}
        
        # 获取缓存信息
        cached_info = self.cache.get(table_name, {})
        cached_file_count = cached_info.get("file_count", 0)
        cached_last_modified = cached_info.get("last_modified")
        
        # 快速检查文件数量
        try:
            current_file_count = len(list(table_dir.glob("**/*.parquet")))
        except OSError:
            return {"has_changes": True, "last_modified": None, "file_count": 0, "method": "file_count_failed"}
        
        # 如果文件数量发生变化，说明有新数据
        if current_file_count != cached_file_count:
            return self._fast_file_scan(table_name, minutes, None)  # 目录mtime不再重要
        
        # 文件数量没变，但仍需检查最新文件的时间戳
        # 进行轻量级文件扫描来获取真实的最新时间戳
        scan_info = self._fast_file_scan(table_name, minutes, None)
        
        # 对比扫描得到的最新时间戳与缓存的时间戳
        if cached_last_modified and scan_info["last_modified"]:
            # 如果最新文件时间戳与缓存不同，说明有变化
            if abs(scan_info["last_modified"] - cached_last_modified) > 1.0:  # 允许1秒误差
                return {
                    "has_changes": True,
                    "last_modified": scan_info["last_modified"],
                    "file_count": current_file_count,
                    "method": "timestamp_changed"
                }
        
        # 文件数量没变且时间戳也没变，检查时间窗口
        if scan_info["last_modified"] and minutes > 0:
            cutoff_time = time.time() - (minutes * 60)
            has_recent_changes = scan_info["last_modified"] > cutoff_time
            return {
                "has_changes": has_recent_changes,
                "last_modified": scan_info["last_modified"],
                "file_count": current_file_count,
                "method": "enhanced_cached_check"
            }
        elif cached_last_modified and minutes > 0:
            # 扫描失败时使用缓存数据作为回退
            cutoff_time = time.time() - (minutes * 60)
            has_recent_changes = cached_last_modified > cutoff_time
            return {
                "has_changes": has_recent_changes,
                "last_modified": cached_last_modified,
                "file_count": current_file_count,
                "method": "cached_by_filecount"
            }
        
        # 缓存不存在或无效，做一次完整扫描
        return self._fast_file_scan(table_name, minutes, None)
    
    def _fast_file_scan(self, table_name: str, minutes: int, dir_mtime: float = None) -> Dict[str, any]:
        """快速文件扫描，优化后的版本。"""
        table_dir = self.parquet_path / table_name
        cutoff_time = time.time() - (minutes * 60) if minutes > 0 else 0
        
        latest_mtime = 0
        file_count = 0
        has_recent_changes = False
        
        # 直接使用 glob 而不是 rglob，减少递归扫描
        try:
            for parquet_file in table_dir.glob("**/*.parquet"):
                try:
                    mtime = parquet_file.stat().st_mtime
                    file_count += 1
                    if mtime > latest_mtime:
                        latest_mtime = mtime
                    if minutes > 0 and mtime > cutoff_time:
                        has_recent_changes = True
                        # 不要提前退出，需要计算所有文件
                except OSError:
                    continue
        except OSError:
            # 目录无法访问
            return {"has_changes": True, "last_modified": None, "file_count": 0, "method": "scan_failed"}
        
        # 获取目录mtime（如果没有提供）
        if dir_mtime is None:
            try:
                dir_mtime = table_dir.stat().st_mtime
            except OSError:
                dir_mtime = time.time()  # 使用当前时间作为默认值
        
        # 更新缓存
        self.cache[table_name] = {
            "dir_mtime": dir_mtime,
            "last_modified": latest_mtime if latest_mtime > 0 else None,
            "file_count": file_count,
            "last_check": time.time()
        }
        
        return {
            "has_changes": has_recent_changes if minutes > 0 else True,
            "last_modified": latest_mtime if latest_mtime > 0 else None,
            "file_count": file_count,
            "method": "fast_scan"
        }
        
    def _format_time_diff(self, timestamp: float) -> str:
        """格式化时间差，返回友好的时间描述。"""
        now = time.time()
        diff = now - timestamp
        
        if diff < 60:
            return f"{int(diff)}秒前"
        elif diff < 3600:
            minutes = int(diff / 60)
            seconds = int(diff % 60)
            return f"{minutes}分{seconds}秒前"
        elif diff < 86400:
            hours = int(diff / 3600)
            minutes = int((diff % 3600) / 60)
            return f"{hours}小时{minutes}分钟前"
        else:
            days = int(diff / 86400)
            hours = int((diff % 86400) / 3600)
            return f"{days}天{hours}小时前"

    def _get_file_info(self, table_name: str, minutes: int = 0) -> Dict[str, any]:
        """检查表目录的文件信息，返回详细状态。"""
        table_dir = self.parquet_path / table_name
        if not table_dir.is_dir():
            return {"has_changes": False, "last_modified": None, "file_count": 0}

        cutoff_time = time.time() - (minutes * 60) if minutes > 0 else 0
        latest_mtime = 0
        file_count = 0
        has_recent_changes = False

        # 使用 rglob 递归检查所有文件和目录
        for path_object in table_dir.rglob("*.parquet*"):
            try:
                mtime = path_object.stat().st_mtime
                file_count += 1
                if mtime > latest_mtime:
                    latest_mtime = mtime
                if minutes > 0 and mtime > cutoff_time:
                    has_recent_changes = True
            except FileNotFoundError:
                # 文件在检查期间可能被删除，安全地忽略
                continue

        return {
            "has_changes": has_recent_changes if minutes > 0 else True,
            "last_modified": latest_mtime if latest_mtime > 0 else None,
            "file_count": file_count
        }

    def _get_physical_partitions(self, table_name: str) -> Set[str]:
        """扫描物理目录，获取一个表实际存在的、且包含数据的分区。"""
        table_dir = self.parquet_path / table_name
        if not table_dir.is_dir():
            return set()

        valid_partitions = set()
        # 递归查找所有包含 parquet 文件的 year= 分区
        for parquet_file in table_dir.rglob("*.parquet"):
            # 从文件路径中提取 year= 分区
            path_parts = parquet_file.parts
            for part in path_parts:
                if re.match(r"year=\d{4}", part):
                    valid_partitions.add(part)
                    break

        return valid_partitions

    def _get_view_partitions(
        self, con: duckdb.DuckDBPyConnection, table_name: str
    ) -> Set[str]:
        """从 DuckDB 视图的 SQL 定义中提取分区信息。"""
        try:
            # 获取视图的 SQL 定义
            result = con.execute(
                f"SELECT sql FROM duckdb_views() WHERE view_name = '{table_name}'"
            ).fetchone()
            
            if not result:
                return set()
            
            sql_definition = result[0]
            # 从 SQL 定义中提取 year=XXXX 模式
            matches = re.findall(r'year=\d{4}', sql_definition)
            return set(matches)
            
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
    
    def _get_table_stats(self, con: duckdb.DuckDBPyConnection, table_name: str) -> Dict[str, any]:
        """获取表的统计信息（行数、分区数等）。"""
        stats = {"row_count": 0, "partition_count": 0, "view_exists": False}
        
        try:
            # 检查视图是否存在
            stats["view_exists"] = self._view_exists(con, table_name)
            
            if stats["view_exists"]:
                # 获取行数
                try:
                    result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    stats["row_count"] = result[0] if result else 0
                except duckdb.Error:
                    stats["row_count"] = 0
                
                # 获取分区数
                partition_set = self._get_view_partitions(con, table_name)
                stats["partition_count"] = len(partition_set)
            
            return stats
        except duckdb.Error:
            return stats

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
                    # 获取文件信息（优化版本）
                    file_info = self._quick_table_check(table_name, mtime_check_minutes)
                    
                    # --- 快速扫描模式 ---
                    if not force_full_scan and mtime_check_minutes > 0:
                        if not file_info["has_changes"]:
                            time_info = ""
                            if file_info["last_modified"]:
                                time_info = f", 最后修改时间: {self._format_time_diff(file_info['last_modified'])}"
                            
                            method_info = f" [{file_info.get('method', 'unknown')}]"
                            print(f"  -> 表 '{table_name}' 无最近变动跳过 (文件数: {file_info['file_count']}个{time_info}){method_info}")
                            skipped_count += 1
                            continue
                    
                    # 只有在需要更新时才获取表统计信息
                    before_stats = self._get_table_stats(con, table_name)
                    
                    # --- 完整扫描模式 ---
                    physical_partitions = self._get_physical_partitions(table_name)

                    if not physical_partitions:
                        table_dir = self.parquet_path / table_name
                        if any(table_dir.glob("*.parquet*")):
                            print(
                                f"  -> 正在为未分区表 '{table_name}' 创建/更新视图..."
                            )
                            glob_pattern = f"{table_dir}/*.parquet"
                            sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{glob_pattern}');"
                            con.execute(sql)
                            
                            # 获取同步后的统计信息
                            after_stats = self._get_table_stats(con, table_name)
                            time_info = f", 最后修改: {self._format_time_diff(file_info['last_modified'])}" if file_info['last_modified'] else ""
                            row_change = after_stats['row_count'] - before_stats['row_count']
                            row_change_str = f" (+{row_change})" if row_change > 0 else f" ({row_change})" if row_change < 0 else ""
                            
                            print(f"     ✓ 更新完成: {after_stats['row_count']:,} 行数据{row_change_str}, 未分区, {file_info['file_count']} 个文件{time_info}")
                            updated_count += 1
                        else:
                            print(
                                f"  -> 表 '{table_name}' 没有任何有效数据文件，跳过。"
                            )
                        continue

                    view_partitions = self._get_view_partitions(con, table_name)
                    
                    # 检查是否需要更新：优先考虑文件变化，其次考虑分区一致性
                    needs_update = False
                    update_reason = ""
                    
                    # 1. 如果文件有变化，必须更新
                    if file_info["has_changes"]:
                        needs_update = True
                        update_reason = f"文件变化 (最后修改: {self._format_time_diff(file_info['last_modified'])})"
                    # 2. 如果没有文件变化，但分区不一致或视图不存在
                    elif physical_partitions != view_partitions or not self._view_exists(con, table_name):
                        needs_update = True
                        if not self._view_exists(con, table_name):
                            update_reason = "视图不存在"
                        else:
                            update_reason = "分区状态不一致"
                    
                    if needs_update:
                        print(
                            f"  -> 检测到表 '{table_name}' 需要更新 ({update_reason})，正在更新视图..."
                        )

                        # 检测分区结构并生成正确的路径模式
                        partition_paths = []
                        for p in sorted(list(physical_partitions)):
                            # 检查是否存在嵌套结构 (ts_code=XXX/year=YYYY)
                            nested_path = self.parquet_path / table_name / "*" / p
                            direct_path = self.parquet_path / table_name / p
                            
                            if any(nested_path.parent.parent.glob(f"*/{p}/*.parquet")):
                                partition_paths.append(f"'{nested_path}/*.parquet'")
                            elif any(direct_path.glob("*.parquet")):
                                partition_paths.append(f"'{direct_path}/*.parquet'")
                        
                        partition_paths_str = ", ".join(partition_paths)

                        sql = f"""
                        CREATE OR REPLACE VIEW {table_name} AS
                        SELECT * FROM read_parquet([{partition_paths_str}], union_by_name=true);
                        """
                        con.execute(sql)
                        
                        # 获取同步后的统计信息
                        after_stats = self._get_table_stats(con, table_name)
                        
                        # 显示详细的更新信息
                        time_info = f", 最后修改: {self._format_time_diff(file_info['last_modified'])}" if file_info['last_modified'] else ""
                        row_change = after_stats['row_count'] - before_stats['row_count']
                        row_change_str = f" (+{row_change})" if row_change > 0 else f" ({row_change})" if row_change < 0 else ""
                        
                        print(f"     ✓ 更新完成: {after_stats['row_count']:,} 行数据{row_change_str}, {after_stats['partition_count']} 个分区, {file_info['file_count']} 个文件{time_info}")
                        updated_count += 1
                    else:
                        # 显示跳过表的详细信息
                        time_info = f", 最后修改: {self._format_time_diff(file_info['last_modified'])}" if file_info['last_modified'] else ""
                        print(f"  -> 表 '{table_name}' 状态最新跳过 ({before_stats['row_count']:,} 行, {before_stats['partition_count']} 分区, {file_info['file_count']} 文件{time_info})")
                        skipped_count += 1

            print(f"\n元数据同步完成！更新: {updated_count}, 跳过: {skipped_count}。")
            # 保存缓存
            self._save_cache()

        except duckdb.Error as e:
            print(f"\n元数据同步过程中发生错误: {e}")
            raise
