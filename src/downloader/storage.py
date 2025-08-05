import pandas as pd
from pathlib import Path
from .utils import normalize_stock_code
import logging

logger = logging.getLogger(__name__)


class ParquetStorage:
    """
    一个纯粹、健壮的 Parquet 存储器。
    支持增量保存(save)和全量覆盖(overwrite)两种模式。
    """

    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path)
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"存储根目录已创建: {self.base_path.resolve()}")

    def _get_file_path(self, data_type: str, entity_id: str) -> Path:
        """
        根据数据类型和实体ID构建文件路径。
        对于系统相关数据，不进行股票代码标准化。
        """
        if data_type == "system":
            # 系统数据不需要股票代码标准化
            return self.base_path / data_type / f"{entity_id}.parquet"
        else:
            # 股票数据需要进行标准化处理
            normalized_id = normalize_stock_code(entity_id)
            return self.base_path / data_type / normalized_id / "data.parquet"

    def get_latest_date(
        self, data_type: str, entity_id: str, date_col: str
    ) -> str | None:
        """获取本地存储的最新日期。支持新格式和旧格式的路径。"""
        # Try new format first
        file_path = self._get_file_path(data_type, entity_id)
        if file_path.exists():
            try:
                df = pd.read_parquet(file_path, engine="pyarrow", columns=[date_col])
                if date_col in df.columns and not df.empty:
                    return df[date_col].max()
            except Exception as e:
                logger.error(f"读取文件 {file_path} 以获取最新日期时出错: {e}")

        # Try legacy format if new format doesn't exist or failed
        if data_type != "system":
            # For stock data, also check legacy path format: entity=entity_id
            normalized_id = normalize_stock_code(entity_id)
            legacy_path = (
                self.base_path / data_type / f"entity={normalized_id}" / "data.parquet"
            )
            if legacy_path.exists():
                try:
                    df = pd.read_parquet(
                        legacy_path, engine="pyarrow", columns=[date_col]
                    )
                    if date_col in df.columns and not df.empty:
                        return df[date_col].max()
                except Exception as e:
                    logger.error(
                        f"读取遗留格式文件 {legacy_path} 以获取最新日期时出错: {e}"
                    )

        return None

    def save(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str):
        """将 DataFrame 增量保存到 Parquet 文件中。"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return

        if date_col not in df.columns:
            logger.error(
                f"[{data_type}/{entity_id}] DataFrame 中缺少日期列 '{date_col}'，"
                "无法增量保存。"
            )
            return

        file_path = self._get_file_path(data_type, entity_id)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            combined_df = df
            if file_path.exists():
                existing_df = pd.read_parquet(file_path, engine="pyarrow")
                # 检查是否需要合并数据
                if not existing_df.empty:
                    # 只有当新数据不为空时才合并
                    if not df.empty:
                        # 使用 pd.concat 前确保不会触发 FutureWarning
                        # 通过显式过滤掉空的或全NA的DataFrame来避免警告
                        dfs_to_concat = []
                        if not existing_df.empty:
                            dfs_to_concat.append(existing_df)
                        if not df.empty:
                            dfs_to_concat.append(df)
                        
                        if dfs_to_concat:
                            combined_df = pd.concat(dfs_to_concat, ignore_index=True)
                        else:
                            combined_df = pd.DataFrame()
                    else:
                        # 新数据为空，使用现有数据
                        combined_df = existing_df
                # 如果 existing_df 为空但 df 不为空，则 combined_df 保持为原始 df
                
                # 只有在 combined_df 不为空时才去重
                if not combined_df.empty:
                    combined_df.drop_duplicates(
                        subset=[date_col], keep="last", inplace=True
                    )

            # 只有在 combined_df 不为空时才排序
            if not combined_df.empty:
                combined_df.sort_values(by=date_col, inplace=True, ignore_index=True)
            combined_df.to_parquet(file_path, engine="pyarrow", index=False)
            logger.debug(
                f"[{data_type}/{entity_id}] 数据已成功增量保存，"
                f"总计 {len(combined_df)} 条。"
            )

        except Exception as e:
            logger.error(
                f"[{data_type}/{entity_id}] 增量保存到 Parquet 文件 {file_path} "
                f"时发生错误: {e}"
            )

    def overwrite(self, df: pd.DataFrame, data_type: str, entity_id: str):
        """将 DataFrame 全量覆盖写入 Parquet 文件。"""
        if not isinstance(df, pd.DataFrame):
            logger.warning(
                f"[{data_type}/{entity_id}] 传入的不是DataFrame，跳过覆盖操作。"
            )
            return

        file_path = self._get_file_path(data_type, entity_id)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            df.to_parquet(file_path, engine="pyarrow", index=False)
            logger.debug(
                f"[{data_type}/{entity_id}] 数据已成功全量覆盖，总计 {len(df)} 条。"
            )
        except Exception as e:
            logger.error(
                f"[{data_type}/{entity_id}] 全量覆盖到 Parquet 文件 {file_path} "
                f"时发生错误: {e}"
            )


import duckdb

class DuckDBStorage:
    """
    使用 DuckDB 进行数据存储，提供高效的增量更新和查询功能。
    
    接口等同于 ParquetStorage。
    支持增量保存(save)和全量覆盖(overwrite)两种模式。
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(database=str(self.db_path), read_only=False)
        logger.debug(f"DuckDB 数据库已连接: {self.db_path.resolve()}")

    def _get_table_name(self, data_type: str, entity_id: str) -> str:
        """根据数据类型和实体ID生成一个合法的 DuckDB 表名。"""
        # 移除非法字符
        safe_entity_id = "".join(c if c.isalnum() else "_" for c in entity_id)
        return f"{data_type}_{safe_entity_id}"

    def get_latest_date(
        self, data_type: str, entity_id: str, date_col: str
    ) -> str | None:
        """从指定的表中获取最新的日期。"""
        table_name = self._get_table_name(data_type, entity_id)
        try:
            # 检查表是否存在
            tables = self.conn.execute("SHOW TABLES;").fetchall()
            if (table_name,) not in tables:
                logger.debug(f"表 {table_name} 不存在，无法获取最新日期。")
                return None

            # 检查日期列是否存在
            columns_info = self.conn.execute(f"PRAGMA table_info('{table_name}');").fetchall()
            column_names = [info[1] for info in columns_info]
            if date_col not in column_names:
                logger.warning(f"表 {table_name} 中不存在日期列 {date_col}。")
                return None

            result = self.conn.execute(
                f"SELECT MAX({date_col}) FROM {table_name}"
            ).fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"从表 {table_name} 获取最新日期时出错: {e}")
            return None

    def save(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str):
        """
        将 DataFrame 数据增量保存（UPSERT）到 DuckDB 表中。
        如果日期已存在，则更新记录；如果不存在，则插入新记录。
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.debug(f"[{data_type}/{entity_id}] DataFrame 为空，跳过保存操作。")
            return

        if date_col not in df.columns:
            logger.error(
                f"[{data_type}/{entity_id}] DataFrame 中缺少日期列 '{date_col}'，"
                "无法增量保存。"
            )
            return

        table_name = self._get_table_name(data_type, entity_id)
        
        try:
            # 1. 创建或确保表存在
            # 使用 IF NOT EXISTS 避免在表已存在时出错
            # 这里只创建表结构，不插入数据
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")

            # 2. 使用 INSERT OR REPLACE (UPSERT) 语义
            # DuckDB v0.7.0+ 支持 ON CONFLICT DO UPDATE
            # 为简化，我们这里使用更通用的先删除后插入的逻辑，效果相同
            
            # 先删除冲突的旧数据
            min_date = df[date_col].min()
            self.conn.execute(f"DELETE FROM {table_name} WHERE {date_col} >= ?", [min_date])
            
            # 插入新数据
            self.conn.from_df(df).insert_into(table_name)

            logger.debug(
                f"[{data_type}/{entity_id}] 数据已成功增量保存到表 {table_name}。"
            )

        except Exception as e:
            logger.error(
                f"[{data_type}/{entity_id}] 增量保存到 DuckDB 表 {table_name} "
                f"时发生错误: {e}"
            )

    def overwrite(self, df: pd.DataFrame, data_type: str, entity_id: str):
        """将 DataFrame 数据全量覆盖写入 DuckDB 表。"""
        if not isinstance(df, pd.DataFrame):
            logger.warning(
                f"[{data_type}/{entity_id}] 传入的不是DataFrame，跳过覆盖操作。"
            )
            return

        table_name = self._get_table_name(data_type, entity_id)
        try:
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            logger.debug(
                f"[{data_type}/{entity_id}] 数据已成功全量覆盖到表 {table_name}，"
                f"总计 {len(df)} 条。"
            )
        except Exception as e:
            logger.error(
                f"[{data_type}/{entity_id}] 全量覆盖到 DuckDB 表 {table_name} "
                f"时发生错误: {e}"
            )


__all__ = ["ParquetStorage", "DuckDBStorage"]
