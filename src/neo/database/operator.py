"""Parquet 数据库操作器

基于 DuckDB 和 Parquet 文件的数据库操作器实现。
"""

import logging
import duckdb
from pathlib import Path
from typing import Dict, List, Optional
from functools import lru_cache
from datetime import datetime

from .schema_loader import SchemaLoader
from .interfaces import IDBQueryer, ISchemaLoader
from ..configs import get_config

logger = logging.getLogger(__name__)


class ParquetDBQueryer(IDBQueryer):
    """基于 Parquet 文件的数据库查询器

    使用 DuckDB 查询 Parquet 数据湖，专门负责数据查询操作。
    不支持数据写入，保持数据湖的只读特性。
    """

    def __init__(self, schema_loader: ISchemaLoader, parquet_base_path: str = None):
        """初始化 Parquet 数据库查询器

        Args:
            schema_loader: 数据库模式加载器
            parquet_base_path: Parquet 文件的基础路径
        """
        if parquet_base_path is None:
            config = get_config()
            parquet_base_path = config.get("storage.parquet_base_path", "data/parquet")

        self.parquet_base_path = Path(parquet_base_path)
        self.schema_loader = schema_loader

    @classmethod
    def create_default(cls) -> "ParquetDBQueryer":
        """创建默认的 Parquet 数据库查询器实例

        使用默认配置创建 ParquetDBQueryer 实例。

        Returns:
            ParquetDBQueryer: 默认配置的 Parquet 数据库查询器实例
        """
        config = get_config()
        parquet_base_path = config.get("storage.parquet_base_path", "data/parquet")
        schema_loader = SchemaLoader()
        return cls(schema_loader=schema_loader, parquet_base_path=parquet_base_path)

    def _table_exists_in_schema(self, table_key: str) -> bool:
        """检查表是否在 schema 中存在

        Args:
            table_key: 表在schema配置中的键名

        Returns:
            表是否存在
        """
        try:
            self.schema_loader.load_schema(table_key)
            return True
        except KeyError:
            return False

    def _get_table_config(self, table_key: str):
        """获取表配置

        Args:
            table_key: 表在schema配置中的键名

        Returns:
            表配置对象
        """
        return self.schema_loader.load_schema(table_key)

    def _get_parquet_path_pattern(self, table_name: str) -> str:
        """获取 Parquet 文件路径模式

        Args:
            table_name: 表名

        Returns:
            Parquet 文件路径模式
        """
        table_path = self.parquet_base_path / table_name
        # 使用 DuckDB 的通配符模式匹配所有分区
        return str(table_path / "**" / "*.parquet")

    def _parquet_files_exist(self, table_name: str) -> bool:
        """检查 Parquet 文件是否存在

        Args:
            table_name: 表名

        Returns:
            Parquet 文件是否存在
        """
        table_path = self.parquet_base_path / table_name
        if not table_path.exists():
            return False

        # 检查是否有 .parquet 文件
        return any(table_path.rglob("*.parquet"))

    def get_max_date(self, table_key: str, ts_codes: List[str]) -> Dict[str, str]:
        """根据 schema 中定义的 date_col，查询指定表中一个或多个股票的最新日期

        Args:
            table_key: 表在schema配置中的键名 (e.g., 'stock_daily')
            ts_codes: 股票代码列表

        Returns:
            一个字典，key为股票代码，value为对应的最新日期 (YYYYMMDD格式字符串)
        """
        if not self._table_exists_in_schema(table_key):
            logger.warning(f"表配置 '{table_key}' 不存在于 schema 中")
            return {}

        table_config = self._get_table_config(table_key)
        table_name = table_config.table_name

        if not hasattr(table_config, "date_col") or not table_config.date_col:
            logger.debug(f"表 '{table_name}' 未定义 date_col 字段，无法查询最大日期")
            return {}

        # 检查 Parquet 文件是否存在
        if not self._parquet_files_exist(table_name):
            logger.debug(f"表 '{table_name}' 的 Parquet 文件不存在，返回空结果")
            return {}

        date_col = table_config.date_col
        parquet_pattern = self._get_parquet_path_pattern(table_name)
        primary_key = table_config.primary_key

        try:
            # 使用 DuckDB 查询 Parquet 文件
            conn = duckdb.connect(":memory:")

            # 检查表是否有 ts_code 字段（股票相关表）
            if "ts_code" in primary_key:
                # 标准化股票代码格式（例如：600519 -> 600519.SH）
                from ..helpers.utils import normalize_stock_code

                normalized_codes = [normalize_stock_code(code) for code in ts_codes]

                # 查询指定股票列表的最新日期
                placeholders = ", ".join([f"'{code}'" for code in normalized_codes])
                sql = f"""
                    SELECT ts_code, MAX({date_col}) as max_date 
                    FROM read_parquet('{parquet_pattern}') 
                    WHERE ts_code IN ({placeholders}) 
                    GROUP BY ts_code
                """

                results = conn.execute(sql).fetchall()
                conn.close()

                # 创建标准化代码到原始代码的映射
                code_mapping = {normalize_stock_code(code): code for code in ts_codes}

                max_dates = {}
                for row in results:
                    if row[1] is not None:
                        # 将标准化的股票代码映射回原始格式
                        original_code = code_mapping.get(row[0], row[0])
                        max_dates[original_code] = str(row[1])

                logger.debug(
                    f"查询表 '{table_name}' 最大日期成功，返回 {len(max_dates)} 条记录"
                )
                return max_dates
            else:
                # 对于非股票表（如 trade_cal），直接查询最大日期
                sql = f"""
                    SELECT MAX({date_col}) as max_date 
                    FROM read_parquet('{parquet_pattern}')
                """

                result = conn.execute(sql).fetchone()
                conn.close()

                if result and result[0] is not None:
                    max_date = str(result[0])
                    # 对于非股票表，为所有请求的代码返回相同的最大日期
                    max_dates = {code: max_date for code in ts_codes}
                    logger.debug(
                        f"查询表 '{table_name}' 最大日期成功，最大日期为 {max_date}"
                    )
                    return max_dates
                else:
                    logger.debug(f"表 '{table_name}' 中没有找到有效的日期数据")
                    return {}

        except Exception as e:
            logger.error(f"❌ 查询表 '{table_name}' Parquet 文件最大日期失败: {e}")
            # 在出错时返回空字典，而不是抛出异常，保持与原有逻辑一致
            return {}

    @lru_cache(maxsize=1)
    def get_all_symbols(self) -> List[str]:
        """获取所有股票代码

        从 stock_basic 表的 Parquet 文件中获取所有股票代码。

        Returns:
            股票代码列表
        """
        table_key = "stock_basic"

        if not self._table_exists_in_schema(table_key):
            logger.warning(f"表配置 '{table_key}' 不存在于 schema 中")
            return []

        table_config = self._get_table_config(table_key)
        table_name = table_config.table_name

        # 检查 Parquet 文件是否存在
        if not self._parquet_files_exist(table_name):
            logger.debug(f"表 '{table_name}' 的 Parquet 文件不存在，返回空列表")
            return []

        parquet_pattern = self._get_parquet_path_pattern(table_name)

        try:
            # 使用 DuckDB 查询 Parquet 文件
            conn = duckdb.connect(":memory:")

            sql = f"""
                SELECT DISTINCT ts_code 
                FROM read_parquet('{parquet_pattern}') 
                WHERE ts_code IS NOT NULL AND ts_code != ''
            """

            results = conn.execute(sql).fetchall()
            conn.close()

            # 提取 ts_code 列表，过滤空值
            ts_codes = [
                row[0] for row in results if row[0] is not None and row[0] != ""
            ]
            logger.debug(
                f"从表 '{table_name}' Parquet 文件查询到 {len(ts_codes)} 个股票代码"
            )
            return ts_codes

        except Exception as e:
            logger.error(f"❌ 查询表 '{table_name}' Parquet 文件的 ts_code 失败: {e}")
            return []

    @lru_cache(maxsize=1)
    def get_latest_trading_day(self, exchange: str = "SSE") -> Optional[str]:
        """获取最近的交易日

        如果今天是交易日，返回今天；否则返回上一个交易日。
        结果会被缓存，在单次运行中只查询一次。

        Args:
            exchange: 交易所代码，默认为 'SSE'

        Returns:
            最近交易日的字符串 (YYYYMMDD)，如果查询失败则返回 None
        """
        table_key = "trade_cal"
        if not self._table_exists_in_schema(table_key):
            logger.warning(f"表配置 '{table_key}' 不存在于 schema 中")
            return None

        table_config = self._get_table_config(table_key)
        table_name = table_config.table_name

        if not self._parquet_files_exist(table_name):
            logger.warning(f"表 '{table_name}' 的 Parquet 文件不存在，无法确定交易日")
            return None

        parquet_pattern = self._get_parquet_path_pattern(table_name)
        today_str = datetime.now().strftime("%Y%m%d")
        conn = None
        try:
            conn = duckdb.connect(":memory:")

            # 1. 尝试查询今天的日期
            sql_today = f"""
                SELECT is_open, pretrade_date
                FROM read_parquet('{parquet_pattern}')
                WHERE cal_date = '{today_str}' AND exchange = '{exchange}'
                LIMIT 1
            """
            result = conn.execute(sql_today).fetchone()

            if result:
                is_open, pretrade_date = result
                if is_open == 1:
                    logger.debug(f"今天是交易日: {today_str}")
                    return today_str
                else:
                    logger.debug(
                        f"今天 ({today_str}) 非交易日，返回上一个交易日: {pretrade_date}"
                    )
                    return str(pretrade_date)
            else:
                # 2. 如果今天的数据不存在，则查询表中最新的一个交易日作为备用
                logger.debug(
                    f"交易日历表中未找到今天 ({today_str}) 的数据，将返回记录中最新的交易日。"
                )
                sql_fallback = f"""
                    SELECT MAX(cal_date)
                    FROM read_parquet('{parquet_pattern}')
                    WHERE is_open = 1 AND exchange = '{exchange}'
                """
                fallback_result = conn.execute(sql_fallback).fetchone()
                if fallback_result and fallback_result[0]:
                    latest_day = str(fallback_result[0])
                    logger.debug(f"回退查询成功，返回最新交易日: {latest_day}")
                    return latest_day
                else:
                    logger.error(f"无法在表中找到任何交易日数据，交易所: {exchange}")
                    return None

        except Exception as e:
            logger.error(f"❌ 查询最近交易日失败: {e}", exc_info=True)
            return None
        finally:
            if conn:
                conn.close()
