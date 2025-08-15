from downloader2.config import get_config
from downloader2.db_connection import get_conn
from pathlib import Path
import duckdb
import logging
import tomllib
from box import Box
from typing import Dict, Optional
from enum import Enum


class TableName(Enum):
    """数据库表名枚举"""

    STOCK_BASIC = "stock_basic"
    STOCK_DAILY = "stock_daily"
    STOCK_ADJ_QFQ = "stock_adj_qfq"
    STOCK_ADJ_RAW = "stock_adj_raw"
    INCOME_STATEMENT = "income_statement"
    BALANCE_SHEET = "balance_sheet"
    CASH_FLOW = "cash_flow"


config = get_config()
logger = logging.getLogger(__name__)


class SchemaTableCreator:
    def __init__(self, schema_file_path: str = None, conn=get_conn):
        self.conn = conn
        schema_file_path = schema_file_path or (
            Path.cwd() / config.database.schema_file_path
        )
        with open(schema_file_path, "rb") as f:
            self.stock_schema = Box(tomllib.load(f))

    def _get_table_schema(self, table_name: str) -> Box:
        """获取表的schema"""
        return self.stock_schema[table_name]

    def _map_schema_type_to_duckdb(self, schema_type: str) -> str:
        """
        将schema中的类型映射到DuckDB类型

        Args:
            schema_type: schema中定义的类型（TEXT, REAL, INTEGER, DATE, BOOLEAN）

        Returns:
            DuckDB对应的数据类型
        """
        type_mapping = {
            "TEXT": "VARCHAR",
            "REAL": "DOUBLE",
            "INTEGER": "INTEGER",
            "DATE": "DATE",
            "BOOLEAN": "BOOLEAN",
        }
        return type_mapping.get(schema_type.upper(), "VARCHAR")

    def _build_column_definition(self, col) -> str:
        """
        构建单个列的定义

        Args:
            col: 列配置信息

        Returns:
            列定义字符串
        """
        if isinstance(col, dict) and "name" in col and "type" in col:
            # 新格式：{name: "字段名", type: "类型"}
            col_name = col["name"]
            col_type = col["type"]
            duckdb_type = self._map_schema_type_to_duckdb(col_type)
            return f"{col_name} {duckdb_type}"
        else:
            # 兼容旧格式：直接是字符串
            return f"{col} VARCHAR"

    def _build_columns_sql(self, columns) -> str:
        """
        构建所有列的SQL定义

        Args:
            columns: 列配置列表

        Returns:
            列定义SQL字符串
        """
        column_definitions = [self._build_column_definition(col) for col in columns]
        return ",\n    ".join(column_definitions)

    def _build_primary_key_sql(self, primary_key) -> str:
        """
        构建主键约束SQL

        Args:
            primary_key: 主键列列表

        Returns:
            主键约束SQL字符串
        """
        if not primary_key:
            return ""
        pk_columns = ", ".join(primary_key)
        return f",\n    PRIMARY KEY ({pk_columns})"

    def extract_column_names(self, columns) -> list:
        """
        从schema配置中提取列名
        
        Args:
            columns: 列配置，可以是字典或列表格式
            
        Returns:
            列名列表
        """
        if isinstance(columns, dict):
            return list(columns.keys())
        elif isinstance(columns, list):
            column_names = []
            for col in columns:
                if isinstance(col, dict):
                    if 'name' in col:
                        # 新格式：{name: "字段名", type: "类型"}
                        column_names.append(col['name'])
                    else:
                        # 旧格式：{"字段名": {"type": "类型"}}
                        column_names.extend(col.keys())
                elif isinstance(col, str):
                    column_names.append(col)
            return column_names
        else:
            raise ValueError(f"不支持的列配置格式: {type(columns)}")

    def _generate_create_table_sql(self, table_config: Box) -> str:
        """
        根据表配置生成CREATE TABLE SQL语句

        Args:
            table_config: 表配置信息

        Returns:
            CREATE TABLE SQL语句
        """
        table_name = table_config.table_name
        columns = table_config.columns
        primary_key = table_config.get("primary_key", [])

        columns_sql = self._build_columns_sql(columns)
        primary_key_sql = self._build_primary_key_sql(primary_key)

        sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql}
            {primary_key_sql}
            )
            """

        return sql

    def create_table(self, table_name: str) -> bool:
        """
        根据schema创建表

        Args:
            table_name: 表名

        Returns:
            创建是否成功
        """
        # 验证表名是否为有效的枚举值
        try:
            table_enum = TableName(table_name)
        except ValueError:
            logger.error(
                f"无效的表名: {table_name}，有效的表名: {[t.value for t in TableName]}"
            )
            return False

        if table_enum.value not in self.stock_schema:
            logger.error(f"表配置不存在: {table_name}")
            return False

        table_config = self.stock_schema[table_enum.value]
        sql = self._generate_create_table_sql(table_config)

        try:
            # 如果 self.conn 是函数，则调用它；如果是连接对象，则直接使用
            if callable(self.conn):
                with self.conn() as conn:
                    conn.execute(sql)
            else:
                self.conn.execute(sql)
            logger.info(f"表 {table_config.table_name} 创建成功")
            logger.debug(f"SQL: {sql}")
            return True
        except Exception as e:
            logger.error(f"表 {table_config.table_name} 创建失败: {e}")
            return False

    def create_all_tables(self) -> Dict[str, bool]:
        """
        创建所有表

        Returns:
            Dict[str, bool]: 每个表的创建结果，键为表名，值为创建是否成功
        """
        results = {}

        for table_enum in TableName:
            table_name = table_enum.value
            # 只创建在schema中存在配置的表
            if table_name in self.stock_schema:
                success = self.create_table(table_name)
                results[table_name] = success
                if not success:
                    logger.warning(f"表 {table_name} 创建失败")
            else:
                logger.warning(f"跳过表 {table_name}：在schema中未找到配置")
                results[table_name] = False

        successful_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        logger.info(f"表创建完成：成功 {successful_count}/{total_count} 个表")

        return results
