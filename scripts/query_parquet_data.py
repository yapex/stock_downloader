"查询 Parquet 数据湖的命令行工具"

import ibis
import pandas as pd
import logging
import typer
import sys
from pathlib import Path
from typing import Optional

# 添加项目根目录到 Python 路径，以便导入 neo 包
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from neo.configs import get_config
from neo.helpers.utils import normalize_stock_code

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

app = typer.Typer(help="从 Parquet 数据湖中查询数据的工具。", no_args_is_help=True)


def get_db_path() -> Path:
    """从配置中获取元数据数据库的绝对路径"""
    config = get_config()
    metadata_db_path = Path(config.database.metadata_path)
    if not metadata_db_path.is_absolute():
        return project_root / metadata_db_path
    return metadata_db_path


@app.command()
def query(
    sql: Optional[str] = typer.Option(
        None,
        "--sql",
        help="要执行的原始 SQL 查询语句。如果使用此选项，将忽略其他所有参数。",
    ),
    table_name: Optional[str] = typer.Argument(
        None, help="要查询的表名 (例如: stock_daily)"
    ),
    symbol: Optional[str] = typer.Option(
        None, "-s", "--symbol", help="要查询的股票代码 (例如: 600519 或 000001.SZ)"
    ),
    limit: int = typer.Option(10, "-l", "--limit", help="要返回的最大行数"),
):
    """连接到元数据数据库，并根据条件查询指定的表。"""
    db_path = get_db_path()
    if not db_path.exists():
        logging.error(f"错误：元数据数据库不存在于路径: {db_path}")
        raise typer.Exit(1)

    logging.info(f"准备查询元数据数据库: {db_path}")

    try:
        # 使用 ibis 连接 DuckDB
        con = ibis.duckdb.connect(str(db_path), read_only=True)

        result_df = None

        if sql:
            logging.info("检测到 --sql 参数，将直接执行提供的 SQL 查询。")
            # 直接使用 ibis 底层的 duckdb 连接对象执行原始 SQL
            result_df = con.con.execute(sql).fetch_df()
        elif table_name:
            logging.info(f"成功连接数据库。查询表: {table_name}")

            # 检查表是否存在
            if table_name not in con.list_tables():
                logging.error(
                    f"错误：表 '{table_name}' 在元数据数据库中不存在。可用表: {con.list_tables()}"
                )
                raise typer.Exit(1)

            # 使用 Ibis API 构建查询表达式
            table_expr = con.table(table_name)

            if symbol:
                try:
                    normalized_symbol = normalize_stock_code(symbol)
                    logging.info(f"标准化股票代码: '{symbol}' -> '{normalized_symbol}'")

                    schema = table_expr.schema()
                    possible_cols = ["ts_code", "symbol"]
                    symbol_col = next(
                        (col for col in possible_cols if col in schema.names),
                        None,
                    )

                    if not symbol_col:
                        logging.error(
                            f"错误：在表 '{table_name}' 中未找到可用于查询股票代码的列。"
                        )
                        raise typer.Exit(1)

                    logging.info(
                        f"在表 '{table_name}' 中使用 '{symbol_col}' 列进行查询。"
                    )
                    # 使用 Ibis filter API
                    table_expr = table_expr.filter(
                        table_expr[symbol_col] == normalized_symbol
                    )

                except ValueError as e:
                    logging.error(f"错误：无效的股票代码 '{symbol}' - {e}")
                    raise typer.Exit(1)

            # 动态添加排序逻辑
            schema = table_expr.schema()
            possible_date_cols = [
                "trade_date",
                "ann_date",
                "end_date",
                "cal_date",
                "list_date",
            ]
            date_col_to_sort = next(
                (col for col in possible_date_cols if col in schema.names),
                None,
            )

            if date_col_to_sort:
                logging.info(f"将按最新日期列 '{date_col_to_sort}' 排序。")
                # 使用 Ibis order_by API
                table_expr = table_expr.order_by(ibis.desc(date_col_to_sort))

            # 使用 Ibis limit API
            table_expr = table_expr.limit(limit)

            # 执行 Ibis 表达式
            logging.info(f"执行 Ibis 表达式: \n{table_expr}")
            result_df = table_expr.execute()

        else:
            logging.error("错误：必须提供 --sql 参数或 table_name 参数。")
            raise typer.Exit(1)

        if result_df.empty:
            logging.warning("查询成功，但未返回任何数据。")
        else:
            pd.set_option("display.max_columns", None)
            pd.set_option("display.width", 200)
            print("\n--- 查询结果 ---")
            print(result_df)
            print("--- End ---")

    except Exception as e:
        logging.error(f"查询数据湖时发生错误: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
