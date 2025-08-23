#!/usr/bin/env python3
"""
数据库表数据摘要展示脚本

用法:
    python show_data_summary.py                    # 显示所有表的数据摘要
    python show_data_summary.py --validate-fields  # 显示所有表的数据摘要并校验字段
    python show_data_summary.py -s 600519          # 显示指定股票代码在各表中的数据摘要
    python show_data_summary.py -v -s 600519       # 显示指定股票代码数据并校验字段
"""

import duckdb
import typer
from pathlib import Path
from typing import Optional, Dict, Any, List
import sys

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from neo.configs.app_config import get_config
from neo.database.schema_loader import SchemaLoader
from neo.helpers.utils import normalize_stock_code


def get_database_path() -> Path:
    """获取数据库文件路径"""
    project_root = Path(__file__).parent.parent
    config = get_config(project_root / "config.toml")
    db_path = config.database.path

    # 如果是相对路径，则相对于项目根目录
    if not Path(db_path).is_absolute():
        return project_root / db_path
    return Path(db_path)


def get_all_tables(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """获取数据库中所有表名"""
    result = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    return [row[0] for row in result]


def _map_schema_type_to_duckdb(schema_type: str) -> str:
    """将schema中的类型映射到DuckDB类型

    这个映射与 SchemaTableCreator 中的映射保持一致
    """
    type_mapping = {
        "TEXT": "VARCHAR",
        "REAL": "DOUBLE",
        "INTEGER": "INTEGER",
        "DATE": "DATE",
        "BOOLEAN": "BOOLEAN",
    }
    return type_mapping.get(schema_type.upper(), "VARCHAR")


def validate_table_schema(
    conn: duckdb.DuckDBPyConnection, table_name: str, schema_loader: SchemaLoader
) -> Dict[str, Any]:
    """校验表结构与 schema 定义的一致性"""
    try:
        # 获取数据库中的实际列信息
        columns_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        db_columns = {row[0]: row[1] for row in columns_result}

        # 获取 schema 定义
        try:
            schema = schema_loader.load_schema(table_name)
            schema_columns = {col["name"]: col["type"] for col in schema.columns}
        except KeyError:
            return {
                "table_name": table_name,
                "validation_status": "schema_not_found",
                "message": f"在 schema 配置中未找到表 '{table_name}' 的定义",
            }

        # 比较字段
        db_column_names = set(db_columns.keys())
        schema_column_names = set(schema_columns.keys())

        missing_in_db = schema_column_names - db_column_names
        extra_in_db = db_column_names - schema_column_names

        # 检查类型匹配（对于共同字段）
        type_mismatches = []
        common_columns = db_column_names & schema_column_names
        for col_name in common_columns:
            db_type = db_columns[col_name].upper()
            schema_type = schema_columns[col_name].upper()
            # 将 schema 类型映射到 DuckDB 类型进行比较
            expected_db_type = _map_schema_type_to_duckdb(schema_type)
            if db_type != expected_db_type:
                type_mismatches.append(
                    {
                        "column": col_name,
                        "db_type": db_type,
                        "schema_type": schema_type,
                        "expected_db_type": expected_db_type,
                    }
                )

        # 确定校验状态
        if not missing_in_db and not extra_in_db and not type_mismatches:
            validation_status = "valid"
            message = "表结构与 schema 定义完全匹配"
        else:
            validation_status = "mismatch"
            message = "表结构与 schema 定义存在差异"

        return {
            "table_name": table_name,
            "validation_status": validation_status,
            "message": message,
            "missing_in_db": list(missing_in_db),
            "extra_in_db": list(extra_in_db),
            "type_mismatches": type_mismatches,
            "total_columns_db": len(db_columns),
            "total_columns_schema": len(schema_columns),
        }

    except Exception as e:
        return {
            "table_name": table_name,
            "validation_status": "error",
            "message": f"校验过程中出错: {str(e)}",
        }


def get_table_summary(
    conn: duckdb.DuckDBPyConnection, table_name: str
) -> Dict[str, Any]:
    """获取表的基本摘要信息"""
    try:
        # 获取总行数
        count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        total_rows = count_result[0] if count_result else 0

        summary = {"table_name": table_name, "total_rows": total_rows}

        return summary
    except Exception as e:
        return {"table_name": table_name, "error": str(e), "total_rows": 0}


def get_symbol_data(
    conn: duckdb.DuckDBPyConnection, table_name: str, symbol: str
) -> Dict[str, Any]:
    """获取指定股票代码在表中的数据"""
    try:
        # 标准化股票代码
        try:
            normalized_symbol = normalize_stock_code(symbol)
        except (ValueError, TypeError) as e:
            return {
                "table_name": table_name,
                "symbol": symbol,
                "error": f"股票代码格式错误: {str(e)}",
                "rows": 0,
                "data": [],
            }

        # 尝试不同的股票代码列名
        possible_symbol_columns = ["symbol", "ts_code", "code", "stock_code"]

        # 获取表结构以确定实际的列名
        columns_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        available_columns = [row[0].lower() for row in columns_result]

        symbol_column = None
        for col in possible_symbol_columns:
            if col in available_columns:
                symbol_column = col
                break

        if not symbol_column:
            return {
                "table_name": table_name,
                "symbol": symbol,
                "message": "未找到股票代码列",
                "rows": 0,
                "data": [],
            }

        # 根据列名决定使用哪个版本的股票代码
        search_symbol = normalized_symbol if symbol_column == "ts_code" else symbol

        # 查询指定股票的数据（只显示最新一条）
        # 尝试按日期列排序，如果没有日期列则直接取第一条
        date_columns = ["trade_date", "end_date", "ann_date", "f_ann_date"]
        order_clause = ""
        for date_col in date_columns:
            if date_col in available_columns:
                order_clause = f" ORDER BY {date_col} DESC"
                break

        query = f"SELECT * FROM {table_name} WHERE {symbol_column} = ?{order_clause} LIMIT 1"
        result = conn.execute(query, [search_symbol]).fetchall()

        # 获取该股票的总行数
        count_query = f"SELECT COUNT(*) FROM {table_name} WHERE {symbol_column} = ?"
        count_result = conn.execute(count_query, [search_symbol]).fetchone()
        total_rows = count_result[0] if count_result else 0

        return {
            "table_name": table_name,
            "symbol": symbol,
            "symbol_column": symbol_column,
            "rows": total_rows,
            "data": result,
        }
    except Exception as e:
        return {
            "table_name": table_name,
            "symbol": symbol,
            "error": str(e),
            "rows": 0,
            "data": [],
        }


def print_validation_result(validation: Dict[str, Any]):
    """打印字段校验结果"""
    print(f"\n{'=' * 60}")
    print(f"表名: {validation['table_name']}")

    status = validation["validation_status"]
    if status == "valid":
        print(f"✅ {validation['message']}")
        print(f"字段数量: {validation['total_columns_db']}")
    elif status == "mismatch":
        print(f"⚠️  {validation['message']}")
        print(f"数据库字段数: {validation['total_columns_db']}")
        print(f"Schema 字段数: {validation['total_columns_schema']}")

        if validation["missing_in_db"]:
            print(f"\n❌ 数据库中缺少的字段 ({len(validation['missing_in_db'])}):")
            for col in validation["missing_in_db"]:
                print(f"  - {col}")

        if validation["extra_in_db"]:
            print(f"\n➕ 数据库中多余的字段 ({len(validation['extra_in_db'])}):")
            for col in validation["extra_in_db"]:
                print(f"  - {col}")

        if validation["type_mismatches"]:
            print(f"\n🔄 类型不匹配的字段 ({len(validation['type_mismatches'])}):")
            for mismatch in validation["type_mismatches"]:
                expected_type = mismatch.get("expected_db_type", "N/A")
                print(
                    f"  - {mismatch['column']}: DB({mismatch['db_type']}) vs Schema({mismatch['schema_type']}) -> Expected({expected_type})"
                )

    elif status == "schema_not_found":
        print(f"❓ {validation['message']}")
    elif status == "error":
        print(f"❌ {validation['message']}")


def print_table_summary(summary: Dict[str, Any], table_name: str = None):
    """打印表摘要信息"""
    if "error" in summary:
        print(f"❌ 错误: {summary['error']}")
        return

    if table_name:
        print(f"表名: {table_name} | 总行数: {summary['total_rows']:,}")
    else:
        print(f"总行数: {summary['total_rows']:,}")


def print_symbol_data(data: Dict[str, Any]):
    """打印指定股票的数据"""
    print(f"\n{'=' * 60}")
    print(f"表名: {data['table_name']} | 股票代码: {data['symbol']}")

    if "error" in data:
        print(f"❌ 错误: {data['error']}")
        return

    if "message" in data:
        print(f"ℹ️  {data['message']}")
        return

    print(f"匹配列: {data['symbol_column']}")
    print(f"数据行数: {data['rows']:,}")

    if data["data"]:
        print("\n最新数据:")
        for i, row in enumerate(data["data"], 1):
            print(f"  {row}")
    elif data["rows"] == 0:
        print("\n📝 未找到该股票的数据")


app = typer.Typer(help="展示数据库表数据摘要")


@app.command()
def main(
    symbol: Optional[str] = typer.Option(
        None, "-s", "--symbol", help="指定股票代码，例如: 600519 或 600519.SH"
    ),
    validate_fields: bool = typer.Option(
        False, "--validate-fields", "-v", help="启用字段校验功能"
    ),
):
    """
    展示数据库中各表的数据摘要

    不指定参数时显示所有表的基本信息。
    指定股票代码时显示该股票在各表中的详细数据。
    使用 --validate-fields 参数启用字段校验功能。
    """
    db_path = get_database_path()

    if not db_path.exists():
        typer.echo(f"❌ 数据库文件不存在: {db_path}", err=True)
        raise typer.Exit(1)

    typer.echo(f"📊 数据库路径: {db_path}")

    # 初始化 SchemaLoader
    try:
        schema_loader = SchemaLoader()
        typer.echo(f"📋 Schema 配置路径: {schema_loader.schema_file_path}")
    except Exception as e:
        typer.echo(f"❌ 加载 Schema 配置失败: {e}", err=True)
        raise typer.Exit(1)

    try:
        with duckdb.connect(str(db_path)) as conn:
            tables = get_all_tables(conn)

            if not tables:
                typer.echo("📝 数据库中没有找到任何表")
                return

            typer.echo(f"\n🔍 找到 {len(tables)} 个表: {', '.join(tables)}")

            if symbol:
                typer.echo(f"\n🎯 查询股票代码: {symbol}")
                for table in tables:
                    symbol_data = get_symbol_data(conn, table, symbol)
                    print_symbol_data(symbol_data)
            else:
                if validate_fields:
                    typer.echo("\n🔍 字段校验结果:")

                    # 统计校验结果
                    valid_count = 0
                    mismatch_count = 0
                    not_found_count = 0
                    error_count = 0

                    for table in tables:
                        validation = validate_table_schema(conn, table, schema_loader)
                        print_validation_result(validation)

                        # 显示数据行数
                        summary = get_table_summary(conn, table)
                        print_table_summary(summary, table)

                        # 统计
                        status = validation["validation_status"]
                        if status == "valid":
                            valid_count += 1
                        elif status == "mismatch":
                            mismatch_count += 1
                        elif status == "schema_not_found":
                            not_found_count += 1
                        elif status == "error":
                            error_count += 1

                    # 显示总体统计
                    typer.echo(f"\n{'=' * 60}")
                    typer.echo("📊 校验结果统计:")
                    typer.echo(f"  ✅ 完全匹配: {valid_count}")
                    typer.echo(f"  ⚠️  存在差异: {mismatch_count}")
                    typer.echo(f"  ❓ 未找到配置: {not_found_count}")
                    typer.echo(f"  ❌ 校验错误: {error_count}")
                    typer.echo(f"  📋 总表数: {len(tables)}")
                else:
                    typer.echo("\n📊 表数据摘要:")
                    for table in tables:
                        summary = get_table_summary(conn, table)
                        print_table_summary(summary, table)

                    typer.echo("\n💡 提示: 使用 --validate-fields 参数启用字段校验功能")

    except Exception as e:
        typer.echo(f"❌ 连接数据库时出错: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
