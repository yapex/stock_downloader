import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

import neo.config as neo_config
import tushare
from typing import List, Union, Dict, Any
import pandas as pd
import argparse
from box import Box
import tomli_w


def generate_schema_toml(
    table_name: str,
    columns: Union[List[str], pd.Index],
    description: str,
    output_path: Union[str, Path],
) -> None:
    """
    生成表结构的 TOML 格式文件

    Args:
        table_name: 表名
        columns: 列名列表或 pandas Index
        description: 表描述
        output_path: 输出文件路径
    """
    # 确保 columns 是列表格式
    if hasattr(columns, "tolist"):
        columns_list = columns.tolist()
    else:
        columns_list = list(columns)

    # 格式化列名
    columns_formatted = ",\n".join(f'    "{col}"' for col in columns_list)

    # 构建 TOML 内容
    toml_content = f"""[{table_name}]
description = "{description}"
columns = [
{columns_formatted}
]
"""

    # 确保输出路径是 Path 对象
    path = Path(output_path)

    # 创建目录（如果不存在）
    path.parent.mkdir(parents=True, exist_ok=True)

    # 写入文件
    with open(path, "w", encoding="utf-8") as f:
        f.write(toml_content)

    print(f"Schema saved to {path}")
    print(f"Columns count: {len(columns_list)}")
    print(f"Columns: {columns_list}")


def generate_combined_schema_toml(
    all_schemas: Dict[str, Dict[str, Any]],
    output_path: Union[str, Path],
) -> None:
    """生成合并的 TOML 格式 schema 文件

    Args:
        all_schemas: 所有表的 schema 数据字典
        output_path: 输出文件路径
    """
    # 确保输出目录存在
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 构建 TOML 数据结构
    toml_data = {}

    for table_name, schema_data in all_schemas.items():
        # 创建有序字典，确保 table_name 和 primary_key 在最上面
        table_config = {}

        # 首先添加 table_name
        table_config["table_name"] = table_name

        # 然后添加其他配置字段（黑名单机制：排除 fields、output_file，但保留 api_method 和 default_params）
        if table_name in TABLE_CONFIGS:
            config = TABLE_CONFIGS[table_name]
            blacklist_fields = {"fields", "output_file"}

            for key, value in config.items():
                # 跳过 table_name（已添加）和黑名单字段
                if key != "table_name" and key not in blacklist_fields:
                    # 只添加非空值
                    if value:
                        # 对于 default_params，确保它是内联表格式
                        if key == "default_params" and isinstance(value, dict):
                            # 创建内联表格式的字典
                            table_config[key] = dict(value)
                        else:
                            table_config[key] = value

        # 最后添加其他字段
        table_config["description"] = schema_data["description"]

        # 合并字段名和类型信息
        if "column_types" in schema_data:
            # 创建包含字段名和类型的字典列表
            columns_with_types = []
            for col_name in schema_data["columns"]:
                col_type = schema_data["column_types"].get(col_name, "TEXT")
                columns_with_types.append({"name": col_name, "type": col_type})
            table_config["columns"] = columns_with_types
        else:
            # 如果没有类型信息，保持原有格式
            table_config["columns"] = schema_data["columns"]

        toml_data[table_name] = table_config

    # 手动构建 TOML 字符串以确保 default_params 是内联表格式
    toml_lines = []

    for table_name, table_config in toml_data.items():
        toml_lines.append(f"[{table_name}]")

        # 处理每个配置项
        for key, value in table_config.items():
            if key == "default_params" and isinstance(value, dict):
                # 将 default_params 写成内联表格式
                inline_params = ", ".join(
                    [
                        f'{k} = "{v}"' if isinstance(v, str) else f"{k} = {v}"
                        for k, v in value.items()
                    ]
                )
                toml_lines.append(f"default_params = {{ {inline_params} }}")
            elif isinstance(value, list) and value and isinstance(value[0], str):
                # 处理字符串数组（如 primary_key）
                formatted_list = (
                    "[\n    " + ",\n    ".join([f'"{item}"' for item in value]) + ",\n]"
                )
                toml_lines.append(f"{key} = {formatted_list}")
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # 处理对象数组（如 columns）
                toml_lines.append(f"{key} = [")
                for item in value:
                    item_str = ", ".join(
                        [
                            f'{k} = "{v}"' if isinstance(v, str) else f"{k} = {v}"
                            for k, v in item.items()
                        ]
                    )
                    toml_lines.append(f"    {{ {item_str} }},")
                toml_lines.append("]")
            elif isinstance(value, str):
                toml_lines.append(f'{key} = "{value}"')
            else:
                toml_lines.append(f"{key} = {value}")

        toml_lines.append("")  # 空行分隔表

    # 写入文件
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(toml_lines))

    print(f"已生成合并的 schema 文件: {output_path}")
    total_columns = sum(len(schema["columns"]) for schema in all_schemas.values())
    print(f"包含 {len(all_schemas)} 个表，共 {total_columns} 个字段")


# 表配置字典，使用 Box 包装以便简化访问
TABLE_CONFIGS = Box(
    {
        "stock_basic": {
            "table_name": "stock_basic",
            "primary_key": ["ts_code"],
            "description": "股票基本信息表字段",
            "api_method": "stock_basic",
            "default_params": {"ts_code": "600519.SH"},
            "fields": [],
            "output_file": "src/stock_schema.toml",
        },
        "stock_daily": {
            "table_name": "stock_daily",
            "primary_key": ["ts_code", "trade_date"],
            "date_col": "trade_date",
            "description": "股票日线数据字段",
            "api_method": "daily",
            "default_params": {"ts_code": "600519.SH", "trade_date": "20150726"},
            "fields": [],
            "output_file": "src/stock_schema.toml",
        },
        "stock_adj_qfq": {
            "table_name": "stock_adj_qfq",
            "primary_key": ["ts_code", "trade_date"],
            "date_col": "trade_date",
            "description": "复权行情数据字段",
            "api_method": "pro_bar",
            "default_params": {
                "ts_code": "600519.SH",
                "adj": "qfq",
                "start_date": "20240101",
                "end_date": "20240131",
            },
            "fields": [],
            "output_file": "src/stock_schema.toml",
        },
        "daily_basic": {
            "table_name": "daily_basic",
            "primary_key": ["ts_code", "trade_date"],
            "date_col": "trade_date",
            "description": "获取全部股票每日重要的基本面指标",
            "api_method": "daily_basic",
            "default_params": {"ts_code": "600519.SH", "trade_date": "20150726"},
            "fields": [],
            "output_file": "src/stock_schema.toml",
        },
        "income_statement": {
            "table_name": "income_statement",
            "primary_key": ["ts_code", "ann_date"],
            "date_col": "ann_date",
            "description": "利润表字段",
            "api_method": "income",
            "default_params": {"ts_code": "600519.SH", "period": "20241231"},
            "fields": [],
            "output_file": "src/stock_schema.toml",
        },
        "balance_sheet": {
            "table_name": "balance_sheet",
            "primary_key": ["ts_code", "ann_date"],
            "date_col": "ann_date",
            "description": "资产负债表字段",
            "api_method": "balancesheet",
            "default_params": {"ts_code": "600519.SH", "period": "20241231"},
            "fields": [],
            "output_file": "src/stock_schema.toml",
        },
        "cash_flow": {
            "table_name": "cash_flow",
            "primary_key": ["ts_code", "ann_date"],
            "date_col": "ann_date",
            "description": "现金流量表字段",
            "api_method": "cashflow",
            "default_params": {"ts_code": "600519.SH", "period": "20241231"},
            "fields": [],
            "output_file": "src/stock_schema.toml",
        },
    }
)


def get_tushare_api():
    """获取配置好的 tushare API 实例"""
    config = neo_config.get_config()
    tushare_token = config.tushare.token
    tushare.set_token(tushare_token)
    return tushare.pro_api()


def get_table_schema_data(pro, table_name: str, config: Box) -> Dict[str, Any]:
    """获取单个表的 schema 数据

    Args:
        pro: tushare API 实例
        table_name: 表名
        config: 表配置 Box 对象

    Returns:
        包含表名、描述和列名的字典
    """
    try:
        print(f"正在获取 {table_name} 表的 schema...")

        # 获取 API 方法
        api_method_name = config.api_method

        # 特殊处理 pro_bar，它是 tushare 模块的方法，不是 pro 实例的方法
        if api_method_name == "pro_bar":
            import tushare as ts

            api_method = ts.pro_bar
        else:
            api_method = getattr(pro, api_method_name)

        # 调用 API 获取数据
        if config.fields:
            # 如果配置中指定了字段，使用指定字段
            df = api_method(**config.default_params, fields=config.fields)
        else:
            # 如果没有指定字段，获取所有字段
            df = api_method(**config.default_params)

        # 获取列名和类型
        columns = df.columns.tolist()

        # 推断字段类型
        column_types = {}
        for col in columns:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                column_types[col] = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                column_types[col] = "REAL"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                column_types[col] = "DATE"
            elif pd.api.types.is_bool_dtype(dtype):
                column_types[col] = "BOOLEAN"
            else:
                # 默认为字符串类型
                column_types[col] = "TEXT"

        print(f"{table_name} 表包含 {len(columns)} 个字段: {columns}")
        print(f"字段类型: {column_types}")

        return {
            "table_name": table_name,
            "description": config.description,
            "columns": columns,
            "column_types": column_types,
        }

    except Exception as e:
        print(f"获取 {table_name} 表 schema 时出错: {e}")
        return None


def create_schemas(table_names: List[str] = None) -> None:
    """批量创建表 schema

    Args:
        table_names: 要创建的表名列表，如果为 None 则创建所有表
    """
    pro = get_tushare_api()

    # 如果没有指定表名，则创建所有表
    if table_names is None:
        table_names = list(TABLE_CONFIGS.keys())

    success_count = 0
    total_count = len(table_names)
    all_schemas = {}

    for table_name in table_names:
        if table_name not in TABLE_CONFIGS:
            print(f"警告: 未知的表名 '{table_name}'，跳过")
            continue

        config = TABLE_CONFIGS[table_name]
        schema_data = get_table_schema_data(pro, table_name, config)
        if schema_data:
            schema_dict = {
                "description": schema_data["description"],
                "columns": schema_data["columns"],
            }
            # 添加字段类型信息（如果存在）
            if "column_types" in schema_data:
                schema_dict["column_types"] = schema_data["column_types"]

            all_schemas[table_name] = schema_dict
            success_count += 1

    # 生成统一的 schema 文件
    if all_schemas:
        output_path = Path.cwd() / "stock_schema.toml"
        generate_combined_schema_toml(all_schemas, output_path)
        print(
            f"\n完成! 成功生成 {success_count}/{total_count} 个表的 schema，输出到 {output_path}"
        )
    else:
        print(f"\n失败! 没有成功获取任何表的 schema")


def main():
    """主函数：支持命令行参数的 schema 生成"""
    parser = argparse.ArgumentParser(description="生成 tushare 数据表的 schema 文件")
    parser.add_argument(
        "--tables",
        nargs="*",
        choices=list(TABLE_CONFIGS.keys()),
        help="指定要生成的表名，不指定则生成所有表",
    )
    parser.add_argument("--list", action="store_true", help="列出所有支持的表名")

    args = parser.parse_args()

    if args.list:
        print("支持的表名:")
        for table_name, config in TABLE_CONFIGS.items():
            print(f"  {table_name}: {config['description']}")
        return

    create_schemas(args.tables)


if __name__ == "__main__":
    main()
