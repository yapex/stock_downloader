"""
数据校验脚本

功能:
1.  扫描 `data` 目录下的所有 Parquet 文件，并报告其基本信息（是否可读、行数）。
2.  如果提供了股票代码，则详细显示该股票所有相关数据文件的摘要。

用法:
# 运行通用报告
python verify_data.py

# 检查特定股票
python verify_data.py --symbol 600519.SH
"""

import argparse
from pathlib import Path
import pandas as pd
from tabulate import tabulate
import sys
import re


def normalize_stock_code(code: str) -> str:
    """
    将股票代码标准化为 `代码.市场` 的格式，例如 `600519.SH`。

    - 支持纯数字、带市场前缀/后缀等多种格式，例如:
      - `600519` -> `600519.SH`
      - `SH600519` -> `600519.SH`
      - `600519SH` -> `600519.SH`
      - `sh600519` -> `600519.SH`
      - `000001.SZ` -> `000001.SZ`
    - 根据A股代码规则自动判断并添加 `.SH` (上海) 或 `.SZ` (深圳) 后缀。
    - 如果代码格式无法识别，则会抛出 ValueError。
    """
    if not isinstance(code, str):
        raise TypeError(f"股票代码必须是字符串，而不是 {type(code)}")

    # 提取6位数字代码
    match = re.search(r"(\d{6})", code)
    if not match:
        raise ValueError(f"无法从 '{code}' 中提取有效的6位股票代码")

    stock_number = match.group(1)

    # 根据前缀判断交易所
    if stock_number.startswith("6"):
        exchange = "SH"
    elif stock_number.startswith(("0", "3")):
        exchange = "SZ"
    elif stock_number.startswith(("8", "9")):
        exchange = "BJ"
    else:
        raise ValueError(f"无法识别的股票代码前缀: {stock_number}")

    return f"{stock_number}.{exchange}"


def generate_general_report(symbol: str = None):
    """
    生成数据文件的健康检查报告。
    如果提供了symbol参数，则只显示与该symbol相关的文件报告，并提供详细数据信息。
    """
    if symbol:
        print(f"--- 开始扫描股票代码为 '{symbol}' 的数据文件... ---")
    else:
        print("--- 开始扫描所有数据文件... ---")

    base_path = Path("data")
    if not base_path.exists():
        print(f"错误: 数据目录 '{base_path}' 不存在。")
        return

    if symbol:
        symbol = normalize_stock_code(symbol)
        # 只查找与指定symbol相关的文件
        all_files = sorted(list(base_path.rglob(f"{symbol}/*.parquet")))
        if not all_files:
            print(f"未找到与股票代码 '{symbol}' 相关的数据文件。")
            return
    else:
        # 查找所有parquet文件
        all_files = sorted(list(base_path.rglob("*.parquet")))
        if not all_files:
            print("未在 'data' 目录中找到任何 .parquet 文件。")
            return

    # 否则显示通用报告
    report_data = []
    headers = ["文件路径", "读取状态", "数据行数"]

    for file_path in all_files:
        try:
            df = pd.read_parquet(file_path)
            status = "✅ OK"
            row_count = len(df)
        except Exception as e:
            status = f"❌ FAIL: {e}"
            row_count = "N/A"

        report_data.append([file_path, status, row_count])

    print(f"\n--- 扫描完成，共找到 {len(all_files)} 个文件 ---\n")
    print(tabulate(report_data, headers=headers, tablefmt="grid"))


def main():
    """
    主函数，解析命令行参数并调用相应的功能。
    """
    # 设置 pandas 显示选项
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    parser = argparse.ArgumentParser(
        description="校验股票下载器的数据文件。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-s",
        "--symbol",
        type=str,
        help=(
            "指定要检查的股票代码 (例如: 600519.SH)。\n"
            "如果未提供，则会生成所有文件的通用报告。"
        ),
    )
    args = parser.parse_args()

    # 根据 Python 版本决定是否需要重置编码（主要用于Windows）
    if sys.platform == "win32":
        try:
            # Python 3.7+
            sys.stdout.reconfigure(encoding="utf-8")
        except TypeError:
            # 旧版本 Python
            import codecs

            sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

    # 使用修改后的generate_general_report函数处理所有情况

    generate_general_report(args.symbol)


if __name__ == "__main__":
    main()
