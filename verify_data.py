
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
import io
import sys


def generate_general_report():
    """
    生成所有数据文件的通用健康检查报告。
    """
    print("--- 开始扫描所有数据文件... ---")
    base_path = Path("data")
    if not base_path.exists():
        print(f"错误: 数据目录 '{base_path}' 不存在。")
        return

    all_files = sorted(list(base_path.rglob("*.parquet")))
    if not all_files:
        print("未在 'data' 目录中找到任何 .parquet 文件。")
        return

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


def generate_symbol_report(symbol: str):
    """
    为指定的股票代码生成详细的数据报告。
    """
    print(f"--- 开始检索股票代码为 '{symbol}' 的所有数据... ---")
    base_path = Path("data")
    if not base_path.exists():
        print(f"错误: 数据目录 '{base_path}' 不存在。")
        return

    # 使用 glob 模式匹配包含实体ID的路径
    symbol_files = sorted(list(base_path.rglob(f"entity={symbol}/*.parquet")))

    if not symbol_files:
        print(f"未找到与股票代码 '{symbol}' 相关的数据文件。")
        return

    print(f"\n--- 找到 {len(symbol_files)} 个相关文件 ---\n")

    for i, file_path in enumerate(symbol_files):
        print(f"--- ({i+1}/{len(symbol_files)}) 正在分析: {file_path} ---")
        
        try:
            df = pd.read_parquet(file_path)

            # 1. 打印数据结构 (Schema)
            buffer = io.StringIO()
            df.info(buf=buffer)
            schema_info = buffer.getvalue()
            print("\n[数据结构]\n" + schema_info)

            # 2. 智能打印数据预览
            # 如果列数过多，则转置表格以获得更好的终端可读性
            if df.shape[1] > 20:
                print("\n[数据预览 (前5条 - 已转置)]")
                # .T 用于转置DataFrame
                print(tabulate(df.head().T, tablefmt='psql'))
                
                print("\n[数据预览 (后5条 - 已转置)]")
                print(tabulate(df.tail().T, tablefmt='psql'))
            else:
                print("\n[数据预览 (前5条)]")
                print(tabulate(df.head(), headers='keys', tablefmt='psql', showindex=True))
                
                print("\n[数据预览 (后5条)]")
                print(tabulate(df.tail(), headers='keys', tablefmt='psql', showindex=True))

        except Exception as e:
            print(f"\n❌ 读取或分析文件时出错: {e}")
        
        print("\n" + "="*80 + "\n")


def main():
    """
    主函数，解析命令行参数并调用相应的功能。
    """
    # 设置 pandas 显示选项
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)

    parser = argparse.ArgumentParser(
        description="校验股票下载器的数据文件。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-s", "--symbol",
        type=str,
        help="指定要检查的股票代码 (例如: 600519.SH)。\n如果未提供，则会生成所有文件的通用报告。"
    )
    args = parser.parse_args()

    # 根据 Python 版本决定是否需要重置编码（主要用于Windows）
    if sys.platform == "win32":
        try:
            # Python 3.7+
            sys.stdout.reconfigure(encoding='utf-8')
        except TypeError:
            # 旧版本 Python
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())


    if args.symbol:
        generate_symbol_report(args.symbol)
    else:
        generate_general_report()


if __name__ == "__main__":
    main()
