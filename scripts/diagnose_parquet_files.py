#!/usr/bin/env python3
"""诊断实际parquet文件质量的脚本"""

import os
import sys
import duckdb
import pandas as pd
from pathlib import Path
import typer
from typing_extensions import Annotated

# 添加项目路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

app = typer.Typer(help="诊断parquet文件质量")


def check_single_parquet_file(file_path: Path):
    """检查单个parquet文件的质量"""
    result = {
        "file": str(file_path),
        "exists": file_path.exists(),
        "size": 0,
        "pandas_readable": False,
        "duckdb_readable": False,
        "row_count": 0,
        "error": None
    }
    
    if not file_path.exists():
        result["error"] = "文件不存在"
        return result
    
    result["size"] = file_path.stat().st_size
    
    # 测试pandas读取
    try:
        df = pd.read_parquet(file_path)
        result["pandas_readable"] = True
        result["row_count"] = len(df)
    except Exception as e:
        result["error"] = f"Pandas读取失败: {e}"
    
    # 测试DuckDB读取
    try:
        with duckdb.connect() as con:
            count_result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path}')").fetchone()
            result["duckdb_readable"] = True
            if not result["pandas_readable"]:  # 如果pandas失败了，用DuckDB的结果
                result["row_count"] = count_result[0]
    except Exception as e:
        if result["error"]:
            result["error"] += f" | DuckDB读取失败: {e}"
        else:
            result["error"] = f"DuckDB读取失败: {e}"
    
    return result


def scan_table_parquet_files(table_path: Path, sample_size: int = None):
    """扫描表目录下的所有parquet文件"""
    if not table_path.is_dir():
        print(f"错误：{table_path} 不是有效目录")
        return []
    
    # 找到所有parquet文件
    parquet_files = list(table_path.rglob("*.parquet"))
    
    if sample_size and len(parquet_files) > sample_size:
        import random
        parquet_files = random.sample(parquet_files, sample_size)
        print(f"从 {len(list(table_path.rglob('*.parquet')))} 个文件中随机抽样 {sample_size} 个")
    
    print(f"检查 {len(parquet_files)} 个parquet文件...")
    
    results = []
    good_files = 0
    bad_files = 0
    
    for i, file_path in enumerate(parquet_files, 1):
        print(f"检查 [{i}/{len(parquet_files)}]: {file_path.relative_to(table_path)}", end=" ... ")
        
        result = check_single_parquet_file(file_path)
        results.append(result)
        
        if result["error"]:
            print(f"❌ {result['error']}")
            bad_files += 1
        else:
            print(f"✅ {result['row_count']} 行, {result['size']} 字节")
            good_files += 1
    
    print(f"\n总结：✅ {good_files} 个正常，❌ {bad_files} 个异常")
    
    # 显示异常文件详情
    if bad_files > 0:
        print("\n异常文件详情：")
        for result in results:
            if result["error"]:
                print(f"  {result['file']}: {result['error']}")
    
    return results


def test_duckdb_batch_read(table_path: Path, partition_pattern: str = None):
    """测试DuckDB批量读取文件"""
    print(f"\n=== 测试DuckDB批量读取 ===")
    
    if partition_pattern:
        # 测试特定分区
        pattern_path = table_path / partition_pattern
        print(f"测试分区模式: {pattern_path}")
    else:
        # 测试所有文件
        pattern_path = table_path / "**/*.parquet"
        print(f"测试所有文件模式: {pattern_path}")
    
    try:
        with duckdb.connect() as con:
            result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{pattern_path}')").fetchone()
            total_rows = result[0]
            print(f"✅ DuckDB批量读取成功，总行数: {total_rows}")
            
            # 尝试按分区分组
            try:
                partition_result = con.execute(f"""
                SELECT 
                    regexp_extract(filename, 'year=(\\d{{4}})', 1) as year_partition,
                    COUNT(*) as row_count
                FROM read_parquet('{pattern_path}', filename=true) 
                WHERE filename IS NOT NULL
                GROUP BY year_partition 
                ORDER BY year_partition
                """).fetchall()
                
                if partition_result:
                    print("按分区统计：")
                    for year, count in partition_result:
                        print(f"  year={year}: {count} 行")
                
            except Exception as e:
                print(f"分区统计失败: {e}")
            
            return True
            
    except Exception as e:
        print(f"❌ DuckDB批量读取失败: {e}")
        return False


@app.command()
def diagnose(
    table_name: Annotated[str, typer.Argument(help="要诊断的表名")] = "stock_adj_hfq",
    parquet_dir: Annotated[str, typer.Option(help="Parquet数据目录")] = None,
    sample_size: Annotated[int, typer.Option(help="抽样文件数量，0表示全部检查")] = 20,
    test_stocks: Annotated[str, typer.Option(help="测试特定股票代码，用逗号分隔")] = None
):
    """诊断parquet文件质量"""
    
    if not parquet_dir:
        parquet_dir = os.path.join(PROJECT_ROOT, "data/parquet")
    
    table_path = Path(parquet_dir) / table_name
    
    if not table_path.exists():
        print(f"错误：表目录 {table_path} 不存在")
        return
    
    print(f"诊断表: {table_name}")
    print(f"数据路径: {table_path}")
    
    if test_stocks:
        # 测试特定股票
        stock_codes = [s.strip() for s in test_stocks.split(",")]
        print(f"测试特定股票: {stock_codes}")
        
        for stock_code in stock_codes:
            print(f"\n=== 测试股票 {stock_code} ===")
            
            # 在所有年份分区中查找该股票的文件
            stock_files = []
            for year_dir in table_path.glob("year=*"):
                stock_pattern = year_dir.glob(f"*{stock_code}*.parquet")
                stock_files.extend(list(stock_pattern))
            
            if stock_files:
                print(f"找到 {len(stock_files)} 个文件:")
                for f in stock_files:
                    result = check_single_parquet_file(f)
                    status = "✅" if not result["error"] else "❌"
                    print(f"  {status} {f.relative_to(table_path)}: {result.get('row_count', 0)} 行")
                    if result["error"]:
                        print(f"      错误: {result['error']}")
            else:
                print(f"未找到股票 {stock_code} 的文件")
    else:
        # 全面诊断
        sample = sample_size if sample_size > 0 else None
        results = scan_table_parquet_files(table_path, sample)
        
        # 测试批量读取
        test_duckdb_batch_read(table_path)
        
        # 按分区测试
        year_dirs = [d for d in table_path.iterdir() if d.is_dir() and d.name.startswith("year=")]
        if year_dirs:
            print(f"\n=== 按分区测试 ===")
            for year_dir in sorted(year_dirs)[:3]:  # 测试前3个分区
                pattern = f"{year_dir.name}/*.parquet"
                test_duckdb_batch_read(table_path, pattern)


@app.command()
def check_missing_stocks(
    missing_stocks: Annotated[str, typer.Argument(help="缺失股票代码，用逗号分隔")],
    table_name: Annotated[str, typer.Option(help="表名")] = "stock_adj_hfq", 
    parquet_dir: Annotated[str, typer.Option(help="Parquet数据目录")] = None
):
    """专门检查缺失股票的parquet文件"""
    
    if not parquet_dir:
        parquet_dir = os.path.join(PROJECT_ROOT, "data/parquet")
    
    table_path = Path(parquet_dir) / table_name
    stock_codes = [s.strip() for s in missing_stocks.split(",")]
    
    print(f"检查缺失股票的parquet文件质量")
    print(f"股票代码: {stock_codes}")
    print(f"表路径: {table_path}")
    
    all_results = []
    
    for stock_code in stock_codes:
        print(f"\n{'='*50}")
        print(f"检查股票: {stock_code}")
        print(f"{'='*50}")
        
        # 查找所有相关文件
        stock_files = []
        for parquet_file in table_path.rglob("*.parquet"):
            if stock_code in parquet_file.name:
                stock_files.append(parquet_file)
        
        if not stock_files:
            print(f"❌ 未找到股票 {stock_code} 的任何parquet文件")
            continue
        
        print(f"找到 {len(stock_files)} 个相关文件:")
        
        stock_results = []
        for file_path in stock_files:
            result = check_single_parquet_file(file_path)
            stock_results.append(result)
            
            status = "✅" if not result["error"] else "❌"
            print(f"  {status} {file_path.relative_to(table_path)}")
            print(f"      大小: {result['size']} 字节")
            print(f"      行数: {result['row_count']}")
            if result["error"]:
                print(f"      错误: {result['error']}")
        
        all_results.extend(stock_results)
        
        # 测试该股票数据的DuckDB批量读取
        if stock_files:
            print(f"\n测试 {stock_code} 的DuckDB批量读取:")
            file_list = "', '".join(str(f) for f in stock_files)
            try:
                with duckdb.connect() as con:
                    result = con.execute(f"SELECT COUNT(*) FROM read_parquet(['{file_list}'])").fetchone()
                    print(f"✅ 批量读取成功，总行数: {result[0]}")
            except Exception as e:
                print(f"❌ 批量读取失败: {e}")
    
    # 总结
    good_count = sum(1 for r in all_results if not r["error"])
    bad_count = len(all_results) - good_count
    
    print(f"\n{'='*50}")
    print(f"总结: ✅ {good_count} 个文件正常，❌ {bad_count} 个文件异常")
    if bad_count > 0:
        print(f"异常率: {bad_count/len(all_results)*100:.1f}%")


if __name__ == "__main__":
    app()
