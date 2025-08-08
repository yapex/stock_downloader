# -*- coding: utf-8 -*-
"""
命令行接口 (CLI) 入口点。

简洁的命令行接口：
- uv run dl                    # 执行默认组任务
- uv run dl --symbol 600519    # 下载特定股票
- uv run dl --group daily      # 执行特定组任务
- uv run dl retry              # 重试死信任务
- uv run dl verify             # 验证数据库状态
"""

import logging
import sys
import warnings
from datetime import datetime
from typing import List, Optional

import typer
from dotenv import load_dotenv

from .app import DownloaderApp
from .config import load_config
from .logging_setup import setup_logging
from .progress_manager import progress_manager
from .missing_symbols import scan_and_log_missing_symbols

# --- 忽略来自 tushare 的 FutureWarning ---
warnings.filterwarnings("ignore", category=FutureWarning, module="tushare")

load_dotenv()

# --- Typer 应用定义 ---
app = typer.Typer(
    name="dl",
    help="股票数据下载器 - 基于 Tushare Pro 的量化数据下载工具",
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    symbol: Optional[str] = typer.Option(
        None,
        "--symbol",
        "-s", 
        help="下载特定股票代码 (例如: 600519, 000001.SZ)"
    ),
    group: str = typer.Option(
        "default",
        "--group",
        "-g",
        help="执行指定的任务组",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="强制执行，忽略冷却期",
        show_default=False,
    ),
    config_file: str = typer.Option(
        "config.yaml",
        "--config",
        "-c",
        help="配置文件路径",
    ),
):
    """
    股票数据下载器主命令
    
    示例:
    \b
      uv run dl                    # 执行默认组任务
      uv run dl --symbol 600519    # 下载特定股票
      uv run dl --group daily      # 执行daily组任务  
      uv run dl --force            # 强制执行忽略冷却期
    """
    if ctx.invoked_subcommand is not None:
        return

    # 精简启动流程
    setup_logging()
    
    # 转换单个symbol为symbols列表
    symbols = [symbol] if symbol else None
    
    progress_manager.print_info(f"启动下载器 - 组: {group}{'，股票: ' + symbol if symbol else ''}")
    
    # 创建并启动下载应用
    downloader_app = DownloaderApp()
    
    try:
        downloader_app.run_download(
            config_path=config_file, 
            group_name=group, 
            symbols=symbols, 
            force=force
        )
        
    except (ValueError, FileNotFoundError) as e:
        progress_manager.print_error(f"启动失败: {e}")
        raise typer.Exit(code=1)
    except KeyboardInterrupt:
        progress_manager.print_warning("用户中断下载")
        raise typer.Exit(code=0)
    except Exception as e:
        progress_manager.print_error(f"执行异常: {e}")
        logging.getLogger(__name__).critical(f"程序执行异常: {e}", exc_info=True)
        raise typer.Exit(code=1)


@app.command()
def retry(
    symbol: Optional[str] = typer.Option(
        None, "--symbol", "-s",
        help="过滤特定股票代码"
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-l",
        help="限制重试任务数量"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n",
        help="预演模式，仅显示将要重试的股票代码"
    ),
    log_path: str = typer.Option(
        "logs/retry_symbols.py",
        "--log-path",
        help="重试日志文件路径"
    ),
    config_file: str = typer.Option(
        "config.yaml",
        "--config",
        "-c",
        help="配置文件路径",
    ),
):
    """
    重试失败的股票代码。
    
    该命令会：
    1. 从重试日志中读取股票代码
    2. 为这些股票代码创建下载任务
    3. 执行下载任务
    
    支持按股票代码过滤，以及限制重试数量。
    """
    try:
        from .retry_policy import RetryLogger
        
        retry_logger = RetryLogger(log_path)
        
        # 读取重试股票代码
        retry_symbols = retry_logger.read_symbols()
        
        if not retry_symbols:
            progress_manager.print_info("没有可重试的股票代码")
            return
        
        # 应用过滤
        if symbol:
            retry_symbols = [s for s in retry_symbols if symbol in s]
        if limit:
            retry_symbols = retry_symbols[:limit]
        
        if dry_run:
            print(f"预演模式：将重试 {len(retry_symbols)} 个股票代码")
            for s in retry_symbols[:50]:
                print(f"  {s}")
            return
        
        if not retry_symbols:
            progress_manager.print_info("过滤后没有可重试的股票代码")
            return
        
        # 使用DownloaderApp执行重试
        downloader_app = DownloaderApp()
        
        try:
            downloader_app.run_download(
                config_path=config_file,
                group_name="default",
                symbols=retry_symbols,
                force=True
            )
            
            # 重试成功后清空重试日志
            retry_logger.clear_symbols()
            progress_manager.print_info("重试完成，已清空重试日志")
            
        except Exception as e:
            progress_manager.print_error(f"重试执行失败: {e}")
            raise
        
    except Exception as e:
        progress_manager.print_error(f"重试失败任务时出错: {e}")
        raise typer.Exit(code=1)


@app.command()
def scan_missing(
    config_file: str = typer.Option(
        "config.yaml",
        "--config",
        "-c",
        help="配置文件路径",
    ),
    missing_log: str = typer.Option(
        "logs/missing_symbols.jsonl",
        "--missing-log",
        help="缺失符号日志路径（覆盖写入）",
    ),
):
    """
    扫描业务表缺失的股票符号，并将结果写入缺失符号日志（覆盖）。
    可配合 'uv run dl retry --missing-log ...' 使用。
    """
    try:
        config = load_config(config_file)
        db_path = config.get("storage", {}).get("db_path") or config.get("database", {}).get("path", "data/stock.db")
        summary = scan_and_log_missing_symbols(db_path=db_path, log_path=missing_log)
        print(f"缺失扫描完成: 总计 {summary.get('total_missing', 0)} 条，文件: {summary.get('log_path', missing_log)}")
    except Exception as e:
        progress_manager.print_error(f"扫描缺失符号失败: {e}")
        raise typer.Exit(code=1)


@app.command()
def verify(
    config_file: str = typer.Option(
        "config.yaml",
        "--config",
        "-c", 
        help="配置文件路径",
    ),
    log_path: str = typer.Option(
        "logs/retry_symbols.py",
        "--log-path",
        help="重试日志文件路径"
    ),
):
    """
    验证数据库状态，按业务分类检查缺失股票数据并写入重试日志。

    功能：
    - 按业务分类（daily_qfq, daily_none, daily_basic, financial_income, financial_balance, financial_cashflow）检查缺失数据
    - 将缺失的股票代码去重后写入 logs/retry_symbols.py
    - 输出按业务分类的缺失数量统计
    """
    try:
        from .storage import DuckDBStorage
        
        import json
        import os
        from datetime import datetime

        # 加载配置
        config = load_config(config_file)
        storage_config = config.get("storage", {})
        db_path = storage_config.get("db_path") or config.get("database", {}).get("path", "data/stock.db")

        # 检查数据库是否存在
        if not os.path.exists(db_path):
            progress_manager.print_error(f"数据库文件不存在: {db_path}")
            raise typer.Exit(code=1)

        storage = DuckDBStorage(db_path)

        # 获取股票列表
        all_stock_codes = set()
        try:
            stock_list_df = storage.query("system", "stock_list")
            if stock_list_df is not None and not stock_list_df.empty:
                all_stock_codes = set(stock_list_df["ts_code"].astype(str).tolist())
        except Exception:
            progress_manager.print_error("无法获取股票列表，请先运行 update_stock_list 任务")
            raise typer.Exit(code=1)

        if not all_stock_codes:
            progress_manager.print_error("股票列表为空，请先运行 update_stock_list 任务")
            raise typer.Exit(code=1)

        # 定义业务分类映射
        business_types = {
            "daily_qfq": "daily",
            "daily_none": "daily", 
            "daily_basic": "daily_basic",
            "financial_income": "financials",
            "financial_balance": "financials",
            "financial_cashflow": "financials"
        }

        # 获取表摘要信息
        try:
            summary_data = storage.get_summary()
        except AttributeError:
            tables = storage.list_tables()
            summary_data = [{"table_name": table, "record_count": "N/A"} for table in tables]

        # 统计每个业务类型的覆盖股票
        from .utils import get_table_name
        business_coverage = {}
        for business_name, table_prefix in business_types.items():
            covered_stocks = set()
            # 检查每个股票代码是否有对应的表
            for stock_code in all_stock_codes:
                expected_table_name = get_table_name(table_prefix, stock_code)
                # 检查该表是否存在于摘要数据中
                for item in summary_data or []:
                    if item["table_name"] == expected_table_name:
                        covered_stocks.add(stock_code)
                        break
            business_coverage[business_name] = covered_stocks

        # 计算缺失数据并收集缺失股票代码
        from .retry_policy import RetryLogger
        
        retry_logger = RetryLogger(log_path)
        all_missing_symbols = set()
        total_count = len(all_stock_codes)
        
        print(f"总股票数: {total_count}")
        print("\n按业务分类缺失统计:")
        
        for business_name, covered_stocks in business_coverage.items():
            missing_stocks = all_stock_codes - covered_stocks
            missing_count = len(missing_stocks)
            covered_count = len(covered_stocks)
            
            print(f"{business_name}: 已覆盖 {covered_count} | 缺失 {missing_count}")
            
            # 收集缺失的股票代码
            all_missing_symbols.update(missing_stocks)

        # 写入重试日志
        if all_missing_symbols:
            missing_symbols_list = sorted(list(all_missing_symbols))
            retry_logger.log_missing_symbols(missing_symbols_list)
            
            print(f"\n已写入重试日志: {log_path}")
            print(f"缺失股票代码数量: {len(missing_symbols_list)}")
        else:
            print("\n无缺失数据，未生成重试任务")

    except Exception as e:
        progress_manager.print_error(f"验证时出错: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
