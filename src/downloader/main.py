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
    task_type: Optional[str] = typer.Option(
        None, "--task-type", "-t", 
        help="过滤特定任务类型 (daily, stock_list, etc.)"
    ),
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
        help="预演模式，仅显示将要重试的任务"
    ),
    log_path: str = typer.Option(
        "logs/dead_letter.jsonl",
        "--log-path",
        help="死信日志文件路径"
    ),
):
    """
    重试死信队列中的失败任务
    
    示例:
    \b
      uv run dl retry              # 重试所有失败任务
      uv run dl retry --symbol 600519  # 重试特定股票的失败任务  
      uv run dl retry --task-type daily  # 重试特定类型的失败任务
      uv run dl retry --dry-run    # 预览将要重试的任务
    """
    try:
        from .dead_letter_cli import DeadLetterCLI
        import asyncio
        
        cli = DeadLetterCLI(log_path)
        
        # 运行异步重试函数
        asyncio.run(cli.retry_failed_tasks(
            task_type=task_type,
            symbol_pattern=symbol,
            limit=limit,
            dry_run=dry_run
        ))
        
    except Exception as e:
        progress_manager.print_error(f"重试失败任务时出错: {e}")
        raise typer.Exit(code=1)


@app.command()
def verify(
    config_file: str = typer.Option(
        "config.yaml",
        "--config",
        "-c", 
        help="配置文件路径",
    ),
    show_missing: bool = typer.Option(
        True,
        "--show-missing/--no-missing",
        help="显示/隐藏缺失股票信息"
    ),
    log_path: str = typer.Option(
        "logs/dead_letter.jsonl",
        "--log-path",
        help="死信日志文件路径"
    ),
):
    """
    验证数据库状态，显示数据完整性和死信统计
    
    示例:
    \b
      uv run dl verify             # 显示完整验证信息
      uv run dl verify --no-missing  # 不显示缺失股票详情
    """
    try:
        from .storage import DuckDBStorage
        from .dead_letter_cli import DeadLetterCLI
        import re
        import os
        
        # 加载配置
        config = load_config(config_file)
        storage_config = config.get("storage", {})
        db_path = storage_config.get("db_path") or config.get("database", {}).get("path", "data/stock.db")
        
        print("\n🔍 数据库验证报告")
        print("=" * 50)
        
        # 检查数据库是否存在
        if not os.path.exists(db_path):
            print(f"❌ 数据库文件不存在: {db_path}")
            raise typer.Exit(code=1)
            
        storage = DuckDBStorage(db_path)
        
        # 获取表摘要信息
        try:
            summary_data = storage.get_summary()
        except AttributeError:
            # 如果get_summary方法不存在，使用基础信息
            tables = storage.list_tables()
            summary_data = [{"table_name": table, "record_count": "N/A"} for table in tables]
        
        if not summary_data:
            print("📊 数据库中没有找到任何表")
        else:
            print(f"📊 数据库概览 ({len(summary_data)} 个表)")
            
            # 分类统计
            sys_tables = []
            data_tables = []
            
            for item in summary_data:
                table_name = item["table_name"]
                if table_name.startswith("sys_") or table_name.startswith("_"):
                    sys_tables.append(item)
                else:
                    data_tables.append(item)
            
            if sys_tables:
                print(f"\n  系统表 ({len(sys_tables)}个):")
                for item in sys_tables:
                    count = item.get("record_count", "N/A")
                    print(f"    {item['table_name']}: {count} 条记录")
            
            if data_tables:
                print(f"\n  数据表 ({len(data_tables)}个)")
                
                # 按业务类型分组统计
                type_stats = {}
                table_pattern = re.compile(r"^(\w+)_(.+)$")
                
                for item in data_tables:
                    table_name = item["table_name"]
                    match = table_pattern.match(table_name)
                    if match:
                        business_type = match.group(1)
                        if business_type not in type_stats:
                            type_stats[business_type] = 0
                        type_stats[business_type] += 1
                
                for btype, count in sorted(type_stats.items()):
                    print(f"    {btype}: {count} 个股票")
        
        # 检查死信日志
        print(f"\n💀 死信队列状态")
        if os.path.exists(log_path):
            cli = DeadLetterCLI(log_path)
            stats = cli.dead_letter_logger.get_statistics()
            
            if stats['total_count'] > 0:
                print(f"   ⚠️  失败任务: {stats['total_count']} 个")
                
                if stats['by_task_type']:
                    print("   按类型分布:")
                    for task_type, count in stats['by_task_type'].items():
                        print(f"     {task_type}: {count}")
                
                print("\n   💡 提示: 使用 'uv run dl retry' 重试失败任务")
            else:
                print("   ✅ 无失败任务")
        else:
            print("   ✅ 死信日志文件不存在，无失败任务")
        
        # 数据完整性检查（可选）
        if show_missing and summary_data:
            try:
                # 尝试获取股票列表进行完整性检查
                stock_list_df = storage.query("system", "stock_list")
                if not stock_list_df.empty:
                    all_stock_codes = set(stock_list_df["ts_code"].tolist())
                    
                    # 统计有数据的股票
                    existing_stocks = set()
                    table_pattern = re.compile(r"^\w+_(.+_\w+)$")
                    
                    for item in summary_data:
                        table_name = item["table_name"]
                        if not table_name.startswith(("sys_", "_")):
                            match = table_pattern.match(table_name)
                            if match:
                                stock_code_part = match.group(1)
                                if "_" in stock_code_part:
                                    standard_code = stock_code_part.replace("_", ".")
                                    existing_stocks.add(standard_code)
                    
                    missing_count = len(all_stock_codes) - len(existing_stocks)
                    completion_rate = len(existing_stocks) / len(all_stock_codes) * 100 if all_stock_codes else 0
                    
                    print(f"\n📈 数据完整性")
                    print(f"   总股票数: {len(all_stock_codes)}")
                    print(f"   有数据股票: {len(existing_stocks)}")
                    print(f"   完整度: {completion_rate:.1f}%")
                    
                    if missing_count > 0:
                        print(f"   缺失: {missing_count} 个股票")
            except Exception:
                # 如果完整性检查失败，跳过但不报错
                pass
        
        print("\n✅ 验证完成")
        
    except Exception as e:
        progress_manager.print_error(f"验证时出错: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
