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
    missing_log: str = typer.Option(
        "logs/missing_symbols.jsonl",
        "--missing-log",
        help="缺失符号日志路径，用于与死信任务合并去重"
    ),
):
    """
    重试死信队列中的失败任务，并可合并缺失符号扫描结果。
    
    步骤：
    1) 尝试读取缺失符号日志并转换为任务
    2) 读取死信日志任务
    3) 合并并按(symbol, task_type)去重
    4) 根据过滤参数筛选，支持dry-run
    """
    try:
        from .dead_letter_cli import DeadLetterCLI
        from .missing_symbols import MissingSymbolsLogger
        from .models import TaskType
        import asyncio
        
        cli = DeadLetterCLI(log_path)
        ms_logger = MissingSymbolsLogger(missing_log)
        
        # 准备任务集合
        combined_tasks: list = []
        
        # 1) 缺失符号日志任务
        missing_tasks = ms_logger.convert_to_tasks()
        if missing_tasks:
            combined_tasks.extend(missing_tasks)
        
        # 2) 死信任务
        # 读取后转换在 DeadLetterCLI 内部完成，这里先取记录，再转任务
        records = cli.dead_letter_logger.read_dead_letters(
            limit=limit,
            task_type=task_type,
            symbol_pattern=symbol
        )
        combined_tasks.extend(cli.dead_letter_logger.convert_to_tasks(records))
        
        if not combined_tasks:
            progress_manager.print_info("没有可重试的任务或缺失符号")
            return
        
        # 3) 合并去重：按 (symbol, task_type)
        unique_map = {}
        for t in combined_tasks:
            key = (t.symbol, t.task_type.value)
            if key not in unique_map:
                unique_map[key] = t
        deduped_tasks = list(unique_map.values())
        
        # 4) 再次应用过滤（如传入task_type/symbol）
        if task_type:
            deduped_tasks = [t for t in deduped_tasks if t.task_type.value == task_type]
        if symbol:
            deduped_tasks = [t for t in deduped_tasks if symbol in t.symbol]
        if limit:
            deduped_tasks = deduped_tasks[:limit]
        
        if dry_run:
            print(f"预演模式：将重试 {len(deduped_tasks)} 个任务(含缺失符号)")
            for t in deduped_tasks[:50]:
                print(f"  {t.symbol} - {t.task_type.value}")
            return
        
        # 直接基于线程池的生产者/消费者执行任务，避免引入额外复杂度
        async def _run_custom_tasks(tasks):
            from .producer_pool import ProducerPool
            from .consumer_pool import ConsumerPool
            from queue import Queue
            import asyncio as _aio
            
            task_q: Queue = Queue(maxsize=1000)
            data_q: Queue = Queue(maxsize=5000)
            
            producer_pool = ProducerPool(
                max_producers=2,
                task_queue=task_q,
                data_queue=data_q
            )
            consumer_pool = ConsumerPool(
                max_consumers=1,
                data_queue=data_q
            )
            
            try:
                producer_pool.start()
                consumer_pool.start()
                
                # 提交任务
                for t in tasks:
                    producer_pool.submit_task(t, timeout=1.0)
                
                print("任务已提交，等待处理完成...")
                
                # 简单轮询直到任务队列清空且数据基本处理完成
                while not task_q.empty() or not data_q.empty():
                    print(f"任务队列: {task_q.qsize()}, 数据队列: {data_q.qsize()}")
                    await _aio.sleep(3)
                
                # 给消费者一点时间刷新缓存
                await _aio.sleep(2)
            finally:
                consumer_pool.stop()
                producer_pool.stop()
        
        asyncio.run(_run_custom_tasks(deduped_tasks))
        
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
        "logs/dead_letter.jsonl",
        "--log-path",
        help="死信日志文件路径"
    ),
):
    """
    验证数据库状态，按业务分类检查缺失股票数据并写入死信日志。

    功能：
    - 按业务分类（daily_qfq, daily_none, daily_basic, financial_income, financial_balance, financial_cashflow）检查缺失数据
    - 将缺失的股票数据去重后写入 logs/dead_letter.jsonl
    - 输出按业务分类的缺失数量统计
    """
    try:
        from .storage import DuckDBStorage
        from .dead_letter_cli import DeadLetterCLI
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

        # 计算缺失数据并准备死信任务
        missing_tasks = []
        total_count = len(all_stock_codes)
        
        print(f"总股票数: {total_count}")
        print("\n按业务分类缺失统计:")
        
        for business_name, covered_stocks in business_coverage.items():
            missing_stocks = all_stock_codes - covered_stocks
            missing_count = len(missing_stocks)
            covered_count = len(covered_stocks)
            
            print(f"{business_name}: 已覆盖 {covered_count} | 缺失 {missing_count}")
            
            # 为缺失的股票创建死信任务
            for stock_code in missing_stocks:
                task_data = {
                    "task_id": f"{business_name}_{stock_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    "task_type": business_types[business_name],
                    "symbol": stock_code,
                    "business_name": business_name,
                    "error_message": "Missing data detected by verify command",
                    "timestamp": datetime.now().isoformat(),
                    "retry_count": 0
                }
                missing_tasks.append(task_data)

        # 去重任务（基于 task_type + symbol 组合）
        unique_tasks = {}
        for task in missing_tasks:
            key = f"{task['task_type']}_{task['symbol']}"
            if key not in unique_tasks:
                unique_tasks[key] = task

        # 写入死信日志文件
        if unique_tasks:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            
            # 读取现有的死信日志（如果存在）
            existing_tasks = {}
            if os.path.exists(log_path):
                try:
                    with open(log_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                existing_task = json.loads(line.strip())
                                key = f"{existing_task.get('task_type', '')}_{existing_task.get('symbol', '')}"
                                existing_tasks[key] = existing_task
                except Exception as e:
                    progress_manager.print_error(f"读取现有死信日志失败: {e}")
            
            # 合并新任务和现有任务（新任务优先）
            all_tasks = {**existing_tasks, **unique_tasks}
            
            # 重写死信日志文件
            with open(log_path, 'w', encoding='utf-8') as f:
                for task in all_tasks.values():
                    f.write(json.dumps(task, ensure_ascii=False) + '\n')
            
            new_count = len(unique_tasks)
            total_count = len(all_tasks)
            print(f"\n已写入死信日志: {log_path}")
            print(f"新增任务: {new_count} | 总任务数: {total_count}")
        else:
            print("\n无缺失数据，未生成死信任务")

    except Exception as e:
        progress_manager.print_error(f"验证时出错: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
