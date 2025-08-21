"""Neo包主程序

基于四层架构的命令行应用程序。
"""

import typer
import logging
from typing import List, Optional
from pathlib import Path

from neo.task_bus.types import DownloadTaskConfig, TaskType, TaskPriority
from neo.downloader import SimpleDownloader
from neo.data_processor import SimpleDataProcessor
from neo.database import DBOperator
from neo.task_bus import HueyTaskBus
from neo.config import get_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="Neo 股票数据处理系统命令行工具")


@app.command()
def dl(
    symbols: str = typer.Option(..., "--symbols", "-s", help="股票代码列表，用引号包裹，逗号分隔，例如: '600519,000001' "),
    task_types: Optional[str] = typer.Option(
        None,
        "--task-types",
        "-t",
        help="任务类型列表，用逗号分隔，例如: 'stock_daily,daily_basic'",
    ),
    priority: str = typer.Option(
        "medium",
        "--priority",
        "-p",
        help="任务优先级: low, medium, high",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="仅验证参数，不实际执行任务"
    ),
):
    """启动下载器，下载股票数据并提交到任务队列"""
    try:
        # 解析股票代码
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
        if not symbol_list:
            typer.echo("错误: 股票代码列表不能为空", err=True)
            raise typer.Exit(1)
        
        # 解析任务类型
        task_type_names = ["STOCK_DAILY"]  # 默认任务类型
        if task_types:
            task_type_names = [t.strip().upper() for t in task_types.split(",") if t.strip()]
        
        task_type_list = []
        for name in task_type_names:
            try:
                task_type_list.append(TaskType[name])
            except KeyError:
                typer.echo(f"错误: 不支持的任务类型: {name}", err=True)
                raise typer.Exit(1)
        
        # 解析优先级
        try:
            task_priority = TaskPriority[priority.upper()]
        except KeyError:
            typer.echo(f"错误: 不支持的优先级: {priority}", err=True)
            raise typer.Exit(1)
        
        # 构建任务列表
        tasks = []
        for symbol in symbol_list:
            for task_type in task_type_list:
                tasks.append(DownloadTaskConfig(
                    symbol=symbol,
                    task_type=task_type,
                    priority=task_priority
                ))
        
        if dry_run:
            typer.echo(f"参数验证成功，将创建 {len(tasks)} 个任务:")
            for task in tasks:
                typer.echo(f"  - {task.symbol}: {task.task_type.value} ({task.priority.name})")
            return
        
        # 启动下载器
        app_instance = NeoApp()
        app_instance.run_producer_mode(tasks)
        typer.echo(f"下载器完成，已处理 {len(tasks)} 个任务")
        
    except ValueError as e:
        typer.echo(f"错误: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"意外错误: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def dp(
    workers: int = typer.Option(1, "--workers", "-w", help="工作进程数"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="详细输出"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="仅验证参数，不实际执行任务"
    ),
):
    """启动数据处理器"""
    import subprocess
    import sys
    import signal
    import os
    
    huey_process = None
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        """信号处理器，用于优雅停止 Huey 消费者"""
        nonlocal huey_process, shutdown_requested
        shutdown_requested = True
        typer.echo("\n接收到停止信号，正在关闭数据处理器...")
        if huey_process:
            try:
                # 向进程组发送 SIGINT 信号（Huey 消费者期望接收 SIGINT）
                os.killpg(os.getpgid(huey_process.pid), signal.SIGINT)
                # 等待子进程结束，最多等待 10 秒
                huey_process.wait(timeout=10)
                typer.echo("数据处理器已优雅停止")
            except subprocess.TimeoutExpired:
                # 如果超时，强制杀死进程组
                typer.echo("强制停止数据处理器...")
                try:
                    os.killpg(os.getpgid(huey_process.pid), signal.SIGKILL)
                except ProcessLookupError:
                    # 如果进程组不存在，直接杀死主进程
                    huey_process.kill()
                huey_process.wait()
                typer.echo("数据处理器已强制停止")
            except ProcessLookupError:
                # 如果进程已经不存在
                typer.echo("数据处理器已停止")
    
    try:
        # 设置日志级别
        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        if dry_run:
            typer.echo(f"参数验证成功，将启动数据处理器，工作进程数: {workers}")
            return
        
        typer.echo(f"启动数据处理器，工作进程数: {workers}")
        typer.echo("提示: 按 Ctrl+C 停止数据处理器")
        
        # 初始化Neo应用以确保任务已注册
        app_instance = NeoApp()
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 构建 huey_consumer 命令
        cmd = [
            sys.executable, "-m", "huey.bin.huey_consumer",
            "neo.task_bus.huey_task_bus.huey",
            "-w", str(workers),
            "-k", "process"
        ]
        
        if verbose:
            cmd.append("-v")
        
        typer.echo(f"执行命令: {' '.join(cmd)}")
        
        # 启动 Huey 消费者进程，创建新的进程组
        huey_process = subprocess.Popen(cmd, preexec_fn=os.setsid)
        
        # 等待子进程结束或接收到停止信号
        while not shutdown_requested:
            try:
                huey_process.wait(timeout=1)
                break  # 进程正常结束
            except subprocess.TimeoutExpired:
                continue  # 继续等待
        
    except KeyboardInterrupt:
        # 这个异常处理器作为备用，主要的处理在 signal_handler 中
        typer.echo("\n数据处理器已停止")
    except Exception as e:
        typer.echo(f"意外错误: {e}", err=True)
        if huey_process:
            huey_process.terminate()
        raise typer.Exit(1)


@app.command()
def demo(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="仅验证参数，不实际执行任务"
    ),
):
    """运行演示程序，展示完整的数据流"""
    try:
        if dry_run:
            typer.echo("参数验证成功，将运行演示程序")
            return
        
        app_instance = NeoApp()
        app_instance.run_demo()
        
    except Exception as e:
        typer.echo(f"演示程序执行失败: {e}", err=True)
        raise typer.Exit(1)


class NeoApp:
    """Neo应用主类
    
    整合四层架构的各个组件。
    """
    
    def __init__(self):
        """初始化Neo应用"""
        logger.info("初始化Neo应用...")
        
        # 初始化各层组件
        self.database = DBOperator()
        self.data_processor = SimpleDataProcessor(db_operator=self.database)
        self.downloader = SimpleDownloader(db_operator=self.database)
        self.task_bus = HueyTaskBus(self.data_processor)
        
        logger.info("Neo应用初始化完成")
    
    def run_producer_mode(self, tasks: List[DownloadTaskConfig]):
        """运行生产者模式
        
        Args:
            tasks: 要执行的任务列表
        """
        logger.info(f"启动生产者模式，任务数量: {len(tasks)}")
        
        for task in tasks:
            try:
                # 使用downloader下载数据
                result = self.downloader.download(task)
                
                # 将结果提交到任务总线
                self.task_bus.submit_task(result)
                
                logger.info(f"任务已提交: {task.task_type.value}, symbol: {task.symbol}")
                
            except Exception as e:
                logger.error(f"任务执行失败: {task.task_type.value}, symbol: {task.symbol}, error: {e}")
        
        logger.info("生产者模式执行完成")
    
    def run_consumer_mode(self):
        """运行消费者模式
        
        注意：实际使用中应该通过命令行启动Huey consumer。
        """
        logger.info("启动消费者模式")
        self.task_bus.start_consumer()
    
    def run_demo(self):
        """运行演示程序
        
        展示完整的数据流：下载 -> 任务总线 -> 数据处理 -> 数据库
        """
        logger.info("=== Neo包演示程序 ===")
        
        # 创建示例任务
        tasks = [
            DownloadTaskConfig(
                symbol="600519.SH", 
                task_type=TaskType.STOCK_DAILY,
                priority=TaskPriority.MEDIUM
            ),
            DownloadTaskConfig(
                symbol="600519.SH",
                task_type=TaskType.DAILY_BASIC,
                priority=TaskPriority.LOW
            )
        ]
        
        # 执行任务（同步模式，用于演示）
        for task in tasks:
            logger.info(f"\n--- 处理任务: {task.task_type.value}, symbol: {task.symbol} ---")
            
            # 1. 下载数据
            logger.info("1. 执行下载...")
            result = self.downloader.download(task)
            
            if result.success:
                logger.info(f"下载成功，数据行数: {len(result.data)}")
                
                # 2. 数据处理
                logger.info("2. 执行数据处理...")
                process_success = self.data_processor.process(result)
                
                if process_success:
                    logger.info("数据处理成功")
                else:
                    logger.warning("数据处理失败")
            else:
                logger.warning(f"下载失败: {result.error}")
        
        logger.info("\n=== 演示程序完成 ===")


def main():
    """主函数"""
    app()


if __name__ == "__main__":
    main()