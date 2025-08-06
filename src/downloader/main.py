import logging
import sys
import warnings
import yaml
import time
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Optional, Dict, Any
import typer
from tqdm import tqdm

from .fetcher import TushareFetcher
from .storage import DuckDBStorage
from .engine import DownloadEngine

# --- 忽略来自 tushare 的 FutureWarning ---
# 这是为了避免在控制台和日志中显示大量关于 Series.fillna(method='bfill') 的弃用警告
warnings.filterwarnings("ignore", category=FutureWarning, module="tushare")

load_dotenv()


class TqdmLoggingHandler(logging.StreamHandler):
    """与tqdm兼容的日志处理器"""

    def __init__(self):
        super().__init__()

    def emit(self, record):
        try:
            msg = self.format(record)
            # 使用 tqdm.write 来输出日志，这样不会打断进度条
            tqdm.write(msg, file=sys.stdout, end="\n")
        except Exception:
            self.handleError(record)


def setup_logging():
    """配置日志系统，同时输出到文件和控制台，与tqdm兼容"""
    root_logger = logging.getLogger()
    
    # 在清理之前，检查是否在测试环境中。如果是，保留pytest的日志处理器
    pytest_handlers = [h for h in root_logger.handlers if 'pytest' in str(type(h)).lower() or 'caplog' in str(type(h)).lower()]
    
    # 清理现有的 handlers（但保留pytest的处理器）
    for handler in root_logger.handlers[:]:
        if handler not in pytest_handlers:
            root_logger.removeHandler(handler)

    # 创建日志格式
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )

    # 文件处理器
    file_handler = logging.FileHandler("downloader.log", mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    # 控制台处理器（与tqdm兼容）
    console_handler = TqdmLoggingHandler()
    console_handler.setFormatter(formatter)

    # 配置根日志记录器
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


def load_config(config_path: str = "config.yaml") -> dict:
    """加载 YAML 配置文件"""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"配置文件 {config_path} 不存在")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class DownloaderApp:
    """
    主应用程序类，封装了数据下载的核心业务逻辑。
    这个类将业务逻辑从 UI 层分离，使其更容易测试。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def process_symbols_config(
        self, config: Dict[str, Any], symbols: Optional[List[str]] = None
    ) -> tuple[Dict[str, Any], bool]:
        """
        根据命令行参数调整配置中的股票符号。

        Args:
            config: 配置字典
            symbols: 命令行指定的股票符号列表

        Returns:
            (修改后的配置字典, 是否被命令行参数覆盖)
        """
        if not symbols:
            return config, False

        # 确保 config 中有 downloader 节点
        if "downloader" not in config:
            config["downloader"] = {}

        if len(symbols) == 1 and symbols[0].lower() == "all":
            self.logger.debug("命令行指定下载所有A股。")
            config["downloader"]["symbols"] = "all"
        else:
            self.logger.debug(f"命令行指定股票池: {list(symbols)}")
            config["downloader"]["symbols"] = list(symbols)

        return config, True

    def create_components(
        self, config: Dict[str, Any]
    ) -> tuple[TushareFetcher, DuckDBStorage]:
        """
        创建下载系统的核心组件。

        Args:
            config: 配置字典

        Returns:
            (fetcher, storage) 元组
        """
        fetcher = TushareFetcher()
        storage = DuckDBStorage(
            db_path=config.get("storage", {}).get("db_path", "data/stock.db")
        )
        return fetcher, storage

    def run_download(
        self,
        config_path: str = "config.yaml",
        group_name: str = "default",
        symbols: Optional[List[str]] = None,
        force: bool = False,
    ) -> bool:
        """
        执行数据下载任务。

        Args:
            config_path: 配置文件路径
            group_name: 要执行的组名
            symbols: 指定的股票符号列表
            force: 是否强制执行

        Returns:
            是否成功执行

        Raises:
            FileNotFoundError: 配置文件不存在
            ValueError: 配置参数错误
            Exception: 其他异常
        """
        self.logger.info(f"开始执行任务组: {group_name}")
        start_time = time.time()
        
        try:
            # 加载配置
            raw_config = load_config(config_path)
            
            # 提取指定的组配置
            if "groups" not in raw_config:
                raise ValueError("配置文件缺少 'groups' 节点")
            
            if group_name not in raw_config["groups"]:
                available_groups = list(raw_config["groups"].keys())
                raise ValueError(f"找不到组 '{group_name}'。可用的组: {available_groups}")
            
            group_config = raw_config["groups"][group_name]
            self.logger.info(f"使用组配置: {group_config.get('description', group_name)}")
            
            # 解析任务引用并转换为旧格式的配置结构
            task_definitions = raw_config.get("tasks", {})
            task_references = group_config.get("tasks", [])
            
            # 将任务引用转换为完整的任务配置
            resolved_tasks = []
            for task_ref in task_references:
                if isinstance(task_ref, str):
                    # 新格式：任务引用
                    if task_ref not in task_definitions:
                        raise ValueError(f"任务引用 '{task_ref}' 在配置中未定义")
                    
                    task_def = task_definitions[task_ref].copy()
                    # 设置默认值
                    task_def["enabled"] = True
                    task_def["update_strategy"] = task_def.get("update_strategy", "incremental")
                    
                    # stock_list 任务特殊处理：使用 cooldown 策略
                    if task_def.get("type") == "stock_list":
                        task_def["update_strategy"] = "cooldown"
                        task_def["update_interval_hours"] = task_def.get("update_interval_hours", 23)
                    
                    resolved_tasks.append(task_def)
                else:
                    # 旧格式：完整任务定义（向后兼容）
                    resolved_tasks.append(task_ref)
            
            config = {
                "storage": raw_config.get("storage", {}),
                "downloader": {
                    "symbols": group_config.get("symbols", []),
                    "max_concurrent_tasks": group_config.get("max_concurrent_tasks", 1)
                },
                "tasks": resolved_tasks
            }
            
            # 处理命令行股票参数覆盖
            config, symbols_overridden = self.process_symbols_config(config, symbols)

            # 创建组件
            fetcher, storage = self.create_components(config)
            engine = DownloadEngine(config, fetcher, storage, force_run=force, symbols_overridden=symbols_overridden, group_name=group_name)

            # 执行下载
            engine.run()

            return True

        except (FileNotFoundError, ValueError) as e:
            self.logger.critical(f"程序启动失败: {e}")
            raise
        except Exception as e:
            self.logger.critical(f"程序主流程发生严重错误: {e}", exc_info=True)
            raise
        finally:
            elapsed_time = time.time() - start_time
            self.logger.info("全部任务已完成，耗时 %.2f 秒", elapsed_time)


# --- Typer 应用定义 ---
app = typer.Typer(
    name="downloader",
    help="一个基于 Tushare Pro 的、可插拔的个人量化数据下载器。",
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    symbols: Optional[List[str]] = typer.Argument(
        None,
        help=(
            "【可选】指定一个或多个股票代码 (例如 600519.SH 000001.SZ)。"
            "如果第一个是 'all'，则下载所有A股。"
            "如果未提供，则使用配置文件中的设置。"
        ),
    ),
    group: str = typer.Option(
        "default",
        "--group",
        "-g",
        help="指定要执行的任务组。",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="强制执行所有启用的任务，无视冷却期。",
        show_default=False,
    ),
    config_file: str = typer.Option(
        "config.yaml",
        "--config",
        "-c",
        help="指定配置文件的路径。",
    ),
):
    """
    程序的主执行函数。
    """
    if ctx.invoked_subcommand is not None:
        return

    # 创建临时的启动处理器，确保"正在启动..."消息能够即时输出
    root_logger = logging.getLogger()
    startup_handler = logging.StreamHandler(sys.stdout)
    startup_handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(startup_handler)
    root_logger.setLevel(logging.INFO)
    
    # 输出启动消息
    logging.info("正在启动...")

    setup_logging()
    logger = logging.getLogger(__name__)
    
    # 初始化完成消息
    logger.info("初始化组件...")

    separator = "=" * 30
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.debug(f"\n\n{separator} 程序开始运行: {timestamp} {separator}\n")

    app = DownloaderApp(logger)

    try:
        app.run_download(config_path=config_file, group_name=group, symbols=symbols, force=force)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.debug(f"\n{separator} 程序运行结束: {timestamp} {separator}\n")

    except (ValueError, FileNotFoundError) as e:
        logger.critical(f"程序启动失败: {e}")
    except Exception as e:
        logger.critical(f"程序主流程发生严重错误: {e}", exc_info=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.fatal(f"\n{separator} 程序异常终止: {timestamp} {separator}\n")


if __name__ == "__main__":
    app()
