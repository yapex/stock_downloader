import logging
import sys
import yaml
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Optional, Dict, Any
import typer
from tqdm import tqdm

from .fetcher import TushareFetcher
from .storage import ParquetStorage
from .engine import DownloadEngine

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
    # 清理现有的 handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
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
        self, 
        config: Dict[str, Any], 
        symbols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        根据命令行参数调整配置中的股票符号。
        
        Args:
            config: 配置字典
            symbols: 命令行指定的股票符号列表
            
        Returns:
            修改后的配置字典
        """
        if not symbols:
            return config
            
        # 确保 config 中有 downloader 节点
        if "downloader" not in config:
            config["downloader"] = {}
            
        if len(symbols) == 1 and symbols[0].lower() == "all":
            self.logger.info("命令行指定下载所有A股。")
            config["downloader"]["symbols"] = "all"
        else:
            self.logger.info(f"命令行指定股票池: {list(symbols)}")
            config["downloader"]["symbols"] = list(symbols)
            
        return config

    def create_components(
        self, 
        config: Dict[str, Any]
    ) -> tuple[TushareFetcher, ParquetStorage, DownloadEngine]:
        """
        创建下载系统的核心组件。
        
        Args:
            config: 配置字典
            
        Returns:
            (fetcher, storage, engine) 元组
        """
        fetcher = TushareFetcher()
        storage = ParquetStorage(
            base_path=config.get("storage", {}).get("base_path", "data")
        )
        return fetcher, storage

    def run_download(
        self,
        config_path: str = "config.yaml",
        symbols: Optional[List[str]] = None,
        force: bool = False
    ) -> bool:
        """
        执行数据下载任务。
        
        Args:
            config_path: 配置文件路径
            symbols: 指定的股票符号列表
            force: 是否强制执行
            
        Returns:
            是否成功执行
            
        Raises:
            FileNotFoundError: 配置文件不存在
            ValueError: 配置参数错误
            Exception: 其他异常
        """
        try:
            # 加载和处理配置
            config = load_config(config_path)
            config = self.process_symbols_config(config, symbols)
            
            # 创建组件
            fetcher, storage = self.create_components(config)
            engine = DownloadEngine(config, fetcher, storage, force_run=force)
            
            # 执行下载
            engine.run()
            
            return True
            
        except (FileNotFoundError, ValueError) as e:
            self.logger.critical(f"程序启动失败: {e}")
            raise
        except Exception as e:
            self.logger.critical(f"程序主流程发生严重错误: {e}", exc_info=True)
            raise


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
            "如果未提供，则使用 config.yaml 中的配置。"
        ),
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

    setup_logging()
    logger = logging.getLogger(__name__)

    separator = "=" * 30
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"\n\n{separator} 程序开始运行: {timestamp} {separator}\n")

    app = DownloaderApp(logger)
    
    try:
        app.run_download(config_path=config_file, symbols=symbols, force=force)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"\n{separator} 程序运行结束: {timestamp} {separator}\n")

    except (ValueError, FileNotFoundError) as e:
        logger.critical(f"程序启动失败: {e}")
    except Exception as e:
        logger.critical(f"程序主流程发生严重错误: {e}", exc_info=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"\n{separator} 程序异常终止: {timestamp} {separator}\n")


if __name__ == "__main__":
    app()
