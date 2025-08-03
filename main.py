import logging
from dotenv import load_dotenv

load_dotenv()

import yaml
from datetime import datetime
import argparse

from downloader.fetcher import TushareFetcher
from downloader.storage import ParquetStorage
from downloader.engine import DownloadEngine


def setup_logging():
    """配置日志系统，同时输出到文件和控制台"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("downloader.log", mode="a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def load_config(config_path: str = "config.yaml") -> dict:
    """加载 YAML 配置文件"""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# --- 主程序入口 ---
def main():
    """
    程序的主执行函数。
    """
    parser = argparse.ArgumentParser(description="Stock Data Downloader (Tushare Pro).")
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="强制执行所有启用的任务，无视冷却期。",
    )
    # 我们可以增加一个 --sync-state 参数
    parser.add_argument(
        "--sync-state", action="store_true", help="同步数据目录和缓存的状态。"
    )
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    separator = "=" * 30
    logger.info(
        f"\n\n{separator} 程序开始运行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {separator}\n"
    )

    try:
        config = load_config()
        fetcher = TushareFetcher()
        storage = ParquetStorage(
            base_path=config.get("storage", {}).get("base_path", "data")
        )

        engine = DownloadEngine(config, fetcher, storage, args)

        # if args.sync_state:
        #     engine.sync_state() # 未来可以实现的状态同步功能
        # else:
        engine.run()

        logger.info(
            f"\n{separator} 程序运行结束: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {separator}\n"
        )

    except (ValueError, FileNotFoundError) as e:
        logger.critical(f"程序启动失败: {e}")
    except Exception as e:
        logger.critical(f"程序主流程发生严重错误: {e}", exc_info=True)
        logger.info(
            f"\n{separator} 程序异常终止: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {separator}\n"
        )


if __name__ == "__main__":
    main()
