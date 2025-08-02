import logging
from dotenv import load_dotenv

load_dotenv()

import yaml
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import argparse

from downloader.fetcher import TushareFetcher
from downloader.storage import ParquetStorage
from downloader.cache import cache


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("downloader.log", mode="a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def record_failed_task(task_name: str, entity_id: str, reason: str):
    with open("failed_tasks.log", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()},{task_name},{entity_id},{reason}\n")


def should_skip_by_interval(entity_id: str, interval_hours: int) -> bool:
    """通用的、基于实体ID和冷却时间的门禁检查函数。"""
    last_meta = cache.get(entity_id)
    if last_meta and "last_updated" in last_meta:
        last_updated_time = datetime.fromisoformat(last_meta["last_updated"])
        if datetime.now() - last_updated_time < timedelta(hours=interval_hours):
            logger.debug(f"实体 '{entity_id}' 处于冷却期，跳过。")
            return True
    return False


def process_stock_list_task(task_config: dict, fetcher: TushareFetcher):
    """处理器：专门负责执行 'stock_list' 类型的任务。"""
    task_name = task_config["name"]
    logger.info(f"========== 开始执行系统任务: '{task_name}' ==========")

    stock_list_df = fetcher.fetch_stock_list()

    if stock_list_df is not None and not stock_list_df.empty:
        # 将获取到的股票列表存入 cache，供其他任务使用
        cache.set("system_stock_list", stock_list_df, expire=3600 * 24)  # 缓存24小时
        logger.info(f"已获取并缓存了 {len(stock_list_df)} 只股票的列表。")
    else:
        logger.error(f"任务 '{task_name}' 执行失败，未能获取股票列表。")
        record_failed_task(task_name, "system_stock_list", "fetch_failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Data Downloader (Tushare Pro).")
    parser.add_argument(
        "-f", "--force", action="store_true", help="强制更新所有数据实体，无视冷却期。"
    )
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        fetcher = TushareFetcher()
        config = load_config()

        storage_base_path = config.get("storage", {}).get("base_path", "data")
        downloader_config = config.get("downloader", {})
        symbols_config = downloader_config.get("symbols", [])
        default_interval_hours = downloader_config.get("update_interval_hours", 23)
        task_templates = config.get("tasks", [])

        storage = ParquetStorage(base_path=storage_base_path)

        # --- 阶段一：执行系统级任务 (如更新股票列表) ---
        logger.info("--- 阶段一：执行系统级任务 ---")
        system_tasks = [
            t
            for t in task_templates
            if t.get("type") == "stock_list" and t.get("enabled", False)
        ]
        for task in system_tasks:
            task_name = task["name"]
            interval = task.get("update_interval_hours", default_interval_hours)

            if not args.force and should_skip_by_interval(
                f"task_meta_{task_name}", interval
            ):
                logger.info(f"系统任务 '{task_name}' 处于冷却期，跳过。")
                continue

            process_stock_list_task(task, fetcher)
            # 成功后更新任务自己的冷却时间戳
            cache.set(
                f"task_meta_{task_name}",
                {"last_updated": datetime.now().isoformat()},
                expire=None,
            )

        # --- 阶段二：确定最终的股票列表 ---
        logger.info("--- 阶段二：确定数据下载的目标股票列表 ---")
        target_symbols = []
        if symbols_config == "all":
            cached_list_df = cache.get("system_stock_list")
            if cached_list_df is not None:
                logger.info("配置为 'all'，已成功从缓存加载股票列表。")
                target_symbols = cached_list_df["ts_code"].tolist()
            else:
                logger.warning("配置为 'all' 但缓存中无股票列表，将尝试实时获取一次。")
                stock_list_df = fetcher.fetch_stock_list()
                if stock_list_df is not None:
                    target_symbols = stock_list_df["ts_code"].tolist()
        else:
            logger.info("将使用配置文件中指定的股票列表。")
            target_symbols = symbols_config

        if not target_symbols:
            logger.error("目标股票列表为空，数据下载任务终止。")
            exit()

        logger.info(f"将为 {len(target_symbols)} 只股票执行数据下载任务。")

        # --- 阶段三：执行数据下载任务 ---
        logger.info("--- 阶段三：开始执行数据下载任务 ---")
        data_tasks = [
            t
            for t in task_templates
            if t.get("type") != "stock_list" and t.get("enabled", False)
        ]

        # 将任务扁平化以获得更好的总进度条
        total_entity_tasks = [
            (ts_code, task) for ts_code in target_symbols for task in data_tasks
        ]

        progress_bar = tqdm(total_entity_tasks, desc="处理数据实体")
        for ts_code, task in progress_bar:
            task_name = task["name"]
            task_type = task["type"]

            entity_id, data_type = "", ""
            if task_type == "daily":
                adjust = task.get("adjust", "none")
                data_type = f"daily_{adjust or 'none'}"
                entity_id = f"{data_type}_{ts_code}"
            else:
                continue

            progress_bar.set_description(f"处理: {entity_id}")

            try:
                # 实体级门禁检查
                if not args.force and should_skip_by_interval(
                    entity_id, default_interval_hours
                ):
                    continue

                # 下载和存储逻辑
                latest_date = storage.get_latest_date(data_type, ts_code)
                start_date = "19901219"
                if latest_date:
                    start_date = (
                        pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
                    ).strftime("%Y%m%d")

                end_date = datetime.now().strftime("%Y%m%d")

                if start_date > end_date:
                    logger.info(f"实体 '{entity_id}' 数据已是最新。")
                    cache.set(
                        entity_id,
                        {"last_updated": datetime.now().isoformat()},
                        expire=None,
                    )
                    continue

                df = fetcher.fetch_daily_history(
                    ts_code, start_date, end_date, task.get("adjust")
                )

                if df is not None:
                    if not df.empty:
                        storage.save(df, data_type, ts_code, date_col="trade_date")
                    cache.set(
                        entity_id,
                        {"last_updated": datetime.now().isoformat()},
                        expire=None,
                    )
                else:
                    record_failed_task(task_name, entity_id, "fetch_returned_none")
            except Exception as e:
                tqdm.write(f"❌ 处理实体 '{entity_id}' 时发生未知错误: {e}")
                record_failed_task(task_name, entity_id, str(e))

        logger.info("所有数据下载任务处理完毕。")

    except (ValueError, FileNotFoundError) as e:
        logger.critical(f"程序启动失败: {e}")
    except Exception as e:
        logger.critical(f"程序运行过程中发生未捕获的严重错误: {e}", exc_info=True)
