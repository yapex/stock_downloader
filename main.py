import logging
from dotenv import load_dotenv

load_dotenv()

import yaml
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import argparse
from pathlib import Path

from downloader.fetcher import TushareFetcher
from downloader.storage import ParquetStorage


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


# --- 任务处理器 ---


def process_incremental_task(
    task_config: dict,
    fetcher: TushareFetcher,
    storage: ParquetStorage,
    target_symbols: list,
):
    """处理器：负责所有“增量更新”策略的任务。"""
    logger = logging.getLogger(__name__)
    task_name = task_config["name"]
    adjust = task_config.get("adjust", "none")
    data_type = f"daily_{adjust or 'none'}"
    date_col = task_config.get("date_col", "trade_date")

    logger.info(f"--- 开始执行增量任务: '{task_name}' (日期列: {date_col}) ---")
    progress_bar = tqdm(target_symbols, desc=f"增量更新: {task_name}")
    for ts_code in progress_bar:
        progress_bar.set_description(f"处理: {data_type}_{ts_code}")
        try:
            latest_date = storage.get_latest_date(data_type, ts_code, date_col=date_col)
            start_date = "19901219"
            if latest_date:
                start_date = (
                    pd.to_datetime(latest_date, format="%Y%m%d") + timedelta(days=1)
                ).strftime("%Y%m%d")

            end_date = datetime.now().strftime("%Y%m%d")
            if start_date > end_date:
                continue

            df = fetcher.fetch_daily_history(ts_code, start_date, end_date, adjust)

            # ---> 核心修正：对三种返回情况进行精确处理 <---
            if df is not None:
                if not df.empty:
                    # 1. 成功获取到新数据
                    storage.save(df, data_type, ts_code, date_col=date_col)
                else:
                    # 2. 成功，但无新数据（正常情况）
                    logger.info(f"实体 '{data_type}_{ts_code}' 无新数据返回。")
                # 只要不是None，就认为是一次成功的“检查/更新”，未来可以更新冷却期
            else:
                # 3. df is None，表示获取失败
                record_failed_task(task_name, ts_code, "fetch_failed")
                tqdm.write(
                    f"❌ 实体 '{data_type}_{ts_code}' 获取失败 (fetcher返回None)，已记录。"
                )

        except Exception as e:
            tqdm.write(f"❌ 处理股票 {ts_code} 时发生未知错误: {e}")
            record_failed_task(task_name, ts_code, str(e))


def process_cooldown_task(
    task_config: dict,
    fetcher: TushareFetcher,
    storage: ParquetStorage,
    args: argparse.Namespace,
):
    """处理器：负责所有“冷却期全量覆盖”策略的任务。"""
    logger = logging.getLogger(__name__)
    task_name = task_config["name"]
    task_type = task_config["type"]

    logger.info(f"--- 开始执行冷却期任务: '{task_name}' ---")

    interval_hours = task_config.get("update_interval_hours", 23)
    entity_id = task_type
    target_file_path = storage._get_file_path("system", entity_id)

    if not args.force and target_file_path.exists():
        last_modified_time = datetime.fromtimestamp(target_file_path.stat().st_mtime)
        if datetime.now() - last_modified_time < timedelta(hours=interval_hours):
            logger.info(f"任务 '{task_name}' 处于冷却期，跳过。")
            return

    if task_type == "stock_list":
        df = fetcher.fetch_stock_list()
        if df is not None:
            storage.overwrite(df, "system", entity_id)
    else:
        logger.warning(f"未知的冷却期任务类型: '{task_type}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Data Downloader (Tushare Pro).")
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="强制执行所有启用的任务，无视冷却期。",
    )
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    separator = "=" * 30
    logger.info(
        f"\n\n{separator} 程序开始运行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {separator}\n"
    )

    try:
        fetcher = TushareFetcher()
        config = load_config()
        storage_instance = ParquetStorage(
            base_path=config.get("storage", {}).get("base_path", "data")
        )

        # 阶段一：执行冷却期任务
        cooldown_tasks = [
            t
            for t in config.get("tasks", [])
            if t.get("update_strategy") == "cooldown" and t.get("enabled")
        ]
        for task in cooldown_tasks:
            process_cooldown_task(task, fetcher, storage_instance, args)

        # 阶段二：执行增量任务
        symbols_config = config.get("downloader", {}).get("symbols", [])
        target_symbols = []
        if symbols_config == "all":
            try:
                stock_list_file = storage_instance._get_file_path(
                    "system", "stock_list"
                )
                if stock_list_file.exists():
                    df_list = pd.read_parquet(stock_list_file)
                    target_symbols = df_list["ts_code"].tolist()
                else:
                    logger.warning(
                        "配置为'all'但未找到股票列表文件，请先运行'更新A股列表'任务。"
                    )
            except Exception as e:
                logger.error(f"读取股票列表文件失败: {e}")
        else:
            target_symbols = symbols_config

        if not target_symbols:
            logger.warning("目标股票列表为空，增量任务将被跳过。")
        else:
            incremental_tasks = [
                t
                for t in config.get("tasks", [])
                if t.get("update_strategy") == "incremental" and t.get("enabled")
            ]
            for task in incremental_tasks:
                process_incremental_task(
                    task, fetcher, storage_instance, target_symbols
                )

        logger.info(
            f"\n{separator} 程序运行结束: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {separator}\n"
        )

    except Exception as e:
        logger.critical(f"程序主流程发生严重错误: {e}", exc_info=True)
        logger.info(
            f"\n{separator} 程序异常终止: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {separator}\n"
        )
