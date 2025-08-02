import logging
from dotenv import load_dotenv

# 在所有其他导入之前，第一时间加载 .env 文件
load_dotenv() 

import yaml
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import argparse

from downloader.fetcher import TushareFetcher
from downloader.storage import ParquetStorage
from downloader.cache import cache # 导入 cache 用于实体级冷却期

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler("downloader.log", mode='a', encoding='utf-8'),
                                  logging.StreamHandler()])

def load_config(config_path: str = 'config.yaml') -> dict:
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def record_failed_task(task_name: str, ts_code: str, data_type: str, reason: str):
    with open("failed_tasks.log", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()},{task_name},{ts_code},{data_type},{reason}\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Stock Data Downloader (Tushare Pro).")
    parser.add_argument('-f', '--force', action='store_true', help="强制更新所有数据实体，无视冷却期。")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        fetcher = TushareFetcher()
        config = load_config()

        # 1. 加载全局配置
        storage_base_path = config.get('storage', {}).get('base_path', 'data')
        downloader_config = config.get('downloader', {})
        symbols_config = downloader_config.get('symbols', [])
        interval_hours = downloader_config.get('update_interval_hours', 23)
        task_templates = config.get('tasks', [])
        
        storage = ParquetStorage(base_path=storage_base_path)

        # 2. 确定最终的股票列表
        target_symbols = []
        if symbols_config == "all":
            logger.info("配置为 'all'，正在获取全市场股票列表...")
            stock_list_df = fetcher.fetch_stock_list()
            if stock_list_df is not None:
                target_symbols = stock_list_df['ts_code'].tolist()
        else:
            target_symbols = symbols_config

        if not target_symbols:
            logger.error("目标股票列表为空，程序终止。")
            exit()
            
        logger.info(f"程序启动，将为 {len(target_symbols)} 只股票检查/更新 {len(task_templates)} 种数据类型。")

        # 3. 将任务扁平化以获得更好的总进度条
        total_tasks_list = [
            (ts_code, task) 
            for ts_code in target_symbols 
            for task in task_templates if task.get('enabled', False)
        ]
        
        progress_bar = tqdm(total_tasks_list, desc="处理数据实体")
        for ts_code, task in progress_bar:
            
            task_name = task['name']
            task_type = task['type']
            
            entity_id, data_type = "", ""
            if task_type == 'daily':
                adjust = task.get('adjust', 'none')
                data_type = f"daily_{adjust or 'none'}"
                entity_id = f"{data_type}_{ts_code}"
            else:
                continue # 跳过未知的任务类型
            
            progress_bar.set_description(f"处理: {entity_id}")

            try:
                # a. 实体级门禁检查
                if not args.force:
                    last_meta = cache.get(entity_id)
                    if last_meta and 'last_updated' in last_meta:
                        last_updated_time = datetime.fromisoformat(last_meta['last_updated'])
                        if datetime.now() - last_updated_time < timedelta(hours=interval_hours):
                            logger.debug(f"实体 '{entity_id}' 处于冷却期，跳过。")
                            continue

                # b. 计算下载日期范围
                latest_date = storage.get_latest_date(data_type, ts_code)
                start_date = "19901219" # A股最早交易日
                if latest_date:
                    start_date = (pd.to_datetime(latest_date, format='%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
                
                end_date = datetime.now().strftime('%Y%m%d')

                if start_date > end_date:
                    logger.info(f"实体 '{entity_id}' 数据已是最新，无需下载。")
                    cache.set(entity_id, {"last_updated": datetime.now().isoformat()}, expire=None)
                    continue

                # c. 执行下载
                df = fetcher.fetch_daily_history(ts_code, start_date, end_date, task.get('adjust'))

                # d. 处理下载结果
                if df is not None:
                    if not df.empty:
                        storage.save(df, data_type, ts_code, date_col='trade_date')
                    else:
                        logger.info(f"实体 '{entity_id}' 无新数据返回。")
                    
                    # 只要过程没出错，就更新冷却时间戳
                    cache.set(entity_id, {"last_updated": datetime.now().isoformat()}, expire=None)
                else: 
                    tqdm.write(f"❌ 获取 '{entity_id}' 数据失败 (fetcher返回None)，已记录。")
                    record_failed_task(task_name, ts_code, data_type, "fetch_returned_none")

            except Exception as e:
                tqdm.write(f"❌ 处理实体 '{entity_id}' 时发生未知错误: {e}")
                record_failed_task(task_name, ts_code, data_type, str(e))
        
        logger.info("所有股票和任务处理完毕。")

    except (ValueError, FileNotFoundError) as e:
        logger.critical(f"程序启动失败: {e}")
    except Exception as e:
        logger.critical(f"程序运行过程中发生未捕获的严重错误: {e}", exc_info=True)