"""数据管道健康检查脚本"""

import logging
import os
import time
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

from neo.configs import get_config

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PipelineHealthChecker:
    """执行一系列检查来验证数据管道的健康状况"""

    def __init__(self):
        self.config = get_config()
        self.project_root = Path(__file__).resolve().parents[1]
        self.log_dir = self.project_root / 'logs'
        self.db_paths = {
            'fast': self.project_root / self.config.huey_fast.sqlite_path,
            'slow': self.project_root / self.config.huey_slow.sqlite_path,
            'maint': self.project_root / self.config.huey_maint.sqlite_path,
        }
        self.metadata_db_path = self.project_root / self.config.database.metadata_path
        self.parquet_base_path = self.project_root / self.config.storage.parquet_base_path
        self.results: List[Tuple[str, str, str]] = []

    def _add_result(self, check_name: str, status: str, message: str):
        self.results.append((check_name, status, message))
        logging.info(f"[{status}] {check_name}: {message}")

    def check_consumer_heartbeat(self, queue_name: str, max_inactive_minutes: int = 30):
        """检查消费者日志是否在近期有更新"""
        log_file = self.log_dir / f"consumer_{queue_name}.log"
        if not log_file.exists():
            self._add_result(f"心跳检查 ({queue_name})", "FAIL", f"日志文件 {log_file} 不存在！")
            return

        last_modified_time = log_file.stat().st_mtime
        if time.time() - last_modified_time > max_inactive_minutes * 60:
            self._add_result(f"心跳检查 ({queue_name})", "FAIL", f"日志文件在过去 {max_inactive_minutes} 分钟内无更新。")
        else:
            self._add_result(f"心跳检查 ({queue_name})", "PASS", "日志文件近期有更新。")

    def check_queue_size(self, queue_name: str, max_size: int = 1000):
        """检查 Huey 队列中积压的任务数"""
        db_path = self.db_paths.get(queue_name)
        if not db_path or not db_path.exists():
            self._add_result(f"队列积压检查 ({queue_name})", "FAIL", f"队列数据库 {db_path} 不存在！")
            return
        
        try:
            with duckdb.connect(str(db_path)) as con:
                # 检查 huey_queue 表是否存在
                tables = con.execute("SHOW TABLES").fetchdf()['name'].tolist()
                if 'huey_queue' not in tables:
                    count = 0
                else:
                    count = con.execute("SELECT COUNT(*) FROM huey_queue").fetchone()[0]
                
                if count > max_size:
                    self._add_result(f"队列积压检查 ({queue_name})", "FAIL", f"队列积压了 {count} 个任务，超过阈值 {max_size}！")
                else:
                    self._add_result(f"队列积压检查 ({queue_name})", "PASS", f"队列中有 {count} 个任务。")
        except Exception as e:
            self._add_result(f"队列积压检查 ({queue_name})", "FAIL", f"检查失败: {e}")

    def check_data_freshness(self, table_name: str = 'stock_daily', date_column: str = 'trade_date', max_days_stale: int = 3):
        """检查核心数据表的数据新鲜度"""
        if not self.metadata_db_path.exists():
            self._add_result("数据新鲜度检查", "FAIL", f"元数据数据库 {self.metadata_db_path} 不存在！")
            return

        try:
            with duckdb.connect(str(self.metadata_db_path)) as con:
                tables = con.execute("SHOW TABLES").fetchdf()['name'].tolist()
                if table_name not in tables:
                    self._add_result("数据新鲜度检查", "FAIL", f"表 '{table_name}' 在元数据数据库中不存在。")
                    return
                
                latest_date_str = con.execute(f"SELECT MAX({date_column}) FROM {table_name}").fetchone()[0]
                if not latest_date_str:
                    self._add_result("数据新鲜度检查", "PASS", f"表 '{table_name}' 为空，跳过检查。")
                    return

                latest_date = datetime.strptime(str(latest_date_str), '%Y%m%d').date()
                if datetime.now().date() - latest_date > timedelta(days=max_days_stale):
                    self._add_result("数据新鲜度检查", "FAIL", f"数据最新的日期是 {latest_date}，超过了 {max_days_stale} 天。")
                else:
                    self._add_result("数据新鲜度检查", "PASS", f"数据最新的日期是 {latest_date}，在容忍范围内。")
        except Exception as e:
            self._add_result("数据新鲜度检查", "FAIL", f"检查失败: {e}")

    def check_metadata_freshness(self, max_minutes_lag: int = 30):
        """检查元数据相对于最新Parquet文件是否过时"""
        if not self.metadata_db_path.exists() or not self.parquet_base_path.is_dir():
            self._add_result("元数据新鲜度检查", "FAIL", "元数据DB或Parquet目录不存在。")
            return

        latest_parquet_mtime = 0
        for p in self.parquet_base_path.glob('**/*.parquet'):
            latest_parquet_mtime = max(latest_parquet_mtime, p.stat().st_mtime)

        if latest_parquet_mtime == 0:
            self._add_result("元数据新鲜度检查", "PASS", "未找到任何Parquet文件，跳过检查。")
            return

        metadata_mtime = self.metadata_db_path.stat().st_mtime
        if metadata_mtime < latest_parquet_mtime and time.time() - metadata_mtime > max_minutes_lag * 60:
            self._add_result("元数据新鲜度检查", "FAIL", f"元数据DB自 {datetime.fromtimestamp(metadata_mtime)} 后未更新，但有更新的Parquet文件。")
        else:
            self._add_result("元数据新鲜度检查", "PASS", "元数据与Parquet文件同步状态良好。")

    def run_all_checks(self):
        """运行所有健康检查并打印总结"""
        logging.info("--- 开始数据管道健康检查 ---")
        self.check_consumer_heartbeat('fast')
        self.check_consumer_heartbeat('slow')
        self.check_consumer_heartbeat('maint')
        self.check_queue_size('fast')
        self.check_queue_size('slow')
        self.check_data_freshness()
        self.check_metadata_freshness()
        logging.info("--- 健康检查结束 ---")

        summary = {"PASS": 0, "FAIL": 0}
        for _, status, _ in self.results:
            summary[status] += 1

        logging.info(f"检查总结: 总共 {len(self.results)} 项, 成功 {summary['PASS']} 项, 失败 {summary['FAIL']} 项.")
        if summary['FAIL'] > 0:
            logging.error("数据管道存在健康问题，请检查以上失败项。")
        else:
            logging.info("✅ 数据管道看起来很健康！")

if __name__ == "__main__":
    checker = PipelineHealthChecker()
    checker.run_all_checks()
