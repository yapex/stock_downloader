"""
缺失符号检测和记录模块

遵循 KISS 原则：
1. 检查 stock_list 中的股票在业务表中的缺失情况
2. 记录到日志文件（覆盖模式）
3. 支持 retry 命令读取并去重执行

简单策略：传入股票symbols列表，按既定策略下载，保存到duckdb，给出执行情况反馈
"""

import logging
import json
from pathlib import Path
from typing import Dict, List, Set, Any
from datetime import datetime

from .storage import DuckDBStorage
from .storage_factory import get_storage
from .models import TaskType, DownloadTask, Priority

logger = logging.getLogger(__name__)


class MissingSymbolsDetector:
    """缺失符号检测器"""
    
    def __init__(self, storage: DuckDBStorage):
        self.storage = storage
        self.logger = logging.getLogger(__name__)
    
    def detect_missing_symbols(self) -> Dict[str, Set[str]]:
        """
        检测各业务类型中缺失的股票代码
        
        Returns:
            Dict[业务类型, 缺失股票集合]
        """
        try:
            # 获取所有股票代码
            stock_list_df = self.storage.get_stock_list()
            if stock_list_df.empty or 'ts_code' not in stock_list_df.columns:
                self.logger.warning("未找到股票列表数据")
                return {}
            
            all_stocks = set(stock_list_df['ts_code'].tolist())
            self.logger.info(f"总股票数: {len(all_stocks)}")
            
            # 获取业务表信息
            business_tables = self.storage.list_business_tables()
            
            # 按业务类型分组已有股票
            existing_by_type = {}
            for table_info in business_tables:
                business_type = table_info['business_type']
                stock_code = table_info['stock_code']
                
                if business_type not in existing_by_type:
                    existing_by_type[business_type] = set()
                existing_by_type[business_type].add(stock_code)
            
            # 计算各业务类型的缺失股票
            missing_by_type = {}
            for business_type in existing_by_type:
                existing_stocks = existing_by_type[business_type]
                missing_stocks = all_stocks - existing_stocks
                if missing_stocks:
                    missing_by_type[business_type] = missing_stocks
                    self.logger.info(f"{business_type}: 缺失 {len(missing_stocks)} 只股票")
            
            # 如果没有任何业务表，所有股票都是缺失的
            if not existing_by_type:
                # 默认业务类型
                default_types = ['daily', 'daily_basic', 'financials']
                for btype in default_types:
                    missing_by_type[btype] = all_stocks.copy()
                    self.logger.info(f"{btype}: 缺失 {len(all_stocks)} 只股票（无业务表）")
            
            return missing_by_type
            
        except Exception as e:
            self.logger.error(f"检测缺失符号失败: {e}")
            return {}


class MissingSymbolsLogger:
    """缺失符号日志记录器"""
    
    def __init__(self, log_path: str = "logs/missing_symbols.jsonl"):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def write_missing_symbols(self, missing_by_type: Dict[str, Set[str]]) -> None:
        """
        写入缺失符号记录（覆盖模式）
        
        Args:
            missing_by_type: 按业务类型分组的缺失股票
        """
        if not missing_by_type:
            self.logger.info("无缺失股票，清空缺失符号日志")
            # 清空文件
            with open(self.log_path, 'w', encoding='utf-8') as f:
                pass
            return
        
        try:
            total_count = 0
            with open(self.log_path, 'w', encoding='utf-8') as f:
                for business_type, symbols in missing_by_type.items():
                    for symbol in symbols:
                        record = {
                            'symbol': symbol,
                            'business_type': business_type,
                            'task_type': business_type,  # 保持兼容性
                            'created_at': datetime.now().isoformat(),
                            'status': 'missing'
                        }
                        json.dump(record, f, ensure_ascii=False)
                        f.write('\n')
                        total_count += 1
            
            self.logger.info(f"已记录 {total_count} 个缺失符号到 {self.log_path}")
            
        except Exception as e:
            self.logger.error(f"写入缺失符号日志失败: {e}")
            raise
    
    def read_missing_symbols(self) -> Dict[str, List[str]]:
        """
        读取缺失符号记录
        
        Returns:
            Dict[业务类型, 缺失股票列表]
        """
        if not self.log_path.exists():
            return {}
        
        missing_by_type = {}
        
        try:
            with open(self.log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record = json.loads(line)
                        business_type = record['business_type']
                        symbol = record['symbol']
                        
                        if business_type not in missing_by_type:
                            missing_by_type[business_type] = []
                        missing_by_type[business_type].append(symbol)
                        
                    except (json.JSONDecodeError, KeyError):
                        self.logger.warning(f"无效记录: {line}")
                        continue
            
        except Exception as e:
            self.logger.error(f"读取缺失符号日志失败: {e}")
        
        return missing_by_type
    
    def convert_to_tasks(self, task_configs: Dict[str, Dict[str, Any]] = None) -> List[DownloadTask]:
        """
        将缺失符号转换为下载任务
        
        Args:
            task_configs: 任务配置字典 {业务类型: 配置}
            
        Returns:
            任务列表
        """
        missing_by_type = self.read_missing_symbols()
        if not missing_by_type:
            return []
        
        tasks = []
        default_configs = {
            'daily': {'adjust': 'hfq'},
            'daily_basic': {},
            'financials': {'financial_type': 'income'},
        }
        
        for business_type, symbols in missing_by_type.items():
            try:
                task_type = TaskType(business_type)
                config = (task_configs or {}).get(business_type, default_configs.get(business_type, {}))
                
                for symbol in symbols:
                    task = DownloadTask(
                        symbol=symbol,
                        task_type=task_type,
                        params=config,
                        priority=Priority.NORMAL
                    )
                    tasks.append(task)
                    
            except ValueError:
                self.logger.warning(f"未知业务类型: {business_type}")
                continue
        
        return tasks
    
    def get_summary(self) -> Dict[str, Any]:
        """
        获取缺失符号摘要
        
        Returns:
            摘要信息
        """
        missing_by_type = self.read_missing_symbols()
        
        total_count = sum(len(symbols) for symbols in missing_by_type.values())
        
        return {
            'total_missing': total_count,
            'by_type': {btype: len(symbols) for btype, symbols in missing_by_type.items()},
            'types': list(missing_by_type.keys()),
            'log_path': str(self.log_path),
            'exists': self.log_path.exists()
        }


def scan_and_log_missing_symbols(db_path: str = "data/stock.db", 
                                log_path: str = "logs/missing_symbols.jsonl") -> Dict[str, Any]:
    """
    扫描并记录缺失符号的主函数
    
    Args:
        db_path: 数据库路径
        log_path: 日志文件路径
        
    Returns:
        扫描结果摘要
    """
    try:
        storage = get_storage(db_path)
        detector = MissingSymbolsDetector(storage)
        logger_obj = MissingSymbolsLogger(log_path)
        
        # 检测缺失符号
        missing_by_type = detector.detect_missing_symbols()
        
        # 记录到日志文件
        logger_obj.write_missing_symbols(missing_by_type)
        
        # 返回摘要
        summary = logger_obj.get_summary()
        summary['scan_completed_at'] = datetime.now().isoformat()
        
        return summary
        
    except Exception as e:
        logger.error(f"扫描缺失符号失败: {e}")
        raise


if __name__ == "__main__":
    # 命令行测试
    result = scan_and_log_missing_symbols()
    print(f"扫描完成: {result}")
