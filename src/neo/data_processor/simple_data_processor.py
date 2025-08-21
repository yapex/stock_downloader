"""简单数据处理器实现

专注数据清洗、转换和验证的数据处理层。
"""

import logging
from typing import Optional, Dict, Any
import pandas as pd
import time
from datetime import datetime, timedelta

from ..config import get_config
from .interfaces import IDataProcessor
from .types import TaskResult
from ..database.operator import DBOperator

logger = logging.getLogger(__name__)


class SimpleDataProcessor(IDataProcessor):
    """简化的数据处理器实现
    
    专注于数据清洗、转换和验证。
    """
    
    def __init__(self, db_operator: Optional[DBOperator] = None):
        """初始化数据处理器
        
        Args:
            db_operator: 数据库操作器，用于保存数据
        """
        self.config = get_config()
        self.db_operator = db_operator or DBOperator()
        
        # 统计信息跟踪
        self.stats = {
            'total_processed': 0,
            'successful_processed': 0,
            'failed_processed': 0,
            'total_rows_processed': 0,
            'start_time': time.time(),
            'last_stats_output': time.time(),
            'task_type_stats': {},  # 按任务类型统计
        }
        
        # 统计输出间隔（秒）
        self.stats_output_interval = 30
    
    def process(self, task_result: TaskResult) -> bool:
        """处理任务结果
        
        Args:
            task_result: 任务执行结果
            
        Returns:
            bool: 处理是否成功
        """
        task_name = f"{task_result.config.symbol}_{task_result.config.task_type.name}" if task_result.config.symbol else task_result.config.task_type.name
        
        # 更新统计信息
        self.stats['total_processed'] += 1
        task_type_name = task_result.config.task_type.name
        if task_type_name not in self.stats['task_type_stats']:
            self.stats['task_type_stats'][task_type_name] = {'count': 0, 'success': 0, 'rows': 0}
        self.stats['task_type_stats'][task_type_name]['count'] += 1
        
        print(f"📊 开始处理: {task_name}")
        logger.info(f"开始处理TaskResult: {task_result.config.task_type.value}, symbol: {task_result.config.symbol}")
        
        # 检查是否需要输出统计信息
        self._maybe_output_stats()
        
        try:
            # 检查任务是否成功
            if not task_result.success:
                print(f"❌ 任务执行失败，跳过处理: {task_name} - {task_result.error}")
                logger.warning(f"任务执行失败，跳过处理: {task_result.error}")
                # 更新失败统计
                self.stats['failed_processed'] += 1
                return False
            
            # 检查数据是否存在
            if task_result.data is None or task_result.data.empty:
                print(f"⚠️  数据为空，跳过处理: {task_name}")
                logger.warning("数据为空，跳过处理")
                return False
            
            print(f"📈 数据行数: {len(task_result.data)} 行，列数: {len(task_result.data.columns)} 列")
            
            # 数据清洗和验证
            print(f"🧹 开始数据清洗: {task_name}")
            cleaned_data = self._clean_data(task_result.data, task_result.config.task_type.value)
            if cleaned_data is None:
                print(f"❌ 数据清洗失败: {task_name}")
                logger.warning("数据清洗失败")
                return False
            
            print(f"✅ 数据清洗完成: {task_name}，清洗后 {len(cleaned_data)} 行")
            
            # 数据转换
            print(f"🔄 开始数据转换: {task_name}")
            transformed_data = self._transform_data(cleaned_data, task_result.config.task_type.value)
            if transformed_data is None:
                print(f"❌ 数据转换失败: {task_name}")
                logger.warning("数据转换失败")
                return False
            
            print(f"✅ 数据转换完成: {task_name}")
            
            # 数据保存
            print(f"💾 开始保存数据: {task_name}")
            success = self._save_data(transformed_data, task_result.config.task_type.value.api_method)
            
            if success:
                print(f"🎉 数据处理完成: {task_name}，成功保存 {len(transformed_data)} 行数据")
                logger.info(f"数据处理完成: {task_result.config.task_type.value}, symbol: {task_result.config.symbol}, rows: {len(transformed_data)}")
                
                # 更新成功统计
                self.stats['successful_processed'] += 1
                self.stats['total_rows_processed'] += len(transformed_data)
                self.stats['task_type_stats'][task_type_name]['success'] += 1
                self.stats['task_type_stats'][task_type_name]['rows'] += len(transformed_data)
            else:
                print(f"❌ 数据保存失败: {task_name}")
                logger.warning(f"数据保存失败: {task_result.config.task_type}, symbol: {task_result.config.symbol}")
                self.stats['failed_processed'] += 1
            
            return success
            
        except Exception as e:
            print(f"💥 处理异常: {task_name} - {str(e)}")
            logger.error(f"处理TaskResult时出错: {e}")
            self.stats['failed_processed'] += 1
            return False
    
    def _clean_data(self, data: pd.DataFrame, task_type: str) -> Optional[pd.DataFrame]:
        """数据清洗
        
        Args:
            data: 原始数据
            task_type: 任务类型
            
        Returns:
            清洗后的数据，如果清洗失败返回None
        """
        try:
            # 移除空值行
            cleaned_data = data.dropna()
            
            # 根据任务类型进行特定清洗
            if task_type == "stock_basic":
                # 股票基础信息清洗
                required_columns = ['ts_code', 'symbol', 'name']
                if not all(col in cleaned_data.columns for col in required_columns):
                    logger.warning(f"股票基础信息缺少必要字段: {required_columns}")
                    return None
            elif task_type in ["daily", "weekly", "monthly"]:
                # 行情数据清洗
                required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close']
                if not all(col in cleaned_data.columns for col in required_columns):
                    logger.warning(f"行情数据缺少必要字段: {required_columns}")
                    return None
                
                # 确保价格数据为正数
                price_columns = ['open', 'high', 'low', 'close']
                for col in price_columns:
                    if col in cleaned_data.columns:
                        cleaned_data = cleaned_data[cleaned_data[col] > 0]
            
            logger.debug(f"数据清洗完成: {len(data)} -> {len(cleaned_data)} rows")
            return cleaned_data
            
        except Exception as e:
            logger.error(f"数据清洗失败: {e}")
            return None
    
    def _transform_data(self, data: pd.DataFrame, task_type: str) -> Optional[pd.DataFrame]:
        """数据转换
        
        Args:
            data: 清洗后的数据
            task_type: 任务类型
            
        Returns:
            转换后的数据，如果转换失败返回None
        """
        try:
            transformed_data = data.copy()
            
            # 根据任务类型进行特定转换
            if task_type in ["daily", "weekly", "monthly"]:
                # 行情数据转换
                if 'trade_date' in transformed_data.columns:
                    # 确保交易日期格式正确
                    transformed_data['trade_date'] = pd.to_datetime(transformed_data['trade_date'], format='%Y%m%d')
                
                # 计算涨跌幅（如果有前收盘价）
                if 'pre_close' in transformed_data.columns:
                    transformed_data['pct_chg'] = ((transformed_data['close'] - transformed_data['pre_close']) / transformed_data['pre_close'] * 100).round(2)
            
            logger.debug(f"数据转换完成: {len(transformed_data)} rows")
            return transformed_data
            
        except Exception as e:
            logger.error(f"数据转换失败: {e}")
            return None
    
    def _save_data(self, data: pd.DataFrame, task_type: str) -> bool:
        """数据保存
        
        将数据保存到数据库。
        
        Args:
            data: 转换后的数据
            task_type: 任务类型
            
        Returns:
            保存是否成功
        """
        try:
            # 调试信息：打印 task_type 的类型和值
            logger.info(f"task_type 类型: {type(task_type)}, 值: {task_type}")
            
            # 根据任务类型确定表名
            table_name_mapping = {
                'stock_basic': 'stock_basic',
                'daily': 'stock_daily',
                'daily_basic': 'daily_basic',
                'weekly': 'stock_weekly',
                'monthly': 'stock_monthly'
            }
            
            table_name = table_name_mapping.get(task_type)
            if not table_name:
                logger.warning(f"未知的任务类型: {task_type}")
                return False
            
            # 保存数据到数据库
            logger.debug(f"保存数据到数据库: {table_name}, {len(data)} rows")
            self.db_operator.upsert(table_name, data)
            logger.info(f"数据保存成功: {table_name}, {len(data)} rows")
            
            return True
            
        except Exception as e:
            logger.error(f"数据保存失败: {e}")
            return False
    
    def _maybe_output_stats(self) -> None:
        """检查是否需要输出统计信息"""
        current_time = time.time()
        if current_time - self.stats['last_stats_output'] >= self.stats_output_interval:
            self._output_stats()
            self.stats['last_stats_output'] = current_time
    
    def _output_stats(self) -> None:
        """输出当前统计信息"""
        current_time = time.time()
        elapsed_time = current_time - self.stats['start_time']
        
        # 计算处理速率
        processing_rate = self.stats['total_processed'] / elapsed_time if elapsed_time > 0 else 0
        success_rate = (self.stats['successful_processed'] / self.stats['total_processed'] * 100) if self.stats['total_processed'] > 0 else 0
        
        print("\n" + "="*60)
        print("📈 数据处理统计信息")
        print("="*60)
        print(f"⏱️  运行时间: {timedelta(seconds=int(elapsed_time))}")
        print(f"📊 总处理任务: {self.stats['total_processed']}")
        print(f"✅ 成功处理: {self.stats['successful_processed']}")
        print(f"❌ 失败处理: {self.stats['failed_processed']}")
        print(f"📈 成功率: {success_rate:.1f}%")
        print(f"🚀 处理速率: {processing_rate:.2f} 任务/秒")
        print(f"📋 总处理行数: {self.stats['total_rows_processed']}")
        
        # 按任务类型统计
        if self.stats['task_type_stats']:
            print("\n📋 按任务类型统计:")
            for task_type, stats in self.stats['task_type_stats'].items():
                task_success_rate = (stats['success'] / stats['count'] * 100) if stats['count'] > 0 else 0
                print(f"  {task_type}: {stats['count']} 任务, {stats['success']} 成功 ({task_success_rate:.1f}%), {stats['rows']} 行")
        
        print("="*60 + "\n")
        
        # 同时记录到日志
        logger.info(f"统计信息 - 总任务: {self.stats['total_processed']}, 成功: {self.stats['successful_processed']}, 失败: {self.stats['failed_processed']}, 成功率: {success_rate:.1f}%, 处理速率: {processing_rate:.2f} 任务/秒, 总行数: {self.stats['total_rows_processed']}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取当前统计信息
        
        Returns:
            包含统计信息的字典
        """
        current_time = time.time()
        elapsed_time = current_time - self.stats['start_time']
        processing_rate = self.stats['total_processed'] / elapsed_time if elapsed_time > 0 else 0
        success_rate = (self.stats['successful_processed'] / self.stats['total_processed'] * 100) if self.stats['total_processed'] > 0 else 0
        
        return {
            'elapsed_time': elapsed_time,
            'total_processed': self.stats['total_processed'],
            'successful_processed': self.stats['successful_processed'],
            'failed_processed': self.stats['failed_processed'],
            'success_rate': success_rate,
            'processing_rate': processing_rate,
            'total_rows_processed': self.stats['total_rows_processed'],
            'task_type_stats': self.stats['task_type_stats'].copy()
        }