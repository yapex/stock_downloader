"""简单数据处理器实现

专注数据清洗、转换和验证的数据处理层。
"""

import logging
from typing import Optional
import pandas as pd

from ..config import get_config
from .interfaces import IDataProcessor
from .types import TaskResult

logger = logging.getLogger(__name__)


class SimpleDataProcessor(IDataProcessor):
    """简化的数据处理器实现
    
    专注于数据清洗、转换和验证。
    """
    
    def __init__(self):
        """初始化数据处理器"""
        self.config = get_config()
    
    def process(self, task_result: TaskResult) -> bool:
        """处理任务结果
        
        Args:
            task_result: 任务执行结果
            
        Returns:
            bool: 处理是否成功
        """
        logger.debug(f"开始处理TaskResult: {task_result.config.task_type.value}, symbol: {task_result.config.symbol}")
        
        try:
            # 检查任务是否成功
            if not task_result.success:
                logger.warning(f"任务执行失败，跳过处理: {task_result.error}")
                return False
            
            # 检查数据是否存在
            if task_result.data is None or task_result.data.empty:
                logger.warning("数据为空，跳过处理")
                return False
            
            # 数据清洗和验证
            cleaned_data = self._clean_data(task_result.data, task_result.config.task_type.value)
            if cleaned_data is None:
                logger.warning("数据清洗失败")
                return False
            
            # 数据转换
            transformed_data = self._transform_data(cleaned_data, task_result.config.task_type.value)
            if transformed_data is None:
                logger.warning("数据转换失败")
                return False
            
            # 数据保存（这里是模拟保存）
            success = self._save_data(transformed_data, task_result.config.task_type.value)
            
            if success:
                logger.info(f"数据处理完成: {task_result.config.task_type.value}, symbol: {task_result.config.symbol}, rows: {len(transformed_data)}")
            else:
                logger.warning(f"数据保存失败: {task_result.config.task_type.value}, symbol: {task_result.config.symbol}")
            
            return success
            
        except Exception as e:
            logger.error(f"处理TaskResult时出错: {e}")
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
        
        这里是模拟保存，实际应该保存到数据库。
        
        Args:
            data: 转换后的数据
            task_type: 任务类型
            
        Returns:
            保存是否成功
        """
        try:
            # 模拟数据保存
            logger.debug(f"模拟保存数据到数据库: {task_type}, {len(data)} rows")
            
            # 这里可以添加实际的数据库保存逻辑
            # 例如：使用database层的接口保存数据
            
            return True
            
        except Exception as e:
            logger.error(f"数据保存失败: {e}")
            return False