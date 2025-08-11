"""数据存储器接口定义"""

from typing import Protocol, List, Dict, Optional, Any
import pandas as pd
from datetime import datetime


class IStorage(Protocol):
    """数据存储器接口"""
    
    def save_stock_list(self, df: pd.DataFrame) -> None:
        """保存股票列表"""
        ...
    
    def save_daily_history(self, df: pd.DataFrame) -> None:
        """保存日K线数据"""
        ...
    
    def save_daily_basic(self, df: pd.DataFrame) -> None:
        """保存每日指标数据"""
        ...
    
    def save_income(self, df: pd.DataFrame) -> None:
        """保存利润表数据"""
        ...
    
    def save_balancesheet(self, df: pd.DataFrame) -> None:
        """保存资产负债表数据"""
        ...
    
    def save_cashflow(self, df: pd.DataFrame) -> None:
        """保存现金流量表数据"""
        ...
    
    def get_latest_date(self, table_name: str, ts_code: str) -> Optional[str]:
        """获取指定股票在指定表中的最新日期"""
        ...
    
    def query_data(
        self,
        table_name: str,
        ts_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """查询数据"""
        ...
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        ...
    
    def close(self) -> None:
        """关闭连接"""
        ...


# 向后兼容的别名
StorageProtocol = IStorage