"""数据存储器接口定义"""

from typing import Protocol, List, Dict, Optional, Any, runtime_checkable
import pandas as pd
from datetime import datetime


@runtime_checkable
class IStorageSaver(Protocol):
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


@runtime_checkable
class IStorageSearcher(Protocol):
    """数据加载器接口"""

    def get_latest_date_by_stock(
        self, table_name: str, data_type: str
    ) -> Optional[str]:
        """获取指定股票在指定表中的最新日期"""
        ...

    def get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码"""
        ...
