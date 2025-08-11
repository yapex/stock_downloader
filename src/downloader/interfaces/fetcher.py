"""数据获取器接口定义"""

from typing import Protocol
import pandas as pd


class IFetcher(Protocol):
    """数据获取器接口"""
    
    def verify_connection(self) -> bool:
        """验证连接是否正常"""
        ...
    
    def fetch_stock_list(self) -> pd.DataFrame | None:
        """获取股票列表"""
        ...
    
    def fetch_daily_history(
        self, ts_code: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame | None:
        """获取日K线数据"""
        ...
    
    def fetch_daily_basic(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """获取每日指标"""
        ...
    
    def fetch_income(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """获取利润表数据"""
        ...
    
    def fetch_balancesheet(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """获取资产负债表数据"""
        ...
    
    def fetch_cashflow(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """获取现金流量表数据"""
        ...


# 向后兼容的别名
FetcherProtocol = IFetcher