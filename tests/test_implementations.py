"""测试专用的实现类，用于替代Mock对象"""

import pandas as pd
from typing import Optional, List
from downloader.interfaces import IFetcher, IStorage


class MockFetcher(IFetcher):
    """测试专用的Fetcher实现，记录调用历史并返回预设数据"""
    
    def __init__(self, token: str = "test_token"):
        self.token = token
        self.call_history: List[str] = []
        self.return_empty = False  # 控制是否返回空DataFrame
    
    def fetch_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        self.call_history.append("fetch_stock_list")
        if self.return_empty:
            return pd.DataFrame()
        return pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"]})
    
    def fetch_daily_history(self, ts_code: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        """获取日线历史数据"""
        self.call_history.append(f"fetch_daily_history_{ts_code}_{start_date}_{end_date}_{adjust}")
        if self.return_empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "ts_code": [ts_code, ts_code],
            "trade_date": ["20230101", "20230102"],
            "close": [100.0, 101.0]
        })
    
    def fetch_daily_basic(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取每日基本面数据"""
        self.call_history.append(f"fetch_daily_basic_{ts_code}_{start_date}_{end_date}")
        if self.return_empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "ts_code": [ts_code, ts_code],
            "trade_date": ["20230101", "20230102"],
            "turnover_rate": [1.5, 2.0]
        })
    
    def fetch_income(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取利润表数据"""
        self.call_history.append(f"fetch_income_{ts_code}_{start_date}_{end_date}")
        if self.return_empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "ts_code": [ts_code],
            "ann_date": ["20230425"],
            "revenue": [1000000]
        })
    
    def fetch_balancesheet(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取资产负债表数据"""
        self.call_history.append(f"fetch_balancesheet_{ts_code}_{start_date}_{end_date}")
        if self.return_empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "ts_code": [ts_code],
            "ann_date": ["20230425"],
            "total_assets": [5000000]
        })
    
    def fetch_cashflow(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取现金流量表数据"""
        self.call_history.append(f"fetch_cashflow_{ts_code}_{start_date}_{end_date}")
        if self.return_empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "ts_code": [ts_code],
            "ann_date": ["20230425"],
            "operating_cash_flow": [800000]
        })
    
    def set_return_empty(self, empty: bool = True):
        """设置是否返回空DataFrame"""
        self.return_empty = empty
    
    def reset_call_history(self):
        """重置调用历史"""
        self.call_history.clear()


class MockStorage(IStorage):
    """测试专用的Storage实现，使用内存存储数据"""
    
    def __init__(self):
        self.call_history: List[str] = []
        self.daily_data: dict = {}  # ts_code -> DataFrame
        self.financial_data: dict = {}  # ts_code -> DataFrame
        self.fundamental_data: dict = {}  # ts_code -> DataFrame
        self.stock_list: Optional[pd.DataFrame] = None
        self.latest_dates: dict = {}  # (ts_code, data_type) -> date
    
    def save_daily_data(self, df: pd.DataFrame) -> None:
        """保存日线数据"""
        self.call_history.append("save_daily_data")
        if not df.empty and "ts_code" in df.columns:
            for ts_code in df["ts_code"].unique():
                stock_data = df[df["ts_code"] == ts_code].copy()
                if ts_code in self.daily_data:
                    # 合并数据，去重
                    combined = pd.concat([self.daily_data[ts_code], stock_data])
                    self.daily_data[ts_code] = combined.drop_duplicates(subset=["trade_date"]).reset_index(drop=True)
                else:
                    self.daily_data[ts_code] = stock_data
                
                # 更新最新日期
                if "trade_date" in stock_data.columns:
                    latest_date = stock_data["trade_date"].max()
                    self.latest_dates[(ts_code, "daily")] = latest_date
    
    def save_financial_data(self, df: pd.DataFrame) -> None:
        """保存财务数据"""
        self.call_history.append("save_financial_data")
        if not df.empty and "ts_code" in df.columns:
            for ts_code in df["ts_code"].unique():
                stock_data = df[df["ts_code"] == ts_code].copy()
                if ts_code in self.financial_data:
                    combined = pd.concat([self.financial_data[ts_code], stock_data])
                    self.financial_data[ts_code] = combined.drop_duplicates(subset=["ann_date"]).reset_index(drop=True)
                else:
                    self.financial_data[ts_code] = stock_data
                
                # 更新最新日期
                if "ann_date" in stock_data.columns:
                    latest_date = stock_data["ann_date"].max()
                    self.latest_dates[(ts_code, "financial")] = latest_date
    
    def save_fundamental_data(self, df: pd.DataFrame) -> None:
        """保存基本面数据"""
        self.call_history.append("save_fundamental_data")
        if not df.empty and "ts_code" in df.columns:
            for ts_code in df["ts_code"].unique():
                stock_data = df[df["ts_code"] == ts_code].copy()
                if ts_code in self.fundamental_data:
                    combined = pd.concat([self.fundamental_data[ts_code], stock_data])
                    self.fundamental_data[ts_code] = combined.drop_duplicates(subset=["trade_date"]).reset_index(drop=True)
                else:
                    self.fundamental_data[ts_code] = stock_data
                
                # 更新最新日期
                if "trade_date" in stock_data.columns:
                    latest_date = stock_data["trade_date"].max()
                    self.latest_dates[(ts_code, "fundamental")] = latest_date
    
    def save_stock_list(self, df: pd.DataFrame) -> None:
        """保存股票列表"""
        self.call_history.append("save_stock_list")
        self.stock_list = df.copy() if not df.empty else pd.DataFrame()
    
    def get_latest_date_by_stock(self, ts_code: str, data_type: str) -> Optional[str]:
        """获取指定股票的最新数据日期"""
        self.call_history.append(f"get_latest_date_{ts_code}_{data_type}")
        return self.latest_dates.get((ts_code, data_type))
    
    def query_daily_data_by_stock(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """查询指定股票的日线数据"""
        self.call_history.append(f"query_daily_data_{ts_code}")
        if ts_code not in self.daily_data:
            return pd.DataFrame()
        
        data = self.daily_data[ts_code].copy()
        if start_date:
            data = data[data["trade_date"] >= start_date]
        if end_date:
            data = data[data["trade_date"] <= end_date]
        return data
    
    def get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码"""
        self.call_history.append("get_all_stock_codes")
        if self.stock_list is not None and not self.stock_list.empty:
            return self.stock_list["ts_code"].tolist()
        return list(set(list(self.daily_data.keys()) + list(self.financial_data.keys()) + list(self.fundamental_data.keys())))
    
    def reset_call_history(self):
        """重置调用历史"""
        self.call_history.clear()
    
    def clear_all_data(self):
        """清空所有数据"""
        self.daily_data.clear()
        self.financial_data.clear()
        self.fundamental_data.clear()
        self.latest_dates.clear()
        self.stock_list = None
        self.call_history.clear()


class MockStrategy:
    """测试专用的工厂策略"""
    
    def __init__(self, fetcher_instance):
        self.fetcher_instance = fetcher_instance
        self.call_count = 0
    
    def __call__(self, token: str):
        self.call_count += 1
        return self.fetcher_instance


class SingletonMockStrategy:
    """测试专用的单例策略"""
    
    def __init__(self, fetcher_instance):
        self.fetcher_instance = fetcher_instance
        self.call_count = 0
        self._instance = None
    
    def __call__(self, token: str):
        self.call_count += 1
        if self._instance is None:
            self._instance = self.fetcher_instance
        return self._instance
    
    def reset(self):
        """重置单例状态"""
        self._instance = None
        self.call_count = 0