"""股票基本信息字段映射模块"""

from threading import local
from downloader2.interfaces.fetchingable import IFetchingable
from downloader.rate_limiter_native import rate_limit
import tushare as ts
import pandas as pd
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class StockBasicFetcher(IFetchingable):
    _thread_local = local()

    @rate_limit("StockBasicFetcher")
    def fetching_by_symbol(
        self, symbol: str, start_date: str, end_date: str, **kwargs
    ) -> pd.DataFrame:
        """
        按照symbol从数据源获取模型数据，symbol不能为空，不支持时间范围
        """
        if not symbol:
            raise ValueError("symbol不能为空")
        if start_date or end_date:
            logger.warning("StockBasicFetcher 不支持时间范围查询")
        try:
            result: pd.DataFrame = ts.pro_api().stock_basic(ts_code=symbol)
            logger.debug(
                f"[API调用成功] fetching_by_symbol {symbol}，共 {len(result)} 只股票"
            )
            return result
        except Exception as e:
            logger.error(f"[API调用失败] fetching_by_symbol {symbol} 失败: {e}")
            raise

    def fetching_by_symbols(
        self, symbols: list[str], start_date: str, end_date: str, **kwargs
    ) -> pd.DataFrame:
        """
        StockBasic 不支持按多个symbol从数据源获取多个模型数据，直接获取所有数据
        """
        return self.fetching_all()

    def fetching_all(self) -> pd.DataFrame:
        """
        从数据源获取所有模型数据
        """
        try:
            result: pd.DataFrame = ts.pro_api().stock_basic()
            logger.debug(f"[API调用成功] fetching_all，共 {len(result)} 只股票")
            return result
        except Exception as e:
            logger.error(f"[API调用失败] fetching_all 失败: {e}")
            raise

    @classmethod
    def get_instance(cls) -> "StockBasicFetcher":
        """获取当前线程的StockBasicFetcher实例(ThreadLocal单例)"""
        if not hasattr(cls._thread_local, "instance"):
            cls._thread_local.instance = cls()
        return cls._thread_local.instance

    @classmethod
    def clear_instance(cls) -> None:
        """清除当前线程的实例"""
        if hasattr(cls._thread_local, "instance"):
            delattr(cls._thread_local, "instance")


@dataclass(frozen=True)
class StockBasic:
    """股票基本信息字段定义"""

    ts_code: str
    symbol: str
    name: str
    area: str
    industry: str
    fullname: str
    enname: str
    cnspell: str
    market: str
    exchange: str
    curr_type: str
    list_status: str
    list_date: str
    delist_date: str
    is_hs: str
    act_name: str
    act_ent_type: str

    @classmethod
    def from_series(cls, series: pd.Series) -> "StockBasic":
        """从Series创建StockBasic实例"""

        # 由于使用了dataclass，直接获取字段名列表
        fields = [field.name for field in cls.__dataclass_fields__.values()]

        # 构建参数字典,只处理Series中存在的字段
        params = {}
        for field in fields:
            if field in series.index:
                params[field] = series[field]

        return cls(**params)

    def to_series(self) -> pd.Series:
        """将StockBasic实例转换为Series"""
        # 由于使用了dataclass，直接获取所有字段值
        data = {field: getattr(self, field) for field in self.__dataclass_fields__}
        return pd.Series(data)
