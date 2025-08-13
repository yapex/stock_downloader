"""股票基本信息字段映射模块"""

from threading import local
from downloader2.interfaces.fetchingable import IFetchingable
from downloader.rate_limiter_native import rate_limit
import tushare as ts
import pandas as pd
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StockBasicFields:
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
    def from_series(cls, series: pd.Series) -> "StockBasicFields":
        """从Series创建StockBasicFields实例"""
        # 获取类的所有大写字段名称
        # 使用列表推导式获取StockBasicFields类中所有大写的属性名
        # 1. dir(StockBasicFields)获取类的所有属性和方法名称
        # 2. attr.isupper()检查属性名是否全部大写
        # 3. 将符合条件的属性名添加到列表中
        # 使用vars()获取类的所有属性

        # 由于使用了dataclass，直接获取字段名列表
        fields = [field.name for field in cls.__dataclass_fields__.values()]

        # 构建参数字典,只处理Series中存在的字段
        params = {}
        for field in fields:
            if field in series.index:
                params[field] = series[field]

        return cls(**params)

    def to_series(self) -> pd.Series:
        """将StockBasicFields实例转换为Series"""
        # 由于使用了dataclass，直接获取所有字段值
        data = {field: getattr(self, field) for field in self.__dataclass_fields__}
        return pd.Series(data)


class StockBasic(IFetchingable):
    _thread_local = local()

    def __init__(self):
        # ts.set_token(token)
        self._pro = ts.pro_api()

    @rate_limit("StockBasic")
    def fetching(self, symbol: str = "") -> pd.DataFrame:
        """
        从数据源获取模型数据
        """
        logger.debug(f"[API调用] 开始获取A股列表 - Fetcher实例ID: {id(self)}")
        try:
            df = self._pro.stock_basic(ts_code=symbol)
            logger.debug(f"[API调用成功] 获取A股列表完成，共 {len(df)} 只股票")
            return df
        except Exception as e:
            logger.error(f"[API调用失败] 获取A股列表失败: {e}")
            raise
        ...

    @classmethod
    def get_instance(cls) -> "StockBasic":
        """获取当前线程的StockBasic实例（ThreadLocal单例）"""
        if not hasattr(cls._thread_local, "instance"):
            cls._thread_local.instance = cls()
        return cls._thread_local.instance

    @classmethod
    def clear_instance(cls) -> None:
        """清除当前线程的实例"""
        if hasattr(cls._thread_local, "instance"):
            delattr(cls._thread_local, "instance")
