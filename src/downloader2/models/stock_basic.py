"""股票基本信息字段映射模块"""

from typing import List
from threading import local
from downloader import simple_retry
from downloader2.interfaces.model import IModel, IModelFetcher
import tushare as ts
import pandas as pd
from ratelimit import rate_limit
import logging

logger = logging.getLogger(__name__)


class StockBasicFields:
    """股票基本信息字段定义"""

    TS_CODE = "ts_code"
    SYMBOL = "symbol"
    NAME = "name"
    AREA = "area"
    INDUSTRY = "industry"
    FULL_NAME = "fullname"
    EN_NAME = "enname"
    CNSPELL = "cnspell"
    MARKET = "market"
    EXCHANGE = "exchange"
    CURR_TYPE = "curr_type"
    LIST_STATUS = "list_status"
    LIST_DATE = "list_date"
    DELIST_DATE = "delist_date"
    IS_HS = "is_hs"
    ACT_NAME = "act_name"
    ACT_ENT_TYPE = "act_ent_type"

    def __init__(self, **kwargs):
        """初始化股票基本信息字段"""
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def from_series(cls, series: pd.Series) -> "StockBasicFields":
        """从Series创建StockBasicFields实例"""
        # 获取类的所有大写字段名称
        # 使用列表推导式获取StockBasicFields类中所有大写的属性名
        # 1. dir(StockBasicFields)获取类的所有属性和方法名称
        # 2. attr.isupper()检查属性名是否全部大写
        # 3. 将符合条件的属性名添加到列表中
        # 使用vars()获取类的所有属性
        class_attrs = vars(cls)
        # 过滤出大写的属性名
        fields = [attr for attr in class_attrs if attr.isupper()]
        # 构建参数字典,只处理Series中存在的字段
        params = {}
        for field in fields:
            field_value = class_attrs[field]
            if field_value in series.index:
                params[field.lower()] = series[field_value]
        return cls(**params)

    @classmethod
    def to_series(cls, fields: "StockBasicFields") -> pd.Series:
        """将StockBasicFields实例转换为Series"""
        data = {}
        for field_name in dir(cls):
            if field_name.isupper():
                field_value = getattr(cls, field_name)
                # 将字段名转换为属性名（处理特殊映射）
                if field_value == "fullname":
                    attr_name = "full_name"
                elif field_value == "enname":
                    attr_name = "en_name"
                else:
                    attr_name = field_value.lower()

                if hasattr(fields, attr_name):
                    data[field_value] = getattr(fields, attr_name)
        return pd.Series(data)


class StockBasic(IModelFetcher):
    _thread_local = local()

    def __init__(self, token: str):
        self._token = token
        ts.set_token(token)
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
    def get_instance(cls, token: str) -> "StockBasic":
        """获取当前线程的StockBasic实例（ThreadLocal单例）"""
        if not hasattr(cls._thread_local, "instance"):
            cls._thread_local.instance = cls(token)
        return cls._thread_local.instance

    @classmethod
    def clear_instance(cls) -> None:
        """清除当前线程的实例"""
        if hasattr(cls._thread_local, "instance"):
            delattr(cls._thread_local, "instance")
