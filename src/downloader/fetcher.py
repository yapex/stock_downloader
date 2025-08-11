import os
import tushare as ts
import pandas as pd
import logging
from ratelimit import limits
from .utils import normalize_stock_code, is_interval_greater_than_7_days
from .simple_retry import simple_retry

# 限流配置常量
RATE_LIMIT_CALLS = 190  # API调用次数限制
RATE_LIMIT_PERIOD = 60  # 时间窗口（秒）

logger = logging.getLogger(__name__)


class TushareFetcher:
    """
    封装 Tushare Pro API 的数据获取器。
    
    通过构造函数注入 token，避免直接依赖环境变量。
    延迟验证 API 连接，避免构造函数中的网络调用。
    """

    def __init__(self, token: str):
        """
        初始化 TushareFetcher
        
        Args:
            token: Tushare Pro API token
            
        Raises:
            ValueError: 当 token 为空或无效时
        """
        if not token or not token.strip():
            raise ValueError("Tushare token 不能为空")
            
        self.logger = logging.getLogger(__name__)
        
        # 设置 token 并初始化 API
        self._token = token.strip()
        ts.set_token(self._token)
        self.pro = ts.pro_api()
        self._verified = False
    
    def verify_connection(self) -> bool:
        """
        验证 API 连接是否正常
        
        Returns:
            bool: 连接正常返回 True，否则返回 False
        """
        try:
            self.pro.trade_cal(exchange="SSE", limit=1)
            self._verified = True
            logger.debug("Tushare Pro API 连接验证成功")
            return True
        except Exception as e:
            logger.error(f"Tushare Pro API 连接验证失败: {e}")
            self._verified = False
            return False
    

    


    @simple_retry(max_retries=3, task_name="获取A股列表")
    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    def fetch_stock_list(self) -> pd.DataFrame | None:
        """获取所有A股的列表。"""
        logger.info(f"[API调用] 开始获取A股列表 - Fetcher实例ID: {id(self)}")
        try:
            df = self.pro.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name,area,industry,market,list_date",
            )
            logger.info(f"[API调用成功] 获取A股列表完成，共 {len(df)} 只股票")
            return df
        except Exception as e:
            logger.warning(f"[API调用失败] 获取A股列表失败: {e}")
            raise

    @simple_retry(max_retries=3, task_name="日K线数据")
    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    def fetch_daily_history(
        self, ts_code: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame | None:
        
        ts_code = normalize_stock_code(ts_code)
        logger.info(f"[API调用] 获取日K线数据 - 股票: {ts_code}, 时间: {start_date}~{end_date}, 复权: {adjust}")
        
        try:
            adj_param = adjust if adjust != "none" else None
            df = ts.pro_bar(
                ts_code=ts_code,
                adj=adj_param,
                start_date=start_date,
                end_date=end_date,
                asset="E",
                freq="D",
            )
            
            data_count = len(df) if df is not None and not df.empty else 0
            logger.info(f"[API调用成功] 日K线数据获取完成 - 股票: {ts_code}, 数据量: {data_count}条")
        except Exception as e:
            logger.warning(f"[API调用失败] 日K线数据获取失败 - 股票: {ts_code}, 错误: {e}")
            raise

        # ---> 如果间隔时间大于 7 天，才需要警告 <---
        if (
            is_interval_greater_than_7_days(start_date=start_date, end_date=end_date)
            and df is None
        ):
            # 这种情况可能发生在 Tushare 服务器返回非标准空数据时
            logger.warning(
                f"Tushare API for {ts_code} 返回了 None，将其视为空DataFrame。"
            )
            df = pd.DataFrame()  # 将 None 规范化为空 DataFrame

        if df is not None and not df.empty:
            df.sort_values(by="trade_date", inplace=True, ignore_index=True)

        return df

    @simple_retry(max_retries=3, task_name="每日指标")
    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    def fetch_daily_basic(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """获取【单只股票】在【一个时间段内】的所有每日指标。"""
        
        ts_code = normalize_stock_code(ts_code)
        logger.info(f"[API调用] 获取每日指标 - 股票: {ts_code}, 时间: {start_date}~{end_date}")
        
        try:
            df = self.pro.daily_basic(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_date,close,turnover_rate,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_mv,circ_mv",
            )
            
            if df is not None:
                if not df.empty:
                    df.sort_values(by="trade_date", inplace=True, ignore_index=True)
                data_count = len(df)
            else:
                df = pd.DataFrame()
                data_count = 0
            
            logger.info(f"[API调用成功] 每日指标获取完成 - 股票: {ts_code}, 数据量: {data_count}条")
            return df
        except Exception as e:
            logger.warning(f"[API调用失败] 每日指标获取失败 - 股票: {ts_code}, 错误: {e}")
            raise

    # ---> 新增的财务报表获取方法 <---
    @simple_retry(max_retries=3, task_name="财务报表-利润表")
    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    def fetch_income(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        normalized_code = normalize_stock_code(ts_code)
        logger.info(f"[API调用] 获取利润表 - 股票: {normalized_code}, 时间: {start_date}~{end_date}")
        
        try:
            df = self.pro.income(
                ts_code=normalized_code, start_date=start_date, end_date=end_date
            )
            
            if df is not None and not df.empty:
                df.sort_values(by="ann_date", inplace=True, ignore_index=True)
                data_count = len(df)
            else:
                data_count = 0
            
            logger.info(f"[API调用成功] 利润表获取完成 - 股票: {normalized_code}, 数据量: {data_count}条")
            return df
        except Exception as e:
            logger.warning(f"[API调用失败] 利润表获取失败 - 股票: {normalized_code}, 错误: {e}")
            raise

    @simple_retry(max_retries=3, task_name="财务报表-资产负债表")
    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    def fetch_balancesheet(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        normalized_code = normalize_stock_code(ts_code)
        logger.info(f"[API调用] 获取资产负债表 - 股票: {normalized_code}, 时间: {start_date}~{end_date}")
        
        try:
            df = self.pro.balancesheet(
                ts_code=normalized_code, start_date=start_date, end_date=end_date
            )
            
            if df is not None and not df.empty:
                df.sort_values(by="ann_date", inplace=True, ignore_index=True)
                data_count = len(df)
            else:
                data_count = 0
            
            logger.info(f"[API调用成功] 资产负债表获取完成 - 股票: {normalized_code}, 数据量: {data_count}条")
            return df
        except Exception as e:
            logger.warning(f"[API调用失败] 资产负债表获取失败 - 股票: {normalized_code}, 错误: {e}")
            raise

    @simple_retry(max_retries=3, task_name="财务报表-现金流量表")
    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    def fetch_cashflow(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        normalized_code = normalize_stock_code(ts_code)
        logger.info(f"[API调用] 获取现金流量表 - 股票: {normalized_code}, 时间: {start_date}~{end_date}")
        
        try:
            df = self.pro.cashflow(
                ts_code=normalized_code, start_date=start_date, end_date=end_date
            )
            
            if df is not None and not df.empty:
                df.sort_values(by="ann_date", inplace=True, ignore_index=True)
                data_count = len(df)
            else:
                data_count = 0
            
            logger.info(f"[API调用成功] 现金流量表获取完成 - 股票: {normalized_code}, 数据量: {data_count}条")
            return df
        except Exception as e:
            logger.warning(f"[API调用失败] 现金流量表获取失败 - 股票: {normalized_code}, 错误: {e}")
            raise
