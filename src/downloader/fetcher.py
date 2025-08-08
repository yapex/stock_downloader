import os
import tushare as ts
import pandas as pd
import logging
from pyrate_limiter import limiter_factory
from .utils import normalize_stock_code, is_interval_greater_than_7_days
from .error_handler import (
    enhanced_retry,
    NETWORK_RETRY_STRATEGY,
    API_LIMIT_RETRY_STRATEGY,
)

logger = logging.getLogger(__name__)


class TushareFetcher:
    """
    封装 Tushare Pro API 的数据获取器。
    """

    def __init__(self, default_rate_limit: int = 150):
        """
        初始化 TushareFetcher

        Args:
            default_rate_limit: 默认的API调用速率限制（每分钟调用次数）
        """
        self.default_rate_limit = default_rate_limit
        
        # 为每个方法创建独立的限流器，每分钟最多200次调用
        # max_delay=0 表示不阻塞，立即返回结果
        self.limiters = {
            'stock_list': limiter_factory.create_inmemory_limiter(rate_per_duration=200, duration=60000, max_delay=0),
            'daily_history': limiter_factory.create_inmemory_limiter(rate_per_duration=200, duration=60000, max_delay=0),
            'daily_basic': limiter_factory.create_inmemory_limiter(rate_per_duration=200, duration=60000, max_delay=0),
            'income': limiter_factory.create_inmemory_limiter(rate_per_duration=200, duration=60000, max_delay=0),
            'balancesheet': limiter_factory.create_inmemory_limiter(rate_per_duration=200, duration=60000, max_delay=0),
            'cashflow': limiter_factory.create_inmemory_limiter(rate_per_duration=200, duration=60000, max_delay=0)
        }
        
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            raise ValueError("错误：未设置 TUSHARE_TOKEN 环境变量。")
        ts.set_token(token)
        self.pro = ts.pro_api()
        try:
            self.pro.trade_cal(exchange="SSE", limit=1)
            logger.debug("Tushare Pro API 初始化并验证成功。")
        except Exception as e:
            logger.error(f"Tushare Pro API 初始化或验证失败: {e}")
            raise

    @enhanced_retry(strategy=NETWORK_RETRY_STRATEGY, task_name="获取A股列表")
    def fetch_stock_list(self) -> pd.DataFrame | None:
        """获取所有A股的列表。"""
        # 限流控制
        self.limiters['stock_list'].try_acquire("stock_list")
        
        logger.debug("开始从 Tushare 获取A股列表...")
        df = self.pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,market,list_date",
        )
        logger.debug(f"成功获取到 {len(df)} 只A股的信息。")
        return df

    @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="日K线数据")
    def fetch_daily_history(
        self, ts_code: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame | None:
        # 限流控制
        self.limiters['daily_history'].try_acquire("daily_history")
        
        ts_code = normalize_stock_code(ts_code)
        adj_param = adjust if adjust != "none" else None
        df = ts.pro_bar(
            ts_code=ts_code,
            adj=adj_param,
            start_date=start_date,
            end_date=end_date,
            asset="E",
            freq="D",
        )

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

    @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="每日指标")
    def fetch_daily_basic(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """获取【单只股票】在【一个时间段内】的所有每日指标。"""
        # 限流控制
        self.limiters['daily_basic'].try_acquire("daily_basic")
        
        ts_code = normalize_stock_code(ts_code)
        df = self.pro.daily_basic(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="ts_code,trade_date,close,turnover_rate,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_mv,circ_mv",
        )
        if df is not None:
            if not df.empty:
                df.sort_values(by="trade_date", inplace=True, ignore_index=True)
        else:
            df = pd.DataFrame()
        return df

    # ---> 新增的财务报表获取方法 <---
    @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="财务报表-利润表")
    def fetch_income(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        # 限流控制
        self.limiters['income'].try_acquire("income")
        
        ts_code = normalize_stock_code(ts_code)
        df = self.pro.income(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            df.sort_values(by="ann_date", inplace=True, ignore_index=True)
        return df

    @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="财务报表-资产负债表")
    def fetch_balancesheet(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        # 限流控制
        self.limiters['balancesheet'].try_acquire("balancesheet")
        
        ts_code = normalize_stock_code(ts_code)
        df = self.pro.balancesheet(
            ts_code=ts_code, start_date=start_date, end_date=end_date
        )
        if df is not None and not df.empty:
            df.sort_values(by="ann_date", inplace=True, ignore_index=True)
        return df

    @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="财务报表-现金流量表")
    def fetch_cashflow(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        # 限流控制
        self.limiters['cashflow'].try_acquire("cashflow")
        
        ts_code = normalize_stock_code(ts_code)
        df = self.pro.cashflow(
            ts_code=ts_code, start_date=start_date, end_date=end_date
        )
        if df is not None and not df.empty:
            df.sort_values(by="ann_date", inplace=True, ignore_index=True)
        return df
