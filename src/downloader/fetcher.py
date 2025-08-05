import os
import tushare as ts
import pandas as pd
import logging
from .utils import normalize_stock_code, is_interval_greater_than_7_days
from .rate_limit import rate_limit

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

    @rate_limit(calls_per_minute=200)
    def fetch_stock_list(self) -> pd.DataFrame | None:
        """获取所有A股的列表。"""
        logger.debug("开始从 Tushare 获取A股列表...")
        try:
            df = self.pro.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name,area,industry,market,list_date",
            )
            logger.debug(f"成功获取到 {len(df)} 只A股的信息。")
            return df
        except Exception as e:
            logger.error(f"获取A股列表失败: {e}", exc_info=True)
            return None

    @rate_limit(calls_per_minute=500)
    def fetch_daily_history(
        self, ts_code: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame | None:
        ts_code = normalize_stock_code(ts_code)
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

            # ---> 如果间隔时间大于 7 天，才需要警告 <---

            if (
                is_interval_greater_than_7_days(
                    start_date=start_date, end_date=end_date
                )
                and df is None
            ):
                # 这种情况可能发生在 Tushare 服务器返回非标准空数据时
                logger.warning(
                    f"Tushare API for {ts_code} 返回了 None，将其视为空DataFrame。"
                )
                df = pd.DataFrame()  # 将 None 规范化为空 DataFrame

            if not df.empty:
                df.sort_values(by="trade_date", inplace=True, ignore_index=True)

            return df

        except Exception as e:
            # ---> 核心修正：日志格式统一 <---
            logger.error(f"获取 {ts_code} 的日线数据失败: {e}", exc_info=True)
            return None

    @rate_limit(calls_per_minute=200)
    def fetch_daily_basic(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """获取【单只股票】在【一个时间段内】的所有每日指标。"""
        ts_code = normalize_stock_code(ts_code)
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
            else:
                df = pd.DataFrame()
            return df
        except Exception as e:
            logger.error(f"获取 {ts_code} 的每日指标失败: {e}", exc_info=True)
            return None

    # ---> 新增的财务报表获取方法 <---
    @rate_limit(calls_per_minute=200)
    def fetch_income(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        ts_code = normalize_stock_code(ts_code)
        try:
            df = self.pro.income(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
            if df is not None and not df.empty:
                df.sort_values(by="ann_date", inplace=True, ignore_index=True)
            return df
        except Exception as e:
            logger.error(f"获取 {ts_code} 的利润表失败: {e}", exc_info=True)
            return None

    @rate_limit(calls_per_minute=200)
    def fetch_balancesheet(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        ts_code = normalize_stock_code(ts_code)
        try:
            df = self.pro.balancesheet(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
            if df is not None and not df.empty:
                df.sort_values(by="ann_date", inplace=True, ignore_index=True)
            return df
        except Exception as e:
            logger.error(f"获取 {ts_code} 的资产负债表失败: {e}", exc_info=True)
            return None

    @rate_limit(calls_per_minute=200)
    def fetch_cashflow(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        ts_code = normalize_stock_code(ts_code)
        try:
            df = self.pro.cashflow(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
            if df is not None and not df.empty:
                df.sort_values(by="ann_date", inplace=True, ignore_index=True)
            return df
        except Exception as e:
            logger.error(f"获取 {ts_code} 的现金流量表失败: {e}", exc_info=True)
            return None
