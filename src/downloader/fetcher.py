import os
import tushare as ts
import pandas as pd
import logging
from .utils import normalize_stock_code

logger = logging.getLogger(__name__)


class TushareFetcher:
    """
    封装 Tushare Pro API 的数据获取器。
    """

    def __init__(self):
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            raise ValueError("错误：未设置 TUSHARE_TOKEN 环境变量。")
        ts.set_token(token)
        self.pro = ts.pro_api()
        try:
            self.pro.trade_cal(exchange="SSE", limit=1)
            logger.info("Tushare Pro API 初始化并验证成功。")
        except Exception as e:
            logger.error(f"Tushare Pro API 初始化或验证失败: {e}")
            raise

    def fetch_stock_list(self) -> pd.DataFrame | None:
        """获取所有A股的列表。"""
        logger.info("开始从 Tushare 获取A股列表...")
        try:
            df = self.pro.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name,area,industry,market,list_date",
            )
            logger.info(f"成功获取到 {len(df)} 只A股的信息。")
            return df
        except Exception as e:
            logger.error(f"获取A股列表失败: {e}", exc_info=True)
            return None

    def fetch_daily_history(
        self, ts_code: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame | None:
        ts_code = normalize_stock_code(ts_code)
        logger.info(
            f"开始获取 {ts_code} 从 {start_date} 到 {end_date} 的日线数据 (复权类型: {adjust or '不复权'})..."
        )
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

            # ---> 核心修正：恢复并优化对 None 返回值的处理 <---
            if df is None:
                # 这种情况可能发生在 Tushare 服务器返回非标准空数据时
                logger.warning(
                    f"Tushare API for {ts_code} 返回了 None，将其视为空DataFrame。"
                )
                df = pd.DataFrame()  # 将 None 规范化为空 DataFrame

            if not df.empty:
                df.sort_values(by="trade_date", inplace=True, ignore_index=True)

            logger.info(f"成功获取到 {ts_code} 的 {len(df)} 条日线数据。")
            return df

        except Exception as e:
            # ---> 核心修正：日志格式统一 <---
            logger.error(f"获取 {ts_code} 的日线数据失败: {e}", exc_info=True)
            return None

    def fetch_daily_basic(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """获取【单只股票】在【一个时间段内】的所有每日指标。"""
        ts_code = normalize_stock_code(ts_code)
        logger.info(f"开始获取 {ts_code} 从 {start_date} 到 {end_date} 的每日指标...")
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
            logger.info(f"成功获取到 {ts_code} 的 {len(df)} 条每日指标数据。")
            return df
        except Exception as e:
            logger.error(f"获取 {ts_code} 的每日指标失败: {e}", exc_info=True)
            return None

    # ---> 新增的财务报表获取方法 <---
    def fetch_income(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        ts_code = normalize_stock_code(ts_code)
        logger.info(f"开始为 {ts_code} 获取利润表 (公告日自 {start_date} 起)...")
        try:
            df = self.pro.income(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
            if df is not None and not df.empty:
                df.sort_values(by="ann_date", inplace=True, ignore_index=True)
            df_len = len(df) if df is not None else 0
            logger.info(f"成功获取到 {ts_code} 的 {df_len} 条利润表数据。")
            return df
        except Exception as e:
            logger.error(f"获取 {ts_code} 的利润表失败: {e}", exc_info=True)
            return None

    def fetch_balancesheet(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        ts_code = normalize_stock_code(ts_code)
        logger.info(f"开始为 {ts_code} 获取资产负债表 (公告日自 {start_date} 起)...")
        try:
            df = self.pro.balancesheet(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
            if df is not None and not df.empty:
                df.sort_values(by="ann_date", inplace=True, ignore_index=True)
            df_len = len(df) if df is not None else 0
            logger.info(f"成功获取到 {ts_code} 的 {df_len} 条资产负债表数据。")
            return df
        except Exception as e:
            logger.error(f"获取 {ts_code} 的资产负债表失败: {e}", exc_info=True)
            return None

    def fetch_cashflow(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        ts_code = normalize_stock_code(ts_code)
        logger.info(f"开始为 {ts_code} 获取现金流量表 (公告日自 {start_date} 起)...")
        try:
            df = self.pro.cashflow(
                ts_code=ts_code, start_date=start_date, end_date=end_date
            )
            if df is not None and not df.empty:
                df.sort_values(by="ann_date", inplace=True, ignore_index=True)
            df_len = len(df) if df is not None else 0
            logger.info(f"成功获取到 {ts_code} 的 {df_len} 条现金流量表数据。")
            return df
        except Exception as e:
            logger.error(f"获取 {ts_code} 的现金流量表失败: {e}", exc_info=True)
            return None
