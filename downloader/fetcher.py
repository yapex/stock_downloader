import os
import tushare as ts
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class TushareFetcher:
    """
    封装 Tushare Pro API 的数据获取器。
    采用 ts.set_token() 的标准方式进行初始化。
    """
    def __init__(self):
        token = os.getenv('TUSHARE_TOKEN')
        if not token:
            raise ValueError("错误：未设置 TUSHARE_TOKEN 环境变量。请检查 .env 文件或系统环境变量。")
        
        try:
            ts.set_token(token)
            self.pro = ts.pro_api()
            # 进行一次简单的验证调用
            self.pro.trade_cal(exchange='SSE', limit=1)
            logger.info("Tushare Pro API 初始化并验证成功。")
        except Exception as e:
            logger.error(f"Tushare Pro API 初始化或验证失败: {e}")
            raise

    def fetch_stock_list(self) -> pd.DataFrame | None:
        """获取所有A股的列表，保留原始列名。"""
        logger.info("开始从 Tushare 获取A股列表...")
        try:
            df = self.pro.stock_basic(
                exchange='', 
                list_status='L', 
                fields='ts_code,symbol,name,area,industry,market,list_date'
            )
            logger.info(f"成功获取到 {len(df)} 只A股的信息。")
            return df
        except Exception as e:
            logger.error(f"获取A股列表失败: {e}", exc_info=True)
            return None

    def fetch_daily_history(self, ts_code: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame | None:
        """
        使用 ts.pro_bar 获取日线历史数据。
        
        Args:
            ts_code (str): Tushare 股票代码, e.g., "000001.SZ"。
            start_date (str): 开始日期, "YYYYMMDD"。
            end_date (str): 结束日期, "YYYYMMDD"。
            adjust (str): 复权类型, 'qfq', 'hfq', 或 'none'。
        
        Returns:
            pd.DataFrame | None: 包含日K线数据的 DataFrame, 或在失败时返回 None。
        """
        logger.info(f"开始获取 {ts_code} 从 {start_date} 到 {end_date} 的日线数据 (复权类型: {adjust or '不复权'})...")
        try:
            adj_param = adjust if adjust != 'none' else None
            
            df = ts.pro_bar(
                ts_code=ts_code, 
                adj=adj_param, 
                start_date=start_date, 
                end_date=end_date,
                asset='E',
                freq='D'
            )
            
            if df is not None and not df.empty:
                # pro_bar 返回的数据是日期降序的，我们将其升序排列
                df.sort_values(by='trade_date', inplace=True, ignore_index=True)
            
            df_len = len(df) if df is not None else 0
            logger.info(f"成功获取到 {ts_code} 的 {df_len} 条日线数据。")
            return df
            
        except Exception as e:
            logger.error(f"获取 {ts_code} 的日线数据失败: {e}", exc_info=True)
            return None