import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class ParquetStorage:
    """
    一个纯粹的、轻量级的 Parquet 存储器。
    """
    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path)
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"存储根目录已创建: {self.base_path.resolve()}")

    def _get_file_path(self, data_type: str, ts_code: str) -> Path:
        """根据数据类型和ts_code构建文件路径。"""
        return self.base_path / data_type / f"symbol={ts_code}" / "data.parquet"

    def get_latest_date(self, data_type: str, ts_code: str, date_col: str = 'trade_date') -> str | None:
        """获取本地存储的最新日期。"""
        file_path = self._get_file_path(data_type, ts_code)
        if not file_path.exists():
            return None
        try:
            df = pd.read_parquet(file_path, engine='pyarrow', columns=[date_col])
            if date_col in df.columns and not df.empty:
                return df[date_col].max()
            return None
        except Exception as e:
            logger.error(f"读取文件 {file_path} 以获取最新日期时出错: {e}")
            return None

    def save(self, df: pd.DataFrame, data_type: str, ts_code: str, date_col: str = 'trade_date'):
        """将 DataFrame 增量保存到 Parquet 文件中。"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return

        if date_col not in df.columns:
            logger.error(f"[{data_type}/{ts_code}] DataFrame 中缺少日期列 '{date_col}'，无法保存。")
            return
        
        file_path = self._get_file_path(data_type, ts_code)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            combined_df = df
            if file_path.exists():
                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.drop_duplicates(subset=[date_col], keep='last', inplace=True)
            
            combined_df.sort_values(by=date_col, inplace=True, ignore_index=True)
            combined_df.to_parquet(file_path, engine='pyarrow', index=False)
            logger.info(f"[{data_type}/{ts_code}] 数据已成功保存/更新，总计 {len(combined_df)} 条。")

        except Exception as e:
            logger.error(f"[{data_type}/{ts_code}] 保存到 Parquet 文件 {file_path} 时发生严重错误: {e}")