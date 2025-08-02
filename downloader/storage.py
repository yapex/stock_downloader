import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class ParquetStorage:
    """
    一个纯粹、健壮的 Parquet 存储器。
    支持增量保存(save)和全量覆盖(overwrite)两种模式。
    """

    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path)
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"存储根目录已创建: {self.base_path.resolve()}")

    def _get_file_path(self, data_type: str, entity_id: str) -> Path:
        """
        根据数据类型和实体ID构建文件路径。
        实体ID可以是 ts_code 或其他唯一标识符如 'stock_list'。
        """
        return self.base_path / data_type / f"entity={entity_id}" / "data.parquet"

    def get_latest_date(
        self, data_type: str, entity_id: str, date_col: str
    ) -> str | None:
        """获取本地存储的最新日期。"""
        file_path = self._get_file_path(data_type, entity_id)
        if not file_path.exists():
            return None
        try:
            df = pd.read_parquet(file_path, engine="pyarrow", columns=[date_col])
            if date_col in df.columns and not df.empty:
                return df[date_col].max()
            return None
        except Exception as e:
            logger.error(f"读取文件 {file_path} 以获取最新日期时出错: {e}")
            return None

    def save(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str):
        """将 DataFrame 增量保存到 Parquet 文件中。"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return

        if date_col not in df.columns:
            logger.error(
                f"[{data_type}/{entity_id}] DataFrame 中缺少日期列 '{date_col}'，无法增量保存。"
            )
            return

        file_path = self._get_file_path(data_type, entity_id)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            combined_df = df
            if file_path.exists():
                existing_df = pd.read_parquet(file_path, engine="pyarrow")
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.drop_duplicates(
                    subset=[date_col], keep="last", inplace=True
                )

            combined_df.sort_values(by=date_col, inplace=True, ignore_index=True)
            combined_df.to_parquet(file_path, engine="pyarrow", index=False)
            logger.info(
                f"[{data_type}/{entity_id}] 数据已成功增量保存，总计 {len(combined_df)} 条。"
            )

        except Exception as e:
            logger.error(
                f"[{data_type}/{entity_id}] 增量保存到 Parquet 文件 {file_path} 时发生错误: {e}"
            )

    def overwrite(self, df: pd.DataFrame, data_type: str, entity_id: str):
        """将 DataFrame 全量覆盖写入 Parquet 文件。"""
        if not isinstance(df, pd.DataFrame):
            logger.warning(
                f"[{data_type}/{entity_id}] 传入的不是DataFrame，跳过覆盖操作。"
            )
            return

        file_path = self._get_file_path(data_type, entity_id)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            df.to_parquet(file_path, engine="pyarrow", index=False)
            logger.info(
                f"[{data_type}/{entity_id}] 数据已成功全量覆盖，总计 {len(df)} 条。"
            )
        except Exception as e:
            logger.error(
                f"[{data_type}/{entity_id}] 全量覆盖到 Parquet 文件 {file_path} 时发生错误: {e}"
            )
