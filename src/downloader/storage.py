import pandas as pd
from pathlib import Path
from .utils import normalize_stock_code
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
            logger.debug(f"存储根目录已创建: {self.base_path.resolve()}")

    def _get_file_path(self, data_type: str, entity_id: str) -> Path:
        """
        根据数据类型和实体ID构建文件路径。
        对于系统相关数据，不进行股票代码标准化。
        """
        if data_type == "system":
            # 系统数据不需要股票代码标准化
            return self.base_path / data_type / f"{entity_id}.parquet"
        else:
            # 股票数据需要进行标准化处理
            normalized_id = normalize_stock_code(entity_id)
            return self.base_path / data_type / normalized_id / "data.parquet"

    def get_latest_date(
        self, data_type: str, entity_id: str, date_col: str
    ) -> str | None:
        """获取本地存储的最新日期。支持新格式和旧格式的路径。"""
        # Try new format first
        file_path = self._get_file_path(data_type, entity_id)
        if file_path.exists():
            try:
                df = pd.read_parquet(file_path, engine="pyarrow", columns=[date_col])
                if date_col in df.columns and not df.empty:
                    return df[date_col].max()
            except Exception as e:
                logger.error(f"读取文件 {file_path} 以获取最新日期时出错: {e}")

        # Try legacy format if new format doesn't exist or failed
        if data_type != "system":
            # For stock data, also check legacy path format: entity=entity_id
            normalized_id = normalize_stock_code(entity_id)
            legacy_path = (
                self.base_path / data_type / f"entity={normalized_id}" / "data.parquet"
            )
            if legacy_path.exists():
                try:
                    df = pd.read_parquet(
                        legacy_path, engine="pyarrow", columns=[date_col]
                    )
                    if date_col in df.columns and not df.empty:
                        return df[date_col].max()
                except Exception as e:
                    logger.error(
                        f"读取遗留格式文件 {legacy_path} 以获取最新日期时出错: {e}"
                    )

        return None

    def save(self, df: pd.DataFrame, data_type: str, entity_id: str, date_col: str):
        """将 DataFrame 增量保存到 Parquet 文件中。"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return

        if date_col not in df.columns:
            logger.error(
                f"[{data_type}/{entity_id}] DataFrame 中缺少日期列 '{date_col}'，"
                "无法增量保存。"
            )
            return

        file_path = self._get_file_path(data_type, entity_id)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            combined_df = df
            if file_path.exists():
                existing_df = pd.read_parquet(file_path, engine="pyarrow")
                # 在连接前检查 DataFrame 是否为空，避免 FutureWarning
                if not existing_df.empty and not df.empty:
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                elif not existing_df.empty:
                    combined_df = existing_df
                # 如果 df 为空但 existing_df 也为空，则 combined_df 保持为原始 df（即空）
                
                # 只有在 combined_df 不为空时才去重
                if not combined_df.empty:
                    combined_df.drop_duplicates(
                        subset=[date_col], keep="last", inplace=True
                    )

            # 只有在 combined_df 不为空时才排序
            if not combined_df.empty:
                combined_df.sort_values(by=date_col, inplace=True, ignore_index=True)
            combined_df.to_parquet(file_path, engine="pyarrow", index=False)
            logger.debug(
                f"[{data_type}/{entity_id}] 数据已成功增量保存，"
                f"总计 {len(combined_df)} 条。"
            )

        except Exception as e:
            logger.error(
                f"[{data_type}/{entity_id}] 增量保存到 Parquet 文件 {file_path} "
                f"时发生错误: {e}"
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
            logger.debug(
                f"[{data_type}/{entity_id}] 数据已成功全量覆盖，总计 {len(df)} 条。"
            )
        except Exception as e:
            logger.error(
                f"[{data_type}/{entity_id}] 全量覆盖到 Parquet 文件 {file_path} "
                f"时发生错误: {e}"
            )
