from typing import Protocol, runtime_checkable, List
import pandas as pd


@runtime_checkable
class IModel(Protocol):
    def to_dataframe(self) -> pd.DataFrame:
        """
        将模型数据转换为DataFrame
        """
        ...

    def from_dataframe(self, df: pd.DataFrame) -> None:
        """
        从DataFrame加载模型数据
        """
        ...

    def persist(self) -> None:
        """
        持久化模型数据
        """
        ...


@runtime_checkable
class IModelFetcher(Protocol):
    def fetching(self, symbol: str) -> pd.DataFrame:
        """
        从数据源获取模型数据
        """
        ...
