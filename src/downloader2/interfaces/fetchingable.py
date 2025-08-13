from typing import Protocol, runtime_checkable
import pandas as pd


@runtime_checkable
class IFetchingable(Protocol):
    def fetching_by_symbol(
        self, symbol: str, start_date: str, end_date: str, **kwargs
    ) -> pd.DataFrame:
        """
        从数据源获取模型数据
        """
        ...

    def fetching_by_symbols(
        self, symbols: list[str], start_date: str, end_date: str, **kwargs
    ) -> pd.DataFrame:
        """
        从数据源获取多个模型数据
        """
        ...
