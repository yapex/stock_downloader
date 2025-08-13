from typing import Protocol, runtime_checkable
import pandas as pd


@runtime_checkable
class IFetchingable(Protocol):
    def fetching(self, symbol: str) -> pd.DataFrame:
        """
        从数据源获取模型数据
        """
        ...
