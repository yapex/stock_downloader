"""任务过滤器模块

根据配置文件中的白名单过滤任务。
"""

from typing import List, Dict, Any, Optional
from pathlib import Path
from ..configs.app_config import get_config


class TaskFilter:
    """任务过滤器

    根据配置文件中的交易所白名单过滤股票相关任务。
    """

    def __init__(self, config_path: Optional[Path] = None):
        """初始化任务过滤器

        Args:
            config_path: 配置文件路径，如果为 None 则使用默认路径
        """
        self.config_path = config_path
        self._load_config()

    def _load_config(self) -> None:
        """加载配置文件"""
        config = get_config(self.config_path)

        # 获取交易所白名单，默认为 SH 和 SZ
        task_filter_config = config.get("task_filter", {})
        self.exchange_whitelist = task_filter_config.get(
            "exchange_whitelist", ["SH", "SZ"]
        )

    def should_process_symbol(self, symbol: str) -> bool:
        """判断是否应该处理指定的股票代码

        Args:
            symbol: 股票代码，格式如 '000001.SZ' 或 '600000.SH'

        Returns:
            bool: 如果应该处理返回True，否则返回False
        """
        if not symbol or "." not in symbol:
            return False

        # 提取交易所代码
        exchange = symbol.split(".")[-1]
        return exchange in self.exchange_whitelist

    def filter_symbols(self, symbols: List[str]) -> List[str]:
        """过滤股票代码列表

        Args:
            symbols: 股票代码列表

        Returns:
            List[str]: 过滤后的股票代码列表
        """
        return [symbol for symbol in symbols if self.should_process_symbol(symbol)]

    def filter_data(
        self, data: List[Dict[str, Any]], symbol_column: str = "ts_code"
    ) -> List[Dict[str, Any]]:
        """过滤数据列表

        Args:
            data: 数据列表，每个元素是包含股票代码的字典
            symbol_column: 股票代码列名，默认为 'ts_code'

        Returns:
            List[Dict[str, Any]]: 过滤后的数据列表
        """
        filtered_data = []
        for item in data:
            if symbol_column in item and self.should_process_symbol(
                item[symbol_column]
            ):
                filtered_data.append(item)
        return filtered_data

    def get_exchange_whitelist(self) -> List[str]:
        """获取交易所白名单

        Returns:
            List[str]: 交易所白名单
        """
        return self.exchange_whitelist.copy()
