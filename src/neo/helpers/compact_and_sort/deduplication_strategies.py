"""
数据去重策略实现
"""

import ibis
from typing import Dict, Any
from .interfaces import DataDeduplicationStrategy


class FinancialTableDeduplicationStrategy(DataDeduplicationStrategy):
    """financial表特殊去重策略（按update_flag去重）"""

    # financial三表列表
    FINANCIAL_TABLES = {"balance_sheet", "income", "cash_flow"}

    def deduplicate(
        self, table: ibis.Table, table_config: Dict[str, Any]
    ) -> ibis.Table:
        """
        对financial表进行去重，按update_flag优先级去重

        Args:
            table: ibis表对象
            table_config: 表配置信息

        Returns:
            去重后的ibis表对象

        Raises:
            ValueError: 当表不是financial表时
            ValueError: 当表缺少update_flag字段时
        """
        table_name = table_config.get("table_name", "")

        if table_name not in self.FINANCIAL_TABLES:
            raise ValueError(
                f"表 {table_name} 不是financial表，不能使用financial去重策略"
            )

        # 检查是否有update_flag字段
        if "update_flag" not in table.columns:
            raise ValueError(
                f"表 {table_name} 缺少update_flag字段，无法进行financial去重"
            )

        # 按ts_code和年份分组，按update_flag降序排列，取每组第一条
        # 实现"有1用1，没1用0"的效果
        return self._unique_by_update_flag(table)

    def _unique_by_update_flag(
        self, table: ibis.Table, key: str = "ts_code"
    ) -> ibis.Table:
        """
        按update_flag去重

        Args:
            table: ibis表对象
            key: 主键字段名

        Returns:
            去重后的ibis表对象
        """
        # 检查是否有指定的主键字段
        if key not in table.columns:
            raise ValueError(f"表缺少主键字段 {key}，无法按update_flag去重")

        # 定义窗口：按公司和报告期分组，按update_flag降序排列
        # 财务数据应该按报告期（ann_date）而不是年份去重，保留每个季度的数据
        # 如果数据中有 ann_date 字段，按 ann_date 分组；否则按 end_date 分组
        if "ann_date" in table.columns:
            group_cols = [key, "ann_date"]  # 按公告日期分组
        elif "end_date" in table.columns:
            group_cols = [key, "end_date"]  # 按报告期结束日期分组
        else:
            # 如果都没有，才按年份分组（兜底策略）
            group_cols = [key, "year"]
        
        window = ibis.window(
            group_by=group_cols,  # 按业务主键和报告期分组
            order_by=ibis.desc("update_flag"),
        )

        # 应用窗口函数添加行号
        ranked_table = table.mutate(row_num=ibis.row_number().over(window))

        # 过滤出每个分组的第一行
        # 这样就实现了"有1用1，没1用0"的效果
        return ranked_table.filter(ranked_table.row_num == 0).drop("row_num")


class GeneralTableDeduplicationStrategy(DataDeduplicationStrategy):
    """通用表去重策略（按主键去重）"""

    def deduplicate(
        self, table: ibis.Table, table_config: Dict[str, Any]
    ) -> ibis.Table:
        """
        对通用表进行去重，按主键去重

        Args:
            table: ibis表对象
            table_config: 表配置信息

        Returns:
            去重后的ibis表对象

        Raises:
            ValueError: 当表没有配置主键时
            ValueError: 当表缺少主键字段时
        """
        primary_key = table_config.get("primary_key", [])
        table_name = table_config.get("table_name", "unknown")

        if not primary_key:
            raise ValueError(f"表 {table_name} 没有配置主键，无法进行去重")

        # 检查主键字段是否都存在
        missing_keys = [key for key in primary_key if key not in table.columns]
        if missing_keys:
            raise ValueError(f"表 {table_name} 缺少主键字段: {missing_keys}")

        # 按主键去重
        return table.distinct(on=primary_key)


class HybridDeduplicationStrategy(DataDeduplicationStrategy):
    """
    混合去重策略：根据表类型自动选择合适的去重策略
    """

    def __init__(self):
        self.financial_strategy = FinancialTableDeduplicationStrategy()
        self.general_strategy = GeneralTableDeduplicationStrategy()

    def deduplicate(
        self, table: ibis.Table, table_config: Dict[str, Any]
    ) -> ibis.Table:
        """
        根据表类型自动选择去重策略

        Args:
            table: ibis表对象
            table_config: 表配置信息

        Returns:
            去重后的ibis表对象
        """
        table_name = table_config.get("table_name", "")

        if table_name in FinancialTableDeduplicationStrategy.FINANCIAL_TABLES:
            return self.financial_strategy.deduplicate(table, table_config)
        else:
            return self.general_strategy.deduplicate(table, table_config)
