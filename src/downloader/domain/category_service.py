"""
分类服务：根据记录中的分类字段对记录分组；以及提供基于集合的缺股对比工具。

设计目标：
- 保持简单（KISS），纯函数接口，便于共享与测试。
- 不依赖外部库；集合运算避免重复，保证 O(N) 时间复杂度。
- 分类字段默认从以下候选键中选择第一个存在且有效的值：
  ["category", "industry", "sector"]。
- 对于缺失、None、空字符串或仅空白字符的分类，归为 "Unknown"。
- 对于非字符串类型的分类，统一归为 "Unknown"，以避免奇异类型带来的歧义。

类型说明：
- 由于当前工程中未定义 Stock 模型：
  - 这里将记录视为 Mapping（通常为 dict），用于分类分组。
  - 缺股计算处将 Stock 视为 str（如 ts_code）。
"""
from typing import Dict, Iterable, List, Mapping, Any, Mapping as TMapping, Set

UNKNOWN_CATEGORY = "Unknown"
CATEGORY_CANDIDATES = ("category", "industry", "sector")


def _extract_category(record: Mapping[str, Any]) -> str:
    """从单条记录中提取分类。

    规则：
    - 按照 CATEGORY_CANDIDATES 顺序查找首个存在的键；
    - 对值进行校验：非 str、None、空/全空白，均视为未知分类；
    - 返回标准化的分类字符串（去除首尾空白）。
    """
    for key in CATEGORY_CANDIDATES:
        if key in record:
            raw = record.get(key)
            if isinstance(raw, str):
                cat = raw.strip()
                if cat:
                    return cat
                return UNKNOWN_CATEGORY
            # 显式处理 None 或其他非字符串类型
            return UNKNOWN_CATEGORY
    return UNKNOWN_CATEGORY


def group_by_category(records: Iterable[Mapping[str, Any]]) -> Dict[str, List[dict]]:
    """根据分类对记录进行分组。"""
    groups: Dict[str, List[dict]] = {}
    if not records:
        return groups

    for rec in records:
        # 容忍传入的单条记录为非 Mapping 的情况：直接作为对象放入 Unknown 类别
        if not isinstance(rec, Mapping):
            groups.setdefault(UNKNOWN_CATEGORY, []).append(rec)  # type: ignore[arg-type]
            continue
        cat = _extract_category(rec)
        groups.setdefault(cat, []).append(dict(rec))

    return groups


# =====================
# 缺股对比核心算法
# =====================

def find_missing(all_stocks: Iterable[str], *business_tables: Iterable[str]) -> Set[str]:
    """计算总体缺股集合（相对于所有业务表的并集）。

    Args:
        all_stocks: 全量应存在的股票代码（可迭代，可含重复）。
        *business_tables: 各业务表已存在的股票代码集合/列表，数量可变。

    Returns:
        Set[str]: 缺失的股票代码集合（去重）。
    """
    # 使用集合运算，时间 O(N)，空间 O(N)
    expected: Set[str] = set(all_stocks)
    if not business_tables:
        return set(expected)

    present: Set[str] = set()
    for tbl in business_tables:
        # 每个业务表转换为集合后取并集，避免重复
        present |= set(tbl)
    return expected - present


def find_missing_by_category(all_stocks: Iterable[str], tables_by_category: TMapping[str, Iterable[str]]) -> Dict[str, Set[str]]:
    """按业务分类计算缺股子集，便于统计展示。

    Args:
        all_stocks: 全量应存在的股票代码。
        tables_by_category: {category: 该分类下已存在股票的可迭代}。

    Returns:
        Dict[str, Set[str]]: {category: 缺失的股票集合}，包含空集以便统一统计。
    """
    expected: Set[str] = set(all_stocks)
    result: Dict[str, Set[str]] = {}
    for cat, stocks in tables_by_category.items():
        result[cat] = expected - set(stocks)
    return result

