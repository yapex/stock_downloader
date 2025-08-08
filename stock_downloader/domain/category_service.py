"""
分类服务：根据记录中的分类字段对记录分组。

设计目标：
- 保持简单（KISS），对外仅暴露一个纯函数接口，方便在 verify 与后续模块共享。
- 不依赖外部库；输入是记录（dict）列表，输出是 {category: [records]} 的字典。
- 分类字段默认从以下候选键中选择第一个存在且有效的值：
  ["category", "industry", "sector"]。
- 对于缺失、None、空字符串或仅空白字符的分类，归为 "Unknown"。
- 对于非字符串类型的分类，统一归为 "Unknown"，以避免奇异类型带来的歧义。

类型说明：
- 由于当前工程中未定义 Stock 模型，这里将记录视为 Mapping（通常为 dict）。
  返回类型为 Dict[str, List[dict]]。
"""
from typing import Dict, Iterable, List, Mapping, Any

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
    """根据分类对记录进行分组。

    Args:
        records: 可迭代的记录（通常为 dict），记录中可能包含 category/industry/sector 字段。

    Returns:
        Dict[str, List[dict]]: 以分类为键、记录列表为值的字典。空输入返回空字典。
    """
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

