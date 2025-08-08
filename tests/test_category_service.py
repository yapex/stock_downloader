import pytest

from src.downloader.domain.category_service import group_by_category, UNKNOWN_CATEGORY


def test_group_by_category_empty_list():
    assert group_by_category([]) == {}


def test_group_by_category_all_unknown_variants():
    records = [
        {},  # 无字段
        {"category": None},  # None
        {"industry": ""},  # 空字符串
        {"sector": "   "},  # 仅空白
        {"category": 123},  # 非字符串
        42,  # 非 mapping 记录
    ]
    grouped = group_by_category(records)  # type: ignore[arg-type]
    assert set(grouped.keys()) == {UNKNOWN_CATEGORY}
    assert len(grouped[UNKNOWN_CATEGORY]) == len(records)


def test_group_by_category_prefers_first_valid_key_order():
    records = [
        {"category": "A", "industry": "B", "sector": "C"},
        {"industry": "B1", "sector": "C1"},
        {"sector": "C2"},
    ]
    grouped = group_by_category(records)
    assert grouped["A"][0]["category"] == "A"
    assert grouped["B1"][0]["industry"] == "B1"
    assert grouped["C2"][0]["sector"] == "C2"


def test_group_by_category_mixed_categories():
    records = [
        {"category": "Tech"},
        {"industry": "Finance"},
        {"sector": "Energy"},
        {"category": "Tech", "extra": 1},
        {"industry": "Finance", "extra": 2},
        {"category": "  Tech  "},  # 需要去除空白
        {"category": ""},  # 空字符串 -> Unknown
    ]
    grouped = group_by_category(records)

    assert set(grouped.keys()) == {"Tech", "Finance", "Energy", UNKNOWN_CATEGORY}

    # Tech 组应包含去除空白后的两条 + 多字段那一条，共 3 条
    assert len(grouped["Tech"]) == 3
    # Finance 两条
    assert len(grouped["Finance"]) == 2
    # Energy 一条
    assert len(grouped["Energy"]) == 1
    # Unknown 一条
    assert len(grouped[UNKNOWN_CATEGORY]) == 1

