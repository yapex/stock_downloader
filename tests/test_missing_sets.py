import pytest

from src.downloader.domain.category_service import find_missing, find_missing_by_category


class TestFindMissing:
    def test_all_present(self):
        # 全部存在：缺股应为空
        all_stocks = {"000001.SZ", "000002.SZ", "600000.SH"}
        daily = {"000001.SZ"}
        financials = {"000002.SZ"}
        daily_basic = {"600000.SH"}

        missing = find_missing(all_stocks, daily, financials, daily_basic)
        assert missing == set()

    def test_all_missing(self):
        # 全部缺失：业务表为空，或业务表中均无全量集合内的元素
        all_stocks = {"000001.SZ", "000002.SZ"}
        missing = find_missing(all_stocks)
        assert missing == all_stocks

        # 业务表有噪声（不在全量集合中），也不应影响缺股
        noise_tbl = {"999999.SH"}
        missing2 = find_missing(all_stocks, noise_tbl)
        assert missing2 == all_stocks

    def test_partial_with_duplicates(self):
        # 部分重复：输入包含重复股票，应通过集合去重
        all_stocks = ["000001.SZ", "000001.SZ", "000002.SZ", "600000.SH"]
        daily = ["000001.SZ", "000001.SZ"]
        financials = ["000002.SZ"]
        # 缺少 600000.SH
        missing = find_missing(all_stocks, daily, financials)
        assert missing == {"600000.SH"}


class TestFindMissingByCategory:
    def test_by_category_all_present(self):
        all_stocks = {"000001.SZ", "000002.SZ"}
        tables = {
            "daily": {"000001.SZ", "000002.SZ"},
            "financials": {"000001.SZ", "000002.SZ"},
        }
        res = find_missing_by_category(all_stocks, tables)
        assert res == {"daily": set(), "financials": set()}

    def test_by_category_all_missing(self):
        all_stocks = {"000001.SZ", "000002.SZ"}
        tables = {
            "daily": set(),
            "financials": set(),
        }
        res = find_missing_by_category(all_stocks, tables)
        assert res == {"daily": all_stocks, "financials": all_stocks}

    def test_by_category_partial_with_duplicates(self):
        all_stocks = ["000001.SZ", "000001.SZ", "000002.SZ", "600000.SH"]
        tables = {
            "daily": ["000001.SZ", "000001.SZ"],
            "financials": ["000002.SZ"],
        }
        res = find_missing_by_category(all_stocks, tables)
        assert res["daily"] == {"000002.SZ", "600000.SH"}
        assert res["financials"] == {"000001.SZ", "600000.SH"}

