import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Set, Any

import pandas as pd
import pytest

from src.downloader.missing_symbols import (
    MissingSymbolsDetector,
    MissingSymbolsLogger,
    scan_and_log_missing_symbols,
)
from src.downloader.models import TaskType, Priority


class FakeStorage:
    """提供 DuckDBStorage 所需最小接口的假实现"""

    def __init__(self, stock_list, business_rows):
        # stock_list: Iterable[str]
        # business_rows: list[dict] with keys: business_type, stock_code
        self._stock_list = list(stock_list)
        self._business_rows = list(business_rows)

    # missing_symbols.MissingSymbolsDetector 仅会调用以下两个方法
    def get_stock_list(self):
        return pd.DataFrame({"ts_code": self._stock_list})

    def list_business_tables(self):
        return self._business_rows


class TestMissingSymbolsDetector:
    def test_all_missing_when_no_business_tables(self):
        storage = FakeStorage(
            stock_list=["000001.SZ", "000002.SZ"],
            business_rows=[],
        )
        detector = MissingSymbolsDetector(storage)
        res = detector.detect_missing_symbols()
        # 无业务表 -> 三个默认业务类型都全缺
        assert set(res.keys()) == {"daily", "daily_basic", "financials"}
        for k, v in res.items():
            assert v == {"000001.SZ", "000002.SZ"}

    def test_all_present_missing_empty(self):
        stocks = ["000001.SZ", "000002.SZ"]
        # daily 业务已有全部股票
        business = [
            {"business_type": "daily", "stock_code": s} for s in stocks
        ]
        storage = FakeStorage(stock_list=stocks, business_rows=business)
        detector = MissingSymbolsDetector(storage)
        res = detector.detect_missing_symbols()
        # 由于不存在缺失股票，按实现不会包含该业务键
        assert res == {}

    def test_partial_missing(self):
        stocks = ["000001.SZ", "000002.SZ", "600000.SH"]
        business = [
            {"business_type": "daily", "stock_code": "000001.SZ"},
            {"business_type": "financials", "stock_code": "000002.SZ"},
        ]
        storage = FakeStorage(stock_list=stocks, business_rows=business)
        detector = MissingSymbolsDetector(storage)
        res = detector.detect_missing_symbols()
        assert res["daily"] == {"000002.SZ", "600000.SH"}
        assert res["financials"] == {"000001.SZ", "600000.SH"}
        # daily_basic 在 existing_by_type 中不存在 -> 不计入结果
        assert "daily_basic" not in res

    def test_empty_stock_list_or_bad_schema(self, caplog):
        # 空表
        class EmptyStockStorage(FakeStorage):
            def get_stock_list(self):
                return pd.DataFrame({"ts_code": []})

        detector = MissingSymbolsDetector(EmptyStockStorage([], []))
        with caplog.at_level("WARNING", logger="downloader.missing_symbols"):
            res = detector.detect_missing_symbols()
            assert res == {}
            # 验证异常被正确处理

        # 缺列
        class BadSchemaStorage(FakeStorage):
            def get_stock_list(self):
                return pd.DataFrame({"wrong": [1, 2]})

        detector = MissingSymbolsDetector(BadSchemaStorage([], []))
        with caplog.at_level("WARNING", logger="downloader.missing_symbols"):
            res = detector.detect_missing_symbols()
            assert res == {}
            # 验证异常被正确处理


class TestMissingSymbolsLogger:
    def test_write_and_read_normal(self, tmp_path: Path):
        log_file = tmp_path / "missing.jsonl"
        logger = MissingSymbolsLogger(str(log_file))
        missing = {
            "daily": {"000001.SZ", "000002.SZ"},
            "financials": {"600000.SH"},
        }
        logger.write_missing_symbols(missing)

        data = logger.read_missing_symbols()
        # read 返回的是 list，且不去重
        assert set(data.keys()) == {"daily", "financials"}
        assert sorted(data["daily"]) == sorted(["000001.SZ", "000002.SZ"])
        assert data["financials"] == ["600000.SH"]

        summary = logger.get_summary()
        assert summary["total_missing"] == 3
        assert summary["by_type"]["daily"] == 2
        assert summary["by_type"]["financials"] == 1
        assert summary["exists"] is True

    def test_overwrite_and_clear(self, tmp_path: Path):
        log_file = tmp_path / "missing.jsonl"
        logger = MissingSymbolsLogger(str(log_file))

        logger.write_missing_symbols({"daily": {"000001.SZ"}})
        # 再次写入不同内容，应覆盖
        logger.write_missing_symbols({"financials": {"600000.SH", "600001.SH"}})
        data = logger.read_missing_symbols()
        assert set(data.keys()) == {"financials"}
        assert len(data["financials"]) == 2

        # 传空 dict 应清空文件
        logger.write_missing_symbols({})
        assert logger.read_missing_symbols() == {}

    def test_read_skips_invalid_lines(self, tmp_path: Path):
        log_file = tmp_path / "missing.jsonl"
        # 手写无效/缺字段行
        log_file.write_text(
            "\n".join([
                "{not-json}",
                json.dumps({"symbol": "000001.SZ"}),  # 缺 business_type
                json.dumps({"business_type": "daily"}),  # 缺 symbol
                json.dumps({"symbol": "000001.SZ", "business_type": "daily"}),
            ]) + "\n",
            encoding="utf-8",
        )
        logger = MissingSymbolsLogger(str(log_file))
        data = logger.read_missing_symbols()
        assert set(data.keys()) == {"daily"}
        assert data["daily"] == ["000001.SZ"]

    def test_convert_to_tasks_default_and_custom(self, tmp_path: Path):
        log_file = tmp_path / "missing.jsonl"
        # 写两条 daily, 一条 financials, 一条未知类型
        lines = [
            {"symbol": "000001.SZ", "business_type": "daily"},
            {"symbol": "000002.SZ", "business_type": "daily"},
            {"symbol": "600000.SH", "business_type": "financials"},
            {"symbol": "BAD", "business_type": "unknown"},
        ]
        with open(log_file, "w", encoding="utf-8") as f:
            for r in lines:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        logger = MissingSymbolsLogger(str(log_file))

        tasks = logger.convert_to_tasks(task_configs={"daily": {"adjust": "qfq"}})
        # unknown 被跳过, 共 3 条
        assert len(tasks) == 3
        # 校验字段
        for t in tasks:
            assert t.priority == Priority.NORMAL
            assert isinstance(t.task_type, TaskType)
        # 校验自定义 daily 配置生效
        daily_tasks = [t for t in tasks if t.task_type.value == "daily"]
        assert all(t.params == {"adjust": "qfq"} for t in daily_tasks)


class TestScanAndLog:
    def test_scan_and_log_integration(self, tmp_path: Path, monkeypatch):
        # 假的 DuckDBStorage 替换
        class DummyDuck:
            def __init__(self, *_args, **_kwargs):
                pass

            def get_stock_list(self):
                return pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"]})

            def list_business_tables(self):
                # 一个业务已有一只，触发部分缺
                return [{"business_type": "daily", "stock_code": "000001.SZ"}]

        from src.downloader import missing_symbols as ms
        monkeypatch.setattr(ms, "DuckDBStorage", DummyDuck)

        log_file = tmp_path / "missing.jsonl"
        summary = scan_and_log_missing_symbols(db_path=str(tmp_path / "db.duckdb"), log_path=str(log_file))

        assert summary["total_missing"] == 1
        assert summary["by_type"]["daily"] == 1
        assert Path(summary["log_path"]).exists()
        # 返回包含时间戳
        datetime.fromisoformat(summary["scan_completed_at"])  # 不抛异常即可
