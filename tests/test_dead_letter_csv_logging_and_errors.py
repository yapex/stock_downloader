import os
import io
import csv
import pytest
from pathlib import Path
from datetime import datetime
from typing import Iterable

from downloader.dead_letter_csv import (
    append_missing,
    retry_missing,
    write_missing_by_map,
    DEFAULT_DIR,
)


def test_append_missing_invalid_date_format(tmp_path: Path, caplog):
    # 使用非法日期格式，期望抛出异常并记录错误日志
    bad_date = "2025-01-01"  # 正确应为 YYYYMMDD
    with caplog.at_level("ERROR", logger="downloader.dead_letter_csv"):
        with pytest.raises(Exception):
            append_missing("daily", ["000001.SZ"], date_str=bad_date, base_dir=tmp_path)
        assert "日期格式错误" in caplog.text


def test_retry_missing_invalid_date_format(tmp_path: Path, caplog):
    bad_date = "2025/01/01"
    with caplog.at_level("ERROR", logger="downloader.dead_letter_csv"):
        with pytest.raises(Exception):
            retry_missing(date_str=bad_date, base_dir=tmp_path)
        assert "日期格式错误" in caplog.text


def test_append_missing_write_ioerror(tmp_path: Path, monkeypatch, caplog):
    # 先确保文件存在，避免 _ensure_file 分支影响 open 打补丁逻辑
    date_str = "20250101"
    dl_file = tmp_path / f"{date_str}.csv"
    dl_file.parent.mkdir(parents=True, exist_ok=True)
    dl_file.write_text(
        ",".join(["code", "category", "ts"]) + "\n",
        encoding="utf-8"
    )

    # 模拟对该文件进行追加写入时触发 IOError
    real_open = open

    def fake_open(path, mode="r", *args, **kwargs):
        # 仅当是对目标 dead_letter 文件进行追加时抛出错误
        if Path(path) == dl_file and "a" in mode:
            raise IOError("Permission denied")
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr("downloader.dead_letter_csv.open", fake_open)

    with caplog.at_level("ERROR", logger="downloader.dead_letter_csv"):
        with pytest.raises(IOError, match="Permission denied"):
            append_missing("daily", ["000001.SZ"], date_str=date_str, base_dir=tmp_path)
        assert "写入 dead_letter 文件失败" in caplog.text


def test_retry_missing_read_ioerror(tmp_path: Path, monkeypatch, caplog):
    date_str = "20250101"
    dl_file = tmp_path / f"{date_str}.csv"
    dl_file.parent.mkdir(parents=True, exist_ok=True)
    dl_file.write_text(
        "code,category,ts\n000001.SZ,daily,2025-01-01T00:00:00\n",
        encoding="utf-8"
    )

    def fake_open(path, mode="r", *args, **kwargs):
        if Path(path) == dl_file and "r" in mode:
            raise IOError("Read blocked")
        return open(path, mode, *args, **kwargs)

    monkeypatch.setattr("downloader.dead_letter_csv.open", fake_open)

    with caplog.at_level("ERROR", logger="downloader.dead_letter_csv"):
        with pytest.raises(IOError, match="Read blocked"):
            retry_missing(date_str=date_str, base_dir=tmp_path)
        assert "读取 dead_letter 文件失败" in caplog.text


def test_append_and_retry_info_logging(tmp_path: Path, caplog):
    date_str = "20250102"
    with caplog.at_level("INFO", logger="downloader.dead_letter_csv"):
        # 首次写入
        added = append_missing("daily", ["000001.SZ", "000002.SZ"], date_str=date_str, base_dir=tmp_path)
        assert added == 2
        # 再次写入重复，验证 info 提示无新增
        added2 = append_missing("daily", ["000001.SZ"], date_str=date_str, base_dir=tmp_path)
        assert added2 == 0
        # 读取
        result = retry_missing(date_str=date_str, base_dir=tmp_path)
        assert result == {"daily": ["000001.SZ", "000002.SZ"]}

        assert "追加缺失记录完成" in caplog.text
        assert "无新增缺失记录" in caplog.text
        assert "读取缺失记录完成" in caplog.text
