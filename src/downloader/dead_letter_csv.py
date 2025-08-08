from __future__ import annotations

import csv
import logging
from builtins import open  # 为了测试能够 monkeypatch downloader.dead_letter_csv.open
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Optional, Dict, List, Tuple, Set


logger = logging.getLogger(__name__)
DEFAULT_DIR = Path("dead_letter")
DATE_FMT = "%Y%m%d"
HEADER = ["code", "category", "ts"]


@dataclass(frozen=True)
class MissingRecord:
    code: str
    category: str
    ts: str  # ISO8601 string

    @property
    def key(self) -> Tuple[str, str]:
        return (self.code, self.category)


def _ensure_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(HEADER)
        except Exception as e:
            logger.error(f"创建 dead_letter 文件失败: {path} - {e}", exc_info=True)
            raise


def _today_str() -> str:
    return date.today().strftime(DATE_FMT)


def _validate_date_str(date_str: str) -> None:
    """校验日期字符串格式。错误时抛出 ValueError。"""
    try:
        datetime.strptime(date_str, DATE_FMT)
    except Exception as e:
        logger.error(f"日期格式错误，应为 {DATE_FMT}: '{date_str}' - {e}", exc_info=True)
        raise


def _load_existing_keys(path: Path) -> Set[Tuple[str, str]]:
    keys: Set[Tuple[str, str]] = set()
    if not path.exists():
        return keys
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                # 跳过空行与表头
                if not row or row == HEADER or row[0] == "code":
                    continue
                if len(row) < 2:
                    continue
                code, category = row[0], row[1]
                keys.add((code, category))
    except Exception as e:
        # 区分读失败：记录错误并抛出，交由上层处理
        logger.error(f"读取 dead_letter 文件失败: {path} - {e}", exc_info=True)
        raise
    return keys


def append_missing(
    category: str,
    codes: Iterable[str],
    *,
    ts: Optional[datetime] = None,
    date_str: Optional[str] = None,
    base_dir: Path = DEFAULT_DIR,
) -> int:
    """将缺股记录追加到 dead_letter/{date}.csv，避免重复写入。

    去重规则：基于 (code, category) 维度去重；即便 ts 不同，也只保留一条。

    Returns:
        实际追加的记录条数。
    """
    codes = list(codes)
    if not codes:
        return 0

    if date_str is None:
        date_str = _today_str()
    else:
        _validate_date_str(date_str)
    dl_path = base_dir / f"{date_str}.csv"

    _ensure_file(dl_path)

    existing = _load_existing_keys(dl_path)
    now_iso = (ts or datetime.now()).isoformat()

    to_add: List[MissingRecord] = []
    for code in codes:
        key = (code, category)
        if key in existing:
            continue
        to_add.append(MissingRecord(code=code, category=category, ts=now_iso))
        existing.add(key)

    if not to_add:
        logger.info(f"[{category}] 无新增缺失记录，跳过写入")
        return 0

    try:
        with open(dl_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for rec in to_add:
                writer.writerow([rec.code, rec.category, rec.ts])
        logger.info(f"[{category}] 追加缺失记录完成: {len(to_add)} 条 -> {dl_path}")
    except Exception as e:
        # 区分写失败
        logger.error(f"写入 dead_letter 文件失败: {dl_path} - {e}", exc_info=True)
        raise

    return len(to_add)


def retry_missing(
    *,
    date_str: Optional[str] = None,
    categories: Optional[Iterable[str]] = None,
    base_dir: Path = DEFAULT_DIR,
) -> Dict[str, List[str]]:
    """读取 dead_letter/{date}.csv，返回 {category: [codes...]} 用于后续重试。

    - 若 categories 为 None，返回文件中所有类别。
    - 自动去重代码（基于 set）。
    """
    if date_str is None:
        date_str = _today_str()
    else:
        _validate_date_str(date_str)
    dl_path = base_dir / f"{date_str}.csv"
    result: Dict[str, List[str]] = {}
    if not dl_path.exists():
        return result

    wanted: Optional[Set[str]] = set(categories) if categories is not None else None

    by_cat: Dict[str, Set[str]] = {}
    try:
        with open(dl_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or row == HEADER or row[0] == "code":
                    continue
                if len(row) < 2:
                    continue
                code, category = row[0], row[1]
                if wanted is not None and category not in wanted:
                    continue
                by_cat.setdefault(category, set()).add(code)
    except Exception as e:
        logger.error(f"读取 dead_letter 文件失败: {dl_path} - {e}", exc_info=True)
        raise

    # 转换为有序列表（字典序），方便稳定输出
    for cat, codes in by_cat.items():
        result[cat] = sorted(codes)
    logger.info(f"读取缺失记录完成: {sum(len(v) for v in result.values())} 条，文件: {dl_path}")
    return result


def write_missing_by_map(
    missing: Dict[str, Iterable[str]],
    *,
    ts: Optional[datetime] = None,
    date_str: Optional[str] = None,
    base_dir: Path = DEFAULT_DIR,
) -> int:
    """批量写入：missing 形如 {category: [codes...]}
    返回累计新增条数。
    """
    total = 0
    for category, codes in missing.items():
        total += append_missing(category, codes, ts=ts, date_str=date_str, base_dir=base_dir)
    logger.info(f"批量写入缺失记录完成: {total} 条")
    return total

