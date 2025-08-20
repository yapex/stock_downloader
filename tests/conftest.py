import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import argparse
import sys
import os
from huey import MemoryHuey

# 确保测试环境下可以直接从 src 导入包
_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
# 对于命名空间包（PEP 420），需要把项目根目录放入sys.path，
# 这样 `src` 目录可作为命名空间包被导入。
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


# 保留原有的mock fixtures以保持向后兼容
@pytest.fixture
def mock_fetcher():
    """一个可供所有测试使用的、模拟的Fetcher实例。"""
    fetcher = MagicMock()
    fetcher.fetch_stock_list.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
    fetcher.fetch_daily_history.return_value = pd.DataFrame(
        {"trade_date": ["20230102"]}
    )
    # ---> 核心修正：mock 正确的方法名 <---
    fetcher.fetch_daily_basic.return_value = pd.DataFrame({"trade_date": ["20230102"]})
    fetcher.fetch_income.return_value = pd.DataFrame({"ann_date": ["20230425"]})
    fetcher.fetch_balancesheet.return_value = pd.DataFrame({"ann_date": ["20230425"]})
    fetcher.fetch_cashflow.return_value = pd.DataFrame({"ann_date": ["20230425"]})
    return fetcher


@pytest.fixture
def mock_storage():
    """一个可供所有测试使用的、模拟的Storage实例。"""
    storage = MagicMock()
    # 为每个方法都设置一个默认的、安全的返回值
    storage.get_latest_date_by_stock.return_value = "20230101"
    storage.save_daily.return_value = None
    storage.save_fundamental_data.return_value = None
    storage.save_financial_data.return_value = None
    storage.save_stock_list.return_value = None

    # _get_file_path 返回一个可配置的 mock_path
    mock_path = MagicMock()
    mock_path.exists.return_value = True  # 默认文件存在
    mock_path.stat.return_value.st_mtime = (
        pd.Timestamp.now().timestamp()
    )  # 默认文件很新
    storage._get_file_path.return_value = mock_path
    return storage


@pytest.fixture
def huey_immediate():
    """为测试提供即时模式的 Huey 实例"""
    test_huey = MemoryHuey(immediate=True)
    
    # 使用 patch 替换 huey_tasks 模块中的 huey 实例
    with patch('downloader2.producer.huey_tasks.huey', test_huey):
        yield test_huey


@pytest.fixture
def mock_args():
    """一个可供所有测试使用的、模拟的命令行参数。"""
    return argparse.Namespace(force=False)
