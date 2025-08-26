import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import sys
import os
from huey import MemoryHuey

# 确保测试环境下可以直接从 src 导入包
_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


@pytest.fixture
def mock_fetcher():
    """一个可供所有测试使用的、模拟的Fetcher实例。"""
    fetcher = MagicMock()
    fetcher.fetch_stock_list.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
    fetcher.fetch_daily_history.return_value = pd.DataFrame(
        {"trade_date": ["20230102"]}
    )
    fetcher.fetch_daily_basic.return_value = pd.DataFrame({"trade_date": ["20230102"]})
    fetcher.fetch_income.return_value = pd.DataFrame({"ann_date": ["20230425"]})
    fetcher.fetch_balancesheet.return_value = pd.DataFrame({"ann_date": ["20230425"]})
    fetcher.fetch_cashflow.return_value = pd.DataFrame({"ann_date": ["20230425"]})
    return fetcher


@pytest.fixture(autouse=True)
def mock_huey_config():
    """自动模拟 huey_config，避免数据库锁定问题"""
    # 创建内存模式的 huey 实例，immediate=True 让任务立即执行
    memory_huey_fast = MemoryHuey('test_fast', immediate=True)
    memory_huey_slow = MemoryHuey('test_slow', immediate=True)
    
    with patch('neo.configs.huey_config.huey_fast', memory_huey_fast), \
         patch('neo.configs.huey_config.huey_slow', memory_huey_slow):
        yield