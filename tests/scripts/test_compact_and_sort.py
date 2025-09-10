"""测试数据压缩和排序脚本"""

import pytest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import Mock

# 假定脚本位于项目根目录下的 scripts/ 文件夹中
# 为了能正确导入，我们需要将 src 目录添加到 sys.path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# 现在可以导入脚本中的函数了
from scripts.compact_and_sort import optimize_table


@pytest.fixture
def partitioned_table_config():
    """模拟一个按年分区的表的 schema 配置"""
    return {
        "table_name": "income",
        "date_col": "f_ann_date",
        "primary_key": ["ts_code", "ann_date"],
    }


@pytest.fixture
def non_partitioned_table_config():
    """模拟一个不分区的表的 schema 配置"""
    return {
        "table_name": "stock_basic",
        "primary_key": ["ts_code"],
    }


class TestOptimizeTable:
    """测试optimize_table函数"""

    @patch("scripts.compact_and_sort.CompactAndSortServiceImpl")
    def test_optimize_table_uses_new_module(
        self, mock_service_class, partitioned_table_config
    ):
        """测试optimize_table使用新模块进行优化"""
        # 设置mock
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # 创建mock的duckdb连接
        mock_con = Mock()

        # 调用函数
        optimize_table(mock_con, "income", partitioned_table_config)

        # 验证服务被正确创建和调用
        mock_service_class.assert_called_once()
        mock_service.compact_and_sort_table.assert_called_once_with(
            "income", partitioned_table_config
        )

    @patch("scripts.compact_and_sort.CompactAndSortServiceImpl")
    def test_optimize_table_handles_exception(
        self, mock_service_class, partitioned_table_config
    ):
        """测试optimize_table处理异常情况"""
        # 设置mock服务抛出异常
        mock_service = Mock()
        mock_service.compact_and_sort_table.side_effect = Exception("测试异常")
        mock_service_class.return_value = mock_service

        # 创建mock的duckdb连接
        mock_con = Mock()

        # 验证异常被正确抛出
        with pytest.raises(Exception, match="测试异常"):
            optimize_table(mock_con, "income", partitioned_table_config)

    @patch("scripts.compact_and_sort.CompactAndSortServiceImpl")
    def test_optimize_table_with_different_table_names(self, mock_service_class):
        """测试optimize_table处理不同的表名"""
        # 设置mock
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # 创建mock的duckdb连接
        mock_con = Mock()

        # 测试不同的表配置
        table_configs = [
            {"table_name": "stock_basic", "primary_key": ["ts_code"]},
            {
                "table_name": "stock_daily",
                "primary_key": ["ts_code", "trade_date"],
                "date_col": "trade_date",
            },
            {
                "table_name": "balance_sheet",
                "primary_key": ["ts_code", "ann_date"],
                "date_col": "f_ann_date",
            },
        ]

        for config in table_configs:
            table_name = config["table_name"]
            optimize_table(mock_con, table_name, config)
            mock_service.compact_and_sort_table.assert_called_with(table_name, config)
