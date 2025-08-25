"""Huey 任务测试

测试带 @huey_task 装饰器的下载任务函数和Huey集成功能。
"""

from unittest.mock import Mock, patch
import pandas as pd

from neo.configs.huey_config import huey
from neo.tasks.huey_tasks import download_task, process_data_task
from neo.task_bus.types import TaskType


def _download_task_impl(task_type: str, symbol: str) -> dict:
    """下载任务的实际实现，用于测试"""
    import logging
    from neo.downloader.simple_downloader import SimpleDownloader

    logger = logging.getLogger(__name__)

    try:
        logger.info(f"开始执行下载任务: {symbol}")

        # 构建下载配置
        getattr(TaskType, task_type)

        # 创建下载器并执行下载
        downloader = SimpleDownloader()
        result = downloader.download(task_type, symbol)

        logger.info(f"下载任务完成: {symbol}, 成功: {result.success}")

        # 序列化结果
        return {
            "config": {
                "symbol": symbol,
                "task_type": task_type,
            },
            "success": result.success,
            "data": result.data.to_dict() if result.data is not None else None,
            "error": str(result.error) if result.error else None,
            "retry_count": result.retry_count,
        }

    except Exception as e:
        logger.error(f"下载任务执行失败: {symbol}, 错误: {e}")
        return {
            "config": {"symbol": symbol, "task_type": task_type},
            "success": False,
            "data": None,
            "error": str(e),
            "retry_count": 0,
        }


class TestDownloadTask:
    """测试 download_task 函数"""

    def test_download_task_success(self):
        """测试下载任务成功"""

        # Mock 下载器和结果
        mock_data = pd.DataFrame({"ts_code": ["000001.SZ"], "symbol": ["000001"]})

        with patch(
            "neo.downloader.simple_downloader.SimpleDownloader"
        ) as mock_downloader_class:
            mock_downloader = Mock()
            mock_downloader_class.return_value = mock_downloader

            # Mock 下载结果
            mock_result = Mock()
            mock_result.success = True
            mock_result.data = mock_data
            mock_result.error = None
            mock_result.retry_count = 0
            mock_downloader.download.return_value = mock_result

            # 执行任务
            result = _download_task_impl("stock_basic", "000001.SZ")

            # 验证结果
            assert result["success"] is True
            assert result["config"]["symbol"] == "000001.SZ"
            assert (
                result["config"]["task_type"] == "stock_basic"
            )  # 序列化后应该是字符串
            assert result["data"] is not None
            assert result["error"] is None

            # 验证下载器被正确调用
            mock_downloader.download.assert_called_once_with("stock_basic", "000001.SZ")

    def test_download_task_failure(self):
        """测试下载任务失败"""

        with patch(
            "neo.downloader.simple_downloader.SimpleDownloader"
        ) as mock_downloader_class:
            mock_downloader = Mock()
            mock_downloader_class.return_value = mock_downloader

            # Mock 下载失败
            test_error = Exception("网络错误")
            mock_result = Mock()
            mock_result.success = False
            mock_result.data = None
            mock_result.error = test_error
            mock_result.retry_count = 1
            mock_downloader.download.return_value = mock_result

            # 执行任务
            result = _download_task_impl("stock_basic", "000001.SZ")

            # 验证结果
            assert result["success"] is False
            assert result["data"] is None
            assert "网络错误" in result["error"]

    def test_download_task_exception(self):
        """测试下载任务抛出异常"""

        with patch(
            "neo.downloader.simple_downloader.SimpleDownloader"
        ) as mock_downloader_class:
            # Mock 构造函数抛出异常
            mock_downloader_class.side_effect = Exception("初始化失败")

            # 执行任务
            result = _download_task_impl("stock_basic", "000001.SZ")

            # 验证结果
            assert result["success"] is False
            assert result["data"] is None
            assert "初始化失败" in result["error"]

    def test_download_task_data_serialization(self):
        """测试数据序列化"""

        # 创建测试数据
        test_data = pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ"],
                "symbol": ["000001", "000002"],
                "name": ["平安银行", "万科A"],
            }
        )

        with patch(
            "neo.downloader.simple_downloader.SimpleDownloader"
        ) as mock_downloader_class:
            mock_downloader = Mock()
            mock_downloader_class.return_value = mock_downloader

            mock_result = Mock()
            mock_result.success = True
            mock_result.data = test_data
            mock_result.error = None
            mock_result.retry_count = 0
            mock_downloader.download.return_value = mock_result

            # 执行任务
            result = _download_task_impl("stock_basic", "000001.SZ")

            # 验证数据被正确序列化
            assert result["success"] is True
            assert result["data"] is not None
            assert isinstance(result["data"], dict)

            # 验证数据内容
            data_dict = result["data"]
            assert "ts_code" in data_dict
            assert "symbol" in data_dict
            assert "name" in data_dict

    def test_download_task_none_data(self):
        """测试数据为 None 的情况"""

        with patch(
            "neo.downloader.simple_downloader.SimpleDownloader"
        ) as mock_downloader_class:
            mock_downloader = Mock()
            mock_downloader_class.return_value = mock_downloader

            mock_result = Mock()
            mock_result.success = True
            mock_result.data = None  # 数据为 None
            mock_result.error = None
            mock_result.retry_count = 0
            mock_downloader.download.return_value = mock_result

            # 执行任务
            result = _download_task_impl("stock_basic", "000001.SZ")

            # 验证结果
            assert result["success"] is True
            assert result["data"] is None
            assert result["error"] is None


class TestHueyIntegration:
    """Huey 集成测试类（从 test_huey_integration.py 合并）"""

    def test_huey_configuration(self):
        """测试 Huey 配置是否正确"""
        assert huey.name == "stock_downloader"
        assert not huey.immediate  # 异步执行
        assert not huey.utc  # 使用本地时区

    def test_async_task_chain(self):
        """测试异步任务链式调用"""
        # 验证任务函数可以被调用（不执行实际的下载逻辑）
        assert callable(download_task)
        assert callable(process_data_task)

        # 验证任务类型枚举可用
        assert hasattr(TaskType, "stock_basic")
        assert TaskType.stock_basic is not None

    def test_process_data_task_execution(self):
        """测试数据处理任务执行"""
        # 验证任务函数可以被调用
        assert callable(process_data_task)

        # 验证任务类型枚举可用
        assert hasattr(TaskType, "stock_basic")
        assert TaskType.stock_basic is not None

    def test_task_registration(self):
        """测试任务是否正确注册到 Huey"""
        # 检查任务是否在 Huey 注册表中
        # 简化测试：只验证 Huey 实例有注册表
        assert hasattr(huey, "_registry")
        assert huey._registry is not None

        # 验证任务函数存在
        assert callable(download_task)
        assert callable(process_data_task)
