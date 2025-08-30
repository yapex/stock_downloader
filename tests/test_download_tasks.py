"""测试下载任务相关功能"""

from datetime import time
from unittest.mock import Mock, patch

from neo.tasks.download_tasks import DownloadTaskManager


class TestDownloadTaskManager:
    """测试 DownloadTaskManager 类"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        # 创建 mock schema_loader
        self.mock_schema_loader = Mock()
        # 创建服务实例
        self.service = DownloadTaskManager(schema_loader=self.mock_schema_loader)

    def test_should_skip_task_no_latest_date(self):
        """测试没有最新日期时不跳过任务"""
        result = self.service._should_skip_task(None, "20240115")
        assert result is False

        result = self.service._should_skip_task("", "20240115")
        assert result is False

    def test_should_skip_task_today_date(self):
        """测试最新日期等于最新交易日时跳过任务"""
        latest_trading_day = "20240115"
        result = self.service._should_skip_task(latest_trading_day, latest_trading_day)
        assert result is True

    def test_should_skip_task_data_behind_should_download_immediately(self):
        """测试本地数据落后时应立即下载，不管收盘时间"""
        with patch("neo.tasks.download_tasks.datetime") as mock_datetime:
            # 模拟今天是 2024-01-15，当前时间是下午5点
            mock_now = Mock()
            mock_now.strftime.return_value = "20240115"  # 今天
            mock_now.time.return_value = time(17, 0)  # 下午5点
            mock_datetime.now.return_value = mock_now

            # 本地数据是昨天的，今天是交易日，但还没到收盘时间
            # 根据新逻辑：本地数据落后时立即下载，不管收盘时间
            result = self.service._should_skip_task("20240112", "20240115")
            assert result is False  # 应该下载，不跳过

    def test_should_skip_task_data_behind_should_download_after_close(self):
        """测试本地数据落后时应立即下载，即使在收盘后"""
        with patch("neo.tasks.download_tasks.datetime") as mock_datetime:
            # 模拟今天是 2024-01-15，当前时间是下午7点
            mock_now = Mock()
            mock_now.strftime.return_value = "20240115"  # 今天
            mock_now.time.return_value = time(19, 0)  # 下午7点
            mock_datetime.now.return_value = mock_now

            # 本地数据是昨天的，今天是交易日，已经过了收盘时间
            # 根据新逻辑：本地数据落后时立即下载
            result = self.service._should_skip_task("20240112", "20240115")
            assert result is False

    def test_should_skip_task_older_date(self):
        """测试最新日期是更早日期时不跳过任务"""
        # 本地数据比最新交易日早，需要下载
        result = self.service._should_skip_task("20240110", "20240115")
        assert result is False

    def test_should_skip_task_future_date(self):
        """测试最新日期是未来日期时的处理"""
        # 本地数据比最新交易日新（异常情况），应该跳过
        result = self.service._should_skip_task("20240120", "20240115")
        assert result is True

    def test_should_skip_task_data_current_before_market_close(self):
        """测试本地数据已是最新交易日，收盘前应跳过"""
        with patch("neo.tasks.download_tasks.datetime") as mock_datetime:
            # 模拟今天是 2024-01-15，当前时间是下午5点
            mock_now = Mock()
            mock_now.strftime.return_value = "20240115"  # 今天
            mock_now.time.return_value = time(17, 0)  # 下午5点
            mock_datetime.now.return_value = mock_now

            # 本地数据已经是最新交易日的，今天是交易日，但还没到收盘时间
            result = self.service._should_skip_task("20240115", "20240115")
            assert result is True  # 应该跳过，等待收盘
    
    def test_should_skip_task_data_current_after_market_close(self):
        """测试本地数据已是最新交易日，收盘后应下载今日数据"""
        with patch("neo.tasks.download_tasks.datetime") as mock_datetime:
            # 模拟今天是 2024-01-15，当前时间是下午7点
            mock_now = Mock()
            mock_now.strftime.return_value = "20240115"  # 今天
            mock_now.time.return_value = time(19, 0)  # 下午7点
            mock_datetime.now.return_value = mock_now

            # 本地数据已经是最新交易日的，今天是交易日，已经过了收盘时间
            result = self.service._should_skip_task("20240115", "20240115")
            assert result is False  # 应该下载今日数据
    
    def test_should_skip_task_data_current_at_market_close_time(self):
        """测试边界条件：本地数据是最新的，正好在收盘时间点"""
        with patch("neo.tasks.download_tasks.datetime") as mock_datetime:
            # 模拟今天是 2024-01-15，当前时间是下午6点整（收盘时间）
            mock_now = Mock()
            mock_now.strftime.return_value = "20240115"  # 今天
            mock_now.time.return_value = time(18, 0)  # 下午6点整
            mock_datetime.now.return_value = mock_now

            # 本地数据已经是最新交易日的，今天是交易日，正好到收盘时间（应该可以下载）
            result = self.service._should_skip_task("20240115", "20240115")
            assert result is False  # 等于收盘时间时不跳过

    def test_should_skip_task_no_latest_trading_day(self):
        """测试没有最新交易日时使用备用逻辑"""
        with patch("neo.tasks.download_tasks.datetime") as mock_datetime:
            # 模拟今天是 2024-01-15，当前时间是下午5点
            mock_now = Mock()
            mock_now.strftime.return_value = "20240115"  # 今天
            mock_now.time.return_value = time(17, 0)  # 下午5点
            mock_datetime.now.return_value = mock_now

            # 模拟 datetime.now() - timedelta(days=1) 的结果
            mock_yesterday = Mock()
            mock_yesterday.strftime.return_value = "20240114"  # 昨天
            mock_datetime.now.return_value.__sub__ = Mock(return_value=mock_yesterday)

            # 没有最新交易日，使用备用逻辑：本地数据是今天的，应该跳过
            result = self.service._should_skip_task("20240115", None)
            assert result is True

            # 本地数据是昨天的，且在收盘前，应该跳过
            result = self.service._should_skip_task("20240114", None)
            assert result is True

    def test_should_skip_task_with_different_date_formats(self):
        """测试不同日期格式的处理"""
        # 测试正确的日期格式 - 当本地数据日期等于最新交易日时应该跳过
        result = self.service._should_skip_task("20240115", "20240115")
        assert result is True

        # 测试错误的日期格式 - 由于字符串比较，"invalid_date" > "20240115"，所以会跳过
        result = self.service._should_skip_task("invalid_date", "20240115")
        assert result is True  # 字符串比较时 "invalid_date" >= "20240115" 为 True

        # 测试一个会返回 False 的无效格式
        result = self.service._should_skip_task("123", "20240115")
        assert result is False  # "123" < "20240115" 为 True，所以不跳过
