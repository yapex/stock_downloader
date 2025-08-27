"""测试下载任务相关功能"""

import pytest
from datetime import datetime, time, timedelta
from unittest.mock import Mock, patch

from neo.tasks.download_tasks import DownloadTaskManager


class TestDownloadTaskManager:
    """测试 DownloadTaskManager 类"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        # 创建服务实例
        self.service = DownloadTaskManager()

    def test_should_skip_task_no_latest_date(self):
        """测试没有最新日期时不跳过任务"""
        result = self.service._should_skip_task(None)
        assert result is False
        
        result = self.service._should_skip_task("")
        assert result is False

    def test_should_skip_task_today_date(self):
        """测试最新日期是今天时跳过任务"""
        today_str = datetime.now().strftime("%Y%m%d")
        result = self.service._should_skip_task(today_str)
        assert result is True

    def test_should_skip_task_yesterday_before_market_close(self):
        """测试最新日期为昨天且当前时间在收盘前的情况"""
        # 使用固定的日期进行测试
        latest_date = "20240101"  # 假设昨天是 2024-01-01
        
        with patch('neo.tasks.download_tasks.datetime') as mock_datetime:
            # 模拟今天是 2024-01-02，当前时间是下午5点
            mock_now = Mock()
            mock_now.strftime.return_value = "20240102"  # 今天
            mock_now.time.return_value = time(17, 0)  # 下午5点
            mock_datetime.now.return_value = mock_now
            
            # 模拟 datetime.now() - timedelta(days=1) 的结果
            mock_yesterday = Mock()
            mock_yesterday.strftime.return_value = "20240101"  # 昨天
            mock_datetime.now.return_value.__sub__ = Mock(return_value=mock_yesterday)
            
            result = self.service._should_skip_task(latest_date)
            assert result is True

    def test_should_skip_task_yesterday_after_market_close(self):
        """测试最新日期为昨天且当前时间在收盘后的情况"""
        # 使用固定的日期进行测试
        latest_date = "20240101"  # 假设昨天是 2024-01-01
        
        with patch('neo.tasks.download_tasks.datetime') as mock_datetime:
            # 模拟今天是 2024-01-02，当前时间是下午7点
            mock_now = Mock()
            mock_now.strftime.return_value = "20240102"  # 今天
            mock_now.time.return_value = time(19, 0)  # 下午7点
            mock_datetime.now.return_value = mock_now
            
            # 模拟 datetime.now() - timedelta(days=1) 的结果
            mock_yesterday = Mock()
            mock_yesterday.strftime.return_value = "20240101"  # 昨天
            mock_datetime.now.return_value.__sub__ = Mock(return_value=mock_yesterday)
            
            result = self.service._should_skip_task(latest_date)
            assert result is False

    def test_should_skip_task_older_date(self):
        """测试最新日期是更早日期时不跳过任务"""
        older_date = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")
        result = self.service._should_skip_task(older_date)
        assert result is False

    def test_should_skip_task_future_date(self):
        """测试最新日期是未来日期时的处理"""
        future_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
        result = self.service._should_skip_task(future_date)
        assert result is False

    def test_should_skip_task_edge_case_market_close_time(self):
        """测试边界条件：正好在收盘时间点"""
        # 使用固定的日期进行测试
        latest_date = "20240101"  # 假设昨天是 2024-01-01
        
        with patch('neo.tasks.download_tasks.datetime') as mock_datetime:
            # 模拟今天是 2024-01-02，当前时间是下午6点整（收盘时间）
            mock_now = Mock()
            mock_now.strftime.return_value = "20240102"  # 今天
            mock_now.time.return_value = time(18, 0)  # 下午6点整
            mock_datetime.now.return_value = mock_now
            
            # 模拟 datetime.now() - timedelta(days=1) 的结果
            mock_yesterday = Mock()
            mock_yesterday.strftime.return_value = "20240101"  # 昨天
            mock_datetime.now.return_value.__sub__ = Mock(return_value=mock_yesterday)
            
            result = self.service._should_skip_task(latest_date)
            assert result is False  # 等于收盘时间时不跳过

    def test_should_skip_task_with_different_date_formats(self):
        """测试不同日期格式的处理"""
        # 测试正确的日期格式
        today_str = datetime.now().strftime("%Y%m%d")
        result = self.service._should_skip_task(today_str)
        assert result is True
        
        # 测试错误的日期格式应该不会导致异常
        # 但由于字符串比较，可能会有意外的结果，这里主要确保不抛异常
        try:
            result = self.service._should_skip_task("2024-01-01")  # 错误格式
            # 这种格式不会匹配今天的日期，所以应该返回False
            assert result is False
        except Exception:
            pytest.fail("不应该因为日期格式错误而抛出异常")