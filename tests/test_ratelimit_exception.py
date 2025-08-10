"""测试RateLimitException的模拟和处理"""

import pytest
import time
from unittest.mock import patch, MagicMock
from src.downloader.error_handler import (
    enhanced_retry,
    API_LIMIT_RETRY_STRATEGY,
    RateLimitException,
    classify_error,
    ErrorCategory
)


class TestRateLimitException:
    """测试RateLimitException异常处理"""
    
    def test_ratelimit_exception_classification(self):
        """测试RateLimitException被正确分类"""
        # 创建RateLimitException实例
        rate_error = RateLimitException("Rate limit exceeded", period_remaining=30.0)
        
        # 验证异常分类
        category = classify_error(rate_error)
        assert category == ErrorCategory.API_LIMIT
    
    def test_ratelimit_exception_with_period_remaining(self):
        """测试带有period_remaining属性的RateLimitException"""
        call_count = 0
        
        @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="测试限流")
        def mock_api_call():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # 模拟RateLimitException，带有period_remaining属性
                raise RateLimitException("Rate limit exceeded", period_remaining=0.1)  # 使用很短的等待时间
            return "success"
        
        # 记录开始时间
        start_time = time.time()
        
        # 应该重试并最终成功
        result = mock_api_call()
        
        # 记录结束时间
        end_time = time.time()
        
        assert result == "success"
        assert call_count == 3  # 第一次失败，第二次失败，第三次成功
        
        # 验证确实等待了period_remaining时间（至少等待了0.2秒，因为失败了2次）
        assert end_time - start_time >= 0.2
    
    def test_ratelimit_exception_without_period_remaining(self):
        """测试没有period_remaining属性的RateLimitException"""
        call_count = 0
        
        @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="测试限流")
        def mock_api_call():
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                # 创建RateLimitException，但使用很小的period_remaining
                error = RateLimitException("Rate limit exceeded", period_remaining=0.01)
                # 删除period_remaining属性来模拟没有该属性的情况
                if hasattr(error, 'period_remaining'):
                    delattr(error, 'period_remaining')
                raise error
            return "success"
        
        # 应该重试并最终成功
        result = mock_api_call()
        assert result == "success"
        assert call_count == 2  # 第一次失败，第二次成功
    
    def test_ratelimit_exception_max_retries_exceeded(self):
        """测试RateLimitException超过最大重试次数"""
        call_count = 0
        
        @enhanced_retry(strategy=API_LIMIT_RETRY_STRATEGY, task_name="测试限流")
        def mock_api_call():
            nonlocal call_count
            call_count += 1
            # 总是抛出RateLimitException
            raise RateLimitException("Rate limit exceeded", period_remaining=0.01)
        
        # 应该重试失败并返回None
        result = mock_api_call()
        assert result is None
        assert call_count == 4  # 最大重试次数+1（API_LIMIT_RETRY_STRATEGY.max_retries = 3）
    
    @patch('src.downloader.fetcher.ts')
    @patch.dict('os.environ', {'TUSHARE_TOKEN': 'test_token'})
    def test_fetcher_with_ratelimit_exception(self, mock_ts):
        """测试fetcher方法在遇到RateLimitException时的处理"""
        from src.downloader.fetcher import TushareFetcher
        
        # 模拟tushare初始化
        mock_pro = MagicMock()
        mock_ts.pro_api.return_value = mock_pro
        mock_ts.set_token.return_value = None
        mock_pro.trade_cal.return_value = MagicMock()
        
        # 模拟stock_basic方法抛出RateLimitException
        call_count = 0
        def mock_stock_basic(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise RateLimitException("Rate limit exceeded", period_remaining=0.01)
            import pandas as pd
            return pd.DataFrame({'ts_code': ['000001.SZ'], 'name': ['测试股票']})
        
        mock_pro.stock_basic.side_effect = mock_stock_basic
        
        fetcher = TushareFetcher()
        
        # 调用fetch_stock_list，应该重试并最终成功
        result = fetcher.fetch_stock_list()
        
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]['ts_code'] == '000001.SZ'
        assert call_count == 3  # 重试了2次后成功
    
    def test_ratelimit_exception_import_fallback(self):
        """测试当ratelimit库不可用时的fallback实现"""
        # 这个测试验证我们的fallback RateLimitException类
        from src.downloader.error_handler import RateLimitException
        
        # 创建异常实例
        error = RateLimitException("Test error", period_remaining=45.0)
        
        # 验证属性存在
        assert hasattr(error, 'period_remaining')
        assert error.period_remaining == 45.0
        
        # 真实的ratelimit库的RateLimitException需要period_remaining参数
        # 所以我们测试带参数的构造函数
        error_with_param = RateLimitException("Test error", 30.0)
        assert hasattr(error_with_param, 'period_remaining')
        assert error_with_param.period_remaining == 30.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])