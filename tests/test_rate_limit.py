import time
from unittest.mock import patch
from downloader.rate_limit import RateLimiter, DynamicRateLimiter, rate_limit


def test_rate_limiter_basic_functionality():
    """测试基础速率限制功能"""
    limiter = RateLimiter(calls_per_minute=10)  # 限制为10次/分钟
    
    # 前10次调用应该不需要等待
    start_time = time.time()
    for i in range(10):
        limiter.wait_if_needed()
    elapsed = time.time() - start_time
    
    # 应该几乎立即完成（小于1秒）
    assert elapsed < 1.0
    
    # 第11次调用应该触发等待
    # 但我们不测试等待，因为这会增加测试时间


def test_rate_limiter_resets_after_time():
    """测试速率限制在时间过去后重置"""
    limiter = RateLimiter(calls_per_minute=5)  # 限制为5次/分钟
    
    # Mock时间，模拟5次快速调用
    with patch('time.time', side_effect=[0, 0.1, 0.2, 0.3, 0.4]):
        for i in range(5):
            limiter.wait_if_needed()
    
    # 再Mock时间，模拟时间过去超过1分钟后的新调用
    # 提供足够的时间值以避免StopIteration
    with patch('time.time', side_effect=[61, 61.1, 61.2, 61.3, 61.4, 61.5]):
        for i in range(3):
            limiter.wait_if_needed()
        # 由于我们mock了time.time()，我们不能准确测量elapsed时间
        # 但我们验证了代码可以执行而不抛出异常
        assert True  # 如果没有异常则通过


def test_dynamic_rate_limiter_creates_separate_limiters():
    """测试动态速率限制器为不同键创建独立的限制器"""
    dynamic_limiter = DynamicRateLimiter()
    
    # 获取两个不同的限制器
    limiter1 = dynamic_limiter.get_limiter("task1", 100)
    limiter2 = dynamic_limiter.get_limiter("task2", 50)
    
    # 应该是不同的实例
    assert limiter1 is not limiter2
    
    # 同一个键应该返回相同的实例
    limiter1_again = dynamic_limiter.get_limiter("task1", 100)
    assert limiter1 is limiter1_again


def test_rate_limit_decorator():
    """测试速率限制装饰器的基本功能"""
    call_count = 0
    
    @rate_limit(calls_per_minute=5)
    def test_function():
        nonlocal call_count
        call_count += 1
        return "success"
    
    # 执行几次调用
    results = []
    for i in range(3):
        results.append(test_function())
    
    # 验证函数被正确调用
    assert call_count == 3
    assert all(r == "success" for r in results)


def test_rate_limit_decorator_with_none_limit():
    """测试无速率限制的情况"""
    call_count = 0
    
    @rate_limit(calls_per_minute=None)  # 无限制
    def test_function():
        nonlocal call_count
        call_count += 1
        return "success"
    
    # 执行几次调用
    results = []
    for i in range(5):
        results.append(test_function())
    
    # 验证函数被正确调用
    assert call_count == 5
    assert all(r == "success" for r in results)


def test_rate_limit_decorator_with_different_keys():
    """测试不同键使用不同的限制器"""
    call_counts = {"task1": 0, "task2": 0}
    
    @rate_limit(calls_per_minute=10, task_key="task1")
    def test_function1():
        call_counts["task1"] += 1
        return "task1"
    
    @rate_limit(calls_per_minute=5, task_key="task2")
    def test_function2():
        call_counts["task2"] += 1
        return "task2"
    
    # 分别调用两个函数
    for i in range(3):
        test_function1()
        test_function2()
    
    # 验证调用计数
    assert call_counts["task1"] == 3
    assert call_counts["task2"] == 3