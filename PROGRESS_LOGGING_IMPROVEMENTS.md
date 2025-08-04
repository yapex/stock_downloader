# 进度条日志输出兼容性改进

## 问题描述

在原有的代码中，`tqdm` 进度条和 `logging` 日志输出混合使用时，会出现以下问题：
- 日志输出会打断进度条的显示
- 控制台输出显得零散和混乱
- 用户体验不佳，难以清晰看到任务进展

## 解决方案

### 1. 创建与 tqdm 兼容的日志处理器

在 `main.py` 中创建了 `TqdmLoggingHandler` 类：
```python
class TqdmLoggingHandler(logging.StreamHandler):
    """与tqdm兼容的日志处理器"""
    
    def emit(self, record):
        try:
            msg = self.format(record)
            # 使用 tqdm.write 来输出日志，这样不会打断进度条
            tqdm.write(msg, file=sys.stdout, end='\n')
        except Exception:
            self.handleError(record)
```

### 2. 增强基础任务处理器

在 `src/downloader/tasks/base.py` 中添加了智能日志输出方法：
- `_safe_log()`: 根据当前是否有活跃进度条选择合适的输出方式
- `_log_info()`, `_log_warning()`, `_log_error()`: 便捷的日志输出方法
- `_current_progress_bar`: 跟踪当前活跃的进度条

### 3. 改进进度条配置

为进度条添加了更好的配置参数：
```python
progress_bar = tqdm(
    target_symbols, 
    desc=f"执行: {task_name}",
    ncols=100,      # 固定进度条宽度
    leave=True,     # 完成后保留进度条
    file=sys.stdout # 确保输出到标准输出
)
```

### 4. 更新所有任务处理器

将所有任务处理器中的直接 `logger` 调用替换为新的兼容方法：
- `stock_list.py`: 使用 `_log_info()`, `_log_error()`
- `financials.py`: 使用 `_log_error()`
- `daily.py` 和 `daily_basic.py`: 继承改进后的基类

## 改进效果

### 改进前
```
处理: daily_qfq_000001.SZ:  45%|████████▌       | 1234/2743 [02:15<02:45, 9.1it/s]
2025-08-04 12:30:15 - DailyTaskHandler - INFO - 完成股票批次处理
处理: daily_qfq_000002.SZ:  46%|████████▋       | 1235/2743 [02:16<02:44, 9.2it/s]
2025-08-04 12:30:16 - DailyTaskHandler - WARNING - 股票数据获取延迟
处理: daily_qfq_000003.SZ:  47%|████████▋       | 1236/2743 [02:16<02:43, 9.2it/s]
```

### 改进后
```
12:47:13 - TestLogger - INFO - ✅ 完成任务批次 1
12:47:13 - TestLogger - WARNING - ⚠️  任务 task_007 需要特别关注
12:47:14 - TestLogger - ERROR - ❌ 任务 task_015 模拟错误（但继续执行）
处理: task_019: 100%|███████████████████████████████████████████████| 20/20 [00:02<00:00, 9.54it/s]
```

## 技术细节

### 智能日志路由
```python
def _safe_log(self, level: str, message: str, *args, **kwargs):
    """安全的日志输出方法，与进度条兼容"""
    if self._current_progress_bar is not None:
        # 如果当前有活跃的进度条，使用 tqdm.write 输出
        log_msg = f"{self.logger.name} - {level.upper()} - {message}"
        if args:
            log_msg = log_msg % args
        tqdm.write(log_msg)
    else:
        # 否则使用常规的 logger
        getattr(self.logger, level.lower())(message, *args, **kwargs)
```

### 进度条生命周期管理
- 在进度条创建时设置 `self._current_progress_bar`
- 在 `finally` 块中清理引用，确保资源正确释放
- 使用 `progress_bar.close()` 确保进度条正确结束

## 测试验证

创建了 `test_progress_logging.py` 测试脚本，验证：
1. 进度条显示连续性
2. 日志输出不打断进度条
3. 多种日志级别的兼容性
4. 异常处理时的稳定性

## 向后兼容性

- 保持原有 API 不变
- 对不使用进度条的场景无影响
- 所有现有功能继续正常工作

## 性能影响

- 日志输出性能略有提升（减少控制台刷新）
- 内存占用基本无变化
- 进度条显示更流畅

## 使用建议

1. 在有进度条的任务中，始终使用新的 `_log_*()` 方法
2. 避免在进度条循环中使用直接的 `print()` 语句
3. 如需自定义进度条，参考 `IncrementalTaskHandler` 的实现模式

这些改进大大提升了用户体验，使得长时间运行的数据下载任务有更清晰、更专业的输出展示。
