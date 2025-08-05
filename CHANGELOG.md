# 更新日志

## [Unreleased] - 2025-08-05

### 重构

- **重构 `IncrementalTaskHandler`**:
  - 在 `src/downloader/tasks/base.py` 中，对 `IncrementalTaskHandler` 类进行了重构，以消除代码重复。
  - 提取了一个新的私有方法 `_process_single_symbol`，该方法封装了处理单个股票的所有核心逻辑（获取最新日期、下载、保存、错误处理）。
  - 现在，主执行方法 `execute` 和重试方法 `_retry_network_errors` 都调用这个统一的方法，显著提高了代码的可维护性和健壮性。