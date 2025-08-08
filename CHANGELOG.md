# 更新日志

## [Unreleased] - 2025-01-13

### 代码清理

- **删除冗余模块**:
  - `enhanced_queue_manager.py` - 完全无引用，0% 覆盖率的队列管理器增强版本
  - `scripts/find_unused.py` - 完成使命的一次性分析脚本
  - `dead_letter_csv.py` - 83% 覆盖率但无运行时引用的死信CSV处理模块
  - `stock_downloader/domain/category_service.py` - 与 `src/downloader/domain/category_service.py` 重复的文件
  - 多个空的 `__init__.py` 文件 - 简化包结构
  - 总计删除约 1000 行冗余代码，同时保持 100% 功能完整性

### CLI 优化

- **精简 CLI 入口与实时进度显示**:
  - CLI 层仅负责参数解析，立即将控制权交给 `DownloadEngine`
  - 集成 `tqdm` 实时进度条，显示任务进度和统计信息
  - 终端仅显示进度条和关键警告，详细日志写入 `logs/downloader.log`
  - 新增全局进度管理器 `progress_manager.py`，支持线程安全的进度更新
  - 优化日志过滤器，确保用户友好的终端输出体验

### 重构

- **重构 `IncrementalTaskHandler`**:
  - 在 `src/downloader/tasks/base.py` 中，对 `IncrementalTaskHandler` 类进行了重构，以消除代码重复。
  - 提取了一个新的私有方法 `_process_single_symbol`，该方法封装了处理单个股票的所有核心逻辑（获取最新日期、下载、保存、错误处理）。
  - 现在，主执行方法 `execute` 和重试方法 `_retry_network_errors` 都调用这个统一的方法，显著提高了代码的可维护性和健壮性。
