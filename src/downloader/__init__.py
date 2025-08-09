# downloader/__init__.py

# 延迟导入，避免在模块初始化时导入依赖包
# 当需要使用这些类时，使用 from .module import ClassName 的形式

__all__ = ['ConsumerPool', 'Producer', 'DownloadEngine', 'models']
