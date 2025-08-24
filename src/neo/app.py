"""中心化的应用引导模块

作为应用的"单一真相来源"，创建、配置并装配唯一的容器实例。
确保无论是主进程还是 Huey 的工作进程，引用的都是同一个经过配置的容器实例。
"""

from .containers import AppContainer
# from .tasks import huey_tasks  # 导入需要被注入依赖的模块
# from .downloader.simple_downloader import SimpleDownloader

# 创建全局唯一的容器实例
container = AppContainer()

# 在这里集中完成"装配"，将容器与模块连接起来
# container.wire(modules=[huey_tasks])
# container.wire(modules=[SimpleDownloader])
