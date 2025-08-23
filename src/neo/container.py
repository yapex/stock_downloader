from dependency_injector import containers, providers
from neo.helpers import TomlConfigProvider
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.task_bus.types import TaskType
from neo.helpers.rate_limit_manager import RateLimitManager
from neo.downloader.simple_downloader import SimpleDownloader


class AppContainer(containers.DeclarativeContainer):
    """应用的核心服务容器"""

    # 在初始化时加载配置
    config = TomlConfigProvider()

    fetcher_builder = providers.Factory(FetcherBuilder)
    rate_limit_manager = providers.Factory(RateLimitManager)
    downloader = providers.Factory(
        SimpleDownloader,
        fetcher_builder=fetcher_builder,
        rate_limit_manager=rate_limit_manager,
    )


if __name__ == "__main__":
    c = AppContainer()
    config = c.config()
    print(config.database.path)

    # task_type = TaskType.stock_daily
    # fetcher_builder = c.fetcher_builder_provider()
    # fetcher = fetcher_builder.build_by_task(task_type, "600519")
    # print(fetcher().head())

    downloader = c.downloader()
    df = downloader.download(TaskType.stock_basic.name, "600519.SH")
    print(df.head(1))
