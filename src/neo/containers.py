from dependency_injector import containers, providers
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.helpers.rate_limit_manager import RateLimitManager

from neo.database.operator import DBOperator
from neo.database.schema_loader import SchemaLoader
from neo.helpers.task_builder import TaskBuilder
from neo.helpers.group_handler import GroupHandler
from neo.services.consumer_runner import ConsumerRunner
from neo.services.downloader_service import DownloaderService


class AppContainer(containers.DeclarativeContainer):
    """应用的核心服务容器"""

    # Services
    consumer_runner = providers.Factory(ConsumerRunner)
    downloader_service = providers.Factory(DownloaderService)

    # Helpers & Managers
    fetcher_builder = providers.Factory(FetcherBuilder)
    rate_limit_manager = providers.Singleton(RateLimitManager.singleton)
    db_operator = providers.Factory(DBOperator)
    schema_loader = providers.Singleton(SchemaLoader)
    task_builder = providers.Singleton(TaskBuilder)
    group_handler = providers.Singleton(GroupHandler)

    # Core Components
    downloader = providers.Singleton(
        "neo.downloader.simple_downloader.SimpleDownloader",
        fetcher_builder=fetcher_builder,
        rate_limit_manager=rate_limit_manager,
    )
    data_processor = providers.Factory(
        "neo.data_processor.simple_data_processor.SimpleDataProcessor",
        db_operator=db_operator,
        schema_loader=schema_loader,
    )

    # Facade
    app_service = providers.Singleton(
        "neo.helpers.app_service.AppService",
        consumer_runner=consumer_runner,
        downloader_service=downloader_service,
    )


if __name__ == "__main__":
    from neo.app import container

    # downloader = container.downloader()
    # df = downloader.download(TaskType.stock_basic.name, "600519.SH")
    # print(df.head(1))

    db_operator = container.db_operator()
    db_operator.drop_all_tables()
    db_operator.create_all_tables()
