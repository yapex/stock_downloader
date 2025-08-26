from dependency_injector import containers, providers
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.helpers.rate_limit_manager import RateLimitManager

# 延迟导入以避免循环导入
# from neo.downloader.simple_downloader import SimpleDownloader
# from neo.helpers.app_service import AppService
from neo.data_processor.simple_data_processor import AsyncSimpleDataProcessor
from neo.database.operator import DBOperator
from neo.database.schema_loader import SchemaLoader
from neo.helpers.task_builder import TaskBuilder
from neo.helpers.group_handler import GroupHandler


class AppContainer(containers.DeclarativeContainer):
    """应用的核心服务容器"""

    fetcher_builder = providers.Factory(FetcherBuilder)
    rate_limit_manager = providers.Singleton(RateLimitManager.singleton)
    db_operator = providers.Factory(DBOperator)
    downloader = providers.Singleton(
        "neo.downloader.simple_downloader.SimpleDownloader",
        fetcher_builder=fetcher_builder,
        rate_limit_manager=rate_limit_manager,
    )
    schema_loader = providers.Singleton(SchemaLoader)
    data_processor = providers.Factory(
        AsyncSimpleDataProcessor,
        enable_batch=False,
        db_operator=db_operator,
        schema_loader=schema_loader,
    )

    app_service = providers.Singleton(
        "neo.helpers.app_service.AppService",
    )

    task_builder = providers.Singleton(TaskBuilder)
    group_handler = providers.Singleton(GroupHandler)


if __name__ == "__main__":
    from neo.app import container

    # downloader = container.downloader()
    # df = downloader.download(TaskType.stock_basic.name, "600519.SH")
    # print(df.head(1))

    db_operator = container.db_operator()
    db_operator.drop_all_tables()
    db_operator.create_all_tables()
