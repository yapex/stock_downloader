from dependency_injector import containers, providers
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.helpers.rate_limit_manager import RateLimitManager

# 延迟导入以避免循环导入
# from neo.downloader.simple_downloader import SimpleDownloader
# from neo.helpers.app_service import AppService
from neo.data_processor.simple_data_processor import AsyncSimpleDataProcessor
from neo.tqmd.progress_tracker import ProgressTrackerFactory, TasksProgressTracker
from neo.database.operator import DBOperator
from neo.database.schema_loader import SchemaLoader
from neo.helpers.task_builder import TaskBuilder
from neo.helpers.group_handler import GroupHandler


class AppContainer(containers.DeclarativeContainer):
    """应用的核心服务容器"""

    fetcher_builder = providers.Factory(FetcherBuilder)
    rate_limit_manager = providers.Singleton(RateLimitManager)
    db_operator = providers.Factory(DBOperator)
    downloader = providers.Singleton(
        "neo.downloader.simple_downloader.SimpleDownloader",
        fetcher_builder=fetcher_builder,
        rate_limit_manager=rate_limit_manager,
        db_operator=db_operator,
    )
    schema_loader = providers.Singleton(SchemaLoader)
    data_processor = providers.Factory(
        AsyncSimpleDataProcessor,
        enable_batch=True,
        db_operator=db_operator,
        schema_loader=schema_loader,
    )
    progress_tracker_factory = providers.Singleton(ProgressTrackerFactory)
    tasks_progress_tracker = providers.Factory(
        TasksProgressTracker,
        factory=progress_tracker_factory,
    )

    app_service = providers.Factory(
        "neo.helpers.app_service.AppService",
        tasks_progress_tracker=tasks_progress_tracker,
    )

    task_builder = providers.Singleton(TaskBuilder)
    group_handler = providers.Singleton(GroupHandler)


if __name__ == "__main__":
    c = AppContainer()

    # downloader = c.downloader()
    # df = downloader.download(TaskType.stock_basic.name, "600519.SH")
    # print(df.head(1))

    db_operator = c.db_operator()
    db_operator.drop_all_tables()
    db_operator.create_all_tables()
