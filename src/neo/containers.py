from dependency_injector import containers, providers
from neo.downloader.fetcher_builder import FetcherBuilder
from neo.helpers.rate_limit_manager import RateLimitManager

from neo.database.operator import ParquetDBQueryer
from neo.database.schema_loader import SchemaLoader
from neo.helpers.task_builder import TaskBuilder
from neo.helpers.group_handler import GroupHandler
from neo.services.consumer_runner import ConsumerRunner
from neo.services.downloader_service import DownloaderService
from neo.writers.parquet_writer import ParquetWriter
from neo.data_processor.full_replace_data_processor import FullReplaceDataProcessor
from neo.configs import get_config


class AppContainer(containers.DeclarativeContainer):
    """应用的核心服务容器"""

    config = providers.Configuration()
    config.from_dict(get_config().to_dict())

    # Services
    consumer_runner = providers.Factory(ConsumerRunner)
    downloader_service = providers.Factory(DownloaderService)

    # Core Components
    fetcher_builder = providers.Factory(FetcherBuilder)
    rate_limit_manager = providers.Singleton(RateLimitManager.singleton)

    # Schema and Database Components
    schema_loader = providers.Singleton(SchemaLoader)

    # Database Components - 职责分离
    db_queryer = providers.Factory(
        ParquetDBQueryer, schema_loader=schema_loader
    )  # 专门负责查询
    task_builder = providers.Singleton(TaskBuilder)
    group_handler = providers.Singleton(GroupHandler)

    # Writers
    parquet_writer = providers.Factory(
        ParquetWriter, base_path=config.storage.parquet_base_path
    )

    # 全量替换数据处理器，用于需要全量替换的表
    full_replace_data_processor = providers.Factory(
        FullReplaceDataProcessor,
        parquet_writer=parquet_writer,
        db_queryer=db_queryer,
        schema_loader=schema_loader,
    )

    # Core Components
    downloader = providers.Singleton(
        "neo.downloader.simple_downloader.SimpleDownloader",
        fetcher_builder=fetcher_builder,
        rate_limit_manager=rate_limit_manager,
    )
    data_processor = providers.Factory(
        "neo.data_processor.simple_data_processor.SimpleDataProcessor",
        parquet_writer=parquet_writer,
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
