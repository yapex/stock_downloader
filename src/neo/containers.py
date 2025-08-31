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

from neo.configs import get_config


class AppContainer(containers.DeclarativeContainer):
    """应用的核心服务容器"""

    config = providers.Configuration()
    config.from_dict(get_config().to_dict())

    # Services
    consumer_runner = providers.Factory(ConsumerRunner)
    downloader_service = providers.Factory(DownloaderService)

    # Schema and Database Components
    schema_loader = providers.Singleton(SchemaLoader)

    # Core Components
    fetcher_builder = providers.Factory(FetcherBuilder, schema_loader=schema_loader)
    rate_limit_manager = providers.Singleton(RateLimitManager.singleton)

    # Database Components - 职责分离
    db_queryer = providers.Factory(
        ParquetDBQueryer, schema_loader=schema_loader
    )  # 专门负责查询
    
    # 为了向后兼容，db_operator 指向 db_queryer
    db_operator = db_queryer
    
    task_builder = providers.Singleton(TaskBuilder)
    group_handler = providers.Singleton(GroupHandler, db_operator=db_queryer)

    # Writers
    parquet_writer = providers.Factory(
        ParquetWriter, base_path=config.storage.parquet_base_path
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

    # 注意：ParquetDBQueryer 只支持查询操作，不支持表的创建和删除
    # 如果需要表管理功能，需要实现 ISchemaTableCreator 接口
    db_queryer = container.db_queryer()
    print(f"数据库查询器已初始化: {type(db_queryer).__name__}")
    
    # 示例：获取所有股票代码（如果有数据的话）
    try:
        symbols = db_queryer.get_all_symbols()
        print(f"找到 {len(symbols)} 个股票代码")
    except Exception as e:
        print(f"获取股票代码时出错: {e}")
