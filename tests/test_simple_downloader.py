"""测试 SimpleDownloader 类"""

from unittest.mock import Mock

from neo.downloader.simple_downloader import SimpleDownloader
from neo.database.operator import DBOperator


class TestSimpleDownloader:
    """测试 SimpleDownloader 类"""

    def test_create_default(self):
        """测试 create_default 工厂方法"""
        downloader = SimpleDownloader.create_default()

        # 验证实例创建成功
        assert isinstance(downloader, SimpleDownloader)

        # 验证实例具有预期的属性和方法
        assert hasattr(downloader, "rate_limiters")
        assert hasattr(downloader, "fetcher_builder")
        assert hasattr(downloader, "download")
        assert hasattr(downloader, "config")

        # 验证 rate_limiters 初始化为空字典
        assert downloader.rate_limiters == {}

        # 验证 fetcher_builder 已正确初始化
        assert downloader.fetcher_builder is not None

        # 验证 config 已正确加载
        assert downloader.config is not None

    def setup_method(self):
        """测试前设置"""
        self.mock_db_operator = Mock(spec=DBOperator)
        self.downloader = SimpleDownloader(db_operator=self.mock_db_operator)

    def test_init_with_default_params(self):
        """测试使用默认参数初始化"""
        downloader = SimpleDownloader()

        assert isinstance(downloader, SimpleDownloader)
        assert downloader.rate_limiters == {}
        assert downloader.fetcher_builder is not None
        assert downloader.config is not None

    def test_init_with_custom_db_operator(self):
        """测试使用自定义 db_operator 初始化"""
        mock_db_operator = Mock(spec=DBOperator)

        downloader = SimpleDownloader(db_operator=mock_db_operator)

        assert downloader.rate_limiters == {}
        assert downloader.fetcher_builder is not None
