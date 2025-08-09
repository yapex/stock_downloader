import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from downloader.storage_factory import (
    StorageFactory,
    StorageSingleton,
    get_storage,
    get_storage_instance_info
)
from downloader.storage import PartitionedStorage


class TestStorageFactory:
    """测试Storage工厂类"""
    
    def setup_method(self):
        """每个测试方法前重置单例"""
        StorageSingleton.reset_instance()
    
    def teardown_method(self):
        """每个测试方法后重置单例"""
        StorageSingleton.reset_instance()
    
    def test_create_storage_singleton(self):
        """测试创建单例Storage"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            
            storage1 = StorageFactory.create_storage(use_singleton=True, db_path=db_path)
            storage2 = StorageFactory.create_storage(use_singleton=True, db_path=db_path)
            
            assert isinstance(storage1, PartitionedStorage)
            assert isinstance(storage2, PartitionedStorage)
            assert storage1 is storage2  # 应该是同一个实例
    
    def test_create_storage_non_singleton(self):
        """测试创建非单例Storage"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            
            storage1 = StorageFactory.create_storage(use_singleton=False, db_path=db_path)
            storage2 = StorageFactory.create_storage(use_singleton=False, db_path=db_path)
            
            assert isinstance(storage1, PartitionedStorage)
            assert isinstance(storage2, PartitionedStorage)
            assert storage1 is not storage2  # 应该是不同的实例
    
    def test_create_partitioned_storage(self):
        """测试直接创建分区表存储"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            
            storage = StorageFactory.create_partitioned_storage(db_path=db_path)
            
            assert isinstance(storage, PartitionedStorage)
            assert storage.db_path == Path(db_path)
    
    def test_get_default_storage_with_config(self):
        """测试根据配置获取默认Storage"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            
            config = {
                "storage": {
                    "db_path": db_path
                }
            }
            
            storage = StorageFactory.get_default_storage(config)
            
            assert isinstance(storage, PartitionedStorage)
            assert storage.db_path == Path(db_path)
    
    def test_get_default_storage_with_database_config(self):
        """测试使用database配置获取默认Storage"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            
            config = {
                "database": {
                    "path": db_path
                }
            }
            
            storage = StorageFactory.get_default_storage(config)
            
            assert isinstance(storage, PartitionedStorage)
            assert storage.db_path == Path(db_path)
    
    def test_get_default_storage_no_config(self):
        """测试无配置时获取默认Storage"""
        storage = StorageFactory.get_default_storage()
        
        assert isinstance(storage, PartitionedStorage)
        assert storage.db_path == Path("data/stock.db")
    
    def test_get_default_storage_path_object(self):
        """测试配置中使用Path对象"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            
            config = {
                "storage": {
                    "db_path": db_path
                }
            }
            
            storage = StorageFactory.get_default_storage(config)
            
            assert isinstance(storage, PartitionedStorage)
            assert storage.db_path == db_path


class TestStorageSingleton:
    """测试Storage单例类"""
    
    def setup_method(self):
        """每个测试方法前重置单例"""
        StorageSingleton.reset_instance()
    
    def teardown_method(self):
        """每个测试方法后重置单例"""
        StorageSingleton.reset_instance()
    
    def test_singleton_same_path(self):
        """测试相同路径返回同一实例"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            
            instance1 = StorageSingleton.get_instance(db_path)
            instance2 = StorageSingleton.get_instance(db_path)
            
            assert instance1 is instance2
    
    def test_singleton_different_path(self):
        """测试不同路径创建新实例"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path1 = str(Path(temp_dir) / "test1.db")
            db_path2 = str(Path(temp_dir) / "test2.db")
            
            instance1 = StorageSingleton.get_instance(db_path1)
            instance2 = StorageSingleton.get_instance(db_path2)
            
            assert instance1 is not instance2
            assert instance1.db_path == Path(db_path1)
            assert instance2.db_path == Path(db_path2)
    
    def test_reset_instance(self):
        """测试重置单例实例"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            
            instance1 = StorageSingleton.get_instance(db_path)
            StorageSingleton.reset_instance()
            instance2 = StorageSingleton.get_instance(db_path)
            
            assert instance1 is not instance2
    
    def test_get_instance_info(self):
        """测试获取实例信息"""
        # 初始状态
        info = StorageSingleton.get_instance_info()
        assert info["has_instance"] is False
        assert info["db_path"] is None
        assert info["instance_id"] is None
        
        # 创建实例后
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            instance = StorageSingleton.get_instance(db_path)
            
            info = StorageSingleton.get_instance_info()
            assert info["has_instance"] is True
            assert info["db_path"] == db_path
            assert info["instance_id"] == id(instance)
            assert info["thread_id"] is not None


class TestGetStorageFunctions:
    """测试便捷函数"""
    
    def setup_method(self):
        """每个测试方法前重置单例"""
        StorageSingleton.reset_instance()
    
    def teardown_method(self):
        """每个测试方法后重置单例"""
        StorageSingleton.reset_instance()
    
    def test_get_storage_singleton(self):
        """测试get_storage单例模式"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            
            storage1 = get_storage(use_singleton=True, db_path=db_path)
            storage2 = get_storage(use_singleton=True, db_path=db_path)
            
            assert storage1 is storage2
    
    def test_get_storage_non_singleton(self):
        """测试get_storage非单例模式"""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "test.db")
            
            storage1 = get_storage(use_singleton=False, db_path=db_path)
            storage2 = get_storage(use_singleton=False, db_path=db_path)
            
            assert storage1 is not storage2
    
    def test_get_storage_instance_info(self):
        """测试get_storage_instance_info函数"""
        info = get_storage_instance_info()
        assert "has_instance" in info
        assert "db_path" in info
        assert "instance_id" in info
        assert "thread_id" in info