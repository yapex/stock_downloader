#!/usr/bin/env python3
"""
Protocol 接口编程演示程序

本程序演示如何使用 Python 的 Protocol 技术来保障接口设计，
实现依赖注入和面向接口编程的最佳实践。
"""

from typing import Protocol, List, Dict, Any, Optional
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
import logging


# ============================================================================
# 数据模型定义
# ============================================================================

@dataclass
class User:
    """用户数据模型"""
    id: int
    name: str
    email: str
    created_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'email': self.email,
            'created_at': self.created_at.isoformat()
        }


@dataclass
class ProcessResult:
    """处理结果模型"""
    success: bool
    message: str
    data: Optional[Any] = None
    error_code: Optional[str] = None


# ============================================================================
# Protocol 接口定义
# ============================================================================

class IDataProcessor(Protocol):
    """数据处理器接口
    
    定义数据处理的标准接口，支持数据验证、转换和处理。
    """
    
    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证数据格式和内容"""
        ...
    
    @abstractmethod
    def process(self, data: Dict[str, Any]) -> ProcessResult:
        """处理数据并返回结果"""
        ...
    
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """转换数据格式"""
        ...


class IStorage(Protocol):
    """存储接口
    
    定义数据存储的标准接口，支持增删改查操作。
    """
    
    @abstractmethod
    def save(self, user: User) -> bool:
        """保存用户数据"""
        ...
    
    @abstractmethod
    def find_by_id(self, user_id: int) -> Optional[User]:
        """根据ID查找用户"""
        ...
    
    @abstractmethod
    def find_all(self) -> List[User]:
        """查找所有用户"""
        ...
    
    @abstractmethod
    def delete(self, user_id: int) -> bool:
        """删除用户"""
        ...


class INotificationService(Protocol):
    """通知服务接口
    
    定义消息通知的标准接口，支持多种通知方式。
    """
    
    @abstractmethod
    def send_notification(self, recipient: str, message: str) -> bool:
        """发送通知消息"""
        ...
    
    @abstractmethod
    def send_batch_notifications(self, notifications: List[Dict[str, str]]) -> List[bool]:
        """批量发送通知"""
        ...


class ILogger(Protocol):
    """日志记录接口
    
    定义日志记录的标准接口。
    """
    
    @abstractmethod
    def info(self, message: str) -> None:
        """记录信息日志"""
        ...
    
    @abstractmethod
    def error(self, message: str, error: Optional[Exception] = None) -> None:
        """记录错误日志"""
        ...
    
    @abstractmethod
    def debug(self, message: str) -> None:
        """记录调试日志"""
        ...


# ============================================================================
# 具体实现类
# ============================================================================

class UserDataProcessor:
    """用户数据处理器实现
    
    实现 IDataProcessor 接口，专门处理用户相关数据。
    """
    
    def __init__(self, logger: ILogger):
        self._logger = logger
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """验证用户数据"""
        required_fields = ['name', 'email']
        
        for field in required_fields:
            if field not in data or not data[field]:
                self._logger.error(f"缺少必需字段: {field}")
                return False
        
        # 验证邮箱格式
        email = data['email']
        if '@' not in email or '.' not in email:
            self._logger.error(f"邮箱格式无效: {email}")
            return False
        
        self._logger.debug(f"数据验证通过: {data}")
        return True
    
    def process(self, data: Dict[str, Any]) -> ProcessResult:
        """处理用户数据"""
        try:
            if not self.validate(data):
                return ProcessResult(
                    success=False,
                    message="数据验证失败",
                    error_code="VALIDATION_ERROR"
                )
            
            # 转换数据
            transformed_data = self.transform(data)
            
            # 创建用户对象
            user = User(
                id=transformed_data.get('id', 0),
                name=transformed_data['name'],
                email=transformed_data['email'],
                created_at=datetime.now()
            )
            
            self._logger.info(f"用户数据处理成功: {user.name}")
            return ProcessResult(
                success=True,
                message="数据处理成功",
                data=user
            )
            
        except Exception as e:
            self._logger.error(f"数据处理失败: {str(e)}", e)
            return ProcessResult(
                success=False,
                message=f"处理异常: {str(e)}",
                error_code="PROCESSING_ERROR"
            )
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """转换用户数据格式"""
        transformed = {
            'name': data['name'].strip().title(),
            'email': data['email'].strip().lower()
        }
        
        if 'id' in data:
            transformed['id'] = int(data['id'])
        
        self._logger.debug(f"数据转换完成: {transformed}")
        return transformed


class MemoryStorage:
    """内存存储实现
    
    实现 IStorage 接口，使用内存存储用户数据。
    """
    
    def __init__(self, logger: ILogger):
        self._users: Dict[int, User] = {}
        self._next_id = 1
        self._logger = logger
    
    def save(self, user: User) -> bool:
        """保存用户到内存"""
        try:
            if user.id == 0:
                user.id = self._next_id
                self._next_id += 1
            
            self._users[user.id] = user
            self._logger.info(f"用户保存成功: ID={user.id}, Name={user.name}")
            return True
            
        except Exception as e:
            self._logger.error(f"用户保存失败: {str(e)}", e)
            return False
    
    def find_by_id(self, user_id: int) -> Optional[User]:
        """根据ID查找用户"""
        user = self._users.get(user_id)
        if user:
            self._logger.debug(f"找到用户: ID={user_id}")
        else:
            self._logger.debug(f"用户不存在: ID={user_id}")
        return user
    
    def find_all(self) -> List[User]:
        """查找所有用户"""
        users = list(self._users.values())
        self._logger.debug(f"查找到 {len(users)} 个用户")
        return users
    
    def delete(self, user_id: int) -> bool:
        """删除用户"""
        try:
            if user_id in self._users:
                del self._users[user_id]
                self._logger.info(f"用户删除成功: ID={user_id}")
                return True
            else:
                self._logger.error(f"用户不存在，无法删除: ID={user_id}")
                return False
                
        except Exception as e:
            self._logger.error(f"用户删除失败: {str(e)}", e)
            return False


class EmailNotificationService:
    """邮件通知服务实现
    
    实现 INotificationService 接口，模拟邮件通知功能。
    """
    
    def __init__(self, logger: ILogger):
        self._logger = logger
    
    def send_notification(self, recipient: str, message: str) -> bool:
        """发送邮件通知"""
        try:
            # 模拟发送邮件
            self._logger.info(f"邮件发送成功 -> {recipient}: {message}")
            return True
            
        except Exception as e:
            self._logger.error(f"邮件发送失败: {str(e)}", e)
            return False
    
    def send_batch_notifications(self, notifications: List[Dict[str, str]]) -> List[bool]:
        """批量发送邮件通知"""
        results = []
        for notification in notifications:
            recipient = notification.get('recipient', '')
            message = notification.get('message', '')
            result = self.send_notification(recipient, message)
            results.append(result)
        
        success_count = sum(results)
        self._logger.info(f"批量邮件发送完成: {success_count}/{len(notifications)} 成功")
        return results


class ConsoleLogger:
    """控制台日志记录器实现
    
    实现 ILogger 接口，将日志输出到控制台。
    """
    
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.DEBUG)
        
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
    
    def info(self, message: str) -> None:
        """记录信息日志"""
        self._logger.info(message)
    
    def error(self, message: str, error: Optional[Exception] = None) -> None:
        """记录错误日志"""
        if error:
            self._logger.error(f"{message} - {str(error)}")
        else:
            self._logger.error(message)
    
    def debug(self, message: str) -> None:
        """记录调试日志"""
        self._logger.debug(message)


# ============================================================================
# 业务服务类 - 演示依赖注入
# ============================================================================

class UserService:
    """用户服务类
    
    演示如何通过构造函数注入依赖，实现面向接口编程。
    所有依赖都通过 Protocol 接口定义，而不是具体实现类。
    """
    
    def __init__(
        self,
        processor: IDataProcessor,
        storage: IStorage,
        notification_service: INotificationService,
        logger: ILogger
    ):
        """通过构造函数注入所有依赖"""
        self._processor = processor
        self._storage = storage
        self._notification_service = notification_service
        self._logger = logger
    
    def create_user(self, user_data: Dict[str, Any]) -> ProcessResult:
        """创建新用户"""
        self._logger.info(f"开始创建用户: {user_data.get('name', 'Unknown')}")
        
        # 使用处理器处理数据
        process_result = self._processor.process(user_data)
        if not process_result.success:
            return process_result
        
        user = process_result.data
        if not isinstance(user, User):
            return ProcessResult(
                success=False,
                message="数据处理返回无效用户对象",
                error_code="INVALID_USER_DATA"
            )
        
        # 保存到存储
        if not self._storage.save(user):
            return ProcessResult(
                success=False,
                message="用户保存失败",
                error_code="STORAGE_ERROR"
            )
        
        # 发送通知
        notification_sent = self._notification_service.send_notification(
            user.email,
            f"欢迎 {user.name}！您的账户已成功创建。"
        )
        
        if not notification_sent:
            self._logger.error(f"用户创建成功但通知发送失败: {user.email}")
        
        self._logger.info(f"用户创建完成: ID={user.id}, Name={user.name}")
        return ProcessResult(
            success=True,
            message="用户创建成功",
            data=user
        )
    
    def get_user(self, user_id: int) -> Optional[User]:
        """获取用户信息"""
        self._logger.debug(f"查询用户: ID={user_id}")
        return self._storage.find_by_id(user_id)
    
    def list_users(self) -> List[User]:
        """列出所有用户"""
        self._logger.debug("查询所有用户")
        return self._storage.find_all()
    
    def delete_user(self, user_id: int) -> bool:
        """删除用户"""
        self._logger.info(f"删除用户: ID={user_id}")
        
        # 先查找用户
        user = self._storage.find_by_id(user_id)
        if not user:
            self._logger.error(f"用户不存在: ID={user_id}")
            return False
        
        # 删除用户
        if self._storage.delete(user_id):
            # 发送删除通知
            self._notification_service.send_notification(
                user.email,
                f"您好 {user.name}，您的账户已被删除。"
            )
            return True
        
        return False


# ============================================================================
# 工厂类 - 演示依赖创建和组装
# ============================================================================

class ServiceFactory:
    """服务工厂类
    
    负责创建和组装各种服务实例，封装复杂的依赖关系。
    """
    
    @staticmethod
    def create_logger() -> ILogger:
        """创建日志记录器"""
        return ConsoleLogger()
    
    @staticmethod
    def create_data_processor(logger: ILogger) -> IDataProcessor:
        """创建数据处理器"""
        return UserDataProcessor(logger)
    
    @staticmethod
    def create_storage(logger: ILogger) -> IStorage:
        """创建存储服务"""
        return MemoryStorage(logger)
    
    @staticmethod
    def create_notification_service(logger: ILogger) -> INotificationService:
        """创建通知服务"""
        return EmailNotificationService(logger)
    
    @staticmethod
    def create_user_service() -> UserService:
        """创建完整的用户服务
        
        这个方法演示了如何组装所有依赖，创建一个完整可用的服务实例。
        """
        # 创建基础依赖
        logger = ServiceFactory.create_logger()
        
        # 创建业务依赖
        processor = ServiceFactory.create_data_processor(logger)
        storage = ServiceFactory.create_storage(logger)
        notification_service = ServiceFactory.create_notification_service(logger)
        
        # 组装用户服务
        return UserService(
            processor=processor,
            storage=storage,
            notification_service=notification_service,
            logger=logger
        )


# ============================================================================
# 演示程序主函数
# ============================================================================

def main():
    """主演示函数"""
    print("=" * 60)
    print("Protocol 接口编程演示程序")
    print("=" * 60)
    
    # 使用工厂创建服务
    user_service = ServiceFactory.create_user_service()
    
    # 演示数据
    test_users = [
        {'name': 'alice wang', 'email': 'alice@example.com'},
        {'name': 'bob chen', 'email': 'bob@example.com'},
        {'name': 'charlie li', 'email': 'charlie@invalid'},  # 无效邮箱
        {'name': '', 'email': 'empty@example.com'},  # 空名称
    ]
    
    print("\n1. 创建用户演示:")
    print("-" * 30)
    
    created_users = []
    for i, user_data in enumerate(test_users, 1):
        print(f"\n创建用户 {i}: {user_data}")
        result = user_service.create_user(user_data)
        
        if result.success:
            print(f"✓ 成功: {result.message}")
            created_users.append(result.data)
        else:
            print(f"✗ 失败: {result.message} (错误码: {result.error_code})")
    
    print("\n2. 查询用户演示:")
    print("-" * 30)
    
    # 查询单个用户
    if created_users:
        user_id = created_users[0].id
        user = user_service.get_user(user_id)
        if user:
            print(f"\n查询用户 ID={user_id}:")
            print(f"  姓名: {user.name}")
            print(f"  邮箱: {user.email}")
            print(f"  创建时间: {user.created_at}")
    
    # 查询所有用户
    all_users = user_service.list_users()
    print(f"\n所有用户列表 (共 {len(all_users)} 个):")
    for user in all_users:
        print(f"  ID={user.id}: {user.name} ({user.email})")
    
    print("\n3. 删除用户演示:")
    print("-" * 30)
    
    if created_users:
        user_to_delete = created_users[0]
        print(f"\n删除用户: {user_to_delete.name}")
        
        if user_service.delete_user(user_to_delete.id):
            print("✓ 用户删除成功")
        else:
            print("✗ 用户删除失败")
        
        # 验证删除结果
        remaining_users = user_service.list_users()
        print(f"\n删除后剩余用户: {len(remaining_users)} 个")
    
    print("\n=" * 60)
    print("演示完成！")
    print("\n本程序演示了以下 Protocol 编程最佳实践:")
    print("1. 使用 Protocol 定义清晰的接口契约")
    print("2. 通过构造函数注入依赖，避免直接引用具体类")
    print("3. 使用工厂模式封装复杂的对象创建过程")
    print("4. 面向接口编程，提高代码的可测试性和可维护性")
    print("=" * 60)


if __name__ == '__main__':
    main()