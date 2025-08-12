"""EngineV2 - 新一代下载引擎

严格遵循依赖注入原则的下载引擎，管理 ThreadPoolExecutor，
处理系统和业务任务，通过 IEventBus 接收消息。
"""

import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum

from .interfaces import IConfig, IEventBus, IEventListener, IProducer
from .models import DownloadTask, TaskType, Priority
from .utils import get_logger


class SystemTaskType(Enum):
    """系统任务类型"""

    SHUTDOWN = "shutdown"
    HEALTH_CHECK = "health_check"
    METRICS_REPORT = "metrics_report"
    CLEANUP = "cleanup"


@dataclass
class SystemTask:
    """系统任务"""

    task_type: SystemTaskType
    params: Dict[str, Any]
    callback: Optional[Callable[[Any], None]] = None


class EngineEvents:
    """引擎事件类型"""

    TASK_SUBMITTED = "engine.task.submitted"
    TASK_STARTED = "engine.task.started"
    TASK_COMPLETED = "engine.task.completed"
    TASK_FAILED = "engine.task.failed"
    ENGINE_STARTED = "engine.started"
    ENGINE_STOPPED = "engine.stopped"
    SYSTEM_TASK_EXECUTED = "engine.system_task.executed"


class EngineV2(IEventListener):
    """新一代下载引擎

    严格遵循依赖注入原则，管理 ThreadPoolExecutor，
    处理系统和业务任务，通过 IEventBus 接收消息。
    """

    def __init__(self, config: IConfig, event_bus: IEventBus):
        """初始化引擎

        Args:
            config: 配置接口
            event_bus: 事件总线
            producer: 生产者接口
        """
        self._config = config
        self._event_bus = event_bus
        self._logger = get_logger(__name__)

        # 线程池管理
        self._executor: Optional[ThreadPoolExecutor] = None
        self._system_executor: Optional[ThreadPoolExecutor] = None

        # 状态管理
        self._running = False
        self._shutdown_requested = False
        self._lock = threading.RLock()

        # 任务跟踪
        self._active_tasks: Dict[str, Future] = {}
        self._completed_tasks = 0
        self._failed_tasks = 0

        # 注册事件监听
        self._register_event_listeners()

    def _register_event_listeners(self) -> None:
        """注册事件监听器"""
        # 监听生产者事件
        self._event_bus.subscribe("producer.task.ready", self._on_task_ready)
        self._event_bus.subscribe("producer.batch.ready", self._on_batch_ready)

        # 监听系统事件
        self._event_bus.subscribe("system.shutdown", self._on_system_shutdown)
        self._event_bus.subscribe("system.health_check", self._on_health_check)

    def start(self) -> None:
        """启动引擎"""
        with self._lock:
            if self._running:
                self._logger.warning("引擎已经在运行中")
                return

            self._logger.info("启动 EngineV2")

            # 从配置读取线程池大小
            max_workers = self._config.get_downloader_config().max_workers

            # 创建线程池
            self._executor = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="engine-worker"
            )

            # 创建系统任务专用线程池（单线程）
            self._system_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="engine-system"
            )

            self._running = True
            self._shutdown_requested = False

            # 发布启动事件
            self._event_bus.publish(
                EngineEvents.ENGINE_STARTED,
                {"max_workers": max_workers, "timestamp": self._get_timestamp()},
            )

            self._logger.info(f"EngineV2 启动完成，工作线程数: {max_workers}")

    def stop(self) -> None:
        """停止引擎"""
        with self._lock:
            if not self._running:
                self._logger.warning("引擎未在运行")
                return

            self._logger.info("停止 EngineV2")
            self._shutdown_requested = True

            # 停止生产者
            self._producer.stop()

            # 等待当前任务完成
            self._wait_for_active_tasks()

            # 关闭线程池
            if self._executor:
                self._executor.shutdown(wait=True)
                self._executor = None

            if self._system_executor:
                self._system_executor.shutdown(wait=True)
                self._system_executor = None

            self._running = False

            # 发布停止事件
            self._event_bus.publish(
                EngineEvents.ENGINE_STOPPED,
                {
                    "completed_tasks": self._completed_tasks,
                    "failed_tasks": self._failed_tasks,
                    "timestamp": self._get_timestamp(),
                },
            )

            self._logger.info("EngineV2 停止完成")

    def submit_system_task(self, task: SystemTask) -> Future:
        """提交系统任务

        Args:
            task: 系统任务

        Returns:
            Future对象
        """
        if not self._running or not self._system_executor:
            raise RuntimeError("引擎未运行")

        self._logger.debug(f"提交系统任务: {task.task_type.value}")

        future = self._system_executor.submit(self._execute_system_task, task)
        return future

    def get_status(self) -> Dict[str, Any]:
        """获取引擎状态"""
        with self._lock:
            return {
                "running": self._running,
                "shutdown_requested": self._shutdown_requested,
                "active_tasks": len(self._active_tasks),
                "completed_tasks": self._completed_tasks,
                "failed_tasks": self._failed_tasks,
                "executor_alive": self._executor is not None
                and not self._executor._shutdown,
                "system_executor_alive": self._system_executor is not None
                and not self._system_executor._shutdown,
            }

    def on_event(self, event_type: str, data: Any) -> None:
        """事件监听器接口实现"""
        # 这个方法由具体的事件处理方法调用
        pass

    def _on_task_ready(self, data: Any) -> None:
        """处理任务就绪事件"""
        if not isinstance(data, dict) or "task" not in data:
            self._logger.warning(f"无效的任务数据: {data}")
            return

        task = data["task"]
        if not isinstance(task, DownloadTask):
            self._logger.warning(f"无效的任务类型: {type(task)}")
            return

        self._submit_business_task(task)

    def _on_batch_ready(self, data: Any) -> None:
        """处理批量任务就绪事件"""
        if not isinstance(data, dict) or "tasks" not in data:
            self._logger.warning(f"无效的批量任务数据: {data}")
            return

        tasks = data["tasks"]
        if not isinstance(tasks, list):
            self._logger.warning(f"无效的任务列表类型: {type(tasks)}")
            return

        for task in tasks:
            if isinstance(task, DownloadTask):
                self._submit_business_task(task)

    def _on_system_shutdown(self, data: Any) -> None:
        """处理系统关闭事件"""
        self._logger.info("收到系统关闭信号")
        shutdown_task = SystemTask(
            task_type=SystemTaskType.SHUTDOWN,
            params=data if isinstance(data, dict) else {},
        )
        self.submit_system_task(shutdown_task)

    def _on_health_check(self, data: Any) -> None:
        """处理健康检查事件"""
        health_task = SystemTask(
            task_type=SystemTaskType.HEALTH_CHECK,
            params=data if isinstance(data, dict) else {},
        )
        self.submit_system_task(health_task)

    def _submit_business_task(self, task: DownloadTask) -> None:
        """提交业务任务"""
        if not self._running or not self._executor:
            self._logger.warning("引擎未运行，忽略任务")
            return

        if self._shutdown_requested:
            self._logger.warning("引擎正在关闭，忽略新任务")
            return

        self._logger.debug(f"提交业务任务: {task.task_id} ({task.task_type.value})")

        # 发布任务提交事件
        self._event_bus.publish(
            EngineEvents.TASK_SUBMITTED,
            {
                "task_id": task.task_id,
                "task_type": task.task_type.value,
                "priority": task.priority.value,
            },
        )

        # 提交到线程池
        future = self._executor.submit(self._execute_business_task, task)

        with self._lock:
            self._active_tasks[task.task_id] = future

        # 添加完成回调
        future.add_done_callback(lambda f: self._on_task_done(task.task_id, f))

    def _execute_business_task(self, task: DownloadTask) -> None:
        """执行业务任务"""
        self._logger.debug(f"开始执行任务: {task.task_id}")

        # 发布任务开始事件
        self._event_bus.publish(
            EngineEvents.TASK_STARTED,
            {"task_id": task.task_id, "task_type": task.task_type.value},
        )

        try:
            # 通过生产者执行任务
            self._producer.submit_task(task)

            # 发布任务完成事件
            self._event_bus.publish(
                EngineEvents.TASK_COMPLETED,
                {"task_id": task.task_id, "task_type": task.task_type.value},
            )

            with self._lock:
                self._completed_tasks += 1

            self._logger.debug(f"任务执行成功: {task.task_id}")

        except Exception as e:
            self._logger.error(f"任务执行失败: {task.task_id}, 错误: {e}")

            # 发布任务失败事件
            self._event_bus.publish(
                EngineEvents.TASK_FAILED,
                {
                    "task_id": task.task_id,
                    "task_type": task.task_type.value,
                    "error": str(e),
                },
            )

            with self._lock:
                self._failed_tasks += 1

    def _execute_system_task(self, task: SystemTask) -> Any:
        """执行系统任务"""
        self._logger.debug(f"执行系统任务: {task.task_type.value}")

        try:
            result = None

            if task.task_type == SystemTaskType.SHUTDOWN:
                result = self._handle_shutdown_task(task)
            elif task.task_type == SystemTaskType.HEALTH_CHECK:
                result = self._handle_health_check_task(task)
            elif task.task_type == SystemTaskType.METRICS_REPORT:
                result = self._handle_metrics_report_task(task)
            elif task.task_type == SystemTaskType.CLEANUP:
                result = self._handle_cleanup_task(task)
            else:
                self._logger.warning(f"未知的系统任务类型: {task.task_type}")

            # 调用回调函数
            if task.callback:
                task.callback(result)

            # 发布系统任务执行事件
            self._event_bus.publish(
                EngineEvents.SYSTEM_TASK_EXECUTED,
                {"task_type": task.task_type.value, "result": result},
            )

            return result

        except Exception as e:
            self._logger.error(f"系统任务执行失败: {task.task_type.value}, 错误: {e}")
            raise

    def _handle_shutdown_task(self, task: SystemTask) -> Dict[str, Any]:
        """处理关闭任务"""
        self._logger.info("执行系统关闭任务")
        # 这里可以执行关闭前的清理工作
        self.stop()
        return {"status": "shutdown_initiated"}

    def _handle_health_check_task(self, task: SystemTask) -> Dict[str, Any]:
        """处理健康检查任务"""
        status = self.get_status()
        self._logger.debug(f"健康检查结果: {status}")
        return status

    def _handle_metrics_report_task(self, task: SystemTask) -> Dict[str, Any]:
        """处理指标报告任务"""
        metrics = {
            "completed_tasks": self._completed_tasks,
            "failed_tasks": self._failed_tasks,
            "active_tasks": len(self._active_tasks),
            "success_rate": self._completed_tasks
            / max(1, self._completed_tasks + self._failed_tasks),
        }
        self._logger.info(f"指标报告: {metrics}")
        return metrics

    def _handle_cleanup_task(self, task: SystemTask) -> Dict[str, Any]:
        """处理清理任务"""
        self._logger.info("执行清理任务")
        # 清理已完成的任务引用
        with self._lock:
            completed_futures = [
                task_id
                for task_id, future in self._active_tasks.items()
                if future.done()
            ]
            for task_id in completed_futures:
                del self._active_tasks[task_id]

        return {"cleaned_tasks": len(completed_futures)}

    def _on_task_done(self, task_id: str, future: Future) -> None:
        """任务完成回调"""
        with self._lock:
            if task_id in self._active_tasks:
                del self._active_tasks[task_id]

    def _wait_for_active_tasks(self, timeout: float = 30.0) -> None:
        """等待活跃任务完成"""
        if not self._active_tasks:
            return

        self._logger.info(f"等待 {len(self._active_tasks)} 个活跃任务完成")

        # 获取所有活跃任务的Future对象
        futures = list(self._active_tasks.values())

        # 等待所有任务完成
        for future in futures:
            try:
                future.result(timeout=timeout / len(futures))
            except Exception as e:
                self._logger.warning(f"等待任务完成时出错: {e}")

    def _get_timestamp(self) -> str:
        """获取当前时间戳"""
        from datetime import datetime

        return datetime.now().isoformat()
