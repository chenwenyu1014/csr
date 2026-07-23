"""
任务管理器

功能说明：
- 管理异步生成任务的生命周期
- 跟踪任务状态和进度
- 提供任务查询和列表功能
- 支持任务结果存储

主要特性：
1. 单例模式：确保全局只有一个任务管理器实例
2. 线程安全：使用锁保护共享数据
3. 状态跟踪：记录任务的完整生命周期
4. 进度更新：支持实时更新任务进度

任务生命周期：
PENDING -> PREPROCESSING -> EXTRACTION -> GENERATION -> POSTPROCESSING -> COMPLETED
                                                              |
                                                              v
                                                           FAILED
"""

import uuid
import time
import threading
from typing import Dict, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class TaskStage(str, Enum):
    """
    任务阶段枚举
    
    定义任务在整个生命周期中可能处于的所有状态。
    状态转换顺序：
    PENDING -> PREPROCESSING -> EXTRACTION -> GENERATION -> POSTPROCESSING -> COMPLETED
    如果发生错误，状态会变为FAILED。
    """
    PENDING = "pending"              # 等待执行：任务已创建，等待开始
    PREPROCESSING = "preprocessing"  # 预处理中：正在预处理输入文件
    EXTRACTION = "extraction"        # 数据提取中：正在从文档中提取数据
    GENERATION = "generation"        # 内容生成中：正在使用LLM生成段落内容
    POSTPROCESSING = "postprocessing"  # 后处理中：正在对生成内容进行后处理
    COMPLETED = "completed"         # 已完成：任务成功完成
    FAILED = "failed"               # 失败：任务执行过程中发生错误


@dataclass
class TaskProgress:
    """
    任务进度信息
    
    记录任务的当前状态和进度详情，用于前端展示和回调通知。
    
    字段说明：
    - stage: 当前任务阶段
    - message: 进度描述信息（如"正在生成段落 3/10"）
    - progress: 总体进度百分比（0-100）
    - current_step: 当前步骤编号
    - total_steps: 总步骤数
    - detail: 详细信息（可选，用于存储额外的进度数据）
    - updated_at: 最后更新时间戳
    """
    stage: TaskStage = TaskStage.PENDING
    message: str = ""
    progress: int = 0  # 0-100，总体进度百分比
    current_step: int = 0  # 当前步骤编号（从1开始）
    total_steps: int = 0  # 总步骤数
    detail: Optional[Dict[str, Any]] = None  # 详细信息（可选）
    updated_at: float = field(default_factory=time.time)  # 最后更新时间戳
    
    def to_dict(self) -> dict:
        return {
            "stage": self.stage.value,
            "message": self.message,
            "progress": self.progress,
            "current_step": self.current_step,
            "total_steps": self.total_steps,
            "detail": self.detail,
            "updated_at": self.updated_at
        }


@dataclass
class TaskInfo:
    """任务信息"""
    task_id: str
    callback_url: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    progress: TaskProgress = field(default_factory=TaskProgress)
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    config: Optional[Dict[str, Any]] = None  # 保存任务配置
    
    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "callback_url": self.callback_url,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "progress": self.progress.to_dict(),
            "result": self.result,
            "error": self.error
        }


class TaskManager:
    """
    任务管理器（单例模式）
    管理所有异步生成任务的状态
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._tasks: Dict[str, TaskInfo] = {}
        self._task_lock = threading.Lock()
        logger.info("TaskManager 初始化完成")
    
    def create_task(self, callback_url: Optional[str] = None, config: Optional[dict] = None) -> str:
        """
        创建新任务
        
        Args:
            callback_url: 进度回调URL
            config: 任务配置
            
        Returns:
            task_id
        """
        task_id = f"task_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        
        task = TaskInfo(
            task_id=task_id,
            callback_url=callback_url,
            config=config
        )
        
        with self._task_lock:
            self._tasks[task_id] = task
        
        logger.info(f"创建任务: {task_id}, callback_url={callback_url}")
        return task_id
    
    def get_task(self, task_id: str) -> Optional[TaskInfo]:
        """获取任务信息"""
        with self._task_lock:
            return self._tasks.get(task_id)
    
    def update_progress(
        self,
        task_id: str,
        stage: TaskStage,
        message: str = "",
        progress: int = 0,
        current_step: int = 0,
        total_steps: int = 0,
        detail: Optional[dict] = None
    ):
        """
        更新任务进度
        
        Args:
            task_id: 任务ID
            stage: 当前阶段
            message: 进度消息
            progress: 进度百分比 (0-100)
            current_step: 当前步骤
            total_steps: 总步骤数
            detail: 额外详情
        """
        with self._task_lock:
            task = self._tasks.get(task_id)
            if not task:
                logger.warning(f"任务不存在: {task_id}")
                return
            
            task.progress = TaskProgress(
                stage=stage,
                message=message,
                progress=progress,
                current_step=current_step,
                total_steps=total_steps,
                detail=detail,
                updated_at=time.time()
            )
            
            if stage == TaskStage.PENDING and task.started_at is None:
                pass
            elif task.started_at is None:
                task.started_at = time.time()
        
        logger.info(f"任务进度更新: {task_id} -> {stage.value} ({progress}%) - {message}")
    
    def complete_task(self, task_id: str, result: dict):
        """标记任务完成"""
        with self._task_lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            
            task.progress.stage = TaskStage.COMPLETED
            task.progress.progress = 100
            task.progress.message = "生成完成"
            task.progress.updated_at = time.time()
            task.completed_at = time.time()
            task.result = result
        
        logger.info(f"任务完成: {task_id}")
    
    def fail_task(self, task_id: str, error: str):
        """标记任务失败"""
        with self._task_lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            
            task.progress.stage = TaskStage.FAILED
            task.progress.message = f"失败: {error}"
            task.progress.updated_at = time.time()
            task.completed_at = time.time()
            task.error = error
        
        logger.error(f"任务失败: {task_id} - {error}")
    
    def list_tasks(self, limit: int = 100) -> list:
        """列出最近的任务"""
        with self._task_lock:
            tasks = sorted(
                self._tasks.values(),
                key=lambda t: t.created_at,
                reverse=True
            )[:limit]
            return [t.to_dict() for t in tasks]
    
    # def cleanup_old_tasks(self, max_age_hours: int = 24):
    #     """清理旧任务"""
    #     cutoff = time.time() - (max_age_hours * 3600)
    #     with self._task_lock:
    #         old_tasks = [
    #             tid for tid, task in self._tasks.items()
    #             if task.created_at < cutoff
    #         ]
    #         for tid in old_tasks:
    #             del self._tasks[tid]
    #
    #     if old_tasks:
    #         logger.info(f"清理了 {len(old_tasks)} 个旧任务")


# 全局单例
task_manager = TaskManager()


def get_task_manager() -> TaskManager:
    """获取任务管理器单例"""
    return task_manager

