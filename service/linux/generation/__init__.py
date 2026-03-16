"""
生成服务模块
============

提供CSR文档生成相关的核心服务功能。

主要组件：
- GenerationService: 文本生成服务（同步/异步）
- TaskManager: 任务管理器
- ProgressCallback: 进度回调服务
- CSRFlowController: 流程控制器
"""

from service.linux.generation.generation_service import GenerationService, get_generation_service
from service.linux.generation.task_manager import TaskManager, get_task_manager
from service.linux.generation.progress_callback import ProgressCallback, create_progress_callback

__all__ = [
    # 生成服务
    'GenerationService',
    'get_generation_service',
    # 任务管理
    'TaskManager',
    'get_task_manager',
    # 进度回调
    'ProgressCallback',
    'create_progress_callback',
]
