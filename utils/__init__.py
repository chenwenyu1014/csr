"""
工具模块
========

提供项目中常用的工具函数和类。

主要组件：
- context_manager: 运行时上下文管理（含请求ID、会话ID、输出目录、段落ID等 contextvars 及跨线程传播）
- json_logging: JSON日志
- timing: 耗时记录工具
"""

# ========== 日志 ==========
from .json_logging import setup_json_logging
from .logging_config import setup_logging, get_service_logger

# ========== 任务日志 ==========
from .task_logger import (
    TaskLogger,
    LogEntry,
    ErrorEntry,
    set_task_logger,
    get_task_logger,
    clear_task_logger,
    task_debug,
    task_info,
    task_warning,
    task_error,
    task_exception,
)

# ========== 上下文管理（含请求ID、会话ID等 contextvars） ==========
from .context_manager import (
    request_id_ctx,
    get_request_id,
    set_current_output_dir,
    get_current_output_dir,
    set_project_desc,
    get_project_desc,
    set_combination_id,
    get_combination_id,
    set_project_context,
    clear_project_context,
    inherit_context,
)

# ========== 耗时记录工具 ==========
from .timing import (
    Timer,
    TimingLogger,
    TimingRecord,
    timed,
    timed_async,
    timing_logger,
    preprocessing_timer,
    generation_timer,
    model_timer,
    log_timing,
    get_global_summary,
    print_global_summary,
    clear_global_timing,
)


__all__ = [
    # 请求上下文
    "request_id_ctx",
    "get_request_id",

    # 日志
    "setup_json_logging",
    "setup_logging",
    "get_service_logger",
    
    # 任务日志
    "TaskLogger",
    "LogEntry",
    "ErrorEntry",
    "set_task_logger",
    "get_task_logger",
    "clear_task_logger",
    "task_debug",
    "task_info",
    "task_warning",
    "task_error",
    "task_exception",
    
    # 上下文管理
    "set_current_output_dir",
    "get_current_output_dir",
    "set_project_desc",
    "get_project_desc",
    "set_combination_id",
    "get_combination_id",
    "set_project_context",
    "clear_project_context",
    "inherit_context",
    
    # 耗时记录工具
    "Timer",
    "TimingLogger",
    "TimingRecord",
    "timed",
    "timed_async",
    "timing_logger",
    "preprocessing_timer",
    "generation_timer",
    "model_timer",
    "log_timing",
    "get_global_summary",
    "print_global_summary",
    "clear_global_timing",
]
