"""
工具模块
========

提供项目中常用的工具函数和类。

主要组件：
- context_manager: 运行时上下文管理
- path_utils: 路径处理工具
- event_bus: 事件总线
- request_context: 请求上下文
- json_logging: JSON日志
- timing: 耗时记录工具
"""

# ========== 事件总线 ==========
from .event_bus import event_bus, EventBus

# ========== 请求上下文 ==========
from .request_context import request_id_ctx, get_request_id

# ========== 日志 ==========
from .json_logging import setup_json_logging

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

# ========== 上下文管理 ==========
from .context_manager import (
    set_current_output_dir,
    get_current_output_dir,
    set_project_desc,
    get_project_desc,
    set_combination_id,
    get_combination_id,
    set_project_context,
    clear_project_context,
    save_context,
    restore_context,
)

# ========== 路径工具 ==========
from .path_utils import (
    normalize_path,
    normalize_aaa_path,
    to_absolute_path,
    ensure_dir_exists,
    ensure_parent_dir_exists,
    get_prompts_dir,
    get_session_dir,
    get_cache_dir,
    safe_file_name,
    get_unique_file_path,
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
    # 事件总线
    "event_bus",
    "EventBus",
    
    # 请求上下文
    "request_id_ctx",
    "get_request_id",
    
    # 日志
    "setup_json_logging",
    
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
    "save_context",
    "restore_context",
    
    # 路径工具
    "normalize_path",
    "normalize_aaa_path",
    "to_absolute_path",
    "ensure_dir_exists",
    "ensure_parent_dir_exists",
    "get_prompts_dir",
    "get_session_dir",
    "get_cache_dir",
    "safe_file_name",
    "get_unique_file_path",
    
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
