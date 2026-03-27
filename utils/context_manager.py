"""
上下文管理器

功能说明：
- 统一管理运行时上下文变量
- 避免分散的环境变量访问
- 提供类型安全的上下文访问接口
- **线程安全**：使用 threading.local 确保多线程环境下上下文隔离

使用示例：
    from utils.context_manager import (
        set_current_output_dir,
        get_current_output_dir,
        set_project_context
    )

    # 设置上下文
    set_current_output_dir("AAA/output/xxx")
    set_project_context(
        project_desc="临床研究",
        combination_id="combo_123"
    )

    # 获取上下文
    output_dir = get_current_output_dir()
    project_desc = get_project_desc()
"""

# ========== 标准库导入 ==========
import os
import threading
from typing import Optional


# ============================================================
# 线程本地存储 - 确保多线程环境下上下文隔离
# ============================================================

_thread_local = threading.local()


def _get_thread_local_attr(name: str, default=None):
    """安全获取线程本地属性"""
    return getattr(_thread_local, name, default)


def _set_thread_local_attr(name: str, value):
    """设置线程本地属性"""
    setattr(_thread_local, name, value)


# ============================================================
# Session ID 管理（用于日志过滤）
# ============================================================

def set_session_id(session_id: str):
    """
    设置当前会话ID（线程本地存储 + 环境变量）

    Args:
        session_id: 会话唯一标识
    
    注意：
        - 用于日志过滤，区分不同会话的日志
        - 子线程需要显式调用此函数继承父线程的 session_id
    """
    _set_thread_local_attr("session_id", str(session_id))
    os.environ["CURRENT_SESSION_ID"] = str(session_id)


def get_session_id(default: str = "") -> str:
    """
    获取当前会话ID
    
    Args:
        default: 默认值
    
    Returns:
        当前会话ID
    """
    # 优先使用线程本地存储
    thread_sid = _get_thread_local_attr("session_id")
    if thread_sid:
        return thread_sid
    # 回退到环境变量
    return os.getenv("CURRENT_SESSION_ID", default)


def clear_session_id():
    """清除当前线程的会话ID"""
    if hasattr(_thread_local, "session_id"):
        delattr(_thread_local, "session_id")


# ============================================================
# 输出目录管理（线程安全版本）
# ============================================================

def set_current_output_dir(output_dir: str):
    """
    设置当前输出目录（同时设置线程本地存储和环境变量）
    
    Args:
        output_dir: 输出目录路径
    
    注意：
        - 线程本地存储确保多线程环境下每个线程有独立的值
        - 环境变量保持向后兼容性
    """
    output_dir_str = str(output_dir)
    # 设置线程本地存储（优先）
    _set_thread_local_attr("output_dir", output_dir_str)
    # 同时设置环境变量（向后兼容）
    os.environ["CURRENT_OUTPUT_DIR"] = output_dir_str


def get_current_output_dir(default: str = "AAA/output") -> str:
    """
    获取当前输出目录（优先使用线程本地存储）

    Args:
        default: 默认目录

    Returns:
        当前输出目录路径

    注意：
        优先级：线程本地存储 > 环境变量 > 默认值
    """
    # 优先使用线程本地存储
    thread_dir = _get_thread_local_attr("output_dir")
    if thread_dir:
        return thread_dir
    # 回退到环境变量
    return os.getenv("CURRENT_OUTPUT_DIR", default)


def clear_thread_output_dir():
    """清除当前线程的输出目录（不影响其他线程）"""
    if hasattr(_thread_local, "output_dir"):
        delattr(_thread_local, "output_dir")


# ============================================================
# 段落 ID 管理（线程安全版本）- 新增
# ============================================================

def set_paragraph_id(paragraph_id: str):
    """
    设置当前段落 ID（线程本地存储 + 环境变量）

    Args:
        paragraph_id: 段落唯一标识

    注意：
        - 线程本地存储确保多线程环境下每个线程有独立的段落 ID
        - 环境变量保持向后兼容性
    """
    paragraph_id_str = str(paragraph_id)
    # 设置线程本地存储（优先）
    _set_thread_local_attr("paragraph_id", paragraph_id_str)
    # 同时设置环境变量（向后兼容）
    os.environ["CURRENT_PARAGRAPH_ID"] = paragraph_id_str


def get_paragraph_id(default: str = "unknown") -> str:
    """
    获取当前段落 ID（优先使用线程本地存储）

    Args:
        default: 默认值

    Returns:
        当前段落 ID

    注意：
        优先级：线程本地存储 > 环境变量 > 默认值
    """
    # 优先使用线程本地存储
    thread_pid = _get_thread_local_attr("paragraph_id")
    if thread_pid:
        return thread_pid
    # 回退到环境变量
    return os.getenv("CURRENT_PARAGRAPH_ID", default)


def clear_paragraph_id():
    """清除当前线程的段落 ID（不影响其他线程）"""
    if hasattr(_thread_local, "paragraph_id"):
        delattr(_thread_local, "paragraph_id")


# ============================================================
# 项目上下文管理
# ============================================================

def set_project_desc(project_desc: str):
    """
    设置项目描述

    Args:
        project_desc: 项目背景描述
    """
    os.environ["CURRENT_PROJECT_DESC"] = str(project_desc)


def get_project_desc(default: str = "") -> str:
    """
    获取项目描述

    Args:
        default: 默认值

    Returns:
        项目描述
    """
    return os.getenv("CURRENT_PROJECT_DESC", default)


def set_combination_id(combination_id: str):
    """
    设置组合ID
    
    Args:
        combination_id: 组合ID
    """
    os.environ["CURRENT_COMBINATION_ID"] = str(combination_id)


def get_combination_id(default: str = "") -> str:
    """
    获取组合ID
    
    Args:
        default: 默认值
    
    Returns:
        组合ID
    """
    return os.getenv("CURRENT_COMBINATION_ID", default)


def set_project_context(
    project_desc: Optional[str] = None,
    combination_id: Optional[str] = None,
    output_dir: Optional[str] = None,
    paragraph_id: Optional[str] = None
):
    """
    批量设置项目上下文
    
    Args:
        project_desc: 项目描述
        combination_id: 组合ID
        output_dir: 输出目录
        paragraph_id: 段落 ID
    """
    if project_desc is not None:
        set_project_desc(project_desc)
    if combination_id is not None:
        set_combination_id(combination_id)
    if output_dir is not None:
        set_current_output_dir(output_dir)
    if paragraph_id is not None:
        set_paragraph_id(paragraph_id)


def clear_project_context():
    """清除所有项目上下文（包括线程本地存储和环境变量）"""
    # 清除线程本地存储
    clear_thread_output_dir()
    clear_paragraph_id()
    # 清除环境变量
    os.environ.pop("CURRENT_OUTPUT_DIR", None)
    os.environ.pop("CURRENT_PROJECT_DESC", None)
    os.environ.pop("CURRENT_COMBINATION_ID", None)
    os.environ.pop("CURRENT_PARAGRAPH_ID", None)


# ============================================================
# 上下文快照（用于测试或临时保存）
# ============================================================

class ProjectContextSnapshot:
    """项目上下文快照（用于保存和恢复）"""
    
    def __init__(self):
        self.output_dir = get_current_output_dir()
        self.project_desc = get_project_desc()
        self.combination_id = get_combination_id()
        self.paragraph_id = get_paragraph_id()

    def restore(self):
        """恢复快照"""
        set_current_output_dir(self.output_dir)
        set_project_desc(self.project_desc)
        set_combination_id(self.combination_id)
        set_paragraph_id(self.paragraph_id)


def save_context() -> ProjectContextSnapshot:
    """保存当前上下文快照"""
    return ProjectContextSnapshot()


def restore_context(snapshot: ProjectContextSnapshot):
    """恢复上下文快照"""
    snapshot.restore()

