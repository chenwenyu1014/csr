"""
上下文管理器

功能说明：
- 统一管理运行时上下文变量
- 避免分散的环境变量访问
- 提供类型安全的上下文访问接口
- 并发隔离：使用 contextvars.ContextVar 确保多任务同进程并发时上下文互不串台

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

注意：
    contextvars 默认不随 ThreadPoolExecutor 传播，提交到线程池的 callable
    需用本模块 inherit_context 包裹，worker 才能读到提交方设置的值。
"""

# ========== 标准库导入 ==========
import contextvars
import functools
from typing import Optional

# ============================================================
# 上下文变量 - 基于 contextvars，并发任务隔离
# ============================================================

request_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("request_id", default=None)
_output_dir_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("current_output_dir", default=None)
_paragraph_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("current_paragraph_id", default=None)
_session_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("current_session_id", default=None)
_project_desc_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("current_project_desc", default=None)
_combination_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("current_combination_id",default=None)


# ============================================================
# 请求 ID 管理
# ============================================================

def get_request_id() -> Optional[str]:
    """获取当前请求ID"""
    return request_id_ctx.get()


# ============================================================
# Session ID 管理（用于日志过滤）
# ============================================================

def set_session_id(session_id: str):
    """
    设置当前会话ID（contextvars）

    Args:
        session_id: 会话唯一标识
    
    注意：
        - 用于日志过滤，区分不同会话的日志
        - 子线程需经本模块 inherit_context 继承提交方的 session_id
    """
    _session_id_ctx.set(str(session_id))


def get_session_id(default: str = "") -> str:
    """
    获取当前会话ID
    
    Args:
        default: 默认值
    
    Returns:
        当前会话ID
    """
    sid = _session_id_ctx.get()
    return sid if sid else default


def clear_session_id():
    """清除当前上下文的会话ID（不影响其他并发上下文）"""
    _session_id_ctx.set(None)


# ============================================================
# 输出目录管理
# ============================================================

def set_current_output_dir(output_dir: str):
    """
    设置当前输出目录（contextvars）
    
    Args:
        output_dir: 输出目录路径
    
    注意：
        - 每个并发任务在自身上下文副本里 set，互不影响
        - worker 线程经本模块 inherit_context 继承提交方的值
    """
    _output_dir_ctx.set(str(output_dir))


def get_current_output_dir(default: str = "AAA/output") -> str:
    """
    获取当前输出目录

    Args:
        default: 默认目录

    Returns:
        当前输出目录路径
    """
    output_dir = _output_dir_ctx.get()
    return output_dir if output_dir else default


def clear_thread_output_dir():
    """清除当前上下文的输出目录（不影响其他并发上下文）"""
    _output_dir_ctx.set(None)


# ============================================================
# 段落 ID 管理
# ============================================================

def set_paragraph_id(paragraph_id: str):
    """
    设置当前段落 ID（contextvars）

    Args:
        paragraph_id: 段落唯一标识

    注意：
        - 每个并发任务在自身上下文副本里 set，互不影响
        - worker 线程经本模块 inherit_context 继承提交方的值
    """
    _paragraph_id_ctx.set(str(paragraph_id))


def get_paragraph_id(default: str = "unknown") -> str:
    """
    获取当前段落 ID

    Args:
        default: 默认值

    Returns:
        当前段落 ID
    """
    paragraph_id = _paragraph_id_ctx.get()
    return paragraph_id if paragraph_id else default


def clear_paragraph_id():
    """清除当前上下文的段落 ID（不影响其他并发上下文）"""
    _paragraph_id_ctx.set(None)


# ============================================================
# 项目上下文管理
# ============================================================

def set_project_desc(project_desc: str):
    """
    设置项目描述

    Args:
        project_desc: 项目背景描述
    """
    _project_desc_ctx.set(str(project_desc))


def get_project_desc(default: str = "") -> str:
    """
    获取项目描述

    Args:
        default: 默认值

    Returns:
        项目描述
    """
    desc = _project_desc_ctx.get()
    return desc if desc else default


def set_combination_id(combination_id: str):
    """
    设置组合ID
    
    Args:
        combination_id: 组合ID
    """
    _combination_id_ctx.set(str(combination_id))


def get_combination_id(default: str = "") -> str:
    """
    获取组合ID
    
    Args:
        default: 默认值

    Returns:
        组合ID
    """
    cid = _combination_id_ctx.get()
    return cid if cid else default


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
    """清除所有项目上下文（仅当前上下文，不影响其他并发上下文）"""
    clear_thread_output_dir()
    clear_paragraph_id()
    _project_desc_ctx.set(None)
    _combination_id_ctx.set(None)


# ============================================================
# 上下文跨线程传播
# ============================================================

def inherit_context(fn):
    """
    包装一个 callable，使其在被 ThreadPoolExecutor 调用时继承提交方线程的上下文。
    
    在提交方线程调用 copy_context() 拍下当前 contextvars 快照，
    在 worker 内通过 ctx.run(fn, ...) 执行，从而让 worker 看到提交方的
    output_dir / paragraph_id / session_id / request_id / project_desc /
    combination_id 等上下文值。

    contextvars 默认不随 ThreadPoolExecutor 传播，提交到线程池的 callable
    需用本函数包裹，worker 才能读到提交方设置的值。

    Args:
        fn: 待提交到线程池的可调用对象

    Returns:
        包装后的可调用对象，签名与 fn 一致

    使用示例：
        from utils.context_manager import inherit_context
        executor.submit(inherit_context(_worker), i, item)
    """
    ctx = contextvars.copy_context()

    @functools.wraps(fn)
    def _wrapper(*args, **kwargs):
        return ctx.run(fn, *args, **kwargs)

    return _wrapper
