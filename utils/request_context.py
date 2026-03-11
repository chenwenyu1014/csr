"""
请求上下文
使用ContextVar管理请求级别的变量
"""

import contextvars
from typing import Optional


# 请求ID的上下文变量
request_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "request_id", default=None
)


def get_request_id() -> Optional[str]:
    """获取当前请求ID"""
    return request_id_ctx.get()
