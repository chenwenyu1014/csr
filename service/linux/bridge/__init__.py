"""
Windows Bridge 客户端模块
========================

提供与 Windows Bridge 服务通信的 HTTP 客户端。

主要组件：
- WindowsBridgeClient: HTTP 客户端类，支持同步和异步调用

功能：
- RTF 文档处理
- Word 文档处理（标记、扫描、导出）
- 文档清理（Content Control、首行）
- 内容插入
- 文件预处理
"""

from service.linux.bridge.windows_bridge_client import WindowsBridgeClient

__all__ = [
    'WindowsBridgeClient',
]
