# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
#
# from __future__ import annotations  # 兼容前向引用的类型注解
#
# import os  # 标准库：环境变量与开关控制
# import sys  # 标准库：平台判断与路径
# import logging  # 标准库：日志记录
# from typing import Optional  # 标准库：类型注解（可选）
#
# try:
#     from service.linux.bridge.windows_bridge_client import WindowsBridgeClient  # 远程 Windows 微服务客户端
# except Exception:
#     WindowsBridgeClient = None  # type: ignore
#
# logger = logging.getLogger(__name__)
#
#
# def _env_true(v: str | None, default: bool = False) -> bool:
#     if v is None:
#         return default
#     return str(v).strip().lower() in ("1", "true", "yes", "y", "on", "是")
#
#
# class ToolInvoker:
#     """统一工具调用层。
#
#     职责：
#       - 优先远程调用 WindowsBridge（受 USE_WINDOWS_BRIDGE/ DISABLE_WINDOWS_BRIDGE 控制）
#       - 在非严格模式下按需回退到本地实现（Windows COM 或 striprtf）
#
#     参数：
#       - prefer_remote: 显式偏好远程（覆盖环境变量）
#       - require_remote: 严格模式，仅允许远程，失败即抛错
#     """
#
#     def __init__(self,
#                  prefer_remote: Optional[bool] = None,
#                  require_remote: Optional[bool] = None) -> None:
#         # 默认：全局启用远程；可通过 DISABLE_WINDOWS_BRIDGE 关闭
#         disabled = _env_true(os.getenv('DISABLE_WINDOWS_BRIDGE'), default=False)
#         use_global = _env_true(os.getenv('USE_WINDOWS_BRIDGE'), default=True)
#         self.remote_enabled = (not disabled) and use_global
#
#         if prefer_remote is not None:
#             self.remote_enabled = bool(prefer_remote)
#
#         # 某些严格场景必须远程
#         self.require_remote = bool(require_remote) if require_remote is not None else False
#
#         self._bridge: Optional[WindowsBridgeClient] = None
#         if self.remote_enabled and WindowsBridgeClient is not None:
#             try:
#                 self._bridge = WindowsBridgeClient()
#                 if not self._bridge.is_configured():
#                     logger.warning("WindowsBridge 未配置，降级")
#                     self._bridge = None
#             except Exception as e:
#                 logger.warning(f"初始化 WindowsBridgeClient 失败: {e}")
#                 self._bridge = None
#         logger.info(f"ToolInvoker init | remote_enabled={self.remote_enabled} | require_remote={self.require_remote} | bridge={self._bridge is not None}")
#
#     # -------- RTF -> TXT --------
#     def rtf_to_txt_strict_from_path(self, input_file: str) -> str:
#         """
#         严格：仅允许远程 WindowsBridge 成功，否则抛错。
#         返回文本（UTF-16/UTF-8 自动探测读取）。
#         """
#         logger.info(f"rtf_to_txt_strict_from_path start | input={input_file}")
#         if self._bridge is None:
#             raise RuntimeError("WindowsBridge 未启用或未配置，严格模式下禁止本地回退")
#         txt_bytes = self._bridge.rtf_to_txt_from_path(input_file)
#         if not txt_bytes:
#             raise RuntimeError("WindowsBridge rtf_to_txt 调用失败")
#         # WindowsBridge 返回 UTF-16 文本
#         try:
#             out = txt_bytes.decode('utf-16', errors='ignore')
#             logger.info(f"rtf_to_txt_strict_from_path success | len={len(out)}")
#             return out
#         except Exception:
#             out = txt_bytes.decode('utf-8', errors='ignore')
#             logger.info(f"rtf_to_txt_strict_from_path success(utf-8) | len={len(out)}")
#             return out
#
#     def rtf_to_txt_from_path(self, input_file: str) -> str:
#         """
#         非严格：优先 WindowsBridge；必要时本地回退（Windows COM 或 striprtf）。
#         """
#         logger.info(f"rtf_to_txt_from_path start | input={input_file}")
#         if self._bridge is not None:
#             txt_bytes = self._bridge.rtf_to_txt_from_path(input_file)
#             if txt_bytes:
#                 try:
#                     out = txt_bytes.decode('utf-16', errors='ignore')
#                     logger.info(f"rtf_to_txt_from_path remote success | len={len(out)}")
#                     return out
#                 except Exception:
#                     out = txt_bytes.decode('utf-8', errors='ignore')
#                     logger.info(f"rtf_to_txt_from_path remote success(utf-8) | len={len(out)}")
#                     return out
#
#         # 回退：Windows COM（仅 win32 平台）
#         if sys.platform == 'win32':
#             try:
#                 from service.windows.preprocessing.preprocessing_function.rtf.native_rtf_parser import rtf_to_txt_with_word  # type: ignore
#                 out_path = rtf_to_txt_with_word(input_file, replace_if_exists=True, visible=False)
#                 with open(out_path, 'r', encoding='utf-16', errors='ignore') as f:
#                     out = f.read()
#                     logger.info(f"rtf_to_txt_from_path COM fallback success | len={len(out)}")
#                     return out
#             except Exception:
#                 pass
#
#         # 最后回退：striprtf 解析（结构损失，但可读）
#         try:
#             from striprtf.striprtf import rtf_to_text  # type: ignore
#             with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
#                 out = rtf_to_text(f.read())
#                 logger.info(f"rtf_to_txt_from_path striprtf fallback success | len={len(out)}")
#                 return out
#         except Exception as e:
#             raise RuntimeError(f"本地回退解析失败: {e}")
#
#     # -------- RTF 头部分节符 --------
#     def insert_rtf_head_section_break_from_path(self, input_file: str) -> bytes:
#         """为 RTF 文首插入“下一页分节符”。
#
#         仅在远程服务启用且配置正确时可用；失败抛出异常。
#
#         Args:
#             input_file: 本地 RTF 文件路径。
#
#         Returns:
#             处理后的 RTF 字节。
#         """
#         logger.info(f"insert_rtf_head_section_break_from_path start | input={input_file}")
#         if self._bridge is None:
#             raise RuntimeError("WindowsBridge 未启用或未配置，无法插入分节符")
#         out_bytes = self._bridge.insert_rtf_head_section_break_from_path(input_file)
#         if not out_bytes:
#             raise RuntimeError("WindowsBridge 分节符插入失败")
#         logger.info(f"insert_rtf_head_section_break_from_path success | bytes={len(out_bytes)}")
#         return out_bytes
#
#     def insert_rtf_head_break_and_clear_from_path(self, input_file: str) -> bytes:
#         """组合操作：插入文首分节符 + 清理首行文本。
#
#         优先使用服务端组合接口；若不可用则退回为两次调用。
#
#         Args:
#             input_file: 本地 RTF 文件路径。
#
#         Returns:
#             处理后的 RTF 字节。
#         """
#         logger.info(f"insert_rtf_head_break_and_clear_from_path start | input={input_file}")
#         if self._bridge is None:
#             raise RuntimeError("WindowsBridge 未启用或未配置，无法执行分节符+清理首行")
#         # 组合操作：优先使用服务端组合接口；若不可用则退回分别调用
#         try:
#             if hasattr(self._bridge, 'insert_rtf_head_break_and_clear_from_path'):
#                 out_bytes = self._bridge.insert_rtf_head_break_and_clear_from_path(input_file)  # type: ignore
#                 if out_bytes:
#                     logger.info(f"insert_rtf_head_break_and_clear_from_path success | bytes={len(out_bytes)}")
#                     return out_bytes
#         except Exception:
#             pass
#         # 分别调用
#         inserted = self._bridge.insert_rtf_head_section_break_from_path(input_file)
#         if not inserted:
#             raise RuntimeError("WindowsBridge 分节符插入失败")
#         cleared = self._bridge.clear_first_line(inserted, filename=os.path.basename(input_file))  # type: ignore
#         out = cleared or inserted
#         logger.info(f"insert_rtf_head_break_and_clear_from_path success(fallback) | bytes={len(out)}")
#         return out
#
#     # -------- 清理首行 --------
#     def clear_first_line_from_path(self, input_file: str) -> bytes:
#         """清理首行文本（去除可能的前导噪声/水印残留）。
#
#         Args:
#             input_file: 本地文件路径（通常 .docx/.rtf）。
#
#         Returns:
#             处理后的字节内容。
#         """
#         logger.info(f"clear_first_line_from_path start | input={input_file}")
#         if self._bridge is None:
#             raise RuntimeError("WindowsBridge 未启用或未配置，无法清理首行")
#         out_bytes = self._bridge.clear_first_line_from_path(input_file)  # type: ignore
#         if not out_bytes:
#             raise RuntimeError("WindowsBridge 清理首行失败")
#         logger.info(f"clear_first_line_from_path success | bytes={len(out_bytes)}")
#         return out_bytes
#
#
