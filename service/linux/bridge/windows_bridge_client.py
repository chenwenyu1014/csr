#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Windows Bridge 客户端模块

功能说明：
- 在Linux主项目中通过HTTP调用Windows微服务（依赖Office/COM/Spire的能力）
- 提供同步和异步两种调用方式

环境变量：
- WINDOWS_BRIDGE_URL: 服务URL（可选，默认 http://192.168.3.70:8081）
- WINDOWS_BRIDGE_TOKEN: 认证Token（可选）
- WINDOWS_BRIDGE_TIMEOUT: 超时时间秒（可选，默认600）

主要类：
- WindowsBridgeClient: Windows Bridge HTTP客户端
"""

from __future__ import annotations

# ========== 标准库导入 ==========
import json
import logging
import os
from typing import Any, Dict, List, Optional

# ========== 第三方库导入 ==========
import requests

# 异步HTTP客户端（延迟导入）
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None  # type: ignore

# ========== 日志配置 ==========
logger = logging.getLogger(__name__)


class WindowsBridgeClient:
    """
    Windows Bridge HTTP客户端
    
    用于在Linux环境中调用Windows微服务。
    支持RTF处理、Word文档处理、内容插入等功能。
    """
    
    # ============================================================
    # 初始化与配置方法
    # ============================================================
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        timeout: Optional[int] = None
    ) -> None:
        """
        初始化客户端
        
        Args:
            base_url: 服务URL（优先级：参数 > 环境变量 > 默认值）
            token: 认证Token（优先级：参数 > 环境变量）
            timeout: 请求超时秒数（默认600）
        """
        # URL配置
        raw_url = (
            base_url
            or os.getenv("WINDOWS_BRIDGE_URL")
            or os.getenv("DEFAULT_WINDOWS_BRIDGE_URL")
            or "http://192.168.3.70:8081"
        )
        self.base_url = (raw_url or "").strip().rstrip("/")
        
        # Token配置
        self.token = token or os.getenv("WINDOWS_BRIDGE_TOKEN") or None
        
        # 超时配置
        try:
            self.timeout = int(timeout or os.getenv("WINDOWS_BRIDGE_TIMEOUT") or 600)
        except (ValueError, TypeError):
            self.timeout = 600
    
    def is_configured(self) -> bool:
        """检查客户端是否已配置"""
        return bool(self.base_url)
    
    # ============================================================
    # 请求头构建（内部方法）
    # ============================================================
    
    def _headers(self) -> Dict[str, str]:
        """构建同步请求头"""
        headers = {"Accept": "application/octet-stream"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        # 透传请求链路ID
        headers.update(self._get_request_id_header())
        return headers
    
    def _async_headers(self) -> Dict[str, str]:
        """构建异步请求头"""
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        headers.update(self._get_request_id_header())
        return headers
    
    def _get_request_id_header(self) -> Dict[str, str]:
        """获取请求链路ID头"""
        try:
            from utils import get_request_id
            rid = get_request_id()
            if rid and rid != "-":
                return {"X-Request-Id": rid}
        except Exception:
            pass
        return {}
    
    # ============================================================
    # 通用文件读取（内部方法）
    # ============================================================
    
    def _read_file(self, file_path: str) -> Optional[bytes]:
        """读取文件内容"""
        try:
            with open(file_path, 'rb') as f:
                return f.read()
        except Exception as e:
            logger.warning(f"读取文件失败 {file_path}: {e}")
            return None

    # # ============================================================
    # # RTF 处理方法
    # # ============================================================
    #
    # def insert_rtf_head_section_break(
    #     self,
    #     rtf_bytes: bytes,
    #     filename: str = "input.rtf"
    # ) -> Optional[bytes]:
    #     """
    #     在RTF文首插入下一页分节符
    #
    #     Returns:
    #         处理后的RTF字节，失败返回None
    #     """
    #     if not self.is_configured():
    #         return None
    #
    #     url = f"{self.base_url}/api/v1/rtf/insert_head_section_break"
    #     files = {"file": (filename, rtf_bytes, "application/rtf")}
    #
    #     try:
    #         resp = requests.post(url, files=files, timeout=self.timeout, headers=self._headers())
    #         if resp.status_code == 200 and resp.content:
    #             return resp.content
    #         logger.warning(f"insert_head_section_break 失败: {resp.status_code}")
    #         return None
    #     except Exception as e:
    #         logger.warning(f"insert_head_section_break 调用失败: {e}")
    #         return None
    #
    # def insert_rtf_head_section_break_from_path(self, file_path: str) -> Optional[bytes]:
    #     """从文件路径读取RTF并插入分节符"""
    #     data = self._read_file(file_path)
    #     if data is None:
    #         return None
    #     return self.insert_rtf_head_section_break(data, filename=os.path.basename(file_path))
    #
    # def rtf_to_txt(
    #     self,
    #     rtf_bytes: bytes,
    #     filename: str = "input.rtf"
    # ) -> Optional[bytes]:
    #     """
    #     RTF转TXT
    #
    #     Returns:
    #         TXT字节（通常UTF-16），失败返回None
    #     """
    #     if not self.is_configured():
    #         return None
    #
    #     url = f"{self.base_url}/api/v1/rtf/to_txt"
    #     files = {"file": (filename, rtf_bytes, "application/rtf")}
    #
    #     try:
    #         resp = requests.post(url, files=files, timeout=self.timeout, headers=self._headers())
    #         if resp.status_code == 200 and resp.content:
    #             return resp.content
    #         logger.warning(f"rtf_to_txt 失败: {resp.status_code}")
    #         return None
    #     except Exception as e:
    #         logger.warning(f"rtf_to_txt 调用失败: {e}")
    #         return None
    #
    # def rtf_to_txt_from_path(self, file_path: str) -> Optional[bytes]:
    #     """从文件路径读取RTF并转换为TXT"""
    #     data = self._read_file(file_path)
    #     if data is None:
    #         return None
    #     return self.rtf_to_txt(data, filename=os.path.basename(file_path))
    #
    # def insert_rtf_head_break_and_clear(
    #     self,
    #     file_bytes: bytes,
    #     filename: str = "input.rtf"
    # ) -> Optional[bytes]:
    #     """组合操作：插入分节符 + 清理首行"""
    #     try:
    #         inserted = self.insert_rtf_head_section_break(file_bytes, filename=filename)
    #         if not inserted:
    #             return None
    #         cleared = self.clear_first_line(inserted, filename=filename)
    #         return cleared or inserted
    #     except Exception as e:
    #         logger.warning(f"组合操作失败: {e}")
    #         return None
    #
    # def insert_rtf_head_break_and_clear_from_path(self, file_path: str) -> Optional[bytes]:
    #     """从文件路径执行组合操作"""
    #     data = self._read_file(file_path)
    #     if data is None:
    #         return None
    #     return self.insert_rtf_head_break_and_clear(data, filename=os.path.basename(file_path))
    #
    # # ============================================================
    # # Word 文档处理方法（同步）
    # # ============================================================
    #
    # def word_mark_tables_images(
    #     self,
    #     docx_bytes: bytes,
    #     filename: str = "input.docx"
    # ) -> Optional[bytes]:
    #     """
    #     标记Word文档中的表格与图片
    #
    #     Returns:
    #         标记后的DOCX字节
    #     """
    #     if not self.is_configured():
    #         return None
    #
    #     url = f"{self.base_url}/api/v1/word/mark_tables_images"
    #     mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    #     files = {"file": (filename, docx_bytes, mime)}
    #
    #     try:
    #         resp = requests.post(url, files=files, timeout=self.timeout, headers=self._headers())
    #         if resp.status_code == 200 and resp.content:
    #             return resp.content
    #         logger.warning(f"word_mark_tables_images 失败: {resp.status_code}")
    #         return None
    #     except Exception as e:
    #         logger.warning(f"word_mark_tables_images 调用失败: {e}")
    #         return None
    #
    # def word_mark_tables_images_from_path(self, file_path: str) -> Optional[bytes]:
    #     """从文件路径标记Word文档"""
    #     data = self._read_file(file_path)
    #     if data is None:
    #         return None
    #     return self.word_mark_tables_images(data, filename=os.path.basename(file_path))
    #
    # def word_scan_regions(
    #     self,
    #     docx_bytes: bytes,
    #     filename: str = "input.docx"
    # ) -> Optional[List[Dict[str, Any]]]:
    #     """
    #     扫描文档中已标记的表格/图片区间
    #
    #     Returns:
    #         区间列表JSON
    #     """
    #     if not self.is_configured():
    #         return None
    #
    #     url = f"{self.base_url}/api/v1/word/scan_regions"
    #     mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    #     files = {"file": (filename, docx_bytes, mime)}
    #
    #     try:
    #         resp = requests.post(url, files=files, timeout=self.timeout, headers=self._headers())
    #         if resp.status_code == 200:
    #             data = resp.json()
    #             if isinstance(data, list):
    #                 return data
    #         logger.warning(f"word_scan_regions 失败: {resp.status_code}")
    #         return None
    #     except Exception as e:
    #         logger.warning(f"word_scan_regions 调用失败: {e}")
    #         return None
    #
    # def word_scan_regions_from_path(self, file_path: str) -> Optional[List[Dict[str, Any]]]:
    #     """从文件路径扫描区间"""
    #     data = self._read_file(file_path)
    #     if data is None:
    #         return None
    #     return self.word_scan_regions(data, filename=os.path.basename(file_path))
    #
    # def word_export_regions(
    #     self,
    #     docx_bytes: bytes,
    #     regions: List[Dict[str, str]],
    #     filename: str = "input.docx"
    # ) -> Optional[bytes]:
    #     """
    #     根据指定区间导出内容
    #
    #     Args:
    #         regions: 形如 [{"start":"{{Table_1_Start}}","end":"{{Table_1_End}}"}]
    #
    #     Returns:
    #         ZIP字节
    #     """
    #     if not self.is_configured():
    #         return None
    #
    #     url = f"{self.base_url}/api/v1/word/export_regions"
    #     mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    #     files = {"file": (filename, docx_bytes, mime)}
    #     data = {"regions": json.dumps(regions, ensure_ascii=False)}
    #
    #     try:
    #         resp = requests.post(url, files=files, data=data, timeout=self.timeout, headers=self._headers())
    #         if resp.status_code == 200 and resp.content:
    #             return resp.content
    #         logger.warning(f"word_export_regions 失败: {resp.status_code}")
    #         return None
    #     except Exception as e:
    #         logger.warning(f"word_export_regions 调用失败: {e}")
    #         return None
    #
    # def word_export_regions_from_path(
    #     self,
    #     file_path: str,
    #     regions: List[Dict[str, str]]
    # ) -> Optional[bytes]:
    #     """从文件路径导出区间"""
    #     data = self._read_file(file_path)
    #     if data is None:
    #         return None
    #     return self.word_export_regions(data, regions, filename=os.path.basename(file_path))
    #
    # def word_export_all_objects(
    #     self,
    #     docx_bytes: bytes,
    #     filename: str = "input.docx"
    # ) -> Optional[bytes]:
    #     """
    #     导出文档中全部对象
    #
    #     Returns:
    #         ZIP字节
    #     """
    #     if not self.is_configured():
    #         return None
    #
    #     url = f"{self.base_url}/api/v1/word/export_all_objects"
    #     mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    #     files = {"file": (filename, docx_bytes, mime)}
    #
    #     try:
    #         resp = requests.post(url, files=files, timeout=self.timeout, headers=self._headers())
    #         if resp.status_code == 200 and resp.content:
    #             return resp.content
    #         logger.warning(f"word_export_all_objects 失败: {resp.status_code}")
    #         return None
    #     except Exception as e:
    #         logger.warning(f"word_export_all_objects 调用失败: {e}")
    #         return None
    #
    # def word_export_all_objects_from_path(self, file_path: str) -> Optional[bytes]:
    #     """从文件路径导出全部对象"""
    #     data = self._read_file(file_path)
    #     if data is None:
    #         return None
    #     return self.word_export_all_objects(data, filename=os.path.basename(file_path))
    #
    # # ============================================================
    # # 文档清理方法（同步）
    # # ============================================================
    #
    # def clear_first_line(
    #     self,
    #     file_bytes: bytes,
    #     filename: str = "input.docx"
    # ) -> Optional[bytes]:
    #     """清理首行（去除前导噪声）"""
    #     if not self.is_configured():
    #         return None
    #
    #     url = f"{self.base_url}/api/v1/word/clear_first_line"
    #     files = {"file": (filename, file_bytes, "application/octet-stream")}
    #
    #     try:
    #         resp = requests.post(url, files=files, timeout=self.timeout, headers=self._headers())
    #         if resp.status_code == 200 and resp.content:
    #             return resp.content
    #         logger.warning(f"clear_first_line 失败: {resp.status_code}")
    #         return None
    #     except Exception as e:
    #         logger.warning(f"clear_first_line 调用失败: {e}")
    #         return None
    #
    # def clear_first_line_from_path(self, file_path: str) -> Optional[bytes]:
    #     """从文件路径清理首行"""
    #     data = self._read_file(file_path)
    #     if data is None:
    #         return None
    #     return self.clear_first_line(data, filename=os.path.basename(file_path))
    #
    # def clean_document(
    #     self,
    #     file_path: str,
    #     output_path: Optional[str] = None,
    #     remove_first_line: bool = True,
    #     remove_content_controls: bool = True
    # ) -> Optional[Dict[str, Any]]:
    #     """
    #     清理Word文档
    #
    #     功能：
    #     1. 清理Content Control控件
    #     2. 删除首行（水印）
    #
    #     Args:
    #         file_path: 文件路径（相对于AAA目录）
    #         output_path: 输出路径（可选）
    #         remove_first_line: 是否删除首行
    #         remove_content_controls: 是否清理控件
    #
    #     Returns:
    #         结果字典
    #     """
    #     if not self.is_configured():
    #         logger.warning("WindowsBridge未配置")
    #         return None
    #
    #     url = f"{self.base_url}/api/v1/document/clean"
    #     data = {
    #         "file_path": file_path,
    #         "remove_first_line": str(remove_first_line).lower(),
    #         "remove_content_controls": str(remove_content_controls).lower(),
    #     }
    #     if output_path:
    #         data["output_path"] = output_path
    #
    #     try:
    #         logger.info(f"清理文档: {file_path}")
    #         resp = requests.post(url, data=data, timeout=self.timeout, headers=self._headers())
    #
    #         if resp.status_code == 200:
    #             result = resp.json()
    #             logger.info(f"文档清理成功")
    #             return result
    #         else:
    #             error_text = resp.text[:200] if resp.text else ""
    #             logger.warning(f"clean_document 失败: {resp.status_code} {error_text}")
    #             return {"success": False, "error": f"HTTP {resp.status_code}: {error_text}"}
    #     except Exception as e:
    #         logger.error(f"clean_document 调用失败: {e}")
    #         return {"success": False, "error": str(e)}
    #
    # # ============================================================
    # # 内容插入方法（同步）
    # # ============================================================
    
    def insert_content(
        self,
        template_file: str,
        data_json: str
    ) -> Optional[Dict[str, Any]]:
        """
        插入内容到模板
        
        Args:
            template_file: 模板文件路径（相对于AAA目录）
            data_json: JSON数据字符串
        
        Returns:
            结果字典
        """
        if not self.is_configured():
            logger.warning("WindowsBridge未配置")
            return None
        
        url = f"{self.base_url}/ky/sys/ai/insert_direct"
        data = {
            "template_file": template_file,
            "data_json": data_json,
        }
        
        try:
            logger.info(f"[同步] 插入内容: {template_file}")
            resp = requests.post(url, data=data, timeout=self.timeout, headers=self._headers())
            
            if resp.status_code == 200:
                result = resp.json()
                logger.info(f"[同步] 内容插入成功")
                return result
            else:
                error_text = resp.text[:200] if resp.text else ""
                logger.warning(f"[同步] insert_content 失败: {resp.status_code} {error_text}")
                return {"success": False, "error": f"HTTP {resp.status_code}: {error_text}"}
        except Exception as e:
            logger.error(f"[同步] insert_content 调用失败: {e}")
            return {"success": False, "error": str(e)}
    
    # ============================================================
    # 异步方法
    # ============================================================
    
    async def _get_aiohttp_session(self) -> "aiohttp.ClientSession":
        """获取aiohttp session"""
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp未安装，请运行: pip install aiohttp")
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        return aiohttp.ClientSession(timeout=timeout)
    
    async def clean_document_async(
        self,
        file_path: str,
        output_path: Optional[str] = None,
        remove_first_line: bool = True,
        remove_content_controls: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        异步清理Word文档
        
        Args:
            file_path: 文件路径
            output_path: 输出路径（可选）
            remove_first_line: 是否删除首行
            remove_content_controls: 是否清理控件
        """
        if not self.is_configured():
            logger.warning("WindowsBridge未配置")
            return None
        
        url = f"{self.base_url}/api/v1/document/clean"
        data = {
            "file_path": file_path,
            "remove_first_line": str(remove_first_line).lower(),
            "remove_content_controls": str(remove_content_controls).lower(),
        }
        if output_path:
            data["output_path"] = output_path
        
        try:
            logger.info(f"[异步] 清理文档: {file_path}")
            async with await self._get_aiohttp_session() as session:
                async with session.post(url, data=data, headers=self._async_headers()) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        logger.info(f"[异步] 文档清理成功")
                        return result
                    else:
                        text = await resp.text()
                        logger.warning(f"[异步] clean_document 失败: {resp.status} {text[:200]}")
                        return {"success": False, "error": f"HTTP {resp.status}: {text[:200]}"}
        except Exception as e:
            logger.error(f"[异步] clean_document 调用失败: {e}")
            return {"success": False, "error": str(e)}
    
    async def insert_content_async(
        self,
        template_file: str,
        data_json: str
    ) -> Optional[Dict[str, Any]]:
        """
        异步插入内容到模板
        
        Args:
            template_file: 模板文件路径
            data_json: JSON数据字符串
        """
        if not self.is_configured():
            logger.warning("WindowsBridge未配置")
            return None
        
        url = f"{self.base_url}/ky/sys/ai/insert_direct"
        data = {
            "template_file": template_file,
            "data_json": data_json,
        }
        
        try:
            logger.info(f"[异步] 插入内容: {template_file}")
            async with await self._get_aiohttp_session() as session:
                async with session.post(url, data=data, headers=self._async_headers()) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        logger.info(f"[异步] 内容插入成功")
                        return result
                    else:
                        text = await resp.text()
                        logger.warning(f"[异步] insert_content 失败: {resp.status} {text[:200]}")
                        return {"success": False, "error": f"HTTP {resp.status}: {text[:200]}"}
        except Exception as e:
            logger.error(f"[异步] insert_content 调用失败: {e}")
            return {"success": False, "error": str(e)}
    
    async def preprocess_file_async(
        self,
        file_path: str,
        folder_path: str,
        filename: str,
        file_id: Optional[str] = None,
        force_ocr: bool = False,
        extract_regions: bool = True,
        extract_assets: bool = True,
        chunking_enabled: bool = True,
        chunking_mode: str = "heading"
    ) -> Optional[Dict[str, Any]]:
        """
        异步预处理文件
        
        Args:
            file_path: 文件相对路径
            folder_path: 项目文件夹路径
            filename: 文件名
            file_id: 文件ID（可选）
            force_ocr: 是否强制OCR
            extract_regions: 是否提取表格图片
            extract_assets: 是否提取资产
            chunking_enabled: 是否启用分块
            chunking_mode: 分块模式
        """
        if not self.is_configured():
            logger.warning("WindowsBridge未配置")
            return None
        
        url = f"{self.base_url}/api/v1/preprocessing/process"
        data = {
            "file_path": file_path,
            "folder_path": folder_path,
            "filename": filename,
            "file_id": file_id or "",
            "force_ocr": str(bool(force_ocr)).lower(),
            "extract_regions": str(bool(extract_regions)).lower(),
            "extract_assets": str(bool(extract_assets)).lower(),
            "chunking_enabled": str(bool(chunking_enabled)).lower(),
            "chunking_mode": chunking_mode,
        }
        
        try:
            logger.info(f"[异步] 预处理: {filename}")
            async with await self._get_aiohttp_session() as session:
                async with session.post(url, data=data, headers=self._async_headers()) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        logger.info(f"[异步] 预处理成功: {filename}")
                        return result
                    else:
                        text = await resp.text()
                        logger.warning(f"[异步] preprocess 失败: {resp.status} {text[:200]}")
                        return {"success": False, "error": f"HTTP {resp.status}: {text[:200]}"}
        except Exception as e:
            logger.error(f"[异步] preprocess 调用失败: {e}")
            return {"success": False, "error": str(e)}
