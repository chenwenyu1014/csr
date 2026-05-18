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
            import traceback
            traceback.print_exc()
            logger.warning(f"读取文件失败 {file_path}: {e}")
            return None

    def insert_content(
    
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
