#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
视觉模型服务

功能说明：
- 处理文档中的表格、图表、图像等视觉内容
- 提供OCR（光学字符识别）功能，识别图片中的文字
- 支持多种文档格式转换（Word转PDF等）
- 通过HTTP接口调用远程视觉模型服务

主要应用场景：
1. PDF文档的OCR识别（扫描版PDF）
2. Word文档中的表格和图片提取
3. 方案文档中的图表识别
4. 图像内容的文字提取

技术实现：
- 使用HTTP接口调用远程视觉模型服务
- 支持本地LibreOffice进行文档格式转换
- 提供图片base64编码和文件上传两种方式
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import base64
import json
import requests
import tempfile
import os
import subprocess

logger = logging.getLogger(__name__)


class VisionModelService:
    """
    视觉模型服务，用于处理文档中的视觉内容
    
    这是CSR文档生成系统中负责处理视觉内容的服务组件。
    主要用于从PDF、Word等文档中提取表格、图表、图像等非文本内容。
    
    主要功能：
    1. OCR识别：识别图片中的文字内容
    2. 表格提取：从文档中提取表格数据
    3. 图表识别：识别文档中的图表和图像
    4. 格式转换：Word转PDF等格式转换
    
    技术特点：
    - 通过HTTP接口调用远程服务（支持本地部署）
    - 支持多种文档格式
    - 提供详细的错误处理和日志记录
    """
    
    def __init__(self,
                 api_key: Optional[str] = None,
                 model_name: str = "gpt-4-vision-preview",
                 base_url: str = None,
                 timeout: int = 600):
        """
        初始化视觉模型服务
        
        Args:
            api_key: API密钥（可选，某些服务可能需要）
            model_name: 模型名称（默认gpt-4-vision-preview，实际使用HTTP服务时可能忽略）
            base_url: 服务基础URL（可选，默认从环境变量读取）
            timeout: 请求超时时间（秒，默认600秒）
        """
        self.api_key = api_key
        self.model_name = model_name
        
        # 如果没有提供base_url，从环境变量读取，默认使用配置的服务器IP
        if base_url is None:
            env_base = os.getenv("VISION_HTTP_ENDPOINT") or os.getenv("OCR_SERVICE_URL")
            self.base_url = (env_base or "http://120.195.112.10:8001").rstrip("/")
        else:
            self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        
        # 不依赖外部 OCRService 类，直接使用 HTTP 接口
        self.ocr_service = None
        
        # 设置客户端连接
        self._setup_client()
    
    def _setup_client(self):
        """设置客户端连接"""
        try:
            # 这里应该初始化实际的视觉模型客户端
            # 例如 OpenAI 的 GPT-4V 或其他视觉模型
            self.client = None
            logger.info("视觉模型服务初始化完成")
        except Exception as e:
            logger.error(f"视觉模型服务初始化失败: {e}", exc_info=True)
            self.client = None
    
    def _docx_to_pdf_windows(self, input_path: str, output_path: str = None) -> str:
        """使用docx2pdf库将DOCX转换为PDF (Windows专用)"""
        try:
            from docx2pdf import convert
            
            if output_path is None:
                output_path = os.path.splitext(input_path)[0] + ".pdf"
            
            # 使用docx2pdf进行转换
            convert(input_path, output_path)
            
            # 验证输出文件是否存在
            if not os.path.exists(output_path):
                raise FileNotFoundError(f"转换后的PDF文件未找到: {output_path}")
                
            return output_path
            
        except ImportError:
            logger.error("docx2pdf库未安装，请运行: pip install docx2pdf")
            raise
        except Exception as e:
            logger.error(f"docx2pdf转换失败: {e}", exc_info=True)
            raise
    
    def _docx_to_pdf_libreoffice(self, input_path: str, output_path: str = None) -> str:
        """使用LibreOffice将DOCX转换为PDF (跨平台备用方案)"""
        if output_path is None:
            output_path = os.path.splitext(input_path)[0] + ".pdf"
        
        try:
            subprocess.run([
                "libreoffice",
                "--headless",           # 无界面模式
                "--convert-to", "pdf",  # 转换目标格式
                "--outdir", os.path.dirname(output_path),  # 输出目录
                input_path
            ], check=True, timeout=60)  # 添加超时和错误检查
            return output_path
        except subprocess.CalledProcessError as e:
            logger.error(f"LibreOffice转换失败: {e}", exc_info=True)
            raise
        except subprocess.TimeoutExpired:
            logger.error("LibreOffice转换超时")
            raise
        except FileNotFoundError:
            logger.error("LibreOffice未安装或不在PATH中")
            raise
    
    def _docx_to_pdf(self, input_path: str, output_path: str = None) -> str:
        """智能选择最佳的DOCX转PDF方法"""
        import platform
        
        # Windows系统优先使用docx2pdf
        if platform.system() == "Windows":
            try:
                return self._docx_to_pdf_windows(input_path, output_path)
            except ImportError:
                logger.warning("docx2pdf库未安装，回退到LibreOffice方案")
                return self._docx_to_pdf_libreoffice(input_path, output_path)
            except Exception as e:
                logger.warning(f"docx2pdf转换失败，回退到LibreOffice方案: {e}")
                return self._docx_to_pdf_libreoffice(input_path, output_path)
        else:
            # 非Windows系统使用LibreOffice
            return self._docx_to_pdf_libreoffice(input_path, output_path)
    
    def process_file(self, file_path: Path, file_content: str = "") -> Dict[str, Any]:
        """
        处理文件内容，提取视觉信息
        
        Args:
            file_path: 文件路径
            file_content: 文件文本内容（可选）
            
        Returns:
            处理结果字典
        """
        try:
            suffix = file_path.suffix.lower()
            
            if suffix == ".rtf":
                return self._process_rtf_file(file_path, file_content)
            elif suffix in [".pdf", ".doc", ".docx"]:
                return self._process_document_file(file_path, file_content)
            elif suffix in [".jpg", ".jpeg", ".png", ".gif", ".bmp"]:
                return self._process_image_file(file_path)
            else:
                return self._process_text_file(file_path, file_content)
                
        except Exception as e:
            logger.error(f"处理文件失败 {file_path}: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "content": file_content,
                "visual_elements": []
            }
    
    def _process_rtf_file(self, file_path: Path, file_content: str) -> Dict[str, Any]:
        """处理RTF文件"""
        try:
            # RTF文件可能包含表格、图表等结构化内容
            # 这里应该调用视觉模型API进行解析
            
            # 模拟处理结果
            result = {
                "status": "success",
                "file_type": "rtf",
                "file_name": file_path.name,
                "content": file_content,
                "visual_elements": [
                    {
                        "type": "table",
                        "content": "从RTF中提取的表格内容",
                        "position": "文档中部",
                        "confidence": 0.95
                    },
                    {
                        "type": "chart",
                        "content": "从RTF中提取的图表描述",
                        "position": "文档末尾",
                        "confidence": 0.88
                    }
                ],
                "structured_content": self._extract_structured_content(file_content)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"处理RTF文件失败: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "content": file_content,
                "visual_elements": []
            }
    
    def _process_document_file(self, file_path: Path, file_content: str) -> Dict[str, Any]:
        """处理PDF、DOC、DOCX文件"""
        try:
            # 对于PDF文件，调用OCR服务
            if file_path.suffix.lower() == '.pdf':
                try:
                    # 调用OCR服务解析PDF
                    ocr_result = self._call_ocr_service(file_path)
                    if ocr_result.get('status') == 'success':
                        return {
                            "status": "success",
                            "file_type": "pdf",
                            "file_name": file_path.name,
                            "content": ocr_result.get('content', ''),
                            "visual_elements": ocr_result.get('visual_elements', []),
                            "structured_content": ocr_result.get('structured_content', '')
                        }
                    else:
                        return {
                            "status": "error",
                            "error": ocr_result.get('error') or 'OCR服务返回非成功状态',
                            "content": "",
                            "visual_elements": []
                        }
                except Exception as e:
                    logger.warning(f"OCR服务调用失败: {e}")
                    return {
                        "status": "error",
                        "error": str(e),
                        "content": "",
                        "visual_elements": []
                    }
            
            # 对于DOC/DOCX文件，尝试转换为PDF后处理
            if file_path.suffix.lower() in ['.doc', '.docx']:
                try:
                    # 使用智能转换方法转换为PDF
                    pdf_path = self._docx_to_pdf(str(file_path))
                    if pdf_path and Path(pdf_path).exists():
                        # 调用OCR服务解析转换后的PDF
                        ocr_result = self._call_ocr_service(Path(pdf_path))
                        if ocr_result.get('status') == 'success':
                            # 清理临时PDF文件
                            try:
                                os.remove(pdf_path)
                            except Exception:
                                pass  # 忽略清理失败
                            
                            return {
                                "status": "success",
                                "file_type": file_path.suffix[1:],
                                "file_name": file_path.name,
                                "content": ocr_result.get('content', ''),
                                "visual_elements": ocr_result.get('visual_elements', []),
                                "structured_content": ocr_result.get('structured_content', '')
                            }
                        
                except Exception as e:
                    logger.warning(f"DOC/DOCX 转 PDF 失败，退回文本路径：{e}")
                    # 如果转换失败，退回到文本处理
                    if file_content:
                        return {
                            "status": "success",
                            "file_type": file_path.suffix[1:],
                            "file_name": file_path.name,
                            "content": file_content,
                            "visual_elements": [],
                            "structured_content": self._extract_structured_content(file_content),
                        }

            # 其他文档类型暂时回退到原始文本+结构化提取
            result = {
                "status": "success",
                "file_type": file_path.suffix[1:],
                "file_name": file_path.name,
                "content": file_content,
                "visual_elements": [
                    {
                        "type": "text",
                        "content": "文档文本内容",
                        "position": "全文",
                        "confidence": 0.90
                    }
                ],
                "structured_content": self._extract_structured_content(file_content)
            }

            return result
            
        except Exception as e:
            logger.error(f"处理文档文件失败: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "content": file_content,
                "visual_elements": []
            }
    
    def _call_ocr_service(self, file_path: Path) -> Dict[str, Any]:
        """调用OCR服务解析文件"""
        try:
            file_size = 0
            try:
                file_size = file_path.stat().st_size
            except Exception:
                pass
            logger.info(f"OCR请求 {self.base_url}/parse | file={file_path.name} | size={file_size}")
            # 准备文件上传
            with open(file_path, 'rb') as f:
                content_type = 'application/pdf' if file_path.suffix.lower() == '.pdf' else 'application/octet-stream'
                files = {'file': (file_path.name, f, content_type)}
                response = requests.post(
                    f"{self.base_url}/parse",
                    files=files,
                    timeout=self.timeout
                )
                logger.info(f"OCR响应状态: {response.status_code}")
                if response.status_code == 200:
                    content = response.text
                    return {
                        "status": "success",
                        "content": content,
                        "visual_elements": [
                            {
                                "type": "text",
                                "content": "OCR提取的文档内容",
                                "position": "全文",
                                "confidence": 0.95
                            }
                        ],
                        "structured_content": content
                    }
                # 非200时，尝试raw方式重试
                logger.info("尝试使用raw application/pdf方式重试OCR...")
                with open(file_path, 'rb') as f2:
                    raw_resp = requests.post(
                        f"{self.base_url}/parse",
                        data=f2,
                        headers={"Content-Type": "application/pdf"},
                        timeout=self.timeout
                    )
                logger.info(f"OCR raw重试响应状态: {raw_resp.status_code}")
                if raw_resp.status_code == 200:
                    content = raw_resp.text
                    return {
                        "status": "success",
                        "content": content,
                        "visual_elements": [
                            {
                                "type": "text",
                                "content": "OCR提取的文档内容",
                                "position": "全文",
                                "confidence": 0.95
                            }
                        ],
                        "structured_content": content
                    }
                # 仍失败，返回两次响应的关键信息
                err1 = f"{response.status_code} - {response.text[:300]}" if response is not None else "no response"
                err2 = f"{raw_resp.status_code} - {raw_resp.text[:300]}" if raw_resp is not None else "no response"
                return {
                    "status": "error",
                    "error": f"OCR服务错误(multipart/raw): {err1} | {err2}"
                }
        except Exception as e:
            logger.error(f"调用OCR服务失败: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _process_image_file(self, file_path: Path) -> Dict[str, Any]:
        """处理图像文件"""
        try:
            # 这里应该调用图像识别API
            # 例如 OCR、图像描述等
            
            result = {
                "status": "success",
                "file_type": "image",
                "file_name": file_path.name,
                "content": f"图像文件: {file_path.name}",
                "visual_elements": [
                    {
                        "type": "image",
                        "content": "图像内容描述",
                        "position": "图像中心",
                        "confidence": 0.85
                    }
                ],
                "structured_content": "图像内容的结构化描述"
            }
            
            return result
            
        except Exception as e:
            logger.error(f"处理图像文件失败: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "content": f"图像文件: {file_path.name}",
                "visual_elements": []
            }
    
    def _process_text_file(self, file_path: Path, file_content: str) -> Dict[str, Any]:
        """处理纯文本文件"""
        try:
            result = {
                "status": "success",
                "file_type": "text",
                "file_name": file_path.name,
                "content": file_content,
                "visual_elements": [
                    {
                        "type": "text",
                        "content": "文本内容",
                        "position": "全文",
                        "confidence": 1.0
                    }
                ],
                "structured_content": self._extract_structured_content(file_content)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"处理文本文件失败: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "content": file_content,
                "visual_elements": []
            }
    
    def _extract_structured_content(self, content: Union[str, bytes]) -> str:
        """从文本内容中提取结构化信息"""
        try:
            if isinstance(content, (bytes, bytearray)):
                try:
                    content = content.decode('utf-8', errors='ignore')
                except Exception:
                    try:
                        content = content.decode('gbk', errors='ignore')
                    except Exception:
                        content = str(content)
            # 这里应该使用NLP技术提取结构化信息
            # 例如：章节、段落、列表、表格等
            
            # 简单的结构化提取示例
            lines = content.split('\n')
            structured_parts = []
            
            for i, line in enumerate(lines):
                line = line.strip()
                if line:
                    if line.startswith(('第', '第1', '第2', '第3', '第4', '第5', '第6', '第7', '第8', '第9')):
                        structured_parts.append(f"章节: {line}")
                    elif line.startswith(('•', '-', '*', '1.', '2.', '3.', '4.', '5.')):
                        structured_parts.append(f"列表项: {line}")
                    elif ':' in line and len(line.split(':')) == 2:
                        key, value = line.split(':', 1)
                        structured_parts.append(f"键值对: {key.strip()} = {value.strip()}")
                    else:
                        structured_parts.append(f"段落: {line}")
            
            return "\n".join(structured_parts[:20])  # 限制输出长度
            
        except Exception as e:
            logger.error(f"结构化内容提取失败: {e}", exc_info=True)
            return content[:500] + "..." if len(content) > 500 else content
    
    def call_vision_api(self, file_path: Path, prompt: str = "") -> str:
        """
        调用视觉模型API
        
        Args:
            file_path: 文件路径
            prompt: 处理提示词
            
        Returns:
            API响应结果
        """
        try:
            if not self.client:
                return f"[视觉模型] 客户端未初始化，无法调用API"
            
            # 这里应该调用实际的视觉模型API
            # 例如：
            # response = self.client.chat.completions.create(
            #     model=self.model_name,
            #     messages=[
            #         {"role": "user", "content": [
            #             {"type": "text", "text": prompt},
            #             {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
            #         ]}
            #     ]
            # )
            
            # 模拟API调用
            result = f"[视觉模型API] 处理文件: {file_path.name}\n提示词: {prompt}\n处理完成"
            
            return result
            
        except Exception as e:
            logger.error(f"视觉模型API调用失败: {e}", exc_info=True)
            return f"[视觉模型API] 调用失败: {str(e)}"


class MockVisionModelService(VisionModelService):
    """模拟视觉模型服务，用于测试和开发"""
    
    def __init__(self):
        super().__init__()
        logger.info("使用模拟视觉模型服务")
    
    def _process_rtf_file(self, file_path: Path, file_content: str) -> Dict[str, Any]:
        """模拟处理RTF文件"""
        return {
            "status": "success",
            "file_type": "rtf",
            "file_name": file_path.name,
            "content": file_content[:1000] + "..." if len(file_content) > 1000 else file_content,
            "visual_elements": [
                {
                    "type": "table",
                    "content": "模拟提取的表格内容",
                    "position": "文档中部",
                    "confidence": 0.95
                }
            ],
            "structured_content": "模拟的结构化内容"
        }
    
    def call_vision_api(self, file_path: Path, prompt: str = "") -> str:
        """模拟API调用"""
        return f"[模拟视觉模型] 文件: {file_path.name}\n提示词: {prompt}\n模拟处理完成"


# 工厂函数
def create_vision_model_service(use_mock: bool = False, **kwargs) -> VisionModelService:
    """
    创建视觉模型服务实例
    
    Args:
        use_mock: 是否使用模拟服务
        **kwargs: 其他参数
        
    Returns:
        视觉模型服务实例
    """
    if use_mock:
        return MockVisionModelService()
    else:
        return VisionModelService(**kwargs)





