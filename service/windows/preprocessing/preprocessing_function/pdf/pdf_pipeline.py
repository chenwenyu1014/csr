# -*- coding: utf-8 -*-
"""
PDF 预处理 Pipeline

PDF → Markdown：
- OCR 优先（调用视觉模型服务）
- OCR 失败/空时用 PyMuPDF 抽取纯文本兜底
- 扫描型（两者均无文本）→ 返回空
"""
from __future__ import annotations
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional

# ContentType 仍定义在 file_processor，按 rtf_pipeline 既有约定引用（仅取一个枚举名，轻量依赖）
from service.windows.preprocessing.file_processor import ContentType

logger = logging.getLogger(__name__)


def pdf_to_markdown_ocr(pdf_path: Path, work_dir: Path) -> Optional[str]:
    """
    PDF → Markdown（通过OCR）

    调用视觉模型服务进行OCR识别
    """
    try:
        # 调用现有的视觉服务
        try:
            from service.models import get_vision_service
            vision_service = get_vision_service(timeout=1200)

            # 读取PDF文件
            pdf_bytes = pdf_path.read_bytes()

            # 调用OCR
            result = vision_service.process_file(pdf_path, pdf_bytes)

            if result.get('status') == 'success':
                content = result.get('structured_content') or result.get('content', '')
                logger.info(f"PDF OCR成功，内容长度: {len(content)} 字符")
                return content
            else:
                logger.error(f"PDF OCR失败: {result.get('error')}")
                return None

        except Exception as e:
            logger.error(f"视觉服务调用失败: {e}", exc_info=True)
            return None

    except Exception as e:
        logger.error(f"PDF→Markdown失败: {e}", exc_info=True)
        return None


def pdf_text_extract(pdf_path: Path) -> Optional[str]:
    """用 PyMuPDF 抽取 PDF 纯文本（不依赖 OCR）。
    作为回退方法：只要能抽到文字就返回；完全抽不到文本（扫描型）才返回 None。"""
    try:
        try:
            import pymupdf as fitz  # type: ignore
        except ImportError:
            import fitz  # type: ignore  # 兼容旧版 pymupdf
    except ImportError:
        logger.warning("未安装 pymupdf，PyMuPDF 兜底不可用")
        return None

    try:
        doc = fitz.open(str(pdf_path))
        try:
            pages_text = []
            for page in doc:
                t = page.get_text("text") or ""
                t = t.strip()
                if t:
                    pages_text.append(t)
        finally:
            doc.close()
        text = "\n\n".join(pages_text).strip()
        if not text:
            logger.info("PyMuPDF 未抽到任何文本，判定为扫描型PDF")
            return None
        logger.info(f"PyMuPDF 抽取文本: {len(text)} 字符")
        return text
    except Exception as e:
        logger.warning(f"PyMuPDF 抽取失败: {e}")
        return None


def pdf_to_markdown_direct(pdf_path: Path, work_dir: Path) -> Dict[str, Any]:
    """PDF 直接转内容：OCR 优先，OCR 失败/空时用 PyMuPDF 抽文本兜底。
    - OCR 成功 → 结构化 Markdown（不变）
    - OCR 失败 + PyMuPDF 抽到文本 → 纯文本 Markdown，走字符分块（suggested_chunking_mode='character'）
    - OCR 失败 + PyMuPDF 也无文本（扫描型）→ 返回空
    """
    # 1. OCR 优先
    markdown_content = None
    try:
        markdown_content = pdf_to_markdown_ocr(pdf_path, work_dir)
    except Exception as e:
        logger.warning(f"PDF OCR 异常，将尝试 PyMuPDF 兜底: {e}")
        markdown_content = None

    if markdown_content:
        return {
            'content': markdown_content,
            'content_type': ContentType.MARKDOWN,
            'text': markdown_content,
            'metadata': {'conversion_method': 'pdf_ocr', 'source': 'direct_pdf'}
        }

    # 2. OCR 失败/空 → PyMuPDF 兜底抽文本
    logger.info("PDF OCR 失败/空，尝试 PyMuPDF 文本抽取兜底")
    text = pdf_text_extract(pdf_path)
    if text:
        return {
            'content': text,
            'content_type': ContentType.MARKDOWN,
            'text': text,
            'metadata': {
                'conversion_method': 'pdf_pymupdf_fallback',
                'source': 'direct_pdf',
                'has_headings': False,
                'suggested_chunking_mode': 'character',
            }
        }

    # 3. 扫描型（OCR 失败且 PyMuPDF 无文本）→ 空
    logger.error("PDF OCR 失败且 PyMuPDF 无文本（可能为扫描版），返回空内容")
    return {
        'content': '',
        'content_type': ContentType.MARKDOWN,
        'text': '',
        'metadata': {'conversion_method': 'pdf_ocr_and_pymupdf_failed', 'source': 'direct_pdf'}
    }


def pdf_run(pdf_path: Path | str, work_dir: Path | str, scanned: bool = False) -> Dict[str, Any]:
    """PDF → Markdown：OCR 优先，PyMuPDF 文本兜底。
    scanned 参数保留兼容，当前文字型与扫描型走同一稳健路径。"""
    pdf_path = Path(pdf_path)
    work_dir = Path(work_dir)
    return pdf_to_markdown_direct(pdf_path, work_dir)