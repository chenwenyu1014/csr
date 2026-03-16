#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
预处理服务（封装）

职责：
  - 统一对外提供文档预处理能力（转换 + 可选分块）
  - 内部复用 FileProcessor，参数通过入参或环境变量控制

说明：
  - 为保持向后兼容，默认沿用 FileProcessor 的分块开关（CHUNKING_ENABLED），
    也可通过入参覆盖，无需上层关心具体细节。
"""

from __future__ import annotations

import os
import logging
from logging.handlers import RotatingFileHandler
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from .file_processor import FileProcessor, PreprocessedDocument

logger = logging.getLogger(__name__)

def _ensure_logging_config() -> None:
    try:
        root = logging.getLogger()
        # if root.handlers:
        #     return
        level = os.getenv("LOG_LEVEL", "INFO").upper()
        try:
            lvl = getattr(logging, level, logging.INFO)
        except Exception:
            lvl = logging.INFO
        root.setLevel(lvl)
        log_dir = os.getenv("LOG_DIR", "AAA/logs")
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception:
            pass
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        try:
            fh = RotatingFileHandler(os.path.join(log_dir, "windows_preprocessing.log"), maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
            fh.setLevel(lvl)
            fh.setFormatter(fmt)
            root.addHandler(fh)
        except Exception:
            pass
        # ch = logging.StreamHandler()
        # ch.setLevel(lvl)
        # ch.setFormatter(fmt)
        # root.addHandler(ch)
    except Exception:
        pass

_ensure_logging_config()


@contextmanager
def _temp_env(**kwargs):
    prev = {}
    try:
        for k, v in kwargs.items():
            prev[k] = os.environ.get(k)
            if v is None:
                if k in os.environ:
                    del os.environ[k]
            else:
                os.environ[k] = str(v)
        yield
    finally:
        for k, v in prev.items():
            if v is None:
                if k in os.environ:
                    del os.environ[k]
            else:
                os.environ[k] = v


class PreprocessingService:
    """预处理服务封装：对外暴露稳定 API。"""

    def __init__(self, cache_dir: str = "AAA/cache/preprocessing") -> None:
        self.cache_dir = cache_dir

    def preprocess(
        self,
        file_path: str | Path,
        *,
        force_ocr: bool = False,
        extract_regions: bool = True,
        extract_assets: bool = True,
        # 可选：覆盖分块参数（若为 None 则沿用环境/默认）
        chunking_enabled: Optional[bool] = None,
        chunking_mode: Optional[str] = None,  # 'heading' | 'character'
        chunk_max_chars: Optional[int] = None,
        chunk_min_chars: Optional[int] = None,
        chunk_overlap_sentences: Optional[int] = None,
        # 可选：手动指定内容类型（覆盖自动分类）
        content_type: Optional[str] = None,
        # 可选：指定输出目录
        output_dir: Optional[str | Path] = None,
        # ✅ 新增：额外信息（如file_id）
        extra_info: Optional[dict] = None,
    ) -> PreprocessedDocument:
        """
        执行预处理并返回标准化结果。必要时按参数覆盖分块行为。
        
        Args:
            file_path: 文件路径
            force_ocr: 强制使用OCR
            extract_regions: 是否拆分区域
            extract_assets: 是否提取资源
            chunking_enabled: 是否启用分块
            chunking_mode: 分块模式 ('heading'=一级标题分块, 'character'=字符分块)
            chunk_max_chars: 分块最大字符数
            chunk_min_chars: 分块最小字符数
            chunk_overlap_sentences: 分块重叠句子数
            content_type: 手动指定的内容类型
                         Word文档支持: 'txt'/'text'（文本）, 'jpg'/'images'（纯图）, 'list'/'tables'（纯表）
            output_dir: 输出目录（如不指定则使用临时目录）
            extra_info: 额外信息字典，如 {"file_id": "xxx"}
        """
        env_overrides = {}
        if chunking_enabled is not None:
            env_overrides["CHUNKING_ENABLED"] = "1" if chunking_enabled else "0"
        if chunking_mode is not None:
            env_overrides["CHUNKING_MODE"] = chunking_mode  # 'heading' or 'character'
        if chunk_max_chars is not None:
            env_overrides["CHUNK_MAX_CHARS"] = str(int(chunk_max_chars))
        if chunk_min_chars is not None:
            env_overrides["CHUNK_MIN_CHARS"] = str(int(chunk_min_chars))
        if chunk_overlap_sentences is not None:
            env_overrides["CHUNK_OVERLAP_SENTENCES"] = str(int(chunk_overlap_sentences))

        with _temp_env(**env_overrides):
            processor = FileProcessor(cache_dir=self.cache_dir)
            return processor.process(
                file_path=file_path,
                force_ocr=force_ocr,
                extract_regions=extract_regions,
                extract_assets=extract_assets,
                content_type_override=content_type,
                output_dir=output_dir,
                extra_info=extra_info,  # ✅ 传递额外信息
            )

    def preprocess_batch(
        self,
        files_config: Dict[str, Any],
        *,
        # 全局默认配置
        force_ocr: bool = False,
        extract_regions: bool = True,
        extract_assets: bool = True,
        chunking_enabled: Optional[bool] = None,
        chunking_mode: Optional[str] = None,  # 'heading' | 'character'
        chunk_max_chars: Optional[int] = None,
        chunk_min_chars: Optional[int] = None,
        chunk_overlap_sentences: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        批量预处理多个文件
        
        Args:
            files_config: 文件配置字典，格式：
                {
                    "docx": {
                        "data": [
                            {
                                "name": "方案文档",
                                "path": ["/path/to/file1.docx", "/path/to/file2.docx"],
                                # 可选：内容类型（覆盖自动分类）
                                # Word文档支持: "txt"（文本类型）, "jpg"（纯图类型）, "list"（纯表类型）
                                "type": "txt",
                                # 可选：文件数量（冗余字段，可从path数组长度自动计算）
                                "num": 10,
                                # 可选：每个文件组的独立配置
                                "force_ocr": false,
                                "extract_regions": true,
                                "extract_assets": true,
                                "chunking_enabled": true,
                                "chunk_max_chars": 1200,
                                "chunk_min_chars": 600,
                                "chunk_overlap_sentences": 1,
                            }
                        ]
                    },
                    "pdf": { ... },
                    "excel": { ... },
                    "img": { ... },
                    # 全局配置（可选，会覆盖函数参数的默认值）
                    "config": {
                        "force_ocr": false,
                        "extract_regions": true,
                        "extract_assets": true,
                        "chunking_enabled": true,
                        "chunk_max_chars": 1200,
                        "chunk_min_chars": 600,
                        "chunk_overlap_sentences": 1,
                    }
                }
            max_workers: 最大并发数（None 表示不限制）
            
        Returns:
            批量处理结果字典
        """
        import time
        start_time = time.time()
        
        # 提取全局配置（如果存在）
        global_config = files_config.get("config", {})
        if global_config:
            force_ocr = global_config.get("force_ocr", force_ocr)
            extract_regions = global_config.get("extract_regions", extract_regions)
            extract_assets = global_config.get("extract_assets", extract_assets)
            chunking_enabled = global_config.get("chunking_enabled", chunking_enabled)
            chunk_max_chars = global_config.get("chunk_max_chars", chunk_max_chars)
            chunk_min_chars = global_config.get("chunk_min_chars", chunk_min_chars)
            chunk_overlap_sentences = global_config.get("chunk_overlap_sentences", chunk_overlap_sentences)
        
        # 收集所有待处理文件
        tasks = []
        for file_type, type_data in files_config.items():
            if file_type == "config":
                continue
            if not isinstance(type_data, dict) or "data" not in type_data:
                continue
            
            for item in type_data.get("data", []):
                name = item.get("name", "未命名")
                paths = item.get("path", [])
                # 兼容：path 可为字符串或数组
                if isinstance(paths, (str, Path)):
                    paths = [str(paths)]
                elif not isinstance(paths, list):
                    paths = []
                if not paths:
                    continue
                
                # 每个文件组的独立配置（覆盖全局配置）
                item_force_ocr = item.get("force_ocr", force_ocr)
                item_extract_regions = item.get("extract_regions", extract_regions)
                item_extract_assets = item.get("extract_assets", extract_assets)
                item_chunking_enabled = item.get("chunking_enabled", chunking_enabled)
                item_chunk_max_chars = item.get("chunk_max_chars", chunk_max_chars)
                item_chunk_min_chars = item.get("chunk_min_chars", chunk_min_chars)
                item_chunk_overlap_sentences = item.get("chunk_overlap_sentences", chunk_overlap_sentences)
                # 获取内容类型（type字段）
                item_content_type = item.get("type")
                
                # 为每个文件路径创建任务
                for file_path in paths:
                    tasks.append({
                        "file_type": file_type,
                        "group_name": name,
                        "file_path": Path(file_path),
                        "force_ocr": item_force_ocr,
                        "extract_regions": item_extract_regions,
                        "extract_assets": item_extract_assets,
                        "chunking_enabled": item_chunking_enabled,
                        "chunk_max_chars": item_chunk_max_chars,
                        "chunk_min_chars": item_chunk_min_chars,
                        "chunk_overlap_sentences": item_chunk_overlap_sentences,
                        "content_type": item_content_type,  # 传递内容类型
                    })
        
        logger.info(f"批量预处理任务数: {len(tasks)}")
        
        # 执行批量处理
        results = []
        errors = []
        
        def _process_task(task: Dict[str, Any]) -> Dict[str, Any]:
            """处理单个任务"""
            try:
                file_path = task["file_path"]
                if not file_path.exists():
                    return {
                        "success": False,
                        "file_path": str(file_path),
                        "error": f"文件不存在: {file_path}",
                        "task": task,
                    }
                
                result = self.preprocess(
                    file_path=file_path,
                    force_ocr=task["force_ocr"],
                    extract_regions=task["extract_regions"],
                    extract_assets=task["extract_assets"],
                    chunking_enabled=task["chunking_enabled"],
                    chunk_max_chars=task["chunk_max_chars"],
                    chunk_min_chars=task["chunk_min_chars"],
                    chunk_overlap_sentences=task["chunk_overlap_sentences"],
                    content_type=task.get("content_type"),  # 传递内容类型
                )
                
                return {
                    "success": True,
                    "file_path": str(file_path),
                    "file_type": task["file_type"],
                    "group_name": task["group_name"],
                    "result": {
                        "source_file": result.source_file,
                        "file_type": result.file_type.value,
                        "content_type": result.content_type.value,
                        "text_content": result.text_content[:1000] if result.text_content else "",  # 限制长度
                        "markdown_content": result.markdown_content[:1000] if result.markdown_content else "",
                        "regions_count": len(result.regions),
                        "assets_count": len(result.assets),
                        "work_dir": str(result.work_dir) if result.work_dir else None,
                        "processing_info": result.processing_info,
                    },
                }
            except Exception as e:
                logger.error(f"处理文件失败 {task['file_path']}: {e}", exc_info=True)
                return {
                    "success": False,
                    "file_path": str(task["file_path"]),
                    "error": str(e),
                    "task": task,
                }
        
        # 并发处理
        if max_workers is None:
            max_workers = min(len(tasks), 8)  # 默认最多8个并发
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(_process_task, task): task for task in tasks}
            
            for future in as_completed(future_to_task):
                result = future.result()
                if result["success"]:
                    results.append(result)
                else:
                    errors.append(result)
        
        processing_time = time.time() - start_time
        
        return {
            "success": True,
            "total": len(tasks),
            "succeeded": len(results),
            "failed": len(errors),
            "processing_time": processing_time,
            "results": results,
            "errors": errors,
        }




