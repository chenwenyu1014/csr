"""
数据提取器V2

功能说明：
- 从多种数据源（Word、PDF、Excel、RTF）中提取结构化数据
- 支持并发提取，提高处理效率
- 提供完整的溯源信息，记录数据来源
- 支持缓存机制，避免重复处理

支持的数据类型：
- word: Word文档（.docx）
- pdf: PDF文档（.pdf）
- excel: Excel表格（.xlsx, .xls）
- rtf: RTF格式文档（.rtf）

技术特点：
- 使用线程池实现并发提取（支持并发数限制）
- 支持OCR识别（PDF图片内容）
- 提供详细的提取日志和溯源信息
- 支持请求间隔控制，避免API限流
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import re
import threading
import traceback

from config import get_settings
from utils.task_logger import get_task_logger
from utils.context_manager import get_request_id, inherit_context

# 导入耗时记录工具
from utils.timing import Timer, generation_timer


def _task_log_error(message: str, exc: Exception = None, **extra):
    """记录错误到任务日志"""
    task_logger = get_task_logger()
    if task_logger:
        task_logger.error(message, exc=exc, logger_name="data_extractor", **extra)


logger = logging.getLogger(__name__)


class DataExtractorV2:
    """
    数据提取器V2 - 基于固定数据类型和字段结构

    这是CSR文档生成系统的核心数据提取组件，负责从各种格式的文档中
    提取结构化数据，供后续的段落生成使用。

    主要功能：
    1. 多格式文档解析（Word、PDF、Excel、RTF）
    2. 智能内容提取（基于LLM和视觉模型）
    3. 并发处理提高效率
    4. 完整的溯源信息记录
    """
    
    def __init__(self, base_data_dir: str = "AAA/project_data", cache_dir: str = "AAA/cache"):
        """
        初始化数据提取器

        Args:
            base_data_dir: 基础数据目录，存储待处理的文档文件
            cache_dir: 缓存目录，用于存储处理后的中间结果
        """
        # 基础数据目录
        self.base_data_dir = Path(base_data_dir)

        # ========== 文件缓存目录配置 ==========
        # 创建缓存目录结构，用于存储处理后的中间结果
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.pdf_cache_dir = self.cache_dir / "pdf"  # PDF处理缓存
        self.ocr_cache_dir = self.cache_dir / "ocr"  # OCR识别缓存
        self.ocr_clean_cache_dir = self.cache_dir / "ocr_clean"  # OCR清理后缓存
        self.pdf_cache_dir.mkdir(exist_ok=True)
        self.ocr_cache_dir.mkdir(exist_ok=True)
        self.ocr_clean_cache_dir.mkdir(exist_ok=True)

        # ========== 服务初始化 ==========
        # 默认连接视觉/LLM服务（可被外部覆盖）
        try:
            from service import VisionModelService  # 延迟导入避免循环依赖
            self.vision_service = VisionModelService(timeout=600)  # 视觉模型服务，用于OCR
        except Exception:
            self.vision_service = None
        self.llm_service = None  # LLM服务（延迟初始化）

        # ========== 数据类型处理器映射 ==========
        # 根据文件类型选择对应的处理函数
        self.type_handlers = {
            "word": self._handle_word_type,  # Word文档处理
            "pdf": self._handle_pdf_type,  # PDF文档处理
            "excel": self._handle_excel_type,  # Excel表格处理
            "rtf": self._handle_rtf_type,  # RTF文档处理
        }

        # ========== 日志和上下文 ==========
        # 线程本地上下文：存放当前段落ID，避免并发冲突
        self._context = threading.local()

    def extract_data_for_paragraph(self, paragraph_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        为单个段落提取所有需要的数据

        这是数据提取的核心方法，会并发处理段落配置中的所有数据项，
        并返回完整的提取结果和溯源信息。

        Args:
            paragraph_data: 段落配置字典，包含：
                - id: 段落ID
                - data: 数据项列表，每个数据项包含：
                    - extract: 提取需求描述
                    - datas: 数据源文件列表
                    - quote: 引用标签（可选）
                - generate: 生成提示词
                - example: 示例文本（可选）
                - insert_original: 是否插入图表

        Returns:
            提取结果字典，包含：
                - paragraph_id: 段落ID
                - generate_prompt: 生成提示词
                - extracted_items: 提取项列表
                - available_resources: 可用资源列表
                - traceability: 溯源信息
        """
        paragraph_id = paragraph_data["id"]

        # 开始段落数据提取计时
        extract_timer = Timer(f"提取段落数据({paragraph_id})", parent="数据提取")
        extract_timer.start()

        extracted_data = {
            "paragraph_id": paragraph_id,
            "generate_prompt": paragraph_data["generate"],
            "example": paragraph_data["example"],
            "extracted_items": [],
            "available_resources": [],  # ✅ 新增：汇总所有可用资源
            "all_placeholders": [],  # 新增：汇总所有占位符(用于插入图表)
            # ✅ 溯源信息
            "traceability": {
                "data_items": [],  # 每个data item的完整溯源
                "total_chunks_loaded": 0,
                "total_matches_found": 0
            }
        }

        # 阶段提示：开始提取
        logger.info(f"正在提取数据... ({len(paragraph_data['data'])}个数据项)")

        # 将请求ID注入线程本地上下文，方便在子线程中进行事件流推送
        try:
            try:
                from utils.context_manager import get_request_id  # type: ignore
            except Exception as e:
                import traceback
                traceback.print_exc()
                from utils.context_manager import get_request_id  # type: ignore
            rid0 = get_request_id()
            if not rid0:
                rid0 = get_request_id()
            if rid0:
                setattr(self._context, 'request_id', rid0)
        except Exception as e:
            import traceback
            traceback.print_exc()
            pass

        # 获取并发配置
        settings = get_settings()
        max_data_item_workers = settings.max_data_item_workers
        llm_request_interval = settings.llm_request_interval

        # 请求间隔控制锁
        _request_lock = threading.Lock()
        _last_request_time = [0.0]  # 使用列表以便在闭包中修改

        # 并发处理每个数据项
        def _worker(item_index: int, item: Dict[str, Any]) -> Dict[str, Any]:
            # 为工作线程设置段落上下文
            setattr(self._context, 'paragraph_id', paragraph_id)
            # 记录当前数据项索引，供流式事件(extraction_delta)引用
            try:
                setattr(self._context, 'current_item_index', item_index)
            except Exception as e:
                import traceback
                traceback.print_exc()
                pass
            # 同步段落级提示词上下文，供TFL增强阶段使用
            try:
                setattr(self._context, 'generate_prompt', paragraph_data.get('generate'))
                setattr(self._context, 'example', paragraph_data.get('example'))
            except Exception as e:
                import traceback
                traceback.print_exc()
                pass

            # 请求间隔控制：避免瞬时高并发
            with _request_lock:
                elapsed = time.time() - _last_request_time[0]
                if elapsed < llm_request_interval:
                    time.sleep(llm_request_interval - elapsed)
                _last_request_time[0] = time.time()

            try:
                res = self.extract_single_data_item(item)
                return res
            except Exception as e:
                import traceback
                traceback.print_exc()
                error_msg = f"提取数据项失败: {item.get('type', '未知类型')} - {e}"
                logger.error(error_msg, exc_info=True)
                _task_log_error(error_msg, exc=e, item_type=item.get('type'))
                return {
                    "item": item,
                    "status": "error",
                    "error_type": "EXTRACTION_EXCEPTION",
                    "error": str(e),
                    "content": None
                }

        from concurrent.futures import ThreadPoolExecutor, as_completed
        # ========== 段落级数据源校验 ==========
        data_items = paragraph_data.get("data", [])
        if not data_items:
            logger.warning(f"⚠️ [extract_paragraph_data] 段落 {paragraph_id} 无数据项")
            logger.warning(f"   请检查JSON配置中该段落的datas字段")
            # 添加一个 error 项到 extracted_items，以便 pipeline.py 能检测到错误
            extracted_data["extracted_items"].append({
                "item": {"item_id": "N/A"},
                "status": "error",
                "error_type": "NO_DATA_SOURCE",
                "error": "段落未分配数据源：datas字段为空。请检查JSON配置。",
                "content": "",
                "data_type": "unknown"
            })
            return extracted_data

        # 检查是否有有效的数据项（至少有一个file_type不为空）
        valid_items = [item for item in data_items if item.get("file_type")]
        if not valid_items:
            logger.warning(f"⚠️ [extract_paragraph_data] 段落 {paragraph_id} 所有数据项均缺少file_type")
            logger.warning(f"   请检查数据源分配逻辑")
            # 添加一个 error 项到 extracted_items，以便 pipeline.py 能检测到错误
            first_item = data_items[0] if data_items else {}
            extracted_data["extracted_items"].append({
                "item": first_item,
                "status": "error",
                "error_type": "NO_DATA_SOURCE",
                "error": "数据项缺少数据源配置：所有数据项的file_type均为空。请检查数据源分配逻辑。",
                "content": "",
                "data_type": "unknown",
                "item_id": first_item.get("item_id"),
                "directory": first_item.get("directory")
            })
            return extracted_data
        items = list(enumerate(paragraph_data["data"]))

        # 限制并发数：取配置值与数据项数量的较小值
        max_workers = min(max_data_item_workers, len(items)) if items else 0
        results_buffer: List[Optional[Dict[str, Any]]] = [None] * len(items)
        # 记录当前请求ID（供聚合线程用来投递事件）
        _rid_for_agg = None
        try:
            try:
                from utils.context_manager import get_request_id  # type: ignore
            except Exception as e:
                import traceback
                traceback.print_exc()
                from utils.context_manager import get_request_id  # type: ignore
            _rid_for_agg = get_request_id()
        except Exception as e:
            import traceback
            traceback.print_exc()
            _rid_for_agg = None
        if not _rid_for_agg:
            _rid_for_agg = get_request_id()

        if max_workers > 0:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_idx = {executor.submit(inherit_context(_worker), i, it): i for i, it in items}
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        res = future.result()
                        results_buffer[idx] = res
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        stack_trace = traceback.format_exc()
                        results_buffer[idx] = {
                            "item": items[idx][1],
                            "status": "error",
                            "error_type": "EXECUTION_EXCEPTION",
                            "error": str(e),
                            "content": None,
                            "stack_trace": stack_trace
                        }
        # 填充结果
        all_placeholders = set()
        for res in results_buffer:
            if res is not None:
                extracted_data["extracted_items"].append(res)
                # ✅ 汇总每个数据项的占位符
                if res.get("insert_original") and res.get("placeholders"):
                    all_placeholders.update(res.get("placeholders", []))
                # ✅ 收集溯源信息
                if res.get("traceability"):
                    extracted_data["traceability"]["data_items"].append(res["traceability"])
                    # 汇总统计
                    trace = res.get("traceability", {})
                    extracted_data["traceability"]["total_chunks_loaded"] += trace.get("chunks_loaded", 0)
                    extracted_data["traceability"]["total_matches_found"] += len(trace.get("matches", []))
        # 保存汇总的占位符
        extracted_data["all_placeholders"] = list(all_placeholders)
        # ✅ 新增：汇总所有data_item中的available_resources
        for item in paragraph_data.get("data", []):
            available_resources = item.get("available_resources", [])
            if available_resources:
                extracted_data["available_resources"].extend(available_resources)

        # 停止计时
        extract_timer.stop()

        # 阶段提示：提取完成
        success_count = len([item for item in extracted_data["extracted_items"] if item.get("status") == "success"])
        failed_count = len([item for item in extracted_data["extracted_items"] if item.get("status") == "error"])
        resource_count = len(extracted_data["available_resources"])
        placeholder_count = len(extracted_data["all_placeholders"])
        logger.info(f"✓ 提取完成: 成功{success_count}个，失败{failed_count}个，可用资源{resource_count}个，收集到占位符{placeholder_count}个(仅word) ，[耗时: {extract_timer.duration_str}]")
        
        # 记录到全局计时器
        if generation_timer:
            generation_timer.record(f"段落数据提取-{paragraph_id}", extract_timer.duration, parent="数据提取",
                                   metadata={"success": success_count, "failed": failed_count, "resources": resource_count})
        
        return extracted_data

    def extract_single_data_item(self, data_item) -> Dict[str, Any]:
        """
        提取单个数据项的内容

        【核心功能】
        根据文件类型调用不同的处理逻辑：
        - word/pdf: 使用两阶段提取（分块筛选 + 内容提取）
        - excel/table: 直接读取表格数据

        【quote字段处理】
        1. 从data_item读取quote字段（如果存在）
        2. 将quote字段传递给具体的处理函数（_handle_word_type, _handle_pdf_type）
        3. 在返回结果中包含quote字段，便于后续生成服务使用
        4. 无论成功还是失败，都要保留quote字段

        【数据流】
        Pipeline -> data_item(包含quote) -> DataExtractor -> 返回结果(包含quote) -> ParagraphGenerationService

        Args:
            data_item: 数据项字典，包含：
                - file_type: 文件类型 (word/pdf/excel/table)
                - extract: 提取提示词
                - chunks_file: 分块数据文件路径
                - source_file: 源文件名
                - quote: 引用标签（可选，用于在生成内容前添加标识）

        Returns:
            提取结果字典，包含：
            - status: "success"/"error"
            - content: 提取到的内容
            - data_type: 数据类型
            - quote: 引用标签（如果原始data_item中有）
            - error: 错误信息（如果失败）
        """
        # 开始单项提取计时
        item_timer = Timer(f"提取数据项({data_item.get('file_type', 'unknown')})", parent="数据提取")
        item_timer.start()

        # 获取文件类型
        # ✅ 步骤1: 读取data_item中的关键字段
        file_type = data_item.get("file_type", "").lower()
        insert_original = data_item.get("insert_original", False)
        extract_prompt = data_item.get("extract", "")
        source_file = data_item.get("source_file", "")
        quote = data_item.get("quote")  # 获取quote字段（用于在生成内容前添加引用标签）
        # ✅ 新增：读取溯源信息字段
        item_id = data_item.get("item_id")           # 资料编号
        directory = data_item.get("directory")       # 来源目录
        file_names = data_item.get("file_name", [])  # 溯源文件名列表

        # ========== 数据项级数据源校验 ==========
        # 检查是否缺少数据源配置（datas为空时传入的占位项）
        if not file_type:
            logger.warning(f"⚠️ [extract_single_data_item] 数据项缺少数据源配置")
            logger.warning(f"   请检查JSON配置中该段落的datas字段是否已分配数据源")
            return {
                "item": data_item,
                "status": "error",
                "error_type": "NO_DATA_SOURCE",  # 新增错误类型字段
                "error": "数据项缺少数据源配置：file_type为空。请检查JSON配置中该段落的datas字段。",
                "content": "",
                "data_type": "unknown",
                "item_id": item_id,
                "directory": directory
            }

        # 映射文件类型到处理器
        type_mapping = {
            "docx": "word",
            "doc": "word",
            "pdf": "pdf",
            "xlsx": "excel",
            "xls": "excel",
            "rtf": "rtf"
        }

        data_type = type_mapping.get(file_type, file_type)

        if data_type not in self.type_handlers:
            return {
                "item": data_item,
                "status": "error",
                "error_type": "UNSUPPORTED_FILE_TYPE",
                "error": f"不支持的文件类型: {file_type}",
                "content": "",
                "data_type": data_type
            }

        # 调用对应的处理器
        handler_func = self.type_handlers[data_type]
        with Timer(f"处理器({data_type})", parent="数据提取") as handler_timer:
            result = handler_func(data_item)

        # 停止单项提取计时
        item_timer.stop()
        logger.info(f"⏱️ 数据项提取完成 [类型: {data_type}, 耗时: {item_timer.duration_str}]")

        # 如果处理器返回的是字典（包含状态信息），直接返回（并补齐必要字段）
        if isinstance(result, dict) and "status" in result:
            if quote:
                result["quote"] = quote
            if extract_prompt:
                result.setdefault("extract", extract_prompt)
                result.setdefault("extract_item", extract_prompt)
            if source_file:
                result.setdefault("source_file", source_file)
            if "chunks_file" in data_item:
                result.setdefault("chunks_file", data_item.get("chunks_file"))
            if "markdown_files" in data_item:
                result.setdefault("markdown_files", data_item.get("markdown_files"))
            result["insert_original"] = insert_original
            # ✅ 新增：添加溯源信息字段
            if item_id is not None:
                result["item_id"] = item_id
            if directory:
                result["directory"] = directory
            if file_names:
                result["file_name"] = file_names
            if result.get("status") == "success":
                result.setdefault("traceability", {
                    "data_type": data_type,
                    "extract_prompt": extract_prompt,
                    "source_file": source_file
                })
            # 记录提取耗时到结果
            result.setdefault("timing", {})
            result["timing"]["extraction_duration"] = item_timer.duration
            result["timing"]["extraction_duration_str"] = item_timer.duration_str
            return result

        # 否则包装成标准格式
        if isinstance(result, dict):
            output = {
                "item": data_item,
                "status": "success",
                "content": result.get("content", result),
                "data_type": data_type
            }
            # ✅ 添加quote字段
            if quote:
                output["quote"] = quote
            # ✅ 新增：添加溯源信息字段
            if item_id is not None:
                output["item_id"] = item_id
            if directory:
                output["directory"] = directory
            if file_names:
                output["file_name"] = file_names
            # 合并除content以外的附加信息
            for k, v in result.items():
                if k == "content":
                    continue
                output[k] = v
            # 添加溯源
            if output.get("status") == "success":
                traceability = {
                    "data_type": data_type,
                    "extract_prompt": extract_prompt,
                    "source_file": source_file,
                    "full_prompt": result.get("full_prompt", ""),
                    "extraction_method": result.get("method", "standard")
                }
                output["traceability"] = traceability
            return output
        else:
            output = {
                "item": data_item,
                "status": "success",
                "content": result,
                "data_type": data_type,
                "insert_original": insert_original
            }
            # ✅ 添加quote字段
            if quote:
                output["quote"] = quote
            # ✅ 新增：添加溯源信息字段
            if item_id is not None:
                output["item_id"] = item_id
            if directory:
                output["directory"] = directory
            if file_names:
                output["file_name"] = file_names
            return output

    def _handle_word_type(self, data_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理Word文档类型的数据提取

        【核心功能】
        - 支持两种模式：原文模式（insert_original=True）和提取模式
        - 原文模式：提取内容（保留{{Table_1_Start}}等占位符），后续根据占位符插入对应文件
        - 提取模式：使用两阶段提取（分块筛选 + 内容提取）

        【quote字段处理】
        将quote字段传递给_extract_from_chunks，最终包含在返回结果中
        """
        # 读取处理所需的关键字段
        extract_prompt = data_item.get("extract", "")
        original_mode = data_item.get("original_mode", False)
        insert_original = data_item.get("insert_original", False)
        chunks_file = data_item.get("chunks_file", "")
        quote = data_item.get("quote")
        per_file_ragflow_list = data_item.get("per_file_ragflow_list", [])  # per-file RAGFlow 内容

        # logger.info(f"🔍 [_handle_word_type] quote字段值: {quote}")
        # if per_file_ragflow_list:
        #     logger.info(f"📌 [_handle_word_type] per-file RAGFlow: {len(per_file_ragflow_list)} 个文件")

        # 如果无提取逻辑，并且是原文模式，则提取全部内容
        if (not extract_prompt or not extract_prompt.strip()) and original_mode:
            logger.info("无提取逻辑且为原文模式，直接加载全部分块内容")
            return self._load_all_chunks_content(chunks_file, quote=quote, doc_type="word", insert_original=insert_original)

        # 有提取逻辑，调用统一的提取入口
        return self._extract_from_chunks(
            chunks_file, extract_prompt, "word",
            original_mode=original_mode, quote=quote, insert_original=insert_original,
            per_file_ragflow_list=per_file_ragflow_list, data_item=data_item
        )

    def _load_all_chunks_content(self, chunks_file, doc_type: str, quote=None,insert_original= False):
        """
        提取逻辑为空，且原文模式为True是，提取所有chunks_file内容
        """
        try:
            import json
            from pathlib import Path

            if isinstance(chunks_file, str):
                chunks_files = [chunks_file]
            elif isinstance(chunks_file, list):
                chunks_files = chunks_file
            else:
                return {"status": "error", "error_type": "INVALID_CHUNKS_FILE", "error": "chunks_file格式错误", "content": ""}

            aggregated_parts = []
            total_sections = 0
            all_placeholders = set()

            for cf in chunks_files:
                cf_path = Path(cf)
                if not cf_path.exists():
                    continue
                with open(cf_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                sections = data.get("sections") or data.get("chunks") or []
                total_sections += len(sections)
                for sec in sections:
                    text = sec.get("content", "")
                    if text:
                        aggregated_parts.append(text)
                    # 如果需要提取占位符
                    if insert_original:
                        placeholders = self._extract_placeholders_from_content(text)
                        all_placeholders.update(placeholders)

            content = "\n\n".join(aggregated_parts)

            # 原文模式：清理占位符（移除 Start-End 之间的内容，只保留 Start 标签）
            content = self._clean_placeholder_content(content)

            result = {
                "status": "success",
                "content": content,
                "data_type": doc_type,
                "method": "direct_full_content_no_llm",
                "traceability": {
                    "chunks_loaded": total_sections,
                    "model_used": False
                }
            }
            if quote:
                result["quote"] = quote
                # 添加占位符
            if insert_original and all_placeholders:
                result["placeholders"] = list(all_placeholders)
            return result

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"全文加载失败: {e}", exc_info=True)
            return {"status": "error",
                    "error_type": "FULL_LOAD_FAILED",
                    "error": str(e),
                    "content": "",
                    "quote": quote}

    def _extract_from_chunks(self, chunks_file, extract_prompt: str, doc_type: str, original_mode: bool = False,
                             quote: Optional[str] = None, insert_original=False,
                             per_file_ragflow_list: Optional[List[Dict[str, str]]] = None,
                             data_item: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        从分块数据中进行两阶段提取

        【逻辑分流】
            - original_mode=True: 模型仅用于筛选分块，返回内容为筛选出的分块原文拼接（保留占位符）
            - original_mode=False : 模型筛选分块 + 模型总结/提取内容
        【两阶段提取流程】
        阶段1: 分块筛选 (Chunk Filtering)
            - 输入：extraction_query（提取需求）、chunks_data（文档分块）
            - 输出：相关分块列表 + 理由
            - 目标：从大量分块中筛选出相关部分，减少后续提取的token消耗

        阶段2: 内容提取 (Content Extraction)
            - 输入：extraction_query、筛选后的相关分块
            - 输出：提取的内容文本
            - 目标：根据需求从相关分块中提取关键信息

        【RAGFlow 集成】
            - 使用 per_file_ragflow_list（每个文件独立检索的 RAGFlow 内容）
            - 在阶段2构建内容时将 RAGFlow 分块拼在本地分块后（标记来源）

        【quote字段处理】
        - 成功时：将quote字段添加到return_data中
        - 失败时：将quote字段添加到error_data中
        - 确保无论成功失败，quote都能传递给后续的生成服务

        Args:
            chunks_file: 字符串或列表，支持多个文件
            extract_prompt: 提取提示词
            doc_type: 文档类型 ("word" 或 "pdf")
            original_mode: 是否为原文模式（保留占位符）
            quote: 引用标签（可选）
            per_file_ragflow_list: per-file RAGFlow 内容列表（每个文件独立检索）

        Returns:
            提取结果字典，包含 status, content, quote 等字段
        """
        try:
            from pathlib import Path
            import json

            if not chunks_file:
                return {"status": "error", "error_type": "MISSING_CHUNKS_FILE", "error": "缺少chunks_file字段", "content": ""}

            # 统一为列表（支持字符串、字典两种格式）
            if isinstance(chunks_file, str):
                chunks_files = [{"chunks_path": chunks_file}]
            elif isinstance(chunks_file, dict):
                chunks_files = [chunks_file]
            elif isinstance(chunks_file, list):
                chunks_files = []
                for cf in chunks_file:
                    if isinstance(cf, str):
                        chunks_files.append({"chunks_path": cf})
                    elif isinstance(cf, dict):
                        chunks_files.append(cf)
            else:
                return {"status": "error", "error_type": "INVALID_CHUNKS_TYPE", "error": f"chunks_file类型不支持: {type(chunks_file)}", "content": ""}

            if not chunks_files:
                return {"status": "error", "error_type": "EMPTY_CHUNKS_FILE", "error": "chunks_file列表为空", "content": ""}

            # 带校验的两阶段提取（逐文件分别提取后拼接）
            from service.linux.generation.extraction.two_stage_extraction_service import two_stage_extraction_service
            from service.linux.generation.extraction.validated_extraction_service import validated_extraction_service

            # 提取模式信号：原文模式 / 引用图表模式
            extraction_modes = []
            if original_mode:
                extraction_modes.append("原文模式")
            if insert_original:
                extraction_modes.append("引用图表模式")
            extraction_mode = "、".join(extraction_modes)
            extraction_query_with_instruction = extract_prompt

            # 环境上下文：段落ID
            import os
            paragraph_id = getattr(self._context, 'paragraph_id', 'unknown')
            from utils.context_manager import set_paragraph_id
            set_paragraph_id(paragraph_id)
            aggregated_content_parts: List[str] = []
            aggregated_ids: List[str] = []
            per_file_extraction_results: List[Dict[str, Any]] = []
            total_sections_sum = 0
            success_count = 0
            full_prompts: List[str] = []
            selected_sources: List[str] = []

            # 检查是否跳过校验（通过环境变量配置）
            # ⚠️ 默认值改为 "1"（跳过校验），避免环境变量未设置或并发覆盖时意外启用校验
            skip_validation = os.getenv("SKIP_EXTRACTION_VALIDATION", "1").strip().lower() in (
            "1", "true", "yes", "y", "on")
            if skip_validation:
                logger.info("📌 已配置跳过提取校验 (SKIP_EXTRACTION_VALIDATION=1 或默认)")

            # 获取并发配置
            settings = get_settings()
            max_file_workers = settings.max_file_extraction_workers
            llm_request_interval = settings.llm_request_interval

            # 请求间隔控制
            _file_request_lock = threading.Lock()
            _file_last_request_time = [0.0]

            def _extract_single_file(chunks_info) -> Dict[str, Any]:
                """单个文件的提取任务"""
                # ✅ 在工作线程中重新设置 paragraph_id（确保并发时获取正确的值）
                from utils.context_manager import set_paragraph_id
                set_paragraph_id(paragraph_id)

                # 支持字符串和字典两种格式
                if isinstance(chunks_info, dict):
                    cf = chunks_info.get("chunks_path", "")
                    file_id = chunks_info.get("file_id", "")
                    original_file_name = chunks_info.get("original_file_name", "")
                else:
                    cf = chunks_info
                    file_id = ""
                    original_file_name = ""

                cf_path = Path(cf) if cf else None
                if not cf_path or not cf_path.exists():
                    logger.warning(f"分块文件不存在: {cf}")
                    return {"success": False, "error": f"文件不存在: {cf}", "cf": cf, "file_id": file_id, "original_file_name": original_file_name}

                # 统计该文件sections数量（用于汇总）
                sections_cnt = 0
                try:
                    with open(cf_path, 'r', encoding='utf-8') as _f:
                        _data = json.load(_f)
                    _secs = _data.get('sections') or []
                    if not _secs and 'chunks' in _data:
                        _secs = _data.get('chunks') or []
                    sections_cnt = len(_secs)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    sections_cnt = 0

                # ✅ 优先使用 per-file RAGFlow 内容（每个文件独立检索）
                file_ragflow_content = None
                file_ragflow_chunks = []  # ✅ 新增：用于溯源的原始分块列表
                if per_file_ragflow_list:
                    parent_dir = cf_path.parent.name
                    for entry in per_file_ragflow_list:
                        # ✅ 优先使用 preprocessed_file_id 字段匹配（去掉 .docx 后缀精确匹配）
                        pp_file_id = entry.get("preprocessed_file_id", "")
                        # 去掉扩展名进行匹配
                        pp_file_id_stem = Path(pp_file_id).stem if pp_file_id else ""
                        if pp_file_id_stem and pp_file_id_stem == parent_dir:
                            file_ragflow_content = entry.get("ragflow_content", "")  # 用于 LLM
                            file_ragflow_chunks = entry.get("ragflow_chunks", [])  # 用于溯源
                            logger.info(f"📌 匹配到 per-file RAGFlow: {entry.get('file_name', '')} (ID: {pp_file_id}), {len(file_ragflow_chunks)} 个分块")
                            break

                # 请求间隔控制：避免瞬时高并发
                with _file_request_lock:
                    elapsed = time.time() - _file_last_request_time[0]
                    if elapsed < llm_request_interval:
                        time.sleep(llm_request_interval - elapsed)
                    _file_last_request_time[0] = time.time()

                result = validated_extraction_service.extract_with_validation(
                        extraction_func=two_stage_extraction_service.extract_from_chunks,
                        extraction_kwargs={
                            "chunks_index_path": str(cf_path),
                            "chunks_dir": str(cf_path.parent),
                            "extraction_query": extraction_query_with_instruction,
                            "task_name": None,
                            "doc_type": doc_type,
                            "ragflow_content": file_ragflow_content,  #  添加RAGFlow检索的信息
                            "extraction_mode": extraction_mode  #  提取模式信号，仅 stage2 使用
                        },
                        source_content="",
                        doc_type=doc_type,
                        enable_validation=not skip_validation
                    )

                # ✅ 新增：获取 localchunks 和 ragflowchunks
                extraction_result = result.get("extraction_result", {})
                localchunks = extraction_result.get("localchunks", [])

                # ✅ 新增：构建 ragflowchunks（使用 file_ragflow_chunks 原始分块列表）
                ragflowchunks = []
                if file_ragflow_chunks:
                    for chunk in file_ragflow_chunks:
                        # chunk 应该是字典格式
                        if isinstance(chunk, dict):
                            content = chunk.get("content", "")
                            ragflowchunks.append({
                                "id": chunk.get("chunk_id", ""),
                                "used": "true",
                                "title": content[:20] if content else "",
                                "score": str(chunk.get("similarity", ""))
                            })

                result["cf"] = cf
                result["cf_name"] = cf_path.name
                result["sections_cnt"] = sections_cnt
                result["file_id"] = file_id
                result["original_file_name"] = original_file_name
                result["localchunks"] = localchunks  # ✅ 新增
                result["ragflowchunks"] = ragflowchunks  # ✅ 新增
                return result

            # 使用受限并发处理多个文件
            if len(chunks_files) == 1:
                # 单文件直接处理，无需并发
                results_list = [_extract_single_file(chunks_files[0])]
            else:
                # 多文件使用受限并发
                from concurrent.futures import ThreadPoolExecutor, as_completed
                actual_workers = min(max_file_workers, len(chunks_files))
                logger.info(f"📂 多文件并发提取: {len(chunks_files)}个文件, 并发数: {actual_workers}")

                results_list = []
                with ThreadPoolExecutor(max_workers=actual_workers) as executor:
                    future_to_cf = {executor.submit(inherit_context(_extract_single_file), cf): cf for cf in chunks_files}
                    for future in as_completed(future_to_cf):
                        try:
                            result = future.result()
                            results_list.append(result)
                        except Exception as e:
                            import traceback
                            traceback.print_exc()
                            cf = future_to_cf[future]
                            logger.error(f"❌ 文件提取异常: {cf} - {e}", exc_info=True)
                            results_list.append({"success": False, "error": str(e), "cf": cf})

            # 汇总所有文件的结果
            all_placeholders = set()
            for result in results_list:
                cf_name = result.get("cf_name", "unknown")
                sections_cnt = result.get("sections_cnt", 0)

                if result.get("success"):
                    success_count += 1
                    content_i = result.get("extracted_content") or result.get("combined_content") or result.get("content", "")
                    if content_i:
                        # aggregated_content_parts.append(f"## Source: {cf_name}\n\n{content_i}")   #添加了溯源信息，这部分暂不需要
                        aggregated_content_parts.append(content_i)
                        # 新增：从内容中提取占位符
                        placeholders = self._extract_placeholders_from_content(content_i)
                        # 如果内容中没有占位符，尝试从原始分块中提取
                        if not placeholders:
                            cf = result.get("cf")
                            relevant_chunks = []
                            er = result.get("extraction_result", {})
                            if er.get("chunks_used"):
                                relevant_chunks = [c.get("chunk_id") for c in er.get("chunks_used", []) if c.get("chunk_id")]
                            if cf and relevant_chunks:
                                placeholders = self._extract_placeholders_from_chunks(cf, relevant_chunks)
                        all_placeholders.update(placeholders)
                    _er = result.get("extraction_result", {}) if isinstance(result.get("extraction_result"), dict) else {}
                    try:
                        _ids = [c.get("chunk_id") for c in (_er.get("chunks_used", []) or []) if c.get("chunk_id")]
                        aggregated_ids.extend(_ids)
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        pass
                    total_sections_sum += sections_cnt
                    full_prompts.append(_er.get("full_prompt", result.get("full_prompt", "")) or "")
                    selected_sources.append(_er.get("selected_chunks_content", result.get("selected_chunks_content", "")) or "")
                    per_file_extraction_results.append(result.get("extraction_result", {}))
                else:
                    logger.error(f"❌ 两阶段提取失败: {result.get('error')}", exc_info=True)
                    per_file_extraction_results.append({"success": False, "error": result.get("error")})

            if success_count == 0:
                # ✅ 记录详细的失败原因
                all_errors = []
                for i, result in enumerate(results_list):
                    if not result.get("success"):
                        err_info = {
                            "file": result.get("cf", f"file_{i}"),
                            "error": result.get("error", "未知错误"),
                            "error_type": result.get("error_type", ""),
                        }
                        # 如果有extraction_result，提取更详细的错误信息
                        er = result.get("extraction_result", {})
                        if isinstance(er, dict):
                            if er.get("error"):
                                err_info["extraction_error"] = er.get("error")
                            if er.get("stage1_result"):
                                s1 = er.get("stage1_result", {})
                                if not s1.get("success"):
                                    err_info["stage1_error"] = s1.get("error", "筛选阶段失败")
                        all_errors.append(err_info)

                logger.error(f"❌ {doc_type.upper()}提取失败：所有{len(results_list)}个文件均失败")
                for err in all_errors:
                    logger.error(f"   - 文件: {err.get('file')}, 错误: {err.get('error')}")
                    if err.get('extraction_error'):
                        logger.error(f"     提取错误: {err.get('extraction_error')}")
                    if err.get('stage1_error'):
                        logger.error(f"     筛选阶段错误: {err.get('stage1_error')}")

                error_data = {
                    "status": "error",
                    "error_type": "ALL_FILES_FAILED",
                    "error": f"{doc_type.upper()}提取失败：所有文件均失败",
                    "content": "",
                    "detailed_errors": all_errors,  # 添加详细错误信息
                    "per_file_results": per_file_extraction_results  # 添加每个文件的结果
                }
                if quote:
                    error_data["quote"] = quote
                return error_data

            aggregated_content = "\n\n".join([p for p in aggregated_content_parts if p])

            # 清理占位符（移除 Start-End 之间的内容，只保留 Start 标签）
            if aggregated_content:
                aggregated_content = self._clean_placeholder_content(aggregated_content)

            # 建立原始文件名 -> content 的映射
            per_file_contents = {}
            for result in results_list:
                content_i = result.get("extracted_content") or result.get("combined_content") or result.get("content", "")
                original_name = result.get("original_file_name", "")
                if result.get("success") and content_i and original_name:
                    per_file_contents[original_name] = content_i

            # ✅ 新增：构建 per_file_chunks（按文件分开的分块信息，含独立 content）
            # ✅ 修改：无论成功还是失败，都要加入 per_file_chunks
            per_file_chunks = []
            for result in results_list:
                original_name = result.get("original_file_name", "") or result.get("cf_name", "未知文件")
                if result.get("success"):
                    # 获取每个文件的独立提取内容
                    file_content = result.get("extracted_content") or result.get("combined_content") or result.get("content", "")
                    per_file_chunks.append({
                        "filename": original_name,
                        "content": file_content,
                        "localchunks": result.get("localchunks", []),
                        "ragflowchunks": result.get("ragflowchunks", [])
                    })
                else:
                    # ✅ 新增：失败的文件也要记录
                    per_file_chunks.append({
                        "filename": original_name,
                        "content": "",  # 失败时为空字符串
                        "localchunks": [],
                        "ragflowchunks": []
                    })

            return_data = {
                "status": "success",
                "content": aggregated_content,
                "method": f"{doc_type}_per_file_extraction_with_validation_{'original' if original_mode else 'standard'}",
                "data_type": doc_type,
                "chunks_used": len(aggregated_ids),
                "chunks_used_sections": aggregated_ids,
                "total_sections": total_sections_sum if total_sections_sum else None,
                "source_files_count": len(chunks_files),
                "source_content": "\n\n".join([s for s in selected_sources if s]),
                "full_prompt": "\n\n".join([fp for fp in full_prompts if fp]),
                "is_validated": True,
                "extraction_result": {
                    "per_file_results": per_file_extraction_results
                },
                "per_file_contents": per_file_contents,
                "per_file_chunks": per_file_chunks  # ✅ 新增：按文件分开的分块信息
            }
            # 添加 file_name 字段（从 data_item 获取）
            if data_item and "file_name" in data_item:
                return_data["file_name"] = data_item["file_name"]
            if quote:
                return_data["quote"] = quote
            # 添加占位符
            if insert_original and all_placeholders:
                    return_data["placeholders"] = list(all_placeholders)

            return return_data

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"{doc_type.upper()}文档提取失败: {e}", exc_info=True)
            _task_log_error(f"{doc_type.upper()}文档提取失败", exc=e, doc_type=doc_type)
            error_data = {"status": "error", "error_type": "DOC_EXTRACTION_FAILED", "error": str(e), "content": ""}
            if quote:
                error_data["quote"] = quote
            return error_data

    def _handle_pdf_type(self, data_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理PDF文档类型的数据提取

        【RAGFlow 支持】
        - 使用 per_file_ragflow_list（每个文件独立检索的 RAGFlow 内容）
        """
        # 处理PDF文件,处理逻辑同word处理逻辑
        extract_prompt = data_item.get("extract", "")
        original_mode = data_item.get("original_mode", False)
        insert_original = data_item.get("insert_original", False)
        chunks_file = data_item.get("chunks_file", "")
        quote = data_item.get("quote")
        per_file_ragflow_list = data_item.get("per_file_ragflow_list", [])

        # 如果无提取逻辑，并且是原文模式，则提取全部内容
        if (not extract_prompt or not extract_prompt.strip()) and original_mode:
            logger.info("无提取逻辑且为原文模式，直接加载全部分块内容")
            return self._load_all_chunks_content(chunks_file, quote=quote, doc_type="pdf", insert_original=insert_original)

        # 有提取逻辑，调用统一的提取入口
        return self._extract_from_chunks(
            chunks_file, extract_prompt, "pdf",
            original_mode=original_mode, quote=quote, insert_original=insert_original,
            per_file_ragflow_list=per_file_ragflow_list, data_item=data_item
        )

    def _handle_excel_type(self, data_item: Dict[str, Any]) -> Dict[str, Any]:
        """处理Excel表格（带智能校验）"""
        extract_prompt = data_item.get("extract", "")
        insert_original = data_item.get("insert_original", False)
        source_file = data_item.get("source_file", "")
        markdown_files = data_item.get("markdown_files", [])
        file_type = data_item.get("file_type", "excel")

        source_file_list = data_item.get("source_file", [])

        #  插入图表   提取内容 + 构建TFL插入占位符
        if insert_original:

            # 情况1：无提取提示词 → 只构建TFL占位符，不提取内容
            if not extract_prompt or not extract_prompt.strip():
                if source_file_list and isinstance(source_file_list, list):
                    # 列表格式：从source_file列表构建TFL占位符
                    return self._build_tfl_insert_mappings(data_item, file_type)
                else:
                    # 单个文件格式：也构建TFL占位符
                    return self._build_tfl_insert_mappings(data_item, file_type)

            # 情况2：有提取提示词 → 提取内容 + 构建占位符
            # 🔑 关键：先提取内容（给LLM作参考），再构建占位符
            logger.info(f"🔍  有extract提示词，开始提取内容...")
            logger.info(f"   - markdown_files数量: {len(markdown_files)}")
            logger.info(
                f"   - source_file_list数量: {len(source_file_list) if isinstance(source_file_list, list) else 0}")

            extracted_content = self._extract_from_markdown_files(markdown_files, extract_prompt, source_file,
                                                                  file_type, data_item=data_item)

            if extracted_content.get("status") != "success":
                logger.warning(f"⚠️ 提取失败: {extracted_content.get('error', 'Unknown error')}")
                return extracted_content  # 提取失败，直接返回错误

            extracted_text = extracted_content.get("content", "")
            logger.info(f"✅ 提取成功: {len(extracted_text)}字符")

            # 构建TFL占位符映射
            if source_file_list and isinstance(source_file_list, list):
                # 新格式：从 source_file 列表构建
                tfl_mappings_result = self._build_tfl_insert_mappings(data_item, file_type)
                tfl_mappings = tfl_mappings_result.get("tfl_insert_mappings", [])
            else:
                # 旧格式：单个占位符
                tfl_mappings = []

            logger.info(f"✅ [插入图表] 构建TFL占位符: {len(tfl_mappings)}个")

            # 返回：提取的内容 + TFL映射
            return {
                "status": "success",
                "content": extracted_text,  # ✅ 有内容，给LLM用
                "data_type": file_type,
                "is_original": True,
                "tfl_insert_mappings": tfl_mappings,  # ✅ 有占位符映射，用于插入
                "extract": extract_prompt,
                "extract_item": extract_prompt,
                "source_file": source_file_list,
                "message": f"插入图表：已提取内容({len(extracted_text)}字符)并构建{len(tfl_mappings)}个TFL占位符"
            }

        # 非原文模式：正常提取
        # if not extract_prompt:
        #     return {"status": "error", "error": "缺少提取提示词", "content": ""}

        return self._extract_from_markdown_files(markdown_files, extract_prompt, source_file, file_type, data_item=data_item)

    def _extract_from_markdown_files(self, markdown_files: list, extract_prompt: str, source_file: str,
                                     doc_type: str, data_item: Dict[str, Any] = None) -> Dict[str, Any]:
        """从Markdown文件列表中提取内容

        Args:
            markdown_files: Markdown文件路径列表，支持两种格式：
                - 字典列表（新格式）：[{"markdown_files": [...], "file_id": "...", "original_file_name": "..."}, ...]
                - 字符串列表（旧格式）：["AAA/.../sheet1.md", ...]
            extract_prompt: 提取提示词
            source_file: 源文件名
            doc_type: 文档类型
            data_item: 数据项字典（可选）
        """
        try:
            from service.linux.generation.extraction.excel_extraction_service import excel_extraction_service
            from service.linux.generation.extraction.validated_extraction_service import validated_extraction_service
            from pathlib import Path

            # ✅ 在方法开始时设置 paragraph_id（确保并发线程获取正确的值）
            paragraph_id = getattr(self._context, 'paragraph_id', 'unknown')
            from utils.context_manager import set_paragraph_id
            set_paragraph_id(paragraph_id)
            logger.info(f"📌 设置段落ID: {paragraph_id}")

            if not markdown_files:
                return {"status": "error", "error_type": "MISSING_MARKDOWN_FILES", "error": "缺少markdown_files字段", "content": ""}

            # 判断输入格式，支持字典结构和字符串列表两种格式
            file_groups = []
            if isinstance(markdown_files[0], dict):
                # 新格式：字典结构列表
                for group in markdown_files:
                    file_groups.append({
                        "markdown_dir": group.get("markdown_dir", ""),
                        "markdown_files": group.get("markdown_files", []),
                        "file_id": group.get("file_id", ""),
                        "original_file_name": group.get("original_file_name", "")
                    })
            else:
                # 旧格式：字符串列表，按目录分组
                md_paths = [Path(p) for p in markdown_files]
                seen = set()
                for p in md_paths:
                    parent = p.parent
                    key = str(parent)
                    if key not in seen:
                        seen.add(key)
                        file_groups.append({
                            "markdown_dir": str(parent),
                            "markdown_files": [str(p) for p in md_paths if p.parent == parent],
                            "file_id": "",
                            "original_file_name": ""
                        })

            logger.info(f"📊 准备处理 {len(file_groups)} 个文件组")

            aggregated_parts: List[str] = []
            per_file_sheets_info: Dict[str, List[Dict[str, Any]]] = {}  # 按文件分开存储 sheets_info
            excel_results: List[Dict[str, Any]] = []
            full_prompts: List[str] = []
            file_names_list: List[str] = []  # 记录每个文件组的原始文件名
            validated_any = False
            success_any = False

            # 检查是否跳过校验（通过环境变量配置）
            # ⚠️ 默认值改为 "1"（跳过校验），避免环境变量未设置或并发覆盖时意外启用校验
            skip_validation = os.getenv("SKIP_EXTRACTION_VALIDATION", "1").strip().lower() in (
            "1", "true", "yes", "y", "on")
            if skip_validation:
                logger.info("📌 已配置跳过提取校验 (SKIP_EXTRACTION_VALIDATION=1 或默认)")

            for idx, group in enumerate(file_groups, 1):
                markdown_dir = group.get("markdown_dir", "")
                md_files = group.get("markdown_files", [])
                original_file_name = group.get("original_file_name", "")

                logger.info(f"📂 处理第 {idx}/{len(file_groups)} 个文件组: {original_file_name}")

                # ✅ 获取当前 paragraph_id，传递给 Excel 提取（确保并发时文件保存到正确目录）
                from utils.context_manager import get_paragraph_id
                current_paragraph_id = get_paragraph_id("unknown")

                result = validated_extraction_service.extract_with_validation(
                    extraction_func=excel_extraction_service.extract_from_excel,
                    extraction_kwargs={
                        "excel_dir": markdown_dir,
                        "extraction_query": extract_prompt,
                        "source_file": None,
                        "paragraph_id": current_paragraph_id  # ✅ 传入 paragraph_id
                    },
                    source_content=self._load_source_content_from_files(md_files),
                    doc_type=doc_type,
                    enable_validation=not skip_validation
                )
                if result.get("success"):
                    success_any = True
                    if result.get("is_validated"):
                        validated_any = True
                    content_i = result.get("extracted_content") or result.get("combined_content") or result.get("content", "")
                    # ✅ 修改：无论是否有内容，都要添加到 aggregated_parts 和 file_names_list
                    aggregated_parts.append(content_i if content_i else "")
                    file_names_list.append(original_file_name)  # ✅ 修改：总是记录原始文件名
                    if content_i:
                        logger.info(f"✅ 文件组 {idx} 提取成功: {len(content_i)} 字符")
                    else:
                        logger.info(f"⚠️ 文件组 {idx} 提取成功但内容为空")
                    _er = result.get("extraction_result", {}) if isinstance(result.get("extraction_result"), dict) else {}
                    # 按文件分开存储 sheets_results
                    sheets_results_i = _er.get("sheets_results", [])
                    if original_file_name and sheets_results_i:
                        per_file_sheets_info[original_file_name] = sheets_results_i
                    excel_results.append(_er)
                    full_prompts.append(result.get("full_prompt", "") or _er.get("full_prompt", "") or "")
                else:
                    # ✅ 修改：失败时也要添加空内容占位
                    aggregated_parts.append("")
                    excel_results.append({"success": False, "error": result.get("error")})
                    file_names_list.append(original_file_name)

            if not success_any:
                return {"status": "error", "error_type": "ALL_DIRS_FAILED", "error": f"{doc_type.upper()}提取失败：所有文件组均失败", "content": ""}

            logger.info(f"📊 提取汇总: 共处理 {len(file_groups)} 个文件组，成功 {len(aggregated_parts)} 个，总内容长度 {sum(len(p) for p in aggregated_parts)} 字符")

            # 建立原始文件名 -> content 的映射
            per_file_contents = {}
            for idx, original_name in enumerate(file_names_list):
                if idx < len(aggregated_parts) and aggregated_parts[idx] and original_name:
                    per_file_contents[original_name] = aggregated_parts[idx]

            # ✅ 新增：构建 per_file_chunks（表格类，按文件分开的分块信息，含独立 content）
            per_file_chunks = []
            for idx, original_name in enumerate(file_names_list):
                # ✅ 修改：无论成功还是失败，都要加入 per_file_chunks
                file_content = ""
                localchunks = []

                if idx < len(excel_results):
                    _er = excel_results[idx]
                    if isinstance(_er, dict):
                        if _er.get("success"):
                            # 构建 localchunks（表格类，每个 sheet 作为分块）
                            sheets_results_i = _er.get("sheets_results", [])
                            for sheet in sheets_results_i:
                                localchunks.append({
                                    "id": sheet.get("sheet_name", ""),
                                    "used": "true" if sheet.get("available") else "false",
                                    "title": sheet.get("sheet_title", ""),
                                    "score": str(sheet.get("relevance_score", 0)),
                                    "reason": sheet.get("reason", "")
                            })
                            # 获取每个文件的独立提取内容
                            file_content = aggregated_parts[idx] if idx < len(aggregated_parts) else ""
                        else:
                            # ✅ 新增：失败的文件也要记录，只是内容为空
                            localchunks = []
                            file_content = ""

                per_file_chunks.append({
                    "filename": original_name,
                    "content": file_content,  # 失败时为空字符串
                    "localchunks": localchunks,
                    "ragflowchunks": []  # 表格类无 RAGFlow 分块
                })

            return {
                "status": "success",
                "content": "\n\n====\n\n".join([p for p in aggregated_parts if p]),
                "method": f"{doc_type}_multi_excel_aggregation_with_validation",
                "data_type": doc_type,
                "is_validated": validated_any,
                "full_prompt": "\n\n".join([fp for fp in full_prompts if fp]),
                "extraction_result": {
                    "excel_results": excel_results,
                    "per_file_sheets_info": per_file_sheets_info
                },
                "per_file_sheets_info": per_file_sheets_info,  # 按文件分开的 sheets_info
                "per_file_contents": per_file_contents,
                "file_name": file_names_list,
                "per_file_chunks": per_file_chunks  # ✅ 新增：按文件分开的分块信息
            }

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"{doc_type.upper()}提取失败: {e}", exc_info=True)
            _task_log_error(f"{doc_type.upper()}提取失败", exc=e, doc_type=doc_type)
            return {"status": "error", "error_type": "EXTRACTION_FAILED", "error": str(e), "content": ""}

    def _load_source_content_from_files(self, markdown_files: list) -> str:
        """从Markdown文件列表加载源内容用于校验"""
        try:
            from pathlib import Path
            content_parts = []

            for md_file_path in markdown_files:
                md_file = Path(md_file_path)
                if md_file.exists():
                    sheet_content = md_file.read_text(encoding='utf-8', errors='ignore')
                    content_parts.append(f"### {md_file.stem}\n{sheet_content}\n")

            return "\n---\n".join(content_parts)
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"加载源内容失败: {e}")
            return ""

    def _build_tfl_insert_mappings(self, data_item: Dict[str, Any], file_type: str) -> Dict[str, Any]:
        """构建TFL插入占位符映射

        新规则：当 file_type in ['rtf', 'xlsx'] 且 insert_original == True 时，
        从 source_file 列表构建占位符映射

        支持两种 source_file 格式：
        1. 字符串列表: ["AAA/path/to/file.rtf", "AAA/path/to/file2.rtf"]
        2. 字典列表: [{"file.xlsx": "data/file.xlsx"}]
        """
        from pathlib import Path

        source_file_list = data_item.get("source_file", [])
        tfl_insert_mappings = []
        extract_prompt = data_item.get("extract", "")

        for source_item in source_file_list:
            try:
                if isinstance(source_item, dict):
                    # 旧格式：字典 {"test_excel.xlsx": "data\\test_excel.xlsx"}
                    for file_name, file_path in source_item.items():
                        stem = Path(file_name).stem  # 去掉后缀
                        placeholder = f"{{{{TFL_{stem}}}}}"
                        abs_path = Path(file_path).absolute()

                        tfl_insert_mappings.append({
                            "Placeholder": placeholder,
                            "Path": str(abs_path),
                            "Source": file_name
                        })
                        logger.info(f"✅ 构建TFL占位符(字典格式): {placeholder} -> {file_name}")

                elif isinstance(source_item, str):
                    # 新格式：字符串路径 "AAA/path/to/file.rtf" 或 "AAA\\path\\to\\file.rtf"
                    file_path = source_item

                    # 🆕 统一路径分隔符，处理Windows和Linux混合路径
                    normalized_path = file_path.replace("\\", "/")

                    # 🆕 正确获取文件名（处理混合分隔符）
                    file_name = normalized_path.split("/")[-1]  # 取最后一部分作为文件名
                    stem = file_name.rsplit(".", 1)[0]  # 去掉后缀
                    placeholder = f"{{{{TFL_{stem}}}}}"

                    # 🆕 保留AAA相对路径格式，不转换为绝对路径
                    # 路径应该是 "AAA/project_data/..." 格式，Windows Bridge会处理
                    if normalized_path.startswith("AAA/"):
                        final_path = normalized_path  # 保持相对路径
                    elif normalized_path.startswith("/AAA/"):
                        final_path = normalized_path[1:]  # 去掉开头的 /
                    else:
                        final_path = normalized_path  # 保持原样

                    tfl_insert_mappings.append({
                        "Placeholder": placeholder,
                        "Path": final_path,
                        "Source": file_name
                    })
                    logger.info(f"✅ 构建TFL占位符(字符串格式): {placeholder} -> {file_name} (路径: {final_path})")
                else:
                    logger.warning(f"⚠️ 未知的source_item格式: {type(source_item)}")

            except Exception as e:
                import traceback
                traceback.print_exc()
                logger.warning(f"构建TFL占位符失败: {source_item} - {e}")

        logger.info(f"📊 TFL占位符构建完成: 共 {len(tfl_insert_mappings)} 个")

        return {
            "status": "success",
            "content": "",  # 原文模式，内容为空
            "data_type": file_type,
            "is_original": True,
            "tfl_insert_mappings": tfl_insert_mappings,  # ⭐ 关键字段
            "extract": extract_prompt,
            "extract_item": extract_prompt,
            "source_file": source_file_list,
            "message": f"原文模式：已构建 {len(tfl_insert_mappings)} 个TFL占位符"
        }

    def _handle_rtf_type(self, data_item: Dict[str, Any]) -> Dict[str, Any]:
        """处理RTF文件"""
        # RTF文件已转换为Excel格式，使用相同的处理逻辑
        # 设置 file_type 为 rtf
        data_item["file_type"] = "rtf"
        return self._handle_excel_type(data_item)



    def _extract_placeholders_from_content(self, content: str) -> List[str]:
        """从内容中提取占位符 ({{Table_X_Start}} 和 {{Image_X_Start}} 等)"""
        import re
        if not content:
            return []
        # 匹配各种占位符格式
        # 1. 标准格式: {{Table_1_Start}}, {{Image_2_Start}}
        # 2. 可能存在的单括号格式: {Table_1_Start}
        pattern = re.compile(r'\{*((Table|Image)_\d+_Start)\}*')
        matches = pattern.findall(content)

        # 去重并标准化为双括号格式
        placeholders = set()
        for match in matches:
            if match and match[0]:
                placeholder = f"{{{{{match[0]}}}}}"  # 统一为双括号
                placeholders.add(placeholder)
        return list(placeholders)

    def _extract_placeholders_from_chunks(self, chunks_file: str, relevant_chunks: List[str]) -> List[str]:
        """从分块文件中提取占位符"""
        try:
            import json
            from pathlib import Path
            if not chunks_file:
                return []
            # 加载分块文件
            cf_path = Path(chunks_file) if isinstance(chunks_file, str) else Path(chunks_file[0])
            if not cf_path.exists():
                return []
            with open(cf_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # 获取所有分块
            sections = data.get('sections', data.get('chunks', []))
            # 如果指定了相关分块，只检查这些分块
            if relevant_chunks:
                sections = [s for s in sections if s.get('section_id') in relevant_chunks]
            # 从每个分块的内容中提取占位符
            all_placeholders = set()
            for section in sections:
                content = section.get('content', '')
                placeholders = self._extract_placeholders_from_content(content)
                all_placeholders.update(placeholders)
            return list(all_placeholders)
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"从分块提取占位符失败: {e}")
            return []
    def _clean_placeholder_content(self, content: str) -> str:
        """
        清理占位符内容（原文模式专用）

        功能：
        1. 先标准化占位符格式：将单花括号 {Table_1_Start} 修复为双花括号 {{Table_1_Start}}
        2. 再执行清理：将 {{Table_1_Start}}...{{Table_1_End}} 替换为 {{Table_1_Start}}
        这样插入时可以根据 {{Table_1_Start}} 等占位符找到对应的资源文件进行替换

        Args:
            content: 原始内容（可能包含单括号或双括号的占位符）
        Returns:
            清理后的内容（只保留Start标签，且为双括号格式）
        """
        import re

        try:
            cleaned = content

            # ===== 第一步：统一占位符格式 =====
            # 匹配所有可能的占位符变体：{Table_X_Start}、{{Table_X_Start}}、{{{Table_X_Start}}}
            # 以及对应的End标签
            # 匹配模式：捕获花括号内的核心内容 (Table|Image)_\d+_(Start|End)
            unified_pattern = re.compile(r'\{*((Table|Image)_(\d+)(?:_(\d+))?_(Start|End))\}*')            # 统计修复数量
            matches = unified_pattern.findall(cleaned)
            # 去重统计（避免重复计数）
            unique_matches = set()
            for match in matches:
                if match and match[0]:
                    unique_matches.add(match[0])
            if unique_matches:
                # 替换为统一的双花括号格式
                cleaned = unified_pattern.sub(r'{{\1}}', cleaned)
                logger.info(f"🔧 统一占位符格式: {len(unique_matches)}个占位符已转为双括号")
            # ===== 第二步：清理占位符内容 =====
            # 清理表格标签：{{Table_X_Start}}...{{Table_X_End}} → {{Table_X_Start}}
            table_pattern = re.compile(
                r'\{\{Table_(\d+)(?:_(\d+))?_Start\}\}[\s\S]*?\{\{Table_\1(?:_\2)?_End\}\}',
                flags=re.DOTALL
            )
            # 统计清理前的匹配数
            before_table_count = len(table_pattern.findall(cleaned))
            # 执行清理
            cleaned = table_pattern.sub(lambda m: f'{{{{Table_{m.group(1)}{"_"+m.group(2) if m.group(2) else ""}_Start}}}}', cleaned)

            # 清理图片标签：{{Image_X_Start}}...{{Image_X_End}} → {{Image_X_Start}}
            image_pattern = re.compile(
                r'\{\{Image_(\d+)(?:_(\d+))?_Start\}\}[\s\S]*?\{\{Image_\1(?:_\2)?_End\}\}',
                flags=re.DOTALL
            )
            # 统计清理前的匹配数
            before_image_count = len(image_pattern.findall(cleaned))
            # 执行清理
            cleaned = image_pattern.sub(lambda m: f'{{{{Image_{m.group(1)}{"_"+m.group(2) if m.group(2) else ""}_Start}}}}', cleaned)

            # ===== 第三步：统计和日志 =====
            table_count = before_table_count
            image_count = before_image_count

            if table_count > 0 or image_count > 0:
                logger.info(f"✅ 清理占位符内容: 表格{table_count}个, 图片{image_count}个")

            return cleaned

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.warning(f"清理占位符内容失败: {e}")
            return content
