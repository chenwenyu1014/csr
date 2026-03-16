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

# 导入耗时记录工具
from utils.timing import Timer, generation_timer


def _task_log_error(message: str, exc: Exception = None, **extra):
    """记录错误到任务日志"""
    task_logger = get_task_logger()
    if task_logger:
        task_logger.error(message, exc=exc, logger_name="data_extractor", **extra)


# Fallback：当 utils.timing 导入失败时使用的空函数
def log_timing(*args, **kwargs): pass

logger = logging.getLogger(__name__)

# 可选依赖：用于读取 Excel 索引表
try:
    import pandas as _pd  # type: ignore
except Exception:  # pragma: no cover
    _pd = None  # type: ignore


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
    
    def __init__(self, base_data_dir: str = "data/rtf&index", index_path: Optional[str] = None, cache_dir: str = "cache"):
        """
        初始化数据提取器

        Args:
            base_data_dir: 基础数据目录，存储待处理的文档文件
            index_path: TFL索引表路径（可选，默认从base_data_dir/index.xlsx加载）
            cache_dir: 缓存目录，用于存储处理后的中间结果
        """
        # 基础数据目录
        self.base_data_dir = Path(base_data_dir)

        # TFL 索引表（相对路径优先，未提供则使用默认位置）
        # 若未显式提供索引路径，则尝试自动加载 data/rtf_files/index.xlsx
        if index_path is None:
            default_idx = (self.base_data_dir / "index.xlsx").resolve()
            self.index_path = default_idx if default_idx.exists() else None
        else:
            self.index_path = Path(index_path)
        # 索引数据框（延迟加载）
        self._index_df = None

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
        # 详细日志系统（可选）
        self.detailed_logger = None
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
                from utils.request_context import get_request_id  # type: ignore
            except Exception as e:
                import traceback
                traceback.print_exc()
                from utils.request_context import get_request_id  # type: ignore
            rid0 = get_request_id()
            if not rid0:
                import os as _os
                rid0 = _os.getenv("CURRENT_REQUEST_ID")
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
                    "error": str(e),
                    "content": None
                }

        from concurrent.futures import ThreadPoolExecutor, as_completed
        items = list(enumerate(paragraph_data["data"]))

        # 限制并发数：取配置值与数据项数量的较小值
        max_workers = min(max_data_item_workers, len(items)) if items else 0
        results_buffer: List[Optional[Dict[str, Any]]] = [None] * len(items)
        # 记录当前请求ID（供聚合线程用来投递事件）
        _rid_for_agg = None
        try:
            try:
                from utils.request_context import get_request_id  # type: ignore
            except Exception as e:
                import traceback
                traceback.print_exc()
                from utils.request_context import get_request_id  # type: ignore
            _rid_for_agg = get_request_id()
        except Exception as e:
            import traceback
            traceback.print_exc()
            _rid_for_agg = None
        if not _rid_for_agg:
            try:
                import os as _os
                _rid_for_agg = _os.getenv("CURRENT_REQUEST_ID")
            except Exception as e:
                import traceback
                traceback.print_exc()
                _rid_for_agg = None

        if max_workers > 0:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_idx = {executor.submit(_worker, i, it): i for i, it in items}
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
        logger.info(f"🔍 [extract_single_data_item] quote字段值: {quote}, 文件类型: {file_type}")

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
        # ✅ 读取处理所需的关键字段
        extract_prompt = data_item.get("extract", "")
        original_mode = data_item.get("original_mode", False)
        insert_original = data_item.get("insert_original", False)
        chunks_file = data_item.get("chunks_file", "")
        quote = data_item.get("quote")  # 获取quote字段，将传递给_extract_from_chunks
        logger.info(f"🔍 [_handle_word_type] quote字段值: {quote}")

        # 如果无提取逻辑，并且是原文模式，则提取全部内容
        if (not extract_prompt or not extract_prompt.strip()) and original_mode:
            logger.info("无提取逻辑且为原文模式，直接加载全部分块内容")
            return self._load_all_chunks_content(chunks_file, quote=quote, doc_type="word",insert_original=insert_original)

        # 有提取逻辑，调用统一的提取入口，内部会根据 original_mode 决定是否让模型改写内容
        # 原文模式处理insert_original
        return self._extract_from_chunks(chunks_file, extract_prompt, "word", original_mode=original_mode, quote=quote,insert_original=insert_original)

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
                return {"status": "error", "error": "chunks_file格式错误", "content": ""}

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
                    "error": str(e),
                    "content": "",
                    "quote": quote}

    def _extract_from_chunks(self, chunks_file, extract_prompt: str, doc_type: str, original_mode: bool = False,
                             quote: Optional[str] = None,insert_original=False) -> Dict[str, Any]:
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

        Returns:
            提取结果字典，包含 status, content, quote 等字段
        """
        try:
            from pathlib import Path
            import json

            if not chunks_file:
                return {"status": "error", "error": "缺少chunks_file字段", "content": ""}

            # 统一为列表
            if isinstance(chunks_file, str):
                chunks_files = [chunks_file]
            elif isinstance(chunks_file, list):
                chunks_files = [cf for cf in chunks_file if cf]
            else:
                return {"status": "error", "error": f"chunks_file类型不支持: {type(chunks_file)}", "content": ""}

            if not chunks_files:
                return {"status": "error", "error": "chunks_file列表为空", "content": ""}

            # 带校验的两阶段提取（逐文件分别提取后拼接）
            from service.linux.generation.extraction.two_stage_extraction_service import two_stage_extraction_service
            from service.linux.generation.extraction.validated_extraction_service import validated_extraction_service

            # 若是原文模式，在提示词中附加保证提取原文本内容
            if original_mode:
                extraction_query_with_instruction = (
                    f"{extract_prompt}\n\n"
                    "【重要】当前为原文模式，请保留原文措辞，不要改写或总结。"
                )
            else:
                extraction_query_with_instruction = extract_prompt
            # 若是引用图表，在提示词中附加保留占位符说明
            if insert_original:
                extraction_query_with_instruction = (
                    f"{extraction_query_with_instruction}\n\n"
                    "【重要】当前为引用图表模式，请保留原文中的所有占位符标签，如 {{Table_1_Start}}、{{Table_1_End}}、"
                    "{{Image_1_Start}}、{{Image_1_End}} 等。不要删除或修改这些标签，保持原样。\n"
                )

            # 环境上下文：段落ID
            import os
            paragraph_id = getattr(self._context, 'paragraph_id', 'unknown')
            os.environ['CURRENT_PARAGRAPH_ID'] = paragraph_id

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

            def _extract_single_file(cf: str) -> Dict[str, Any]:
                """单个文件的提取任务"""
                cf_path = Path(cf)
                if not cf_path.exists():
                    logger.warning(f"分块文件不存在: {cf}")
                    return {"success": False, "error": f"文件不存在: {cf}", "cf": cf}

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
                            "doc_type": doc_type
                        },
                        source_content="",
                        doc_type=doc_type,
                        enable_validation=not skip_validation
                    )

                result["cf"] = cf
                result["cf_name"] = cf_path.name
                result["sections_cnt"] = sections_cnt
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
                    future_to_cf = {executor.submit(_extract_single_file, cf): cf for cf in chunks_files}
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
                        aggregated_content_parts.append(f"## Source: {cf_name}\n\n{content_i}")
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
                    "error": f"{doc_type.upper()}提取失败：所有文件均失败",
                    "content": "",
                    "detailed_errors": all_errors,  # 添加详细错误信息
                    "per_file_results": per_file_extraction_results  # 添加每个文件的结果
                }
                if quote:
                    error_data["quote"] = quote
                return error_data

            aggregated_content = "\n\n---\n\n".join([p for p in aggregated_content_parts if p])

            # 清理占位符（移除 Start-End 之间的内容，只保留 Start 标签）
            if aggregated_content:
                aggregated_content = self._clean_placeholder_content(aggregated_content)

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
                }
            }
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
            error_data = {"status": "error", "error": str(e), "content": ""}
            if quote:
                error_data["quote"] = quote
            return error_data

    def _handle_pdf_type(self, data_item: Dict[str, Any]) -> Dict[str, Any]:
        # 处理PDF文件,处理逻辑同word处理逻辑
        extract_prompt = data_item.get("extract", "")
        original_mode = data_item.get("original_mode", False)
        insert_original = data_item.get("insert_original", False)
        chunks_file = data_item.get("chunks_file", "")
        quote = data_item.get("quote")  # 获取quote字段，将传递给_extract_from_chunks
        logger.info(f"🔍 [_handle_word_type] quote字段值: {quote}")

        # 如果无提取逻辑，并且是原文模式，则提取全部内容
        if (not extract_prompt or not extract_prompt.strip()) and original_mode:
            logger.info("无提取逻辑且为原文模式，直接加载全部分块内容")
            return self._load_all_chunks_content(chunks_file, quote=quote, doc_type="pdf",insert_original=insert_original)

        # 有提取逻辑，调用统一的提取入口，内部会根据 original_mode 决定是否让模型改写内容
        # 原文模式处理
        return self._extract_from_chunks(chunks_file, extract_prompt, "pdf", original_mode=original_mode, quote=quote,insert_original=insert_original)

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
            # if not extract_prompt or not extract_prompt.strip():
            #     if source_file_list and isinstance(source_file_list, list):
            #         # 列表格式：从source_file列表构建TFL占位符
            #         return self._build_tfl_insert_mappings(data_item, file_type)
            #     else:
            #         # 单个文件格式：也构建TFL占位符
            #         return self._build_tfl_insert_mappings(data_item, file_type)

            # 情况2：有提取提示词 → 提取内容 + 构建占位符
            # 🔑 关键：先提取内容（给LLM作参考），再构建占位符
            logger.info(f"🔍  有extract提示词，开始提取内容...")
            logger.info(f"   - extract_prompt: {extract_prompt[:100]}...")
            logger.info(f"   - markdown_files数量: {len(markdown_files)}")
            logger.info(
                f"   - source_file_list数量: {len(source_file_list) if isinstance(source_file_list, list) else 0}")

            extracted_content = self._extract_from_markdown_files(markdown_files, extract_prompt, source_file,
                                                                  file_type)

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

        return self._extract_from_markdown_files(markdown_files, extract_prompt, source_file, file_type)

    def _extract_from_markdown_files(self, markdown_files: list, extract_prompt: str, source_file: str,
                                     doc_type: str) -> Dict[str, Any]:
        """从Markdown文件列表中提取内容"""
        try:
            from service.linux.generation.extraction.excel_extraction_service import excel_extraction_service
            from service.linux.generation.extraction.validated_extraction_service import validated_extraction_service
            from pathlib import Path

            if not markdown_files:
                return {"status": "error", "error": "缺少markdown_files字段", "content": ""}

            # 将所有 md 文件按父目录分组（每个目录代表一个Excel/RTF）
            md_paths = [Path(p) for p in markdown_files]
            dirs_in_order: List[Path] = []
            seen = set()
            for p in md_paths:
                parent = p.parent
                key = str(parent)
                if key not in seen:
                    seen.add(key)
                    dirs_in_order.append(parent)

            # ✅ 改进日志：显示将要处理的所有目录
            logger.info(f"📊 准备处理 {len(dirs_in_order)} 个目录（对应 {len(markdown_files)} 个 markdown 文件）")
            for i, d in enumerate(dirs_in_order, 1):
                dir_label = d.parent.name if d.name == "markdown" else d.name
                logger.info(f"  {i}. {dir_label} ({d})")

            aggregated_parts: List[str] = []
            aggregated_sheets_results: List[Dict[str, Any]] = []
            excel_results: List[Dict[str, Any]] = []
            full_prompts: List[str] = []
            validated_any = False
            success_any = False

            # 检查是否跳过校验（通过环境变量配置）
            # ⚠️ 默认值改为 "1"（跳过校验），避免环境变量未设置或并发覆盖时意外启用校验
            skip_validation = os.getenv("SKIP_EXTRACTION_VALIDATION", "1").strip().lower() in (
            "1", "true", "yes", "y", "on")
            if skip_validation:
                logger.info("📌 已配置跳过提取校验 (SKIP_EXTRACTION_VALIDATION=1 或默认)")

            for idx, d in enumerate(dirs_in_order, 1):
                md_in_dir = [str(p) for p in md_paths if p.parent == d]
                # ✅ 改进日志：显示正在处理的目录序号和完整路径
                logger.info(f"📂 处理第 {idx}/{len(dirs_in_order)} 个目录: {d}")
                result = validated_extraction_service.extract_with_validation(
                    extraction_func=excel_extraction_service.extract_from_excel,
                    extraction_kwargs={
                        "excel_dir": str(d),
                        "extraction_query": extract_prompt,
                        "source_file": None
                    },
                    source_content=self._load_source_content_from_files(md_in_dir),
                    doc_type=doc_type,
                    enable_validation=not skip_validation  # 如果skip_validation=True，则enable_validation=False
                )
                if result.get("success"):
                    success_any = True
                    if result.get("is_validated"):
                        validated_any = True
                    content_i = result.get("extracted_content") or result.get("combined_content") or result.get(
                        "content", "")
                    if content_i:
                        # ✅ 改进标识：使用完整路径的父目录名（预处理目录名）而非只用 markdown
                        dir_label = d.parent.name if d.name == "markdown" else d.name
                        aggregated_parts.append(f"## 来源: {dir_label}\n\n{content_i}")
                        logger.info(f"✅ 目录 {idx} 提取成功: {len(content_i)} 字符")
                    _er = result.get("extraction_result", {}) if isinstance(result.get("extraction_result"),
                                                                            dict) else {}
                    aggregated_sheets_results.extend(_er.get("sheets_results", []))
                    excel_results.append(_er)
                    full_prompts.append(result.get("full_prompt", "") or _er.get("full_prompt", "") or "")
                else:
                    excel_results.append({"success": False, "error": result.get("error")})

            if not success_any:
                return {"status": "error", "error": f"{doc_type.upper()}提取失败：所有目录均失败", "content": ""}

            # ✅ 改进日志：显示最终汇总结果
            logger.info(
                f"📊 提取汇总: 共处理 {len(dirs_in_order)} 个目录，成功 {len(aggregated_parts)} 个，总内容长度 {sum(len(p) for p in aggregated_parts)} 字符")

            return {
                "status": "success",
                "content": "\n\n====\n\n".join([p for p in aggregated_parts if p]),
                "method": f"{doc_type}_multi_excel_aggregation_with_validation",
                "data_type": doc_type,
                "is_validated": validated_any,
                "full_prompt": "\n\n".join([fp for fp in full_prompts if fp]),
                "extraction_result": {
                    "excel_results": excel_results,
                    "sheets_results": aggregated_sheets_results
                },
                "sheets_results": aggregated_sheets_results
            }

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"{doc_type.upper()}提取失败: {e}", exc_info=True)
            _task_log_error(f"{doc_type.upper()}提取失败", exc=e, doc_type=doc_type)
            return {"status": "error", "error": str(e), "content": ""}

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

        logger.info(f"🔍 [_build_tfl_insert_mappings] source_file_list类型: {type(source_file_list)}")
        logger.info(f"🔍 [_build_tfl_insert_mappings] source_file_list内容: {source_file_list}")

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

    # # ---------- 索引与文件定位 ----------
    # def _read_file_generic(self, file_path: Path) -> str:
    #     """读取文件内容，支持 RTF/TXT，其他类型做最简兜底。"""
    #     try:
    #         suffix = file_path.suffix.lower()
    #         if suffix == ".rtf":
    #             try:
    #                 from striprtf.striprtf import rtf_to_text  # type: ignore
    #                 with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
    #                     return rtf_to_text(f.read())
    #             except Exception:
    #                 return self._simple_text_read(file_path)
    #         elif suffix in (".txt", ".log"):
    #             return self._simple_text_read(file_path)
    #         else:
    #             # 其他类型先返回文件名占位
    #             return f"[未实现的文件类型读取] {file_path.name}"
    #     except Exception as e:
    #         logger.warning("读取文件失败 %s: %s", str(file_path), e)
    #         return ""
    #
    # def _simple_text_read(self, file_path: Path) -> str:
    #     try:
    #         with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
    #             return f.read()
    #     except Exception:
    #         return ""
    #
    # # [死代码已清理] 原有的旧版原文处理函数已删除，现使用 _extract_from_chunks + _clean_placeholder_content
    #
    # def _clean_placeholder_content(self, content: str) -> str:
    #     """
    #     清理占位符内容（原文模式专用）
    #
    #     将 {{Table_1_Start}}...{{Table_1_End}} 替换为 {{Table_1_Start}}
    #     将 {{Image_1_Start}}...{{Image_1_End}} 替换为 {{Image_1_Start}}
    #
    #     这样插入时可以根据 {{Table_1_Start}} 等占位符找到对应的资源文件进行替换
    #
    #     Args:
    #         content: 原始内容（包含Start-End标签对）
    #
    #     Returns:
    #         清理后的内容（只保留Start标签）
    #     """
    #     import re
    #
    #     try:
    #         cleaned = content
    #
    #         # 清理表格标签：{{Table_X_Start}}...{{Table_X_End}} → {{Table_X_Start}}
    #         table_pattern = re.compile(
    #             r'\{\{Table_(\d+)_Start\}\}[\s\S]*?\{\{Table_\1_End\}\}',
    #             flags=re.DOTALL
    #         )
    #         cleaned = table_pattern.sub(r'{{Table_\1_Start}}', cleaned)
    #
    #         # 清理图片标签：{{Image_X_Start}}...{{Image_X_End}} → {{Image_X_Start}}
    #         image_pattern = re.compile(
    #             r'\{\{Image_(\d+)_Start\}\}[\s\S]*?\{\{Image_\1_End\}\}',
    #             flags=re.DOTALL
    #         )
    #         cleaned = image_pattern.sub(r'{{Image_\1_Start}}', cleaned)
    #
    #         # 统计清理数量
    #         table_count = len(table_pattern.findall(content))
    #         image_count = len(image_pattern.findall(content))
    #
    #         if table_count > 0 or image_count > 0:
    #             logger.info(f"✅ 清理占位符内容: 表格{table_count}个, 图片{image_count}个")
    #
    #         return cleaned
    #
    #     except Exception as e:
    #         logger.warning(f"清理占位符内容失败: {e}")
    #         return content

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
            unified_pattern = re.compile(r'\{*((Table|Image)_\d+_(Start|End))\}*')
            # 统计修复数量
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
                r'\{\{Table_(\d+)_Start\}\}[\s\S]*?\{\{Table_\1_End\}\}',
                flags=re.DOTALL
            )
            # 统计清理前的匹配数
            before_table_count = len(table_pattern.findall(cleaned))
            # 执行清理
            cleaned = table_pattern.sub(r'{{Table_\1_Start}}', cleaned)

            # 清理图片标签：{{Image_X_Start}}...{{Image_X_End}} → {{Image_X_Start}}
            image_pattern = re.compile(
                r'\{\{Image_(\d+)_Start\}\}[\s\S]*?\{\{Image_\1_End\}\}',
                flags=re.DOTALL
            )
            # 统计清理前的匹配数
            before_image_count = len(image_pattern.findall(cleaned))
            # 执行清理
            cleaned = image_pattern.sub(r'{{Image_\1_Start}}', cleaned)

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
    #
    # def build_complete_json(self, tfl_data: List[Dict[str, Any]], plan_data: Dict[str, Any], output_path: str) -> Dict[str, Any]:
    #     """构建完整的JSON结构，包含TFL、Plan和Output部分
    #
    #     最终结构（按当前需求）：
    #     - tfl: [{ id, Path }]
    #     - plan: { table: [{ id, Path }], Image: [{ id, Path }] }
    #     - output: 最终Word路径
    #     """
    #     try:
    #         # 构建TFL部分（仅输出 id 与 Path）
    #         tfl_list: List[Dict[str, str]] = []
    #         for item in tfl_data:
    #             if not isinstance(item, dict):
    #                 continue
    #             # 1) insert=true 的映射
    #             mappings = item.get("tfl_insert_mappings")
    #             if mappings and isinstance(mappings, list):
    #                 for m in mappings:
    #                     tfl_list.append({
    #                         "id": m.get("id", ""),
    #                         "Path": m.get("Path", "")
    #                     })
    #                 continue
    #             # 2) 单文件读取成功的条目（read_ok）
    #             if item.get("status") == "read_ok" and item.get("source_file"):
    #                 sf = item.get("source_file", "")
    #                 tfl_list.append({
    #                     "id": (item.get("item") or {}).get("title", "") or item.get("title", ""),
    #                     "Path": sf
    #                 })
    #                 continue
    #             # 3) 聚合结果的来源文件列表
    #             srcs = item.get("tfl_source_files") or []
    #             for sf in srcs:
    #                 tfl_list.append({
    #                     "id": (item.get("item") or {}).get("title", "") or item.get("title", ""),
    #                     "Path": sf
    #                 })
    #
    #         # 仅保留 id 与 Path 字段
    #         complete_json = {
    #             "tfl": [
    #                 {k: v for k, v in item.items() if k in ("id", "Path")}
    #                 for item in tfl_list
    #             ],
    #             "plan": plan_data,
    #             "output": output_path or "output\\result.docx"
    #         }
    #
    #         return complete_json
    #
    #     except Exception as e:
    #         logger.error(f"构建完整JSON失败: {e}", exc_info=True)
    #         return {
    #             "tfl": [],
    #             "plan": {},
    #             "output": output_path
    #         }
    #
    # # ---------- 文件缓存相关方法 ----------
    # def _get_pdf_cache_path(self, word_file_path: Path) -> Path:
    #     """获取 Word 文件对应的 PDF 缓存路径"""
    #     cache_name = f"{word_file_path.stem}.pdf"
    #     return self.pdf_cache_dir / cache_name
    #
    # def _get_ocr_cache_path(self, file_path: Path) -> Path:
    #     """获取文件对应的 OCR 缓存路径"""
    #     cache_name = f"{file_path.stem}.md"
    #     return self.ocr_cache_dir / cache_name
    #
    # def _get_ocr_clean_cache_path(self, file_path: Path) -> Path:
    #     """获取文件对应的 清洗后OCR 缓存路径"""
    #     cache_name = f"{file_path.stem}.clean.md"
    #     return self.ocr_clean_cache_dir / cache_name
    #
    # def _is_pdf_cache_valid(self, word_file_path: Path, pdf_cache_path: Path) -> bool:
    #     """检查 PDF 缓存是否有效（Word 文件未更新）"""
    #     if not pdf_cache_path.exists():
    #         return False
    #     return pdf_cache_path.stat().st_mtime >= word_file_path.stat().st_mtime
    #
    # def _is_ocr_cache_valid(self, file_path: Path, ocr_cache_path: Path) -> bool:
    #     """检查 OCR 缓存是否有效（文件未更新）"""
    #     if not ocr_cache_path.exists():
    #         return False
    #
    #     # 对于标记文件，如果缓存存在就直接使用（不需要检查时间）
    #     if "_marked" in file_path.name:
    #         return True
    #
    #     # 对于普通文件，检查时间
    #     return ocr_cache_path.stat().st_mtime >= file_path.stat().st_mtime
    #
    # def _load_from_cache(self, cache_path: Path) -> Optional[str]:
    #     """从缓存文件加载内容"""
    #     try:
    #         with open(cache_path, 'r', encoding='utf-8') as f:
    #             return f.read()
    #     except Exception as e:
    #         logger.warning(f"加载缓存失败 {cache_path}: {e}")
    #         return None
    #
    # def _save_to_cache(self, cache_path: Path, content: str) -> None:
    #     """保存内容到缓存文件"""
    #     try:
    #         with open(cache_path, 'w', encoding='utf-8') as f:
    #             f.write(content)
    #         logger.info(f"已缓存到: {cache_path}")
    #     except Exception as e:
    #         logger.warning(f"保存缓存失败 {cache_path}: {e}")
    #
    # def _clean_ocr_markdown(self, content: str) -> str:
    #     """清理OCR Markdown：仅移除图片相关内容，保留所有其他内容。"""
    #     try:
    #         lines = content.splitlines()
    #         cleaned_lines: List[str] = []
    #
    #         for line in lines:
    #             stripped = line.strip()
    #
    #             # 只跳过图片相关行：
    #             # 1. Markdown图片格式：![alt](url)
    #             if stripped.startswith('![') and '](' in stripped and stripped.endswith(')'):
    #                 continue
    #             # 2. HTML图片标签：<img ...>
    #             if '<img' in stripped.lower():
    #                 continue
    #             # 3. base64数据URI图片
    #             if 'data:image/' in stripped and 'base64,' in stripped:
    #                 continue
    #             # 4. 纯图片URL行（以常见图片扩展名结尾）
    #             if re.match(r'^https?://.*\.(jpg|jpeg|png|gif|bmp|webp|svg)(\?.*)?$', stripped, re.IGNORECASE):
    #                 continue
    #
    #             # 保留所有其他内容，包括表格、文本等
    #             cleaned_lines.append(line)
    #
    #         # 压缩多余空行
    #         out: List[str] = []
    #         prev_blank = False
    #         for l in cleaned_lines:
    #             if l.strip() == "":
    #                 if not prev_blank:
    #                     out.append("")
    #                 prev_blank = True
    #             else:
    #                 out.append(l)
    #                 prev_blank = False
    #         return "\n".join(out).strip()
    #     except Exception as e:
    #         logger.warning(f"清理OCR Markdown失败，返回原文: {e}")
    #         return content
    #
    # def _convert_word_to_pdf(self, word_file_path: Path, pdf_output_path: Path) -> Optional[Path]:
    #     """将 Word 文件转换为 PDF"""
    #     try:
    #         if self.vision_service:
    #             # 使用视觉服务的转换方法
    #             pdf_path = self.vision_service._docx_to_pdf(str(word_file_path), str(pdf_output_path))
    #             return Path(pdf_path) if pdf_path else None
    #         else:
    #             # 模拟转换
    #             logger.warning("视觉服务未配置，无法转换 Word 到 PDF")
    #             return None
    #     except Exception as e:
    #         logger.error(f"Word 转 PDF 失败: {e}", exc_info=True)
    #         return None
    #
    # def _call_ocr_service(self, file_path: Path) -> Dict[str, Any]:
    #     """调用 OCR 服务解析文件"""
    #     try:
    #         if self.vision_service:
    #             return self.vision_service._call_ocr_service(file_path)
    #         else:
    #             # 模拟 OCR 结果
    #             return {
    #                 "status": "success",
    #                 "content": f"[模拟OCR] 文件: {file_path.name}\n模拟的 Markdown 内容...",
    #                 "visual_elements": [],
    #                 "structured_content": f"[模拟OCR] 文件: {file_path.name}\n模拟的 Markdown 内容..."
    #             }
    #     except Exception as e:
    #         logger.error(f"OCR 服务调用失败: {e}", exc_info=True)
    #         return {
    #             "status": "error",
    #             "error": str(e)
    #         }
    #
#     def _backup_word_document(self, word_file: Path) -> Optional[Path]:
#         """备份Word文档"""
#         try:
#             from datetime import datetime
#             import shutil
#
#             # 创建备份文件名，包含时间戳
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             backup_name = f"{word_file.stem}_backup_{timestamp}{word_file.suffix}"
#             backup_path = word_file.parent / backup_name
#
#             # 复制文件
#             shutil.copy2(word_file, backup_path)
#             logger.info(f"Word文档已备份到: {backup_path}")
#             return backup_path
#
#         except Exception as e:
#             logger.error(f"备份Word文档失败: {e}", exc_info=True)
#             return None
#
# def main():
#     """测试数据提取功能"""
#     import sys
#     import os
#     sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#
#     from service.linux.generation.parsers.config_parser import ConfigParser
#
#     # 解析配置（示例已废弃，配置应从API传入）
#     # parser = ConfigParser("configs/paragraphs-v1.1(1).json")
#     # paragraphs = parser.parse()
#
#     # 创建数据提取器
#     extractor = DataExtractorV2()
#
#     # 测试提取第一个段落
#     if paragraphs:
#         test_paragraph = paragraphs[0]
#         logger.info(f"测试提取段落: {test_paragraph.id}")
#
#         # 转换为字典格式
#         paragraph_dict = {
#             "id": test_paragraph.id,
#             "generate": test_paragraph.generate,
#             "example": test_paragraph.example,
#             "data": [
#                 {
#                     "extract": item.extract,
#                     "datas": item.datas,
#                     "insert_original": item.insert_original
#                 }
#                 for item in test_paragraph.data
#             ]
#         }
#
#         result = extractor.extract_data_for_paragraph(paragraph_dict)
#
#         logger.info("提取结果:")
#         logger.info(json.dumps(result, ensure_ascii=False, indent=2))
#
#
# if __name__ == "__main__":
#     main()
