#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
段落生成服务

功能说明：
- 将数据提取器提取的原始数据整合成结构化的CSR段落
- 使用LLM模型根据用户需求生成专业的医学研究叙述文本
- 支持示例风格参考，确保生成内容符合特定格式要求
- 提供完整的提示词管理和输出保存功能

工作流程：
1. 接收提取的数据和生成需求
2. 构建包含系统提示词、用户需求、提取数据的完整提示词
3. 调用LLM模型生成段落内容
4. 保存提示词和模型输出（用于调试和溯源）
5. 返回生成的段落内容

技术特点：
- 支持多种LLM模型（通过LLMService抽象）
- 自动管理提示词模板
- 提供详细的日志记录
- 支持流式输出（可选）
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime

from service.models import get_llm_service
from service.prompts.system_prompt_manager import system_prompt_manager
from utils.context_manager import get_current_output_dir
from utils.task_logger import get_task_logger

# 导入耗时记录工具
from utils.timing import Timer, generation_timer


def _task_log_error(message: str, exc: Exception = None, **extra):
    """记录错误到任务日志"""
    task_logger = get_task_logger()
    if task_logger:
        task_logger.error(message, exc=exc, logger_name="paragraph_generation", **extra)


logger = logging.getLogger(__name__)


class ParagraphGenerationService:
    """
    段落生成服务
    
    这是CSR文档生成系统的核心组件之一，负责将提取的原始数据
    转换为符合医学研究规范的专业叙述文本。
    
    主要功能：
    1. 整合多个数据源的提取结果
    2. 根据用户的生成逻辑生成段落
    3. 支持示例风格参考
    4. 生成专业的CSR叙述文本
    
    支持的数据源：
    - Word/PDF文档提取结果
    - Excel/RTF表格提取结果
    - TFL数据分析结果
    - 其他结构化数据
    
    技术实现：
    - 使用LLMService进行文本生成
    - 通过system_prompt_manager管理提示词模板
    - 支持保存提示词和模型输出用于调试
    """

    def __init__(self, model_name: Optional[str] = None):
        """
        初始化段落生成服务
        
        Args:
            model_name: 指定使用的LLM模型名称（可选）
                       如果不提供，将使用生成任务的默认模型
        """
        # 使用统一的模型管理器获取LLM实例
        self.llm = get_llm_service("generation", model_name)
        # 可选的详细日志系统（用于保存提示词和输出）
        self.detailed_logger = None
        logger.info("段落生成服务已初始化，使用统一模型管理器")

    def _save_prompt_to_file(self, paragraph_id: Optional[str], prompt: str) -> Optional[Path]:
        """保存提示词到文件（用于调试）"""
        try:
            # 使用线程安全的方式获取当前会话目录
            session_dir = get_current_output_dir(default="output")
            safe_pid = (paragraph_id or "unknown").replace("/", "_").replace("\\", "_")
            prompts_dir = Path(session_dir) / "prompts" / safe_pid
            prompts_dir.mkdir(parents=True, exist_ok=True)
            from datetime import datetime
            import uuid
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            r6 = uuid.uuid4().hex[:6]
            pid = (paragraph_id or "unknown").strip() or "unknown"
            filename = f"generation_prompt_{pid}_{ts}_{r6}.txt"
            prompt_file = prompts_dir / filename
            with open(prompt_file, 'w', encoding='utf-8') as f:
                f.write(prompt)
            logger.debug(f"生成提示词已保存: {prompt_file}")
            return prompt_file
        except Exception as e:
            logger.warning(f"保存生成提示词失败: {e}")
            return None

    def _save_raw_output_to_file(self, paragraph_id: Optional[str], content: str) -> Optional[Path]:
        """保存模型原始输出到会话 prompts 目录"""
        try:
            # 使用线程安全的方式获取当前会话目录
            session_dir = get_current_output_dir(default="output")
            safe_pid = (paragraph_id or "unknown").replace("/", "_").replace("\\", "_")
            outputs_dir = Path(session_dir) / "prompts" / safe_pid
            outputs_dir.mkdir(parents=True, exist_ok=True)
            from datetime import datetime
            import uuid
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            r6 = uuid.uuid4().hex[:6]
            pid = (paragraph_id or "unknown").strip() or "unknown"
            filename = f"generation_output_{pid}_{ts}_{r6}.txt"
            out_file = outputs_dir / filename
            with open(out_file, 'w', encoding='utf-8') as f:
                f.write(content if content is not None else "")
            logger.debug(f"生成原始输出已保存: {out_file}")
            return out_file
        except Exception as e:
            logger.warning(f"保存生成原始输出失败: {e}")
            return None

    def generate_paragraph(self,
                           generate_prompt: str,
                           extracted_data: Dict[str, Any],
                           example: str = "",
                           insert_original: bool = False,
                           paragraph_id: Optional[str] = None) -> Dict[str, Any]:
        """
        生成CSR段落
        
        【核心功能】
        1. 整合多个数据源的提取结果（scheme_data, tfl_data）
        2. 处理quote字段：在提取内容前添加引用标签【quote】
        3. 构建完整的生成提示词
        4. 调用LLM生成段落内容
        5. 保存溯源信息（提示词、原始输出、占位符等）
        
        【quote字段处理流程】
        数据流：Pipeline -> DataExtractor -> extracted_items[*].quote -> 本服务
        处理逻辑：
        1. 从extracted_items中遍历每个提取结果
        2. 检查item.get("quote")是否存在
        3. 如果存在且item.status == "success"且content非空：
           将content修改为：f"【{quote}】\n{content}"
        4. 将处理后的content添加到scheme_data或tfl_data中
        5. 最终LLM会看到带有【引用标签】前缀的内容
        
        【数据源分类】
        - scheme_data: 来自word/pdf/excel/rtf的文档提取内容
        - tfl_data: 来自TFL数据分析的结果
        - available_resources: 可用的表格/图片占位符列表
        
        Args:
            generate_prompt: 用户的生成逻辑/需求（从配置JSON的generate字段）
            extracted_data: 提取的数据，格式：
                {
                    "extracted_items": [  # 主要数据来源
                        {
                            "status": "success",
                            "content": "提取的内容",
                            "data_type": "word/pdf/excel/tfl",
                            "quote": "引用标签"  # 可选，如果存在则会添加到内容前
                        },
                        ...
                    ],
                    "available_resources": [  # 可用占位符
                        {"label": "Table_1_Start", "type": "table"},
                        ...
                    ],
                    "traceability": {...}  # 溯源信息
                }
            example: 示例文本（可选，从配置JSON的example字段）
            paragraph_id: 段落ID（可选）
        
        Returns:
            Dict: 生成结果
            {
                "success": bool,
                "paragraph_id": str,
                "generated_content": str,  # 生成的段落内容
                "generation_prompt": str,  # 完整的生成提示词
                "raw_output": str,  # 模型原始输出
                "traceability": {...}  # 溯源信息
            }
        """
        # 开始段落生成总计时
        para_timer = Timer(f"生成段落({paragraph_id})", parent="段落生成")
        para_timer.start()

        try:
            # 从extracted_items中提取内容
            # 1.1 从extracted_items中提取内容（新格式）
            scheme_data = ""
            extracted_items = extracted_data.get("extracted_items", [])
            logger.info(f"📋 extracted_items数量: {len(extracted_items)}")
            logger.info(f"📋 extracted_data的所有键: {list(extracted_data.keys())}")

            # 调试：输出extracted_data的完整结构（前1000字符）
            import json
            try:
                debug_data = json.dumps(extracted_data, ensure_ascii=False, indent=2)
                logger.info(f"📋 extracted_data结构预览:\n{debug_data[:1000]}...")
            except Exception as e:
                logger.warning(f"无法序列化extracted_data: {e}")

            # ✅ 遍历extracted_items，处理每个提取结果
            for idx, item in enumerate(extracted_items):
                logger.info(
                    f"📋 Item {idx}: status={item.get('status')}, data_type={item.get('data_type')}, content_length={len(item.get('content', ''))}")

                # 只处理提取成功的项
                if item.get("status") == "success":
                    content = item.get("content", "")
                    if not content:
                        logger.warning(f"⚠️ Item {idx} 状态为success但content为空")
                        continue

                    data_type = item.get("data_type", "word")
                    quote = item.get("quote")  # 从提取结果中获取quote字段

                    # ✅ 关键逻辑：如果有quote字段，在内容前添加引用标签
                    # 这样LLM在生成时就能看到内容的来源标识，例如：
                    # 【方案1.0】
                    # 研究目的是评估...
                    if quote:
                        content = f"【{quote}】\n{content}"
                        logger.info(f"✅ 添加quote标签: 【{quote}】")
                        logger.info(f"   处理后内容预览: {content[:100]}...")

                    # 根据数据类型分类到不同的数据容器
                    if data_type in ["word", "doc", "docx", "pdf", "excel", "xlsx", "xls", "rtf"]:
                        # 文档类型归类为方案数据（scheme_data）
                        if scheme_data:
                            scheme_data += f"\n\n{content}"  # 多个数据项之间用双换行分隔
                        else:
                            scheme_data = content
                        logger.info(f"✅ 添加到scheme_data: {len(content)}字符")
                else:
                    # 提取失败的项，记录错误日志
                    logger.error(f"❌ Item {idx} 提取失败: {item.get('error', 'Unknown error')}")
                    logger.error(f"   失败项的完整信息: {item}")

            # 1.2 兼容旧格式：如果没有extracted_items，尝试直接获取scheme_data
            if not extracted_items:
                scheme_data = extracted_data.get("scheme_data", "")

                # 如果有其他数据源，也整合进来（纯数据，不添加标签）
                other_data = []
                for key, value in extracted_data.items():
                    if key not in ["scheme_data", "extracted_items", "available_resources",
                                   "traceability"] and value:
                        other_data.append(str(value))

                # 如果有其他数据，附加到方案数据
                if other_data:
                    other_data_text = "\n\n".join(other_data)
                    if scheme_data:
                        scheme_data += f"\n\n{other_data_text}"
                    else:
                        scheme_data = other_data_text

            # 2. 收集可用占位符
            available_resources = extracted_data.get("available_resources", [])
            #  从 extracted_items 的 tfl_insert_mappings 收集TFL占位符.
            tfl_placeholders = []
            for item in extracted_items:
                tfl_mappings = item.get("tfl_insert_mappings", [])
                if tfl_mappings:
                    for m in tfl_mappings:
                        ph = m.get("Placeholder", "")
                        if ph:
                            tfl_placeholders.append(ph)
                            logger.info(f"📌 收集TFL占位符到placeholders_list: {ph}")
            placeholders_list = extracted_data.get("all_placeholders", [])

            # 3. 构建生成提示词
            logger.info(f"📊 最终数据统计:")
            logger.info(f"  - scheme_data长度: {len(scheme_data)}字符")
            logger.info(f"  - scheme_data预览: {scheme_data[:300] if scheme_data else '(空)'}...")
            logger.info(f"  - available_resources数量: {len(available_resources)}个")
            logger.info(f"  - generate_prompt: {generate_prompt[:100]}...")

            if not scheme_data:
                logger.warning("⚠️ 警告: scheme_data为空！LLM将没有参考资料")

            variables = {
                "generate_prompt": generate_prompt,
                "scheme_data": scheme_data,  # ✅ 直接传递，空值由模板处理
                "example": example,
                "project_desc": os.getenv("CURRENT_PROJECT_DESC", "")
            }

            # 构建提示词计时
            with Timer("构建生成提示词", parent="段落生成") as prompt_timer:
                generation_prompt = system_prompt_manager.build_prompt(
                    "csr_generation",
                    variables
                )

            logger.info(f"📝 生成提示词长度: {len(generation_prompt)}字符 [构建耗时: {prompt_timer.duration_str}]")
            logger.info(f"📝 生成提示词预览（最后500字符）:\n{generation_prompt[-500:]}")

            # ✅ 保存生成阶段的完整溯源数据
            provenance_data = {
                "stage": "generation",
                "paragraph_id": paragraph_id,
                "timestamp": datetime.now().isoformat(),
                "input_data": {
                    "generate_prompt": generate_prompt,
                    "example": example,
                    "extracted_items_count": len(extracted_items),
                    "available_resources_count": len(available_resources)
                },
                "variables": variables,
                "generation_prompt": generation_prompt,
                "prompt_length": len(generation_prompt)
            }

            # 保存提示词（用于调试）
            prompt_path: Optional[Path] = None
            if self.detailed_logger:
                self.detailed_logger.save_prompt("paragraph_generation", generation_prompt, {
                    "paragraph_id": paragraph_id,
                    "prompt_length": len(generation_prompt)
                })
            else:
                prompt_path = self._save_prompt_to_file(paragraph_id, generation_prompt)

            # 阶段提示：开始生成
            logger.info("正在生成段落...")

            # 3. 调用模型生成 (带耗时记录)
            with Timer("LLM模型生成", parent="段落生成") as llm_timer:
                model_output = self.llm.generate_single(generation_prompt)

            logger.info(f"⏱️ LLM生成完成 [耗时: {llm_timer.duration_str}, 输出: {len(model_output)}字符]")

            raw_output_path = self._save_raw_output_to_file(paragraph_id, model_output)

            # ✅ 添加输出到溯源数据
            provenance_data["model_output"] = model_output
            provenance_data["output_length"] = len(model_output)
            provenance_data["placeholders_in_output"] = self._extract_placeholders(model_output)

            # ✅ 保存完整的溯源数据到JSON文件
            provenance_path = self._save_provenance_data(paragraph_id, provenance_data, extracted_data)

            # 停止总计时
            para_timer.stop()

            # 阶段提示：生成完成
            logger.info(f"✓ 生成完成: {paragraph_id} [总耗时: {para_timer.duration_str}]")

            # 记录到全局计时器
            if generation_timer:
                generation_timer.record(f"段落生成-{paragraph_id}", para_timer.duration, parent="段落生成",
                                        metadata={"prompt_len": len(generation_prompt),
                                                  "output_len": len(model_output)})

            # 4. 根据插入图表开关 ，附加TFL与占位符到生成内容末尾
            final_content = model_output
            if insert_original:
                if tfl_placeholders:
                    placeholders_suffix = "\n\n" + "\n".join(tfl_placeholders)
                    final_content = final_content + placeholders_suffix
                    logger.info(f"✅ 已附加{len(tfl_placeholders)}个TFL占位符到段落末尾: {tfl_placeholders}")
                if placeholders_list:
                    unique_placeholders = list(set(placeholders_list))
                    placeholders_suffix = "\n\n" + "\n".join(unique_placeholders)
                    final_content = final_content + placeholders_suffix
                    logger.info(f"✅ 已附加{len(unique_placeholders)}个图表占位符到段落末尾: {placeholders_list}")

            # 5. 返回结果（包含完整溯源信息）
            return {
                "success": True,
                "paragraph_id": paragraph_id,
                "generated_content": final_content,  # 🆕 使用附加占位符后的内容
                "generation_prompt": generation_prompt,
                "raw_output": model_output,
                "tfl_placeholders": tfl_placeholders,  # 🆕 返回TFL占位符列表
                "prompt_path": str(prompt_path) if prompt_path else None,
                "raw_output_path": str(raw_output_path) if raw_output_path else None,
                # ✅ 完整的溯源信息
                "traceability": {
                    "timestamp": datetime.now().isoformat(),
                    "full_generation_prompt": generation_prompt,
                    "prompt_length": len(generation_prompt),
                    "user_generate_prompt": generate_prompt,
                    "example_provided": bool(example and example.strip()),
                    "scheme_data_length": len(scheme_data) if scheme_data else 0,
                    "available_resources_count": len(available_resources),
                    "tfl_placeholders_count": len(tfl_placeholders),  # 🆕
                    "model_output_length": len(model_output),
                    "placeholders_in_output": self._extract_placeholders(final_content),  # 🆕 使用最终内容
                    "extracted_items_count": len(extracted_items),
                    "llm_model": getattr(self.llm, 'model_name', 'unknown'),
                    "prompt_file": str(prompt_path) if prompt_path else None,
                    "output_file": str(raw_output_path) if raw_output_path else None,
                    "provenance_file": str(provenance_path) if provenance_path else None,  # ✅ 完整溯源文件
                    # ⏱️ 耗时信息
                    "timing": {
                        "total_duration": para_timer.duration,
                        "total_duration_str": para_timer.duration_str,
                        "prompt_build_duration": prompt_timer.duration,
                        "prompt_build_duration_str": prompt_timer.duration_str,
                        "llm_generation_duration": llm_timer.duration,
                        "llm_generation_duration_str": llm_timer.duration_str
                    }
                }
            }

        except Exception as e:
            para_timer.stop()
            logger.error(f"✗ 生成失败: {e} [耗时: {para_timer.duration_str}]")
            _task_log_error(f"段落生成失败: {paragraph_id}", exc=e, paragraph_id=paragraph_id)
            return {
                "success": False,
                "paragraph_id": paragraph_id,
                "error": str(e),
                "timing": {
                    "total_duration": para_timer.duration,
                    "total_duration_str": para_timer.duration_str
                }
            }

    # def generate_paragraph_from_extractions(self,
    #                                         generate_prompt: str,
    #                                         extraction_results: List[Dict[str, Any]],
    #                                         example: str = "",
    #                                         paragraph_id: Optional[str] = None) -> Dict[str, Any]:
    #     """
    #     从多个提取结果生成段落
    #
    #     Args:
    #         generate_prompt: 用户的生成逻辑
    #         extraction_results: 多个提取结果的列表（来自extracted_items）
    #         example: 示例文本（可选）
    #         paragraph_id: 段落ID（可选）
    #
    #     Returns:
    #         Dict: 生成结果
    #     """
    #     try:
    #         # 1. 整合提取结果和收集TFL占位符
    #         extracted_data = {}
    #         tfl_placeholders = []  # 收集TFL占位符（RTF/Excel原文模式）
    #         tfl_insert_mappings = []  # 收集TFL插入映射
    #
    #         for result in extraction_results:
    #             # 提取状态检查
    #             if result.get("status") != "success":
    #                 continue
    #
    #             data_type = result.get("data_type", "")
    #             content = result.get("content", "")
    #             is_original = result.get("is_original", False)
    #
    #             # 🆕 收集TFL插入映射中的占位符（RTF/Excel原文模式）
    #             mappings = result.get("tfl_insert_mappings", [])
    #             if mappings and isinstance(mappings, list):
    #                 for m in mappings:
    #                     ph = m.get("Placeholder", "")
    #                     if ph:
    #                         tfl_placeholders.append(ph)
    #                         tfl_insert_mappings.append(m)
    #                         logger.info(f"📌 收集TFL占位符: {ph} -> {m.get('Source', 'unknown')}")
    #
    #             # 如果是原文且没有内容，跳过
    #             if is_original and not content:
    #                 continue
    #
    #             if not content:
    #                 continue
    #
    #             # 根据数据类型分类（Word/PDF的content已包含{{Table_1_Start}}等占位符）
    #             if data_type in ["word","docx", "doc", "pdf"]:
    #                 # Word/PDF归类为方案数据
    #                 if "scheme_data" not in extracted_data:
    #                     extracted_data["scheme_data"] = ""
    #                 extracted_data["scheme_data"] += f"\n\n{content}"
    #
    #             elif data_type in ["tfl"]:
    #                 # TFL归类为TFL数据
    #                 if "tfl_data" not in extracted_data:
    #                     extracted_data["tfl_data"] = ""
    #                 extracted_data["tfl_data"] += f"\n\n{content}"
    #
    #             else:
    #                 # 其他类型（Excel、RTF等）归类为方案数据
    #                 if "scheme_data" not in extracted_data:
    #                     extracted_data["scheme_data"] = ""
    #                 extracted_data["scheme_data"] += f"\n\n{content}"
    #
    #         # 2. 调用段落生成
    #         generation_result = self.generate_paragraph(
    #             generate_prompt=generate_prompt,
    #             extracted_data=extracted_data,
    #             example=example,
    #             paragraph_id=paragraph_id
    #         )
    #
    #         # 3. 如果有TFL占位符，附加到生成内容的末尾
    #         if generation_result.get("success") and tfl_placeholders:
    #             generated_content = generation_result.get("generated_content", "")
    #             placeholders_text = "\n\n" + "\n".join(tfl_placeholders)
    #             generation_result["generated_content"] = generated_content + placeholders_text
    #             generation_result["tfl_placeholders"] = tfl_placeholders
    #             logger.info(f"✅ 已附加{len(tfl_placeholders)}个TFL占位符到段落末尾: {tfl_placeholders}")
    #
    #         # 4. 附加TFL插入映射信息到结果
    #         if tfl_insert_mappings:
    #             generation_result["tfl_insert_mappings"] = tfl_insert_mappings
    #             logger.info(f"📊 TFL插入映射: {len(tfl_insert_mappings)} 个")
    #
    #         return generation_result
    #
    #     except Exception as e:
    #         logger.error(f"从提取结果生成段落失败: {e}")
    #         _task_log_error(f"从提取结果生成段落失败: {paragraph_id}", exc=e, paragraph_id=paragraph_id)
    #         return {
    #             "success": False,
    #             "paragraph_id": paragraph_id,
    #             "error": str(e)
    #         }
    #
    # def generate_with_validated_extractions(self,
    #                                        generate_prompt: str,
    #                                        validated_results: List[Dict[str, Any]],
    #                                        example: str = "",
    #                                        paragraph_id: Optional[str] = None,
    #                                        use_only_valid: bool = True) -> Dict[str, Any]:
    #     """
    #     使用校验后的提取结果生成段落
    #
    #     Args:
    #         generate_prompt: 用户的生成逻辑
    #         validated_results: 校验后的提取结果列表
    #         example: 示例文本（可选）
    #         paragraph_id: 段落ID（可选）
    #         use_only_valid: 是否只使用通过校验的结果（默认True）
    #
    #     Returns:
    #         Dict: 生成结果
    #     """
    #     try:
    #         # 筛选提取结果
    #         extraction_results = []
    #
    #         for result in validated_results:
    #             # 检查是否成功
    #             if not result.get("success"):
    #                 logger.warning(f"跳过失败的提取结果")
    #                 continue
    #
    #             # 检查是否通过校验
    #             is_valid = result.get("is_valid", True)
    #             if use_only_valid and not is_valid:
    #                 logger.warning(f"跳过未通过校验的提取结果（评分:{result.get('validation_result', {}).get('overall_score', 0)}）")
    #                 continue
    #
    #             # 提取内容
    #             extracted_content = result.get("extracted_content", "")
    #             if not extracted_content:
    #                 continue
    #
    #             extraction_results.append({
    #                 "data_type": result.get("data_type", "unknown"),
    #                 "name": result.get("name", "未命名"),
    #                 "extracted_content": extracted_content
    #             })
    #
    #         logger.info(f"使用{len(extraction_results)}个有效的提取结果生成段落")
    #
    #         # 调用生成
    #         return self.generate_paragraph_from_extractions(
    #             generate_prompt=generate_prompt,
    #             extraction_results=extraction_results,
    #             example=example,
    #             paragraph_id=paragraph_id
    #         )
    #     except Exception as e:
    #         logger.error(f"使用校验结果生成段落失败: {e}")
    #         _task_log_error(f"使用校验结果生成段落失败: {paragraph_id}", exc=e, paragraph_id=paragraph_id)
    #         return {
    #             "success": False,
    #             "paragraph_id": paragraph_id,
    #             "error": str(e)
    #         }

    def _save_provenance_data(self, paragraph_id: Optional[str], provenance_data: Dict[str, Any],
                              extracted_data: Dict[str, Any]) -> Optional[Path]:
        """保存完整的生成阶段溯源数据到JSON文件
        
        Args:
            paragraph_id: 段落ID
            provenance_data: 溯源数据（包含输入、变量、提示词、输出）
            extracted_data: 提取阶段的完整数据
            
        Returns:
            保存的文件路径
        """
        try:
            import json

            # 使用线程安全的方式获取当前会话目录
            session_dir = get_current_output_dir(default="output")
            safe_para_id = (paragraph_id or "unknown").replace("/", "_").replace("\\", "_")
            prompts_dir = Path(session_dir) / "prompts" / safe_para_id
            prompts_dir.mkdir(parents=True, exist_ok=True)

            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"generation_provenance_{timestamp}.json"
            filepath = prompts_dir / filename

            # 构建完整的溯源数据
            full_provenance = {
                **provenance_data,
                "extracted_data_summary": {
                    "paragraph_id": extracted_data.get("paragraph_id"),
                    "extracted_items": [
                        {
                            "status": item.get("status"),
                            "data_type": item.get("data_type"),
                            "content_length": len(item.get("content", "")),
                            "content_preview": item.get("content", "")[:200] if item.get("content") else None,
                            "error": item.get("error") if item.get("status") == "error" else None
                        }
                        for item in extracted_data.get("extracted_items", [])
                    ],
                    "available_resources": extracted_data.get("available_resources", []),
                    "traceability": extracted_data.get("traceability", {})
                }
            }

            # 保存到文件
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(full_provenance, f, ensure_ascii=False, indent=2)

            logger.info(f"✅ 生成阶段溯源数据已保存: {filepath}")
            return filepath

        except Exception as e:
            logger.warning(f"保存生成溯源数据失败: {e}")
            return None

    def _extract_placeholders(self, content: str) -> List[str]:
        """从生成内容中提取占位符"""
        import re
        # 匹配 {{xxx}} 格式的占位符
        pattern = r'\{\{([^\}]+)\}\}'
        matches = re.findall(pattern, content)
        return matches if matches else []


# 创建全局服务实例
paragraph_generation_service = ParagraphGenerationService()
