# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
#
# """
# Word集成模块
# 将CSR生成结果集成到Word文档中
# """
#
# import logging
# import time
# import re
# import json
# import os
# from pathlib import Path
# from typing import Dict, List, Optional, Any
# from dataclasses import dataclass
#
# from service.windows.insertion.word_document_service import WordDocumentService, WordInsertionResult
# from utils.context_manager import get_current_output_dir
#
# logger = logging.getLogger(__name__)
#
# @dataclass
# class WordIntegrationConfig:
#     """Word集成配置"""
#     template_file: str  # Word模板文件路径
#     output_file: Optional[str] = None  # 输出文件路径
#     placeholder_format: str = "{{%s}}"  # 占位符格式
#     auto_create_template: bool = False  # 是否自动创建模板
#     backup_original: bool = True  # 是否备份原文件
#
# @dataclass
# class WordIntegrationResult:
#     """Word集成结果"""
#     success: bool
#     message: str
#     output_file: Optional[str] = None
#     inserted_paragraphs: List[str] = None
#     error: Optional[str] = None
#     template_info: Optional[Dict[str, Any]] = None
#
# class WordIntegrationService:
#     """Word集成服务"""
#
#     def __init__(self, config: WordIntegrationConfig):
#         """
#         初始化Word集成服务
#
#         Args:
#             config: Word集成配置
#         """
#         self.config = config
#         self.word_service = WordDocumentService()
#         self._last_backup_path: Optional[str] = None
#         logger.info("Word集成服务初始化完成")
#
#     def integrate_csr_results(self,
#                              generation_results: List[Dict[str, Any]],
#                              paragraph_config: List[Dict[str, Any]]) -> WordIntegrationResult:
#         """
#         将CSR生成结果集成到Word文档中
#
#         Args:
#             generation_results: CSR生成结果列表
#             paragraph_config: 段落配置列表
#
#         Returns:
#             WordIntegrationResult: 集成结果
#         """
#         try:
#             # 准备内容数据
#             content_data = self._prepare_content_data(generation_results, paragraph_config)
#
#             # 检查模板文件是否存在
#             if not Path(self.config.template_file).exists():
#                 return WordIntegrationResult(
#                     success=False,
#                     message="模板文件不存在",
#                     error=f"模板文件不存在: {self.config.template_file}",
#                     template_info={"valid": False, "error": "文件不存在"}
#                 )
#
#             # 检查模板文件中的占位符（允许部分占位符缺失时继续插入）
#             template_info = self._check_template_file(content_data.keys())
#             try:
#                 allow_partial = str(os.getenv("ALLOW_PARTIAL_INSERT", "1")).strip().lower() in (
#                     "1", "true", "yes", "y", "on", "是"
#                 )
#             except Exception:
#                 allow_partial = True
#             if not template_info.get("valid", False):
#                 missing_list = template_info.get('missing_placeholders', [])
#                 if allow_partial:
#                     logger.warning(f"模板缺少占位符，将继续插入可用内容: {missing_list}")
#                 else:
#                     return WordIntegrationResult(
#                         success=False,
#                         message="模板文件验证失败",
#                         error=f"缺少占位符: {missing_list}",
#                         template_info=template_info
#                     )
#
#             # 备份原文件（备份路径优先写入当前会话outputs目录）
#             if self.config.backup_original and Path(self.config.template_file).exists():
#                 backup_path = self._backup_template_file()
#                 if isinstance(template_info, dict):
#                     template_info["backup_file"] = backup_path
#
#             # 插入内容到Word文档
#             insertion_result = self.word_service.insert_content_to_word(
#                 template_file=self.config.template_file,
#                 content_data=content_data,
#                 output_file=self.config.output_file,
#                 placeholder_format=self.config.placeholder_format
#             )
#
#             if insertion_result.success:
#                 return WordIntegrationResult(
#                     success=True,
#                     message=f"成功集成CSR结果到Word文档: {insertion_result.message}",
#                     output_file=insertion_result.output_file,
#                     inserted_paragraphs=insertion_result.inserted_paragraphs,
#                     template_info=template_info
#                 )
#             else:
#                 return WordIntegrationResult(
#                     success=False,
#                     message="Word文档集成失败",
#                     error=insertion_result.error
#                 )
#
#         except Exception as e:
#             logger.error(f"Word集成失败: {e}", exc_info=True)
#             return WordIntegrationResult(
#                 success=False,
#                 message="Word集成失败",
#                 error=str(e)
#             )
#
#     def _prepare_content_data(self,
#                              generation_results: List[Dict[str, Any]],
#                              paragraph_config: List[Dict[str, Any]]) -> Dict[str, str]:
#         """
#         准备内容数据
#
#         Args:
#             generation_results: CSR生成结果列表
#             paragraph_config: 段落配置列表
#
#         Returns:
#             Dict[str, str]: 段落ID到内容的映射
#         """
#         content_data = {}
#
#         def _sanitize_plain_text(text: str) -> str:
#             """移除JSON/代码块与说明性标志，只保留纯正文。
#             - 去掉 ```...``` 代码块（含 ```json）
#             - 去掉 Markdown 标题行（# 开头）
#             - 去掉仅包含中文书名号的标题行（如 【方案】）
#             - 去掉多余空行
#             """
#             try:
#                 import re as _re
#                 if not isinstance(text, str):
#                     return ""
#                 s = text
#                 # 删除 fenced code blocks
#                 s = _re.sub(r"```[a-zA-Z]*[\s\S]*?```", "", s)
#                 # 逐行过滤标题/标志行
#                 out_lines = []
#                 for line in (s.splitlines()):
#                     t = line.strip()
#                     if not t:
#                         out_lines.append("")
#                         continue
#                     if t.startswith("#"):
#                         continue
#                     if _re.fullmatch(r"[（(【\[]?\s*[^\s\w]*\s*[)】\]]?", t):
#                         # 极短的纯符号行，跳过
#                         continue
#                     if _re.fullmatch(r"[\u3010\u3011【】]?[\u4e00-\u9fa5A-Za-z0-9_]+[\u3010\u3011【】]?", t) and len(t) <= 12:
#                         # 纯标题样式，长度很短，跳过（如【方案】、方案等）
#                         continue
#                     out_lines.append(line)
#                 # 压缩空行
#                 res = []
#                 prev_blank = False
#                 for l in out_lines:
#                     if l.strip() == "":
#                         if not prev_blank:
#                             res.append("")
#                         prev_blank = True
#                     else:
#                         res.append(l)
#                         prev_blank = False
#                 return "\n".join(res).strip()
#             except Exception:
#                 return text if isinstance(text, str) else ""
#
#         # 从生成结果中提取内容
#         for result in generation_results:
#             paragraph_id = result.get('paragraph_id')
#             generated_content = result.get('generated_content', '')
#             status = result.get('status', 'error')
#
#             # 预先收集本段落中的占位符（含区分图片类型）
#             extracted = result.get('extracted_data') or {}
#             items = extracted.get('extracted_items', []) if isinstance(extracted, dict) else []
#             placeholders: List[str] = []
#             image_placeholders: List[str] = []
#             try:
#                 for it in items:
#                     if not isinstance(it, dict):
#                         continue
#                     mappings = it.get('tfl_insert_mappings') or []
#                     per_file = it.get('tfl_per_file_results') or []
#                     # 允许的图片型路径集合（skip_processing=image_type）
#                     allowed_img_paths = set()
#                     for r in per_file:
#                         if not isinstance(r, dict):
#                             continue
#                         info = (r.get('tfl_processing_info') or {})
#                         if info.get('skipped') and str(info.get('reason', '')) == 'image_type':
#                             sp = r.get('source_file')
#                             if sp:
#                                 allowed_img_paths.add(sp)
#                     for m in mappings:
#                         ph = (m or {}).get('Placeholder')
#                         mp = (m or {}).get('Path')
#                         if ph:
#                             placeholders.append(ph)
#                             if mp and mp in allowed_img_paths:
#                                 image_placeholders.append(ph)
#             except Exception as _e:
#                 logger.warning(f"收集TFL占位符失败: {_e}")
#
#             # 去重并保持顺序
#             def _uniq(seq: List[str]) -> List[str]:
#                 seen = set()
#                 out: List[str] = []
#                 for s in seq:
#                     if s not in seen:
#                         out.append(s)
#                         seen.add(s)
#                 return out
#
#             uniq_all = _uniq(placeholders)
#             uniq_img = _uniq(image_placeholders)
#
#             if paragraph_id and status == 'success' and generated_content:
#                 # 先剥离“标签信息 (JSON)”并单独保存为文件，仅保留清理后的正文
#                 try:
#                     # 匹配形如：## 标签信息 (JSON) \n ```json { ... } ```
#                     json_block = re.search(r"##\s*标签信息\s*\(JSON\)\s*```json\s*(\{[\s\S]*?\})\s*```", generated_content)
#                     if json_block:
#                         json_str = json_block.group(1)
#                         try:
#                             data = json.loads(json_str)
#                         except Exception:
#                             data = None
#                         # 使用线程安全的方式获取当前会话 outputs 目录
#                         outdir = get_current_output_dir(default="output")
#                         outputs_dir = Path(outdir) / "outputs"
#                         outputs_dir.mkdir(parents=True, exist_ok=True)
#                         ts = time.strftime("%Y%m%d_%H%M%S")
#                         safe_pid = (paragraph_id or "para").replace("/", "_").replace("\\", "_")
#                         plan_json_path = outputs_dir / f"plan_labels_{safe_pid}_{ts}.json"
#                         if data is not None:
#                             with plan_json_path.open('w', encoding='utf-8') as f:
#                                 json.dump(data, f, ensure_ascii=False, indent=2)
#                             logger.info(f"已保存方案标签JSON: {plan_json_path}")
#                         # 从段落内容中移除整段“标签信息 (JSON)”块
#                         generated_content = re.sub(r"##\s*标签信息\s*\(JSON\)[\s\S]*?```\s*", "", generated_content, count=1).strip()
#                         # 若存在“清理后的原文内容”，则仅保留其后的正文
#                         parts = generated_content.split("## 清理后的原文内容", 1)
#                         if len(parts) > 1:
#                             generated_content = parts[1].strip()
#                 except Exception as _e:
#                     logger.warning(f"处理方案标签JSON失败: {_e}")
#                 # 纯文本化处理
#                 generated_content = _sanitize_plain_text(generated_content)
#
#                 # 有正文时：将所有占位符追加到末尾（带分节符）
#                 if uniq_all:
#                     try:
#                         # 🆕 添加分节符包裹占位符
#                         placeholders_with_breaks = [
#                             "{{SECTION_BREAK}}",  # 开始分节符
#                             *uniq_all,            # 所有占位符
#                             "{{SECTION_BREAK}}"   # 结束分节符
#                         ]
#                         suffix = "\n\n" + "\n\n".join(placeholders_with_breaks)
#                         generated_content = (generated_content or "") + suffix
#                         logger.info(f"段落 {paragraph_id} 追加 {len(uniq_all)} 个占位符（含分节符）")
#                     except Exception as _e:
#                         logger.warning(f"追加TFL占位符失败: {_e}")
#                 content_data[paragraph_id] = generated_content
#                 logger.info(f"准备插入段落 {paragraph_id}，内容长度: {len(generated_content)}")
#             elif paragraph_id:
#                 # 无正文时：若存在图片类型的占位符，直接用它们替换段落占位符
#                 if uniq_img:
#                     only_placeholders = "\n".join(uniq_img)
#                     content_data[paragraph_id] = only_placeholders
#                     logger.info(f"段落 {paragraph_id} 无正文，使用图片占位符替换，占位符数: {len(uniq_img)}")
#                 else:
#                     logger.warning(f"段落 {paragraph_id} 生成失败或内容为空")
#
#         # 按需求：仅当存在实际内容时才替换占位符；无内容则保持模板原样
#         # 因此这里不再为缺失内容的段落写入占位文本
#
#         return content_data
#
#     def _check_template_file(self, paragraph_ids: List[str]) -> Dict[str, Any]:
#         """
#         检查模板文件
#
#         Args:
#             paragraph_ids: 段落ID列表
#
#         Returns:
#             Dict: 检查结果
#         """
#         if not Path(self.config.template_file).exists():
#             return {
#                 "valid": False,
#                 "error": f"模板文件不存在: {self.config.template_file}",
#                 "missing_placeholders": paragraph_ids
#             }
#
#         return self.word_service.validate_template(
#             template_file=self.config.template_file,
#             paragraph_ids=paragraph_ids,
#             placeholder_format=self.config.placeholder_format
#         )
#
#     def _create_template_file(self, paragraph_ids: List[str]) -> bool:
#         """
#         创建模板文件
#
#         Args:
#             paragraph_ids: 段落ID列表
#
#         Returns:
#             bool: 是否创建成功
#         """
#         return self.word_service.create_template_with_placeholders(
#             template_file=self.config.template_file,
#             paragraph_ids=paragraph_ids,
#             placeholder_format=self.config.placeholder_format
#         )
#
#     def _backup_template_file(self) -> Optional[str]:
#         """
#         备份模板文件
#
#         Returns:
#             Optional[str]: 备份文件路径（成功）或None
#         """
#         try:
#             template_path = Path(self.config.template_file)
#             # 使用线程安全的方式获取当前会话目录
#             try:
#                 outdir = get_current_output_dir(default="")
#                 if outdir:
#                     backup_dir = Path(outdir) / "outputs" / "word_backups"
#                     backup_dir.mkdir(parents=True, exist_ok=True)
#                     backup_path = backup_dir / f"{template_path.stem}_backup{template_path.suffix}"
#                 else:
#                     backup_path = template_path.parent / f"{template_path.stem}_backup{template_path.suffix}"
#             except Exception:
#                 backup_path = template_path.parent / f"{template_path.stem}_backup{template_path.suffix}"
#
#             import shutil
#             shutil.copy2(template_path, backup_path)
#             logger.info(f"模板文件已备份到: {backup_path}")
#             self._last_backup_path = str(backup_path)
#             return str(backup_path)
#
#         except Exception as e:
#             logger.error(f"备份模板文件失败: {e}", exc_info=True)
#             return None
#
#     def get_template_status(self, paragraph_ids: List[str]) -> Dict[str, Any]:
#         """
#         获取模板状态
#
#         Args:
#             paragraph_ids: 段落ID列表
#
#         Returns:
#             Dict: 模板状态信息
#         """
#         return self._check_template_file(paragraph_ids)
#
#     def create_template_from_config(self, paragraph_config: List[Dict[str, Any]]) -> bool:
#         """
#         根据配置创建模板文件
#
#         Args:
#             paragraph_config: 段落配置列表
#
#         Returns:
#             bool: 是否创建成功
#         """
#         paragraph_ids = [para.get('id') for para in paragraph_config if para.get('id')]
#         return self._create_template_file(paragraph_ids)
#
#
# class CSRWordIntegrator:
#     """CSR Word集成器 - 简化接口"""
#
#     def __init__(self,
#                  template_file: str,
#                  output_file: Optional[str] = None,
#                  placeholder_format: str = "{{%s}}",
#                  auto_create_template: bool = False):
#         """
#         初始化CSR Word集成器
#
#         Args:
#             template_file: Word模板文件路径
#             output_file: 输出文件路径
#             placeholder_format: 占位符格式
#             auto_create_template: 是否自动创建模板（仅用于测试）
#         """
#         self.config = WordIntegrationConfig(
#             template_file=template_file,
#             output_file=output_file,
#             placeholder_format=placeholder_format,
#             auto_create_template=auto_create_template
#         )
#         self.integration_service = WordIntegrationService(self.config)
#
#     def integrate_results(self,
#                          generation_results: List[Dict[str, Any]],
#                          paragraph_config: List[Dict[str, Any]]) -> WordIntegrationResult:
#         """
#         集成生成结果到Word文档
#
#         Args:
#             generation_results: CSR生成结果列表
#             paragraph_config: 段落配置列表
#
#         Returns:
#             WordIntegrationResult: 集成结果
#         """
#         return self.integration_service.integrate_csr_results(
#             generation_results, paragraph_config
#         )
#
#     def create_template(self, paragraph_config: List[Dict[str, Any]]) -> bool:
#         """
#         创建模板文件
#
#         Args:
#             paragraph_config: 段落配置列表
#
#         Returns:
#             bool: 是否创建成功
#         """
#         return self.integration_service.create_template_from_config(paragraph_config)
#
#     def get_status(self, paragraph_config: List[Dict[str, Any]]) -> Dict[str, Any]:
#         """
#         获取集成状态
#
#         Args:
#             paragraph_config: 段落配置列表
#
#         Returns:
#             Dict: 状态信息
#         """
#         paragraph_ids = [para.get('id') for para in paragraph_config if para.get('id')]
#         return self.integration_service.get_template_status(paragraph_ids)
