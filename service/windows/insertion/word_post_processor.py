# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
#
# """
# Word后处理器
# 在CSR生成完成后，将结果插入到Word文档中
# """
#
# import logging
# import time
# from pathlib import Path
# from typing import Dict, List, Optional, Any
# from dataclasses import dataclass
#
# from service.windows.insertion.word_integration import CSRWordIntegrator, WordIntegrationResult
# from utils.context_manager import get_current_output_dir
#
# logger = logging.getLogger(__name__)
#
# @dataclass
# class WordPostProcessConfig:
#     """Word后处理配置"""
#     template_file: str  # Word模板文件路径
#     output_file: Optional[str] = None  # 输出文件路径
#     placeholder_format: str = "{{%s}}"  # 占位符格式
#     backup_original: bool = True  # 是否备份原文件
#     auto_generate_output_name: bool = True  # 是否自动生成输出文件名
#
# class WordPostProcessor:
#     """Word后处理器"""
#
#     def __init__(self, config: WordPostProcessConfig):
#         """
#         初始化Word后处理器
#
#         Args:
#             config: Word后处理配置
#         """
#         self.config = config
#         self.integrator = CSRWordIntegrator(
#             template_file=config.template_file,
#             output_file=config.output_file,
#             placeholder_format=config.placeholder_format,
#             auto_create_template=False
#         )
#         logger.info("Word后处理器初始化完成")
#
#     def process_csr_results(self,
#                            generation_results: List[Dict[str, Any]],
#                            paragraph_config: List[Dict[str, Any]]) -> WordIntegrationResult:
#         """
#         处理CSR生成结果，插入到Word文档中
#
#         Args:
#             generation_results: CSR生成结果列表
#             paragraph_config: 段落配置列表
#
#         Returns:
#             WordIntegrationResult: 处理结果
#         """
#         try:
#             logger.info("开始Word后处理...")
#
#             # 使用线程安全的方式获取当前会话目录
#             try:
#                 outdir = get_current_output_dir(default="")
#                 if outdir:
#                     ts = time.strftime("%Y%m%d_%H%M%S")
#                     target = Path(outdir) / "outputs" / f"csr_result_{ts}.docx"
#                     # 同步设置到两处配置，确保下游使用
#                     self.config.output_file = str(target)
#                     self.integrator.config.output_file = str(target)
#                     logger.info(f"Word输出已重定向到当前会话: {target}")
#             except Exception as _e:
#                 logger.warning(f"设置Word输出到会话目录失败: {_e}")
#
#             # 自动生成输出文件名（如果未指定）
#             if self.config.auto_generate_output_name and not self.config.output_file:
#                 output_file = self._generate_output_filename()
#                 self.config.output_file = output_file
#                 self.integrator.config.output_file = output_file
#                 logger.info(f"自动生成输出文件名: {output_file}")
#
#             # 执行Word集成
#             result = self.integrator.integrate_results(generation_results, paragraph_config)
#
#             if result.success:
#                 logger.info(f"Word后处理完成: {result.output_file}")
#             else:
#                 logger.error(f"Word后处理失败: {result.error}")
#
#             return result
#
#         except Exception as e:
#             logger.error(f"Word后处理异常: {e}", exc_info=True)
#             return WordIntegrationResult(
#                 success=False,
#                 message="Word后处理异常",
#                 error=str(e)
#             )
#
#     def _generate_output_filename(self) -> str:
#         """
#         自动生成输出文件名
#
#         Returns:
#             str: 输出文件路径
#         """
#         template_path = Path(self.config.template_file)
#         timestamp = time.strftime("%Y%m%d_%H%M%S")
#
#         # 生成输出文件名：原文件名_时间戳.docx
#         output_name = f"{template_path.stem}_filled_{timestamp}{template_path.suffix}"
#         output_path = template_path.parent / output_name
#
#         return str(output_path)
#
#     def validate_template(self, paragraph_config: List[Dict[str, Any]]) -> Dict[str, Any]:
#         """
#         验证模板文件
#
#         Args:
#             paragraph_config: 段落配置列表
#
#         Returns:
#             Dict: 验证结果
#         """
#         return self.integrator.get_status(paragraph_config)
#
#     def get_template_info(self) -> Dict[str, Any]:
#         """
#         获取模板文件信息
#
#         Returns:
#             Dict: 模板信息
#         """
#         template_path = Path(self.config.template_file)
#
#         return {
#             "template_file": self.config.template_file,
#             "template_exists": template_path.exists(),
#             "template_size": template_path.stat().st_size if template_path.exists() else 0,
#             "placeholder_format": self.config.placeholder_format,
#             "output_file": self.config.output_file
#         }
#
#
# class CSRWordPostProcessor:
#     """CSR Word后处理器 - 简化接口"""
#
#     def __init__(self,
#                  template_file: str,
#                  output_file: Optional[str] = None,
#                  placeholder_format: str = "{{%s}}",
#                  backup_original: bool = True):
#         """
#         初始化CSR Word后处理器
#
#         Args:
#             template_file: Word模板文件路径
#             output_file: 输出文件路径
#             placeholder_format: 占位符格式
#             backup_original: 是否备份原文件
#         """
#         self.config = WordPostProcessConfig(
#             template_file=template_file,
#             output_file=output_file,
#             placeholder_format=placeholder_format,
#             backup_original=backup_original
#         )
#         self.processor = WordPostProcessor(self.config)
#
#     def process_results(self,
#                        generation_results: List[Dict[str, Any]],
#                        paragraph_config: List[Dict[str, Any]]) -> WordIntegrationResult:
#         """
#         处理生成结果
#
#         Args:
#             generation_results: CSR生成结果列表
#             paragraph_config: 段落配置列表
#
#         Returns:
#             WordIntegrationResult: 处理结果
#         """
#         return self.processor.process_csr_results(generation_results, paragraph_config)
#
#     def validate_template(self, paragraph_config: List[Dict[str, Any]]) -> Dict[str, Any]:
#         """
#         验证模板
#
#         Args:
#             paragraph_config: 段落配置列表
#
#         Returns:
#             Dict: 验证结果
#         """
#         return self.processor.validate_template(paragraph_config)
#
#     def get_template_info(self) -> Dict[str, Any]:
#         """
#         获取模板信息
#
#         Returns:
#             Dict: 模板信息
#         """
#         return self.processor.get_template_info()
#
#
# # 便捷函数
# def create_word_post_processor(template_file: str, **kwargs) -> CSRWordPostProcessor:
#     """
#     创建Word后处理器实例
#
#     Args:
#         template_file: Word模板文件路径
#         **kwargs: 其他配置参数
#
#     Returns:
#         CSRWordPostProcessor: 后处理器实例
#     """
#     return CSRWordPostProcessor(template_file=template_file, **kwargs)
# #
#
# def process_csr_to_word(generation_results: List[Dict[str, Any]],
#                        paragraph_config: List[Dict[str, Any]],
#                        template_file: str,
#                        output_file: Optional[str] = None,
#                        placeholder_format: str = "{{%s}}") -> WordIntegrationResult:
#     """
#     便捷函数：将CSR结果处理到Word文档
#
#     Args:
#         generation_results: CSR生成结果列表
#         paragraph_config: 段落配置列表
#         template_file: Word模板文件路径
#         output_file: 输出文件路径
#         placeholder_format: 占位符格式
#
#     Returns:
#         WordIntegrationResult: 处理结果
#     """
#     processor = create_word_post_processor(
#         template_file=template_file,
#         output_file=output_file,
#         placeholder_format=placeholder_format
#     )
#
#     return processor.process_results(generation_results, paragraph_config)
