# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
#
# """
# 批量文件处理器
# 支持单文件和压缩包输入，批量处理并打包返回结果
# """
#
# import os
# import logging
# import shutil
# import tempfile
# import zipfile
# from pathlib import Path
#
# # RAR支持是可选的
# try:
#     import rarfile
#     RAR_SUPPORT = True
# except ImportError:
#     RAR_SUPPORT = False
#     rarfile = None
# from typing import List, Dict, Any, Optional, Union
# import json
# from dataclasses import dataclass
#
# from .service import PreprocessingService
# from .file_processor import FileType
# from service.windows.preprocessing.relation_builder import build_intermediate_files, build_relationship_mapping
#
# logger = logging.getLogger(__name__)
#
# @dataclass
# class BatchProcessResult:
#     """批量处理结果"""
#     success: bool
#     total_files: int
#     processed_files: int
#     failed_files: int
#     output_package: Optional[str]  # 输出压缩包路径
#     processing_summary: Dict[str, Any]
#     error_details: List[Dict[str, str]]
#
# class BatchProcessor:
#     """批量文件处理器"""
#
#     def __init__(self):
#         """初始化批量处理器"""
#         self.preprocessing_service = PreprocessingService()
#
#         # 根据RAR支持情况设置支持的压缩格式
#         if RAR_SUPPORT:
#             self.supported_archive_formats = ['.zip', '.rar']
#         else:
#             self.supported_archive_formats = ['.zip']
#
#         self.supported_file_formats = ['.docx', '.doc', '.rtf', '.pdf', '.xlsx', '.xls']
#
#     def process_input(self,
#                      input_path: Union[str, Path],
#                      output_dir: Optional[Union[str, Path]] = None,
#                      force_ocr: bool = False,
#                      extract_regions: bool = True,
#                      extract_assets: bool = True) -> BatchProcessResult:
#         """
#         处理输入（单文件或压缩包）
#
#         Args:
#             input_path: 输入文件或压缩包路径
#             output_dir: 输出目录（可选，默认为临时目录）
#             force_ocr: 是否强制OCR
#             extract_regions: 是否提取区域
#             extract_assets: 是否提取资源
#
#         Returns:
#             BatchProcessResult: 批量处理结果
#         """
#         input_path = Path(input_path)
#
#         if not input_path.exists():
#             return BatchProcessResult(
#                 success=False,
#                 total_files=0,
#                 processed_files=0,
#                 failed_files=0,
#                 output_package=None,
#                 processing_summary={"error": f"输入文件不存在: {input_path}"},
#                 error_details=[{"file": str(input_path), "error": "文件不存在"}]
#             )
#
#         # 创建工作目录
#         if output_dir:
#             work_dir = Path(output_dir)
#             work_dir.mkdir(parents=True, exist_ok=True)
#         else:
#             work_dir = Path(tempfile.mkdtemp(prefix="batch_processing_"))
#
#         try:
#             # 判断输入类型
#             if self._is_archive_file(input_path):
#                 logger.info(f"检测到压缩包: {input_path}")
#                 return self._process_archive(input_path, work_dir, force_ocr, extract_regions, extract_assets)
#             elif self._is_supported_file(input_path):
#                 logger.info(f"检测到单个文件: {input_path}")
#                 return self._process_single_file(input_path, work_dir, force_ocr, extract_regions, extract_assets)
#             else:
#                 return BatchProcessResult(
#                     success=False,
#                     total_files=1,
#                     processed_files=0,
#                     failed_files=1,
#                     output_package=None,
#                     processing_summary={"error": f"不支持的文件格式: {input_path.suffix}"},
#                     error_details=[{"file": str(input_path), "error": f"不支持的文件格式: {input_path.suffix}"}]
#                 )
#
#         except Exception as e:
#             logger.error(f"批量处理失败: {e}", exc_info=True)
#             return BatchProcessResult(
#                 success=False,
#                 total_files=0,
#                 processed_files=0,
#                 failed_files=0,
#                 output_package=None,
#                 processing_summary={"error": str(e)},
#                 error_details=[{"file": str(input_path), "error": str(e)}]
#             )
#
#     def _is_archive_file(self, file_path: Path) -> bool:
#         """检查是否为压缩包文件"""
#         return file_path.suffix.lower() in self.supported_archive_formats
#
#     def _is_supported_file(self, file_path: Path) -> bool:
#         """检查是否为支持的文件格式"""
#         return file_path.suffix.lower() in self.supported_file_formats
#
#     def _process_archive(self,
#                         archive_path: Path,
#                         work_dir: Path,
#                         force_ocr: bool,
#                         extract_regions: bool,
#                         extract_assets: bool) -> BatchProcessResult:
#         """处理压缩包"""
#
#         # 创建解压目录
#         extract_dir = work_dir / "extracted"
#         extract_dir.mkdir(parents=True, exist_ok=True)
#
#         # 解压文件
#         try:
#             extracted_files = self._extract_archive(archive_path, extract_dir)
#             logger.info(f"从压缩包中提取了 {len(extracted_files)} 个文件")
#         except Exception as e:
#             logger.error(f"解压失败: {e}", exc_info=True)
#             return BatchProcessResult(
#                 success=False,
#                 total_files=0,
#                 processed_files=0,
#                 failed_files=0,
#                 output_package=None,
#                 processing_summary={"error": f"解压失败: {e}"},
#                 error_details=[{"file": str(archive_path), "error": f"解压失败: {e}"}]
#             )
#
#         # 过滤支持的文件
#         supported_files = [f for f in extracted_files if self._is_supported_file(f)]
#         logger.info(f"找到 {len(supported_files)} 个支持的文件")
#
#         if not supported_files:
#             return BatchProcessResult(
#                 success=False,
#                 total_files=len(extracted_files),
#                 processed_files=0,
#                 failed_files=len(extracted_files),
#                 output_package=None,
#                 processing_summary={"error": "压缩包中没有支持的文件格式"},
#                 error_details=[{"file": "压缩包", "error": "没有支持的文件格式"}]
#             )
#
#         # 批量处理文件
#         return self._process_multiple_files(supported_files, work_dir, force_ocr, extract_regions, extract_assets)
#
#     def _process_single_file(self,
#                            file_path: Path,
#                            work_dir: Path,
#                            force_ocr: bool,
#                            extract_regions: bool,
#                            extract_assets: bool) -> BatchProcessResult:
#         """处理单个文件"""
#
#         return self._process_multiple_files([file_path], work_dir, force_ocr, extract_regions, extract_assets)
#
#     def _process_multiple_files(self,
#                               file_paths: List[Path],
#                               work_dir: Path,
#                               force_ocr: bool,
#                               extract_regions: bool,
#                               extract_assets: bool) -> BatchProcessResult:
#         """处理多个文件"""
#
#         total_files = len(file_paths)
#         processed_files = 0
#         failed_files = 0
#         error_details = []
#         processing_results = {}
#
#         # 创建结果目录
#         results_dir = work_dir / "processing_results"
#         results_dir.mkdir(parents=True, exist_ok=True)
#
#         logger.info(f"开始处理 {total_files} 个文件...")
#
#         for i, file_path in enumerate(file_paths, 1):
#             logger.info(f"处理文件 {i}/{total_files}: {file_path.name}")
#
#             try:
#                 # 为每个文件创建独立的输出目录（处理长文件名）
#                 safe_name = self._get_safe_filename(file_path.stem)
#                 file_output_dir = results_dir / f"{safe_name}_processed"
#                 file_output_dir.mkdir(parents=True, exist_ok=True)
#
#                 # 处理文件
#                 result = self.preprocessing_service.preprocess(
#                     file_path=str(file_path),
#                     force_ocr=force_ocr,
#                     extract_regions=extract_regions,
#                     extract_assets=extract_assets
#                 )
#
#                 # 保存处理结果到文件输出目录
#                 self._save_processing_result(result, file_output_dir, file_path)
#
#                 processed_files += 1
#                 processing_results[file_path.name] = {
#                     "status": "success",
#                     "output_dir": str(file_output_dir.relative_to(work_dir)),
#                     "file_type": result.file_type.value if result.file_type else "unknown",
#                     "content_type": result.content_type.value if result.content_type else "unknown",
#                     "text_length": len(result.text_content) if result.text_content else 0,
#                     "markdown_length": len(result.markdown_content) if result.markdown_content else 0,
#                     "regions_count": len(result.regions),
#                     "assets_count": len(result.assets)
#                 }
#
#                 logger.info(f"✅ 文件处理成功: {file_path.name}")
#
#             except Exception as e:
#                 failed_files += 1
#                 error_msg = str(e)
#                 error_details.append({
#                     "file": file_path.name,
#                     "error": error_msg
#                 })
#                 processing_results[file_path.name] = {
#                     "status": "failed",
#                     "error": error_msg
#                 }
#
#                 logger.error(f"❌ 文件处理失败: {file_path.name} - {error_msg}")
#
#         # 生成处理摘要
#         summary = {
#             "total_files": total_files,
#             "processed_files": processed_files,
#             "failed_files": failed_files,
#             "success_rate": f"{(processed_files/total_files*100):.1f}%" if total_files > 0 else "0%",
#             "processing_results": processing_results
#         }
#
#         # 保存摘要文件
#         summary_file = results_dir / "processing_summary.json"
#         with open(summary_file, 'w', encoding='utf-8') as f:
#             json.dump(summary, f, ensure_ascii=False, indent=2)
#
#         # 创建输出压缩包
#         output_package = None
#         if processed_files > 0:
#             try:
#                 output_package = self._create_output_package(work_dir, results_dir)
#                 logger.info(f"✅ 输出包创建成功: {output_package}")
#             except Exception as e:
#                 logger.error(f"❌ 创建输出包失败: {e}", exc_info=True)
#
#         return BatchProcessResult(
#             success=processed_files > 0,
#             total_files=total_files,
#             processed_files=processed_files,
#             failed_files=failed_files,
#             output_package=output_package,
#             processing_summary=summary,
#             error_details=error_details
#         )
#
#     def _extract_archive(self, archive_path: Path, extract_dir: Path) -> List[Path]:
#         """解压压缩包"""
#         extracted_files = []
#
#         if archive_path.suffix.lower() == '.zip':
#             with zipfile.ZipFile(archive_path, 'r') as zip_ref:
#                 zip_ref.extractall(extract_dir)
#                 for file_info in zip_ref.filelist:
#                     if not file_info.is_dir():
#                         extracted_files.append(extract_dir / file_info.filename)
#
#         elif archive_path.suffix.lower() == '.rar':
#             if not RAR_SUPPORT:
#                 raise ValueError("RAR支持未安装，请安装rarfile库: pip install rarfile")
#
#             with rarfile.RarFile(archive_path, 'r') as rar_ref:
#                 rar_ref.extractall(extract_dir)
#                 for file_info in rar_ref.infolist():
#                     if not file_info.is_dir():
#                         extracted_files.append(extract_dir / file_info.filename)
#
#         else:
#             raise ValueError(f"不支持的压缩格式: {archive_path.suffix}")
#
#         return extracted_files
#
#     def _save_processing_result(self, result, output_dir: Path, original_file: Path):
#         """保存处理结果到指定目录（文件夹结构版本）"""
#
#         # 使用原始文件名（安全处理）
#         safe_stem = self._get_safe_filename(original_file.stem)
#
#         # 直接使用 output_dir（已经是 {safe_name}_processed）
#         file_dir = output_dir
#         file_dir.mkdir(parents=True, exist_ok=True)
#
#         # 1. 保存Markdown内容
#         if result.markdown_content:
#             markdown_file = file_dir / f"{safe_stem}.md"
#             markdown_file.write_text(result.markdown_content, encoding='utf-8')
#
#         # 2. 保存分块JSON（使用原始文件名）
#         chunks_data = self._save_chunks_with_original_name(result, file_dir, safe_stem)
#
#         # 3. 保存提取的图片和表格文件（使用标签命名）
#         extracted_files = self._save_extracted_files_with_structure(result, file_dir)
#
#         # 4. 保存转换过程中的中间文件
#         intermediate_files = self._save_intermediate_files(result, file_dir, safe_stem)
#
#         # 5. 创建关系映射JSON
#         relationship_data = self._create_relationship_mapping(
#             result, original_file, safe_stem, chunks_data, extracted_files, intermediate_files
#         )
#
#         # 保存关系映射JSON
#         relationship_file = file_dir / f"{safe_stem}_relationship.json"
#         with open(relationship_file, 'w', encoding='utf-8') as f:
#             json.dump(relationship_data, f, ensure_ascii=False, indent=2)
#
#     def _create_output_package(self, work_dir: Path, results_dir: Path) -> str:
#         """创建输出压缩包"""
#
#         # 输出包路径
#         output_package = work_dir / "batch_processing_results.zip"
#
#         # 创建ZIP文件
#         with zipfile.ZipFile(output_package, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
#             # 添加所有结果文件
#             for file_path in results_dir.rglob('*'):
#                 if file_path.is_file():
#                     # 计算相对路径
#                     arcname = file_path.relative_to(results_dir)
#                     zip_ref.write(file_path, arcname)
#
#         return str(output_package)
#
#     def _get_safe_filename(self, filename: str, max_length: int = 50) -> str:
#         """
#         生成安全的文件名，避免路径过长问题
#
#         Args:
#             filename: 原始文件名
#             max_length: 最大长度
#
#         Returns:
#             str: 安全的文件名
#         """
#         import re
#         import hashlib
#
#         # 移除特殊字符
#         safe_name = re.sub(r'[^\w\u4e00-\u9fff\-_.]', '_', filename)
#
#         # 如果文件名太长，截断并添加哈希
#         if len(safe_name) > max_length:
#             # 计算原始文件名的哈希
#             hash_suffix = hashlib.md5(filename.encode('utf-8')).hexdigest()[:8]
#             # 截断文件名并添加哈希后缀
#             safe_name = safe_name[:max_length-9] + '_' + hash_suffix
#
#         return safe_name
#
#     def _create_enhanced_structured_data(self, result, original_file: Path) -> Dict[str, Any]:
#         """
#         创建增强的结构化数据，整合所有信息
#
#         Args:
#             result: 预处理结果
#             original_file: 原始文件路径
#
#         Returns:
#             Dict: 增强的结构化数据
#         """
#         import datetime
#
#         # 基础文件信息
#         enhanced_data = {
#             "document_info": {
#                 "source_file": str(original_file),
#                 "file_name": original_file.name,
#                 "file_type": result.file_type.value if result.file_type else "unknown",
#                 "content_type": result.content_type.value if result.content_type else "unknown",
#                 "processed_at": datetime.datetime.now().isoformat(),
#                 "processing_time": result.processing_info.get("processing_time", 0)
#             },
#             "content_stats": {
#                 "text_length": len(result.text_content) if result.text_content else 0,
#                 "markdown_length": len(result.markdown_content) if result.markdown_content else 0,
#                 "regions_count": len(result.regions),
#                 "assets_count": len(result.assets)
#             }
#         }
#
#         # 加载并整合结构化分块数据
#         structured_chunks_file = result.processing_info.get('structured_chunks_file')
#         if structured_chunks_file and Path(structured_chunks_file).exists():
#             try:
#                 with open(structured_chunks_file, 'r', encoding='utf-8') as f:
#                     chunks_data = json.load(f)
#
#                 enhanced_data["content"] = {
#                     "total_sections": chunks_data.get("total_sections", 0),
#                     "sections": chunks_data.get("sections", [])
#                 }
#             except Exception as e:
#                 logger.warning(f"无法加载结构化分块数据: {e}")
#                 enhanced_data["content"] = {
#                     "total_sections": 0,
#                     "sections": []
#                 }
#         else:
#             enhanced_data["content"] = {
#                 "total_sections": 0,
#                 "sections": []
#             }
#
#         # 整合区域信息
#         if result.regions:
#             enhanced_data["regions"] = []
#             for i, region in enumerate(result.regions):
#                 region_data = {
#                     "id": f"region_{i+1}",
#                     "type": region.get("type", "unknown") if isinstance(region, dict) else "text",
#                     "name": region.get("name", f"region_{i+1}") if isinstance(region, dict) else f"region_{i+1}",
#                     "content": str(region)
#                 }
#                 enhanced_data["regions"].append(region_data)
#
#         # 整合资源信息
#         if result.assets:
#             enhanced_data["assets"] = []
#             for i, asset in enumerate(result.assets):
#                 asset_data = {
#                     "id": f"asset_{i+1}",
#                     "type": asset.get("type", "unknown") if isinstance(asset, dict) else "unknown",
#                     "name": asset.get("name", f"asset_{i+1}") if isinstance(asset, dict) else f"asset_{i+1}",
#                     "path": asset.get("path", "") if isinstance(asset, dict) else "",
#                     "content": str(asset)
#                 }
#                 enhanced_data["assets"].append(asset_data)
#
#         # 添加处理元数据
#         enhanced_data["processing_metadata"] = {
#             "conversion_method": result.processing_info.get("conversion_method", "unknown"),
#             "ocr_used": result.processing_info.get("ocr_used", False),
#             "chunking_mode": result.processing_info.get("chunking_mode", "unknown"),
#             "llm_summary_enabled": result.processing_info.get("llm_summary_enabled", False)
#         }
#
#         return enhanced_data
#
#     def _save_chunks_with_original_name(self, result, file_dir: Path, safe_stem: str) -> Dict[str, str]:
#         """
#         保存分块文件，使用原始文件名
#
#         Args:
#             result: 预处理结果
#             file_dir: 文件目录
#             safe_stem: 安全的文件名前缀
#
#         Returns:
#             Dict: 分块文件路径映射
#         """
#         chunks_files = {}
#
#         # 保存结构化分块JSON
#         structured_chunks_file = result.processing_info.get('structured_chunks_file')
#         if structured_chunks_file and Path(structured_chunks_file).exists():
#             target_chunks_file = file_dir / f"{safe_stem}_chunks.json"
#             shutil.copy2(structured_chunks_file, target_chunks_file)
#             chunks_files['structured_chunks'] = f"{safe_stem}_chunks.json"
#
#         # 移除 chunks_index.json 复制，只保留 structured_chunks_file
#         return chunks_files
#
#     def _save_extracted_files_with_structure(self, result, file_dir: Path) -> Dict[str, List[Dict]]:
#         """
#         保存提取的文件，使用标签命名（仅Word文档，无描述文件）
#
#         Args:
#             result: 预处理结果
#             file_dir: 文件目录
#
#         Returns:
#             Dict: 提取文件的路径映射
#         """
#         extracted_files = {
#             'regions': [],
#             'assets': []
#         }
#
#         # 保存区域文件（表格等）- 仅Word文档才有实际文件
#         if result.regions:
#             regions_dir = file_dir / "regions"
#             regions_dir.mkdir(exist_ok=True)
#
#             for i, region in enumerate(result.regions):
#                 region_info = {}
#
#                 if isinstance(region, dict):
#                     # 使用区域名称或标签作为文件名
#                     region_name = region.get('name', f"region_{i+1}")
#                     region_type = region.get('type', 'unknown')
#
#                     # 检查 Word 文件路径（word_file）或通用文件路径（file_path）
#                     src_path = region.get('word_file') or region.get('file_path')
#                     if src_path:
#                         src_file = Path(src_path)
#                         if src_file.exists():
#                             # 使用标签名称 + 原始扩展名
#                             dst_file = regions_dir / f"{region_name}{src_file.suffix}"
#                             try:
#                                 shutil.copy2(src_file, dst_file)
#                                 region_info = {
#                                     'id': region_name,
#                                     'type': region_type,
#                                     'file_path': f"regions/{region_name}{src_file.suffix}"
#                                 }
#                                 logger.info(f"保存表格文件: {dst_file}")
#                             except Exception as e:
#                                 logger.warning(f"复制区域文件失败: {e}")
#                                 continue
#                     else:
#                         # 无实际文件的区域，跳过
#                         continue
#                 else:
#                     # 简单区域内容，跳过
#                     continue
#
#                 if region_info:  # 只添加有实际文件的区域
#                     extracted_files['regions'].append(region_info)
#
#         # 保存资源文件（图片等）- 仅Word文档才有实际文件
#         if result.assets:
#             assets_dir = file_dir / "assets"
#             assets_dir.mkdir(exist_ok=True)
#
#             for i, asset in enumerate(result.assets):
#                 asset_info = {}
#
#                 if isinstance(asset, dict):
#                     # 使用资源名称或标签作为文件名
#                     asset_name = asset.get('name', f"asset_{i+1}")
#                     asset_type = asset.get('type', 'unknown')
#
#                     # 只有Word文档才会有实际的图片文件
#                     if 'path' in asset and asset['path']:
#                         src_file = Path(asset['path'])
#                         if src_file.exists():
#                             # 使用标签名称 + 原始扩展名
#                             dst_file = assets_dir / f"{asset_name}{src_file.suffix}"
#                             try:
#                                 shutil.copy2(src_file, dst_file)
#                                 asset_info = {
#                                     'id': asset_name,
#                                     'type': asset_type,
#                                     'file_path': f"assets/{asset_name}{src_file.suffix}"
#                                 }
#                                 logger.info(f"保存图片文件: {dst_file}")
#                             except Exception as e:
#                                 logger.warning(f"复制资源文件失败: {e}")
#                                 continue
#                     else:
#                         # 非Word文档或无实际文件的资源，跳过
#                         continue
#                 else:
#                     # 简单资源内容，跳过
#                     continue
#
#                 if asset_info:  # 只添加有实际文件的资源
#                     extracted_files['assets'].append(asset_info)
#
#         return extracted_files
#
#     def _save_intermediate_files(self, result, file_dir: Path, safe_stem: str) -> Dict[str, Any]:
#         """委派到 relation_builder 以保存中间文件并返回描述信息"""
#         return build_intermediate_files(result, file_dir, safe_stem)
#
#     def _create_relationship_mapping(self, result, original_file: Path, safe_stem: str,
#                                    chunks_data: Dict, extracted_files: Dict, intermediate_files: Dict) -> Dict[str, Any]:
#         """委派到 relation_builder 构建关系映射 JSON"""
#         return build_relationship_mapping(result, original_file, safe_stem, chunks_data, extracted_files, intermediate_files)
#         if chunks_data.get('structured_chunks'):
#             try:
#                 chunks_file_path = Path(safe_stem) / chunks_data['structured_chunks']
#                 structured_chunks_file = result.processing_info.get('structured_chunks_file')
#                 if structured_chunks_file and Path(structured_chunks_file).exists():
#                     with open(structured_chunks_file, 'r', encoding='utf-8') as f:
#                         chunks_json = json.load(f)
#                     relationship_data["content_stats"]["total_sections"] = chunks_json.get("total_sections", 0)
#             except Exception as e:
#                 logger.warning(f"无法读取分块文件获取章节数量: {e}")
#
#         return relationship_data
#
#     def _save_extracted_files(self, result, output_dir: Path, safe_stem: str):
#         """
#         保存提取的图片和表格文件
#
#         Args:
#             result: 预处理结果
#             output_dir: 输出目录
#             safe_stem: 安全的文件名前缀
#         """
#
#         # 保存区域文件（表格等）
#         if result.regions:
#             regions_dir = output_dir / "regions"
#             regions_dir.mkdir(exist_ok=True)
#
#             for i, region in enumerate(result.regions):
#                 if isinstance(region, dict):
#                     # 如果区域包含文件路径，复制文件
#                     if 'file_path' in region and region['file_path']:
#                         src_file = Path(region['file_path'])
#                         if src_file.exists():
#                             # 保持原始文件名或使用区域名称
#                             file_name = region.get('name', f"region_{i+1}")
#                             if not file_name.endswith(src_file.suffix):
#                                 file_name += src_file.suffix
#
#                             dst_file = regions_dir / file_name
#                             try:
#                                 shutil.copy2(src_file, dst_file)
#                                 logger.info(f"复制区域文件: {src_file} -> {dst_file}")
#                             except Exception as e:
#                                 logger.warning(f"复制区域文件失败: {e}")
#
#                     # 保存区域内容为文本文件
#                     region_text_file = regions_dir / f"region_{i+1}.txt"
#                     region_content = region.get('content', str(region))
#                     region_text_file.write_text(region_content, encoding='utf-8')
#                 else:
#                     # 简单的区域内容
#                     region_file = regions_dir / f"region_{i+1}.txt"
#                     region_file.write_text(str(region), encoding='utf-8')
#
#         # 保存资源文件（图片等）
#         if result.assets:
#             assets_dir = output_dir / "assets"
#             assets_dir.mkdir(exist_ok=True)
#
#             for i, asset in enumerate(result.assets):
#                 if isinstance(asset, dict):
#                     # 如果资源包含文件路径，复制文件
#                     if 'path' in asset and asset['path']:
#                         src_file = Path(asset['path'])
#                         if src_file.exists():
#                             # 保持原始文件名或使用资源名称
#                             file_name = asset.get('name', f"asset_{i+1}")
#                             if not file_name.endswith(src_file.suffix):
#                                 file_name += src_file.suffix
#
#                             dst_file = assets_dir / file_name
#                             try:
#                                 shutil.copy2(src_file, dst_file)
#                                 logger.info(f"复制资源文件: {src_file} -> {dst_file}")
#                             except Exception as e:
#                                 logger.warning(f"复制资源文件失败: {e}")
#
#                     # 保存资源描述为文本文件
#                     asset_text_file = assets_dir / f"asset_{i+1}.txt"
#                     asset_content = asset.get('content', str(asset))
#                     asset_text_file.write_text(asset_content, encoding='utf-8')
#                 else:
#                     # 简单的资源内容
#                     asset_file = assets_dir / f"asset_{i+1}.txt"
#                     asset_file.write_text(str(asset), encoding='utf-8')
#
#     def _safe_copy_tree(self, src_dir: Path, dst_dir: Path):
#         """
#         安全复制目录树，处理长文件名问题
#
#         Args:
#             src_dir: 源目录
#             dst_dir: 目标目录
#         """
#         try:
#             # 创建目标目录
#             dst_dir.mkdir(parents=True, exist_ok=True)
#
#             # 遍历源目录中的所有文件
#             for src_file in src_dir.rglob('*'):
#                 if src_file.is_file():
#                     # 计算相对路径
#                     rel_path = src_file.relative_to(src_dir)
#
#                     # 生成安全的目标路径
#                     safe_rel_path = self._get_safe_path(rel_path)
#                     dst_file = dst_dir / safe_rel_path
#
#                     # 确保目标目录存在
#                     dst_file.parent.mkdir(parents=True, exist_ok=True)
#
#                     # 复制文件
#                     try:
#                         shutil.copy2(src_file, dst_file)
#                     except Exception as e:
#                         logger.warning(f"复制文件失败: {src_file} -> {dst_file}: {e}")
#                         # 尝试只复制内容，不保留元数据
#                         try:
#                             shutil.copy(src_file, dst_file)
#                         except Exception as e2:
#                             logger.error(f"文件复制完全失败: {src_file}: {e2}", exc_info=True)
#
#         except Exception as e:
#             logger.error(f"目录复制失败: {src_dir} -> {dst_dir}: {e}", exc_info=True)
#
#     def _get_safe_path(self, path: Path) -> Path:
#         """
#         生成安全的路径，处理长文件名
#
#         Args:
#             path: 原始路径
#
#         Returns:
#             Path: 安全的路径
#         """
#         parts = []
#         for part in path.parts:
#             safe_part = self._get_safe_filename(part, max_length=30)
#             parts.append(safe_part)
#
#         return Path(*parts) if parts else Path(".")
#
# # 创建全局实例
# batch_processor = BatchProcessor()
