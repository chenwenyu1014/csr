# # -*- coding: utf-8 -*-
# from __future__ import annotations
# from pathlib import Path
# from typing import Dict, Any, List
# import shutil
# import json
# import logging
#
# logger = logging.getLogger(__name__)
#
#
# def build_intermediate_files(result, file_dir: Path, safe_stem: str) -> Dict[str, Any]:
#     """
#     Copy intermediate files into file_dir/intermediate and return a normalized info dict.
#     Expected result has 'processing_info' with optional keys used below.
#     """
#     intermediate_info = {
#         'conversion_steps': [],
#         'intermediate_files': []
#     }
#
#     metadata = result.processing_info if hasattr(result, 'processing_info') else (result.get('metadata') or {})
#     conversion_method = metadata.get('conversion_method', '')
#
#     intermediate_dir = file_dir / "intermediate"
#     intermediate_files_found = False
#
#     # 1. PDF intermediate
#     if 'pdf_file' in metadata and metadata['pdf_file']:
#         pdf_file = Path(metadata['pdf_file'])
#         if pdf_file.exists():
#             intermediate_dir.mkdir(exist_ok=True)
#             dst_pdf = intermediate_dir / f"{safe_stem}_intermediate.pdf"
#             try:
#                 shutil.copy2(pdf_file, dst_pdf)
#                 intermediate_info['intermediate_files'].append({
#                     'type': 'pdf',
#                     'file_path': f"intermediate/{dst_pdf.name}",
#                     'description': 'RTF/DOC转换的中间PDF文件'
#                 })
#                 intermediate_files_found = True
#                 logger.info(f"保存中间PDF文件: {dst_pdf}")
#             except Exception as e:
#                 logger.warning(f"保存中间PDF文件失败: {e}")
#
#     # 2. Excel split sheets
#     if 'excel_sheets' in metadata and metadata['excel_sheets']:
#         sheets_info = metadata['excel_sheets']
#         if isinstance(sheets_info, list) and sheets_info:
#             intermediate_dir.mkdir(exist_ok=True)
#             for i, sheet_info in enumerate(sheets_info):
#                 if isinstance(sheet_info, dict) and 'file_path' in sheet_info:
#                     src_file = Path(sheet_info['file_path'])
#                     if src_file.exists():
#                         sheet_name = sheet_info.get('sheet_name', f'Sheet_{i+1}')
#                         dst_file = intermediate_dir / f"{safe_stem}_{sheet_name}.xlsx"
#                         try:
#                             shutil.copy2(src_file, dst_file)
#                             intermediate_info['intermediate_files'].append({
#                                 'type': 'excel_sheet',
#                                 'file_path': f"intermediate/{dst_file.name}",
#                                 'sheet_name': sheet_name,
#                                 'description': f'Excel工作表: {sheet_name}'
#                             })
#                             intermediate_files_found = True
#                             logger.info(f"保存Excel工作表: {dst_file}")
#                         except Exception as e:
#                             logger.warning(f"保存Excel工作表失败: {e}")
#
#     # 2.b Excel Markdown
#     if 'excel_markdown_regions' in metadata and metadata['excel_markdown_regions']:
#         md_list = metadata['excel_markdown_regions']
#         if isinstance(md_list, list) and md_list:
#             md_dir = (file_dir / "intermediate" / "markdown")
#             md_dir.mkdir(parents=True, exist_ok=True)
#             # Try best-effort copy, handling either absolute or relative paths
#             for md in md_list:
#                 try:
#                     if isinstance(md, dict):
#                         md_rel = md.get('markdown_file') or md.get('file_path')
#                         sheet_name = md.get('sheet_name') or 'Sheet'
#                         if md_rel:
#                             # md_rel should be absolute path from excel_pipeline
#                             src_md = Path(md_rel)
#                             if src_md.exists():
#                                 dst_md = md_dir / src_md.name
#                                 shutil.copy2(src_md, dst_md)
#                                 intermediate_info['intermediate_files'].append({
#                                     'type': 'markdown',
#                                     'file_path': f"intermediate/markdown/{dst_md.name}",
#                                     'sheet_name': sheet_name
#                                 })
#                                 intermediate_files_found = True
#                                 logger.info(f"保存Excel Markdown: {dst_md}")
#                 except Exception as e:
#                     logger.warning(f"保存Excel Markdown失败: {e}")
#
#     # 3. Word intermediate DOCX
#     if 'word_intermediate_docx' in metadata and metadata['word_intermediate_docx']:
#         docx_file = Path(metadata['word_intermediate_docx'])
#         if docx_file.exists():
#             intermediate_dir.mkdir(exist_ok=True)
#             dst_docx = intermediate_dir / f"{safe_stem}_intermediate.docx"
#             try:
#                 shutil.copy2(docx_file, dst_docx)
#                 intermediate_info['intermediate_files'].append({
#                     'type': 'docx',
#                     'file_path': f"intermediate/{dst_docx.name}",
#                     'description': 'Word转换的中间DOCX文件'
#                 })
#                 intermediate_files_found = True
#                 logger.info(f"保存中间DOCX文件: {dst_docx}")
#             except Exception as e:
#                 logger.warning(f"保存中间DOCX文件失败: {e}")
#
#     # 4. rtf->excel intermediate
#     if 'rtf_intermediate_excel' in metadata and metadata['rtf_intermediate_excel']:
#         xfile = Path(metadata['rtf_intermediate_excel'])
#         if xfile.exists():
#             intermediate_dir.mkdir(exist_ok=True)
#             dst_excel = intermediate_dir / f"{safe_stem}_rtf_intermediate.xlsx"
#             try:
#                 shutil.copy2(xfile, dst_excel)
#                 intermediate_info['intermediate_files'].append({
#                     'type': 'excel',
#                     'file_path': f"intermediate/{dst_excel.name}",
#                     'description': 'RTF转换生成的中间Excel'
#                 })
#                 intermediate_files_found = True
#                 logger.info(f"保存RTF中间Excel: {dst_excel}")
#             except Exception as e:
#                 logger.warning(f"保存RTF中间Excel失败: {e}")
#
#     # Steps
#     if 'steps' in metadata:
#         intermediate_info['conversion_steps'] = metadata['steps']
#     elif conversion_method:
#         if 'pdf_ocr' in conversion_method:
#             intermediate_info['conversion_steps'] = ['word_to_pdf', 'pdf_to_markdown']
#         elif 'word_com' in conversion_method:
#             intermediate_info['conversion_steps'] = ['word_to_markdown']
#         elif 'excel' in conversion_method:
#             intermediate_info['conversion_steps'] = ['excel_split_sheets']
#
#     # Best-effort cleanup of empty dir is handled by caller if needed
#     return intermediate_info
#
#
# def build_relationship_mapping(result, original_file: Path, safe_stem: str,
#                                chunks_data: Dict, extracted_files: Dict,
#                                intermediate_files: Dict) -> Dict[str, Any]:
#     import datetime
#     return {
#         "document_info": {
#             "original_file": str(original_file),
#             "file_name": original_file.name,
#             "safe_name": safe_stem,
#             "file_type": result.file_type.value if getattr(result, 'file_type', None) else "unknown",
#             "content_type": result.content_type.value if getattr(result, 'content_type', None) else "unknown",
#             "processed_at": datetime.datetime.now().isoformat(),
#             "processing_time": (result.processing_info or {}).get("processing_time", 0)
#         },
#         "file_structure": {
#             "base_folder": safe_stem,
#             "markdown_file": f"{safe_stem}.md",
#             "relationship_file": f"{safe_stem}_relationship.json"
#         },
#         "content_files": {
#             "chunks": chunks_data,
#             "regions": extracted_files.get('regions', []),
#             "assets": extracted_files.get('assets', [])
#         },
#         "conversion_process": {
#             "conversion_steps": intermediate_files.get('conversion_steps', []),
#             "intermediate_files": intermediate_files.get('intermediate_files', [])
#         },
#         "content_stats": {
#             "text_length": len(result.text_content) if getattr(result, 'text_content', None) else 0,
#             "markdown_length": len(result.markdown_content) if getattr(result, 'markdown_content', None) else 0,
#             "total_sections": 0,
#             "regions_count": len(extracted_files.get('regions', [])),
#             "assets_count": len(extracted_files.get('assets', []))
#         },
#         "processing_metadata": {
#             "conversion_method": (result.processing_info or {}).get("conversion_method", "unknown"),
#         }
#     }
