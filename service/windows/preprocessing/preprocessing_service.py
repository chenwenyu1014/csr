# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
#
# """
# 预处理服务
# 统一封装文件预处理功能
# """
#
# import json
# import logging
# from pathlib import Path
# from typing import Dict, List, Any, Optional
# from dataclasses import dataclass, field
# from datetime import datetime
#
# from service.windows.preprocessing.file_processor import FileProcessor
#
# # 导入耗时记录工具
# try:
#     from utils.timing import Timer, preprocessing_timer, log_timing
# except ImportError:
#     class Timer:
#         def __init__(self, *args, **kwargs): pass
#         def start(self): return self
#         def stop(self): return 0
#         def __enter__(self): return self
#         def __exit__(self, *args): pass
#     preprocessing_timer = None
#     def log_timing(*args, **kwargs): pass
#
# logger = logging.getLogger(__name__)
#
#
# @dataclass
# class PreprocessingResult:
#     """预处理结果"""
#     success: bool
#     message: str
#     preprocessed_files: List[str] = field(default_factory=list)  # preprocessed.json路径列表
#     errors: List[str] = field(default_factory=list)
#
#     def to_dict(self) -> Dict[str, Any]:
#         """转换为字典"""
#         return {
#             "success": self.success,
#             "message": self.message,
#             "preprocessed_files": self.preprocessed_files,
#             "errors": self.errors
#         }
#
#
# class PreprocessingService:
#     """
#     预处理服务
#
#     功能：
#     1. 批量处理多个文件
#     2. 自动识别文件类型
#     3. 调用对应Pipeline
#     4. 返回统一的结果
#     """
#
#     def __init__(self, work_dir: str = None, input_dir: str = None, output_dir: str = None):
#         """
#         初始化预处理服务
#
#         Args:
#             work_dir: 工作目录（可选，如果指定input_dir和output_dir则不需要）
#             input_dir: 输入文件目录（可选）
#             output_dir: 输出目录（可选）
#
#         使用方式：
#             # 方式1: 只指定工作目录（自动创建输入文件和预处理结果子目录）
#             service = PreprocessingService(work_dir="data")
#
#             # 方式2: 分别指定输入输出目录
#             service = PreprocessingService(
#                 input_dir="data/docx",
#                 output_dir="data/预处理结果"
#             )
#
#             # 方式3: 使用当前目录
#             service = PreprocessingService()
#         """
#         # 如果指定了input_dir和output_dir，直接使用
#         if input_dir and output_dir:
#             self.input_dir = Path(input_dir)
#             self.output_dir = Path(output_dir)
#             self.work_dir = self.input_dir.parent if self.input_dir.parent == self.output_dir.parent else Path.cwd()
#         # 否则使用work_dir
#         else:
#             if work_dir is None:
#                 self.work_dir = Path.cwd()
#             else:
#                 self.work_dir = Path(work_dir)
#
#             self.work_dir.mkdir(parents=True, exist_ok=True)
#
#             # 默认从work_dir直接读取，输出到work_dir/预处理结果
#             self.input_dir = self.work_dir
#             self.output_dir = self.work_dir / "预处理结果"
#
#         self.output_dir.mkdir(parents=True, exist_ok=True)
#
#         # 索引文件路径
#         self.index_file = self.output_dir / "preprocessing_index.json"
#         self.index_data = self._load_or_create_index()
#
#         self.processor = FileProcessor()
#         logger.info(f"预处理服务初始化完成")
#         logger.info(f"  工作目录: {self.work_dir}")
#         logger.info(f"  输入目录: {self.input_dir}")
#         logger.info(f"  输出目录: {self.output_dir}")
#         logger.info(f"  索引文件: {self.index_file}")
#
#     def preprocess_files(
#         self,
#         file_paths: List[str],
#         options: Optional[Dict[str, Any]] = None
#     ) -> PreprocessingResult:
#         """
#         批量预处理文件
#
#         Args:
#             file_paths: 文件路径列表（相对于input_dir的相对路径，或绝对路径）
#             options: 处理选项
#                 - extract_tables: 是否提取表格 (默认True)
#                 - extract_images: 是否提取图片 (默认True)
#                 - chunk_content: 是否分块 (默认True)
#
#         Returns:
#             PreprocessingResult
#
#         示例:
#             service = PreprocessingService(work_dir="工作目录")
#             # 文件应该放在: 工作目录/输入文件/
#             result = service.preprocess_files(
#                 file_paths=["研究方案.docx", "统计数据.xlsx"],  # 相对路径
#                 options={"extract_tables": True}
#             )
#         """
#         # 开始批量预处理计时
#         batch_timer = Timer(f"批量预处理({len(file_paths)}个文件)", parent="预处理服务")
#         batch_timer.start()
#
#         logger.info("=" * 70)
#         logger.info("开始批量预处理")
#         logger.info(f"文件数量: {len(file_paths)}")
#         logger.info("=" * 70)
#
#         if options is None:
#             options = {}
#
#         preprocessed_files = []
#         errors = []
#         file_timings = []  # 记录每个文件的耗时
#
#         for file_path_str in file_paths:
#             file_timer = Timer(f"预处理: {Path(file_path_str).name}", parent="批量预处理")
#             file_timer.start()
#             try:
#                 # 转换路径：如果是相对路径，则相对于input_dir
#                 file_path = Path(file_path_str)
#                 if not file_path.is_absolute():
#                     file_path = self.input_dir / file_path_str
#
#                 logger.info(f"\n处理文件: {file_path}")
#
#                 # 处理单个文件
#                 result = self._preprocess_single_file(str(file_path), options)
#
#                 file_timer.stop()
#                 file_timings.append({
#                     "file": file_path.name,
#                     "duration": file_timer.duration,
#                     "duration_str": file_timer.duration_str,
#                     "success": result is not None
#                 })
#
#                 if result:
#                     preprocessed_files.append(result)
#                     logger.info(f"✅ 成功: {result} [耗时: {file_timer.duration_str}]")
#                 else:
#                     error_msg = f"处理失败: {file_path}"
#                     errors.append(error_msg)
#                     logger.error(f"❌ {error_msg} [耗时: {file_timer.duration_str}]")
#
#             except Exception as e:
#                 file_timer.stop()
#                 file_timings.append({
#                     "file": Path(file_path_str).name,
#                     "duration": file_timer.duration,
#                     "duration_str": file_timer.duration_str,
#                     "success": False
#                 })
#                 error_msg = f"处理异常 {file_path_str}: {str(e)}"
#                 errors.append(error_msg)
#                 logger.error(f"❌ {error_msg} [耗时: {file_timer.duration_str}]", exc_info=True)
#
#         batch_timer.stop()
#
#         # 构建结果
#         success = len(preprocessed_files) > 0
#         message = f"成功处理 {len(preprocessed_files)}/{len(file_paths)} 个文件"
#
#         logger.info("\n" + "=" * 70)
#         logger.info(f"预处理完成: {message}")
#         logger.info(f"⏱️ 批量预处理总耗时: {batch_timer.duration_str}")
#         logger.info("=" * 70)
#
#         # 打印每个文件的耗时摘要
#         logger.info("📊 文件预处理耗时明细:")
#         for ft in file_timings:
#             status = "✅" if ft["success"] else "❌"
#             logger.info(f"  {status} {ft['file']}: {ft['duration_str']}")
#
#         # 记录到全局计时器
#         if preprocessing_timer:
#             preprocessing_timer.record(f"批量预处理({len(file_paths)}文件)", batch_timer.duration, parent="预处理服务")
#
#         result = PreprocessingResult(
#             success=success,
#             message=message,
#             preprocessed_files=preprocessed_files,
#             errors=errors
#         )
#
#         # 保存索引
#         if success:
#             with Timer("保存索引", parent="预处理服务"):
#                 self._save_index()
#             logger.info(f"✅ 索引已更新: {self.index_file}")
#
#         return result
#
#     def _preprocess_single_file(
#         self,
#         file_path: str,
#         options: Dict[str, Any]
#     ) -> Optional[str]:
#         """
#         预处理单个文件
#
#         Returns:
#             preprocessed.json的路径，失败返回None
#         """
#         file_path = Path(file_path)
#
#         if not file_path.exists():
#             logger.error(f"文件不存在: {file_path}")
#             return None
#
#         # 为每个文件创建独立的输出目录
#         file_output_dir = self.output_dir / file_path.stem
#         file_output_dir.mkdir(parents=True, exist_ok=True)
#
#         # 调用FileProcessor
#         result = self.processor.process(
#             file_path=str(file_path),
#             output_dir=str(file_output_dir)
#         )
#
#         if result:
#             # result是PreprocessedDocument对象
#             # 保存预处理包
#             self.processor._save_preprocessed_package(result)
#
#             # 返回preprocessed.json路径
#             work_dir = result.work_dir
#             if work_dir:
#                 preprocessed_json = Path(work_dir) / "preprocessed.json"
#                 if preprocessed_json.exists():
#                     # 更新索引
#                     self._update_index_for_file(file_path.name, str(preprocessed_json))
#                     return str(preprocessed_json)
#
#         return None
#
#     def get_preprocessed_data(self, preprocessed_json_path: str) -> Optional[Dict[str, Any]]:
#         """
#         读取预处理结果
#
#         Args:
#             preprocessed_json_path: preprocessed.json路径
#
#         Returns:
#             预处理数据字典
#         """
#         try:
#             with open(preprocessed_json_path, 'r', encoding='utf-8') as f:
#                 return json.load(f)
#         except Exception as e:
#             logger.error(f"读取预处理数据失败: {e}", exc_info=True)
#             return None
#
#     def get_all_regions(self, preprocessed_files: List[str]) -> List[Dict[str, Any]]:
#         """
#         获取所有文件的regions
#
#         Args:
#             preprocessed_files: preprocessed.json路径列表
#
#         Returns:
#             所有regions的列表
#         """
#         all_regions = []
#
#         for preprocessed_file in preprocessed_files:
#             data = self.get_preprocessed_data(preprocessed_file)
#             if data and "regions" in data:
#                 all_regions.extend(data["regions"])
#
#         return all_regions
#
#     def get_all_chunks(self, preprocessed_files: List[str]) -> List[Dict[str, Any]]:
#         """
#         获取所有文件的chunks
#
#         Args:
#             preprocessed_files: preprocessed.json路径列表
#
#         Returns:
#             所有chunks的列表
#         """
#         all_chunks = []
#
#         for preprocessed_file in preprocessed_files:
#             data = self.get_preprocessed_data(preprocessed_file)
#             if data and "chunks_file" in data:
#                 # 读取chunks文件
#                 chunks_file = Path(preprocessed_file).parent / data["chunks_file"]
#                 if chunks_file.exists():
#                     with open(chunks_file, 'r', encoding='utf-8') as f:
#                         chunks_data = json.load(f)
#                         if "sections" in chunks_data:
#                             all_chunks.extend(chunks_data["sections"])
#
#         return all_chunks
#
#     def _load_or_create_index(self) -> Dict[str, Any]:
#         """加载或创建索引文件"""
#         if self.index_file.exists():
#             try:
#                 with open(self.index_file, 'r', encoding='utf-8') as f:
#                     index = json.load(f)
#                     logger.info(f"已加载索引文件: {len(index.get('files', {}))} 个文件")
#                     return index
#             except Exception as e:
#                 logger.warning(f"加载索引文件失败: {e}，将创建新索引")
#
#         return {
#             "version": "1.0",
#             "generated_at": datetime.now().isoformat(),
#             "base_output_dir": str(self.output_dir),
#             "files": {}
#         }
#
#     def _save_index(self):
#         """保存索引文件"""
#         try:
#             self.index_data["generated_at"] = datetime.now().isoformat()
#             with open(self.index_file, 'w', encoding='utf-8') as f:
#                 json.dump(self.index_data, f, ensure_ascii=False, indent=2)
#         except Exception as e:
#             logger.error(f"保存索引文件失败: {e}", exc_info=True)
#
#     def _update_index_for_file(self, file_name: str, preprocessed_json_path: str):
#         """更新文件的索引信息"""
#         try:
#             # 读取预处理结果
#             with open(preprocessed_json_path, 'r', encoding='utf-8') as f:
#                 preprocessed_data = json.load(f)
#
#             work_dir = Path(preprocessed_json_path).parent
#             file_type = preprocessed_data.get("file_type", "")
#
#             # 构建索引条目
#             index_entry = {
#                 "file_type": file_type,
#                 "source_file": preprocessed_data.get("source_file", ""),
#                 "preprocessed_json": str(preprocessed_json_path),
#                 "work_dir": str(work_dir),
#                 "status": "success",
#                 "processed_at": datetime.now().isoformat()
#             }
#
#             # 根据文件类型添加特定信息
#             if file_type in ["docx", "doc", "pdf"]:
#                 # Word/PDF类型
#                 chunks_file = preprocessed_data.get("chunks_file", "")
#                 if chunks_file:
#                     index_entry["chunks_file"] = str(work_dir / chunks_file)
#
#                 processing_info = preprocessed_data.get("processing_info", {})
#                 markdown_file = processing_info.get("markdown_file", "")
#                 if markdown_file:
#                     index_entry["markdown_file"] = str(markdown_file)
#
#                 regions_dir = work_dir / "regions_word"
#                 if regions_dir.exists():
#                     index_entry["regions_dir"] = str(regions_dir)
#
#                 # 添加regions信息
#                 regions = preprocessed_data.get("regions", [])
#                 if regions:
#                     index_entry["regions"] = regions
#
#             elif file_type in ["rtf", "xlsx", "xls"]:
#                 # RTF/Excel类型
#                 regions = preprocessed_data.get("regions", [])
#                 sheets = []
#
#                 for region in regions:
#                     sheet_info = {
#                         "sheet_name": region.get("Label", "").replace("Sheet_", ""),
#                         "excel_file": region.get("path", ""),
#                         "markdown_file": region.get("markdown_path", "")
#                     }
#                     sheets.append(sheet_info)
#
#                 index_entry["sheets"] = sheets
#                 index_entry["regions"] = regions
#
#                 # markdown目录
#                 if sheets and sheets[0].get("markdown_file"):
#                     markdown_dir = str(Path(sheets[0]["markdown_file"]).parent)
#                     index_entry["markdown_dir"] = markdown_dir
#
#             # 更新索引
#             self.index_data["files"][file_name] = index_entry
#             logger.debug(f"已更新索引: {file_name}")
#
#         except Exception as e:
#             logger.error(f"更新索引失败 {file_name}: {e}", exc_info=True)
#
#     def get_index(self) -> Dict[str, Any]:
#         """获取索引数据"""
#         return self.index_data
#
#     def get_file_info(self, file_name: str) -> Optional[Dict[str, Any]]:
#         """从索引获取文件信息"""
#         return self.index_data["files"].get(file_name)
#
#     def list_preprocessed_files(self) -> List[str]:
#         """列出所有已预处理的文件"""
#         return list(self.index_data["files"].keys())
#
#     def get_available_files(self) -> List[Dict[str, Any]]:
#         """获取可用文件列表（供前端使用）"""
#         available = []
#         for file_name, file_info in self.index_data["files"].items():
#             available.append({
#                 "file_name": file_name,
#                 "file_type": file_info.get("file_type", ""),
#                 "processed_at": file_info.get("processed_at", ""),
#                 "has_chunks": "chunks_file" in file_info,
#                 "has_regions": "regions_dir" in file_info or "sheets" in file_info,
#                 "source_file": file_info.get("source_file", "")
#             })
#         return available
#
#
# # 示例使用
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#
#     # 创建服务
#     service = PreprocessingService(output_dir="预处理结果")
#
#     # 预处理文件
#     result = service.preprocess_files(
#         file_paths=[
#             "test_data/test.docx",
#             "test_data/index.xlsx"
#         ],
#         options={
#             "extract_tables": True,
#             "extract_images": True
#         }
#     )
#
#     print(f"\n结果: {result.message}")
#     print(f"成功文件: {len(result.preprocessed_files)}")
#     print(f"失败文件: {len(result.errors)}")
#
#     # 获取所有regions
#     if result.success:
#         all_regions = service.get_all_regions(result.preprocessed_files)
#         print(f"\n总共提取了 {len(all_regions)} 个regions")
