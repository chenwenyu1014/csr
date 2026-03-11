# -*- coding: utf-8 -*-
from __future__ import annotations
import logging
import re
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from docx import Document
from lxml import etree
import zipfile
import tempfile

# Word XML 命名空间
WNS = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
WNS_P = f'{{{WNS}}}'



# We orchestrate existing FileProcessor internals to avoid duplicating complex logic now.
from service.windows.preprocessing.file_processor import FileProcessor

logger = logging.getLogger(__name__)


def run_docx(word_path: Path | str, work_dir: Path | str, mode: Optional[str] = None) -> Dict[str, Any]:
    """
    Word (.docx/.rtf-as-docx-class) pipeline.
    mode: Optional logical hint ('text'|'images'|'tables'), currently passed through to existing logic.
    """
    fp = FileProcessor()
    word_path = Path(word_path)
    work_dir = Path(work_dir)
    # Delegate to existing robust path
    normalized = mode
    return fp._convert_word_by_kind(word_path, work_dir, content_type_override=normalized)  # type: ignore


def run_doc(word_path: Path | str, work_dir: Path | str, mode: Optional[str] = None) -> Dict[str, Any]:
    """
    Legacy Word (.doc) pipeline.
    """
    fp = FileProcessor()
    word_path = Path(word_path)
    work_dir = Path(work_dir)
    normalized = (mode or '').strip().lower() or None
    if normalized == 'tables':
        return fp._word_tables_split_to_docx(word_path, work_dir)  # type: ignore
    if normalized == 'images':
        return fp._word_doc_images_export_to_docx(word_path, work_dir)  # type: ignore
    return fp._word_doc_to_markdown(word_path, work_dir)  # type: ignore

class WordRegionExtractor:
    """
    Word区域提取器

    从包含 {{Table_N_Start}} / {{Table_N_End}} 或
    {{Image_N_Start}} / {{Image_N_End}} 标记的Word文档中
    提取每个区域，生成独立的、格式完整的Word文件。

    核心思路：
      - 以源文档为模板整体复制（保留所有样式、主题、字体、关系）
      - 直接操作 document.xml，删除不属于目标区域的元素
      - 重新打包为合法的 .docx，无需手动处理图片关系
    """

    # 匹配开始/结束标记，如 {{Table_1_Start}}
    START_PATTERN = re.compile(r'^\{\{(Table|Image)_(\d+)_Start\}\}$')
    END_PATTERN   = re.compile(r'^\{\{(Table|Image)_(\d+)_End\}\}$')


    def extract_regions(self, marked_file: str, export_dir: str) -> List[Dict[str, str]]:
        """
        从标记的Word文档中提取所有区域，输出为独立Word文件。

        Args:
            marked_file: 含标记的源 .docx 路径
            export_dir:  输出目录

        Returns:
            [{"name": "Table_1_Start", "path": "/some/dir/Table_1_Start.docx"}, ...]
        """
        source_path = Path(marked_file)
        export_path = Path(export_dir)
        export_path.mkdir(parents=True, exist_ok=True)

        # 用 python-docx 读取文档，仅用于解析区域边界索引
        doc = Document(str(source_path))
        regions = self._find_regions(doc)
        logger.info(f"共找到 {len(regions)} 个区域: {[r['name'] for r in regions]}")

        results = []
        for region in regions:
            output_name = f"{region['name']}_Start"
            output_file = export_path / f"{output_name}.docx"
            try:
                self._export_region_by_clone(source_path, region, output_file)
                results.append({"name": output_name, "path": str(output_file)})
                logger.info(f"成功导出: {output_file.name}")
            except Exception as e:
                logger.error(f"导出区域 {region['name']} 失败: {e}", exc_info=True)

        return results

    def _find_regions(self, doc: Document) -> List[Dict]:
        """
        遍历文档体，收集所有区域的 (name, start_idx, end_idx)。

        索引基于 body 的直接子元素列表（段落 + 表格），
        start_idx 指向开始标记的下一个元素，
        end_idx   指向结束标记本身（不含）。
        """
        # 收集 body 直接子元素及类型
        elements = self._collect_body_elements(doc)

        regions: List[Dict] = []
        current: Optional[Dict] = None

        for idx, (elem_type, elem) in enumerate(elements):
            if elem_type != 'paragraph':
                continue

            text = self._get_element_text(elem)

            start_match = self.START_PATTERN.match(text)
            if start_match:
                if current is not None:
                    logger.warning(f"在区域 {current['name']} 未结束时遇到新的开始标记，丢弃前者")
                current = {
                    'name':      f"{start_match.group(1)}_{start_match.group(2)}",
                    'type':      start_match.group(1).lower(),
                    'start_idx': idx + 1,   # 内容从下一个元素开始
                    'end_idx':   None,
                }
                continue

            end_match = self.END_PATTERN.match(text)
            if end_match and current is not None:
                expected = f"{end_match.group(1)}_{end_match.group(2)}"
                if current['name'] == expected:
                    current['end_idx'] = idx   # 不含结束标记本身
                    regions.append(current)
                    current = None
                else:
                    logger.warning(
                        f"结束标记 {expected} 与当前开放区域 {current['name']} 不匹配，忽略"
                    )

        if current is not None:
            logger.warning(f"区域 {current['name']} 有开始标记但没有结束标记，已忽略")

        return regions

    def _export_region_by_clone(
        self,
        source_path: Path,
        region: Dict,
        output_file: Path,
    ) -> None:
        """
        以源文档为模板，仅保留目标区域内容后输出新文档。

        步骤：
          1. 复制源文件到临时位置
          2. 解压，修改 word/document.xml
          3. 重新打包为最终输出文件
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path    = Path(tmp_dir)
            extract_dir = tmp_path / "extracted"

            # ---- 1. 解压源文档 ----------------------------------------
            with zipfile.ZipFile(source_path, 'r') as zf:
                zf.extractall(extract_dir)

            # ---- 2. 修改 document.xml ------------------------------------
            doc_xml_path = extract_dir / "word" / "document.xml"
            self._trim_document_xml(doc_xml_path, region)

            # ---- 3. 重新打包为 .docx -------------------------------------
            output_file.parent.mkdir(parents=True, exist_ok=True)
            self._repack_docx(extract_dir, output_file)

    def _trim_document_xml(self, doc_xml_path: Path, region: Dict) -> None:
        """
        解析 document.xml，只保留目标区域内的 body 子元素。

        保留规则：
          - 索引在 [start_idx, end_idx) 范围内的元素保留
          - sectPr（页面/节属性）始终保留，放在最后
        """
        parser = etree.XMLParser(remove_blank_text=False)
        tree   = etree.parse(str(doc_xml_path), parser)
        body   = tree.find(f'.//{WNS_P}body')

        if body is None:
            raise ValueError("document.xml 中找不到 <w:body>")

        # 取出所有直接子元素（含 sectPr）
        all_children = list(body)

        # 单独摘出 sectPr（可能不存在）
        sect_pr = body.find(f'{WNS_P}sectPr')

        # 过滤出有效的内容元素（段落 + 表格），用于对齐索引
        indexed_elements = self._get_indexed_content_elements(all_children)

        keep_start = region['start_idx']
        keep_end   = region['end_idx']

        # 收集要保留的元素
        kept_elements: List[etree._Element] = []
        for idx, elem in indexed_elements:
            if keep_start <= idx < keep_end:
                kept_elements.append(elem)

        # 清空 body，重新填入
        for child in list(body):
            body.remove(child)

        for elem in kept_elements:
            body.append(elem)

        # sectPr 始终放最后（保持页面设置）
        if sect_pr is not None:
            body.append(sect_pr)

        # 写回文件
        tree.write(
            str(doc_xml_path),
            xml_declaration=True,
            encoding='UTF-8',
            standalone=True,
        )

    def _get_indexed_content_elements(
        self,
        all_children: List[etree._Element],
    ) -> List[Tuple[int, etree._Element]]:
        """
        从 body 子元素列表中提取段落和表格，并给出连续索引。
        索引与 _find_regions 中的索引保持一致。
        """
        result: List[Tuple[int, etree._Element]] = []
        idx = 0
        for elem in all_children:
            local = etree.QName(elem.tag).localname
            if local in ('p', 'tbl'):
                result.append((idx, elem))
                idx += 1
        return result

    @staticmethod
    def _collect_body_elements(doc: Document) -> List[Tuple[str, etree._Element]]:
        """收集文档 body 的直接子元素（段落 + 表格）"""
        elements: List[Tuple[str, etree._Element]] = []
        for elem in doc.element.body:
            local = etree.QName(elem.tag).localname
            if local == 'p':
                elements.append(('paragraph', elem))
            elif local == 'tbl':
                elements.append(('table', elem))
        return elements

    @staticmethod
    def _get_element_text(p_element: etree._Element) -> str:
        """提取段落元素的纯文本（拼接所有 w:t 子节点）"""
        try:
            texts = p_element.itertext(
                f'{WNS_P}t',
                with_tail=False,
            )
            return ''.join(texts).strip()
        except Exception:
            return ''

    @staticmethod
    def _repack_docx(extract_dir: Path, output_file: Path) -> None:
        """将解压目录重新打包为合法的 .docx 文件。

        注意：[Content_Types].xml 和 _rels/.rels 必须不压缩或正确压缩，
        此处统一使用 ZIP_DEFLATED，与标准 Office 生成方式一致。
        """
        with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as zout:
            for file_path in sorted(extract_dir.rglob('*')):
                if file_path.is_file():
                    arcname = file_path.relative_to(extract_dir)
                    # [Content_Types].xml 有些工具要求 STORED，但 DEFLATED 同样合法
                    zout.write(file_path, arcname)
word_region_extractor = WordRegionExtractor()

# Word区域提取器 - 简单可靠的实现
# 直接使用 python-docx 提取标签之间的内容，不依赖COM
# class WordRegionExtractor:
#     """Word区域提取器"""
#
#     def __init__(self):
#         self.start_pattern = re.compile(r'^\{\{(Table|Image)_(\d+)_Start\}\}$')
#         self.end_pattern = re.compile(r'^\{\{(Table|Image)_(\d+)_End\}\}$')
#
#     def extract_regions(self, marked_file: str, export_dir: str) -> List[Dict[str, str]]:
#         """
#         从标记的Word文档中提取区域，创建独立的Word文件
#
#         Args:
#             marked_file: 标记后的Word文档路径
#             export_dir: 导出目录
#
#         Returns:
#             导出文件列表 [{"name": "Table_1", "path": "..."}]
#         """
#         try:
#             doc = Document(marked_file)
#             export_path = Path(export_dir)
#             export_path.mkdir(parents=True, exist_ok=True)
#
#             results = []
#             marked_file_path = Path(marked_file)
#
#             # 第一步：找到所有区域的边界
#             regions = self._find_regions(doc)
#             logger.info(f"找到 {len(regions)} 个区域")
#
#             # 第二步：为每个区域创建独立的Word文件
#             for region in regions:
#                 try:
#                     # 文件名使用 region_name_Start 格式
#                     output_file = export_path / f"{region['name']}_Start.docx"
#                     self._export_region(doc, region, output_file, marked_file_path)
#                     results.append({
#                         "name": f"{region['name']}_Start",
#                         "path": str(output_file)
#                     })
#                     logger.info(f"成功导出区域: {region['name']}_Start")
#                 except Exception as e:
#                     logger.error(f"导出区域失败 {region['name']}: {e}")
#
#             return results
#
#         except Exception as e:
#             logger.error(f"提取区域失败: {e}")
#             return []
#
#     def _find_regions(self, doc: Document) -> List[Dict]:
#         """找到所有区域的边界"""
#         regions = []
#         current_region = None
#
#         # 遍历所有元素（段落和表格）
#         elements = []
#         for element in doc.element.body:
#             if element.tag.endswith('p'):  # 段落
#                 elements.append(('paragraph', element))
#             elif element.tag.endswith('tbl'):  # 表格
#                 elements.append(('table', element))
#
#         for idx, (elem_type, elem) in enumerate(elements):
#             if elem_type == 'paragraph':
#                 # 获取段落文本
#                 text = self._get_paragraph_text(elem)
#
#                 # 检查是否是开始标签
#                 start_match = self.start_pattern.match(text)
#                 if start_match:
#                     region_type = start_match.group(1)
#                     region_num = start_match.group(2)
#                     region_name = f"{region_type}_{region_num}"
#                     current_region = {
#                         'name': region_name,
#                         'type': region_type.lower(),
#                         'start_idx': idx + 1,  # 内容从下一个元素开始
#                         'end_idx': None
#                     }
#                     continue
#
#                 # 检查是否是结束标签
#                 end_match = self.end_pattern.match(text)
#                 if end_match and current_region:
#                     region_type = end_match.group(1)
#                     region_num = end_match.group(2)
#                     expected_name = f"{region_type}_{region_num}"
#
#                     if current_region['name'] == expected_name:
#                         current_region['end_idx'] = idx  # 内容到当前元素之前
#                         regions.append(current_region)
#                         current_region = None
#
#         return regions
#
#     def _get_paragraph_text(self, p_element) -> str:
#         """获取段落的纯文本"""
#         try:
#             from docx.text.paragraph import Paragraph
#             para = Paragraph(p_element, None)
#             return para.text.strip()
#         except:
#             return ""
#
#     def _export_region(self, source_doc: Document, region: Dict, output_file: Path, source_file_path: Path):
#         """导出单个区域到新的Word文件（包含图片）"""
#         # 创建新文档
#         new_doc = Document()
#
#         # 获取源文档的所有元素
#         elements = []
#         for element in source_doc.element.body:
#             if element.tag.endswith('p'):
#                 elements.append(('paragraph', element))
#             elif element.tag.endswith('tbl'):
#                 elements.append(('table', element))
#
#         # 复制区域内的元素
#         start_idx = region['start_idx']
#         end_idx = region['end_idx']
#
#         for idx in range(start_idx, end_idx):
#             if idx >= len(elements):
#                 break
#
#             elem_type, elem = elements[idx]
#
#             try:
#                 if elem_type == 'paragraph':
#                     self._copy_paragraph(elem, new_doc)
#                 elif elem_type == 'table':
#                     self._copy_table(elem, new_doc)
#             except Exception as e:
#                 logger.warning(f"复制元素失败: {e}")
#
#         # 保存文档到临时位置
#         temp_output = output_file.parent / f"_temp_{output_file.name}"
#         new_doc.save(str(temp_output))
#
#         # 从源文档复制媒体文件到新文档
#         try:
#             self._copy_media_files(source_file_path, temp_output, output_file)
#             if temp_output.exists():
#                 temp_output.unlink()  # 删除临时文件
#         except Exception as e:
#             logger.warning(f"复制媒体文件失败: {e}，使用不含完整媒体的版本")
#             # 如果复制失败，至少保留文档结构
#             if temp_output.exists():
#                 shutil.move(str(temp_output), str(output_file))
#
#     def _copy_paragraph(self, source_p_element, target_doc: Document):
#         """复制段落到目标文档（包含图片）"""
#         try:
#             # 深度复制元素，保留所有子元素和属性
#             new_p_element = deepcopy(source_p_element)
#             # 添加到目标文档
#             target_doc._element.body.append(new_p_element)
#         except Exception as e:
#             logger.warning(f"复制段落失败: {e}")
#
#     def _copy_table(self, source_tbl_element, target_doc: Document):
#         """复制表格到目标文档（包含内嵌图片）"""
#         try:
#             # 深度复制表格元素，保留所有单元格内容和格式
#             new_tbl_element = deepcopy(source_tbl_element)
#             # 添加到目标文档
#             target_doc._element.body.append(new_tbl_element)
#         except Exception as e:
#             logger.warning(f"复制表格失败: {e}")
#
#     def _copy_media_files(self, source_doc_path: Path, temp_doc_path: Path, final_doc_path: Path):
#         """从源文档复制媒体文件到新文档
#
#         智能复制：
#         1. 解析目标文档的关系文件，找到实际引用的图片
#         2. 只从源文档复制这些图片
#         3. 重新打包
#         """
#         try:
#             with tempfile.TemporaryDirectory() as temp_dir:
#                 temp_path = Path(temp_dir)
#
#                 # 解压源文档和目标文档
#                 source_extract = temp_path / "source"
#                 target_extract = temp_path / "target"
#
#                 with zipfile.ZipFile(source_doc_path, 'r') as source_zip:
#                     source_zip.extractall(source_extract)
#
#                 with zipfile.ZipFile(temp_doc_path, 'r') as target_zip:
#                     target_zip.extractall(target_extract)
#
#                 # 找到文档中引用的图片ID和对应的文件
#                 import xml.etree.ElementTree as ET
#
#                 # 1. 从document.xml找到所有图片的rId引用
#                 doc_xml_file = target_extract / "word" / "document.xml"
#                 referenced_rids = set()
#
#                 if doc_xml_file.exists():
#                     try:
#                         doc_tree = ET.parse(doc_xml_file)
#                         # 查找所有blip元素的r:embed属性
#                         for blip in doc_tree.findall('.//{http://schemas.openxmlformats.org/drawingml/2006/main}blip'):
#                             rid = blip.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed')
#                             if rid:
#                                 referenced_rids.add(rid)
#                         logger.info(f"文档引用的图片rId: {referenced_rids}")
#                     except Exception as e:
#                         logger.warning(f"解析document.xml失败: {e}")
#
#                 # 2. 从源文档的关系文件中获取这些rId对应的图片文件
#                 source_rels_file = source_extract / "word" / "_rels" / "document.xml.rels"
#                 target_rels_file = target_extract / "word" / "_rels" / "document.xml.rels"
#                 rid_to_image = {}  # {rId: image_filename}
#
#                 if source_rels_file.exists() and referenced_rids:
#                     try:
#                         source_rels_tree = ET.parse(source_rels_file)
#                         source_rels_root = source_rels_tree.getroot()
#
#                         # 创建目标关系文件（如果不存在）
#                         target_rels_file.parent.mkdir(parents=True, exist_ok=True)
#
#                         # 创建新的关系XML
#                         ns = {'r': 'http://schemas.openxmlformats.org/package/2006/relationships'}
#                         ET.register_namespace('', ns['r'])
#
#                         target_rels_root = ET.Element('{' + ns['r'] + '}Relationships')
#
#                         # 复制需要的关系
#                         for rel in source_rels_root.findall('{' + ns['r'] + '}Relationship'):
#                             rel_id = rel.get('Id')
#                             rel_target = rel.get('Target', '')
#
#                             # 如果是文档引用的图片关系，复制它
#                             if rel_id in referenced_rids and 'media/' in rel_target:
#                                 target_rels_root.append(rel)
#                                 image_name = rel_target.split('/')[-1]
#                                 rid_to_image[rel_id] = image_name
#                                 logger.info(f"复制关系: {rel_id} -> {image_name}")
#
#                         # 保存新的关系文件
#                         target_rels_tree = ET.ElementTree(target_rels_root)
#                         target_rels_tree.write(target_rels_file, encoding='utf-8', xml_declaration=True)
#
#                     except Exception as e:
#                         logger.warning(f"处理关系文件失败: {e}")
#                         import traceback
#                         logger.warning(traceback.format_exc())
#
#                 referenced_images = set(rid_to_image.values())
#
#                 # 复制引用的图片
#                 source_media = source_extract / "word" / "media"
#                 target_media = target_extract / "word" / "media"
#
#                 if source_media.exists() and referenced_images:
#                     target_media.mkdir(parents=True, exist_ok=True)
#
#                     copied_count = 0
#                     for image_name in referenced_images:
#                         source_image = source_media / image_name
#                         target_image = target_media / image_name
#
#                         if source_image.exists():
#                             shutil.copy2(source_image, target_image)
#                             copied_count += 1
#                         else:
#                             logger.warning(f"源图片不存在: {image_name}")
#
#                     logger.info(f"复制了 {copied_count}/{len(referenced_images)} 个图片")
#
#                     # 更新[Content_Types].xml以包含media文件的类型声明
#                     self._update_content_types(target_extract, referenced_images)
#
#                 elif source_media.exists():
#                     # 如果没有找到引用，复制所有图片（兜底）
#                     if target_media.exists():
#                         shutil.rmtree(target_media)
#                     shutil.copytree(source_media, target_media)
#                     logger.info(f"复制了所有媒体文件（兜底）")
#
#                 # 重新打包为docx文件
#                 with zipfile.ZipFile(final_doc_path, 'w', zipfile.ZIP_DEFLATED) as final_zip:
#                     for file_path in target_extract.rglob('*'):
#                         if file_path.is_file():
#                             arc_name = file_path.relative_to(target_extract)
#                             final_zip.write(file_path, arc_name)
#
#                 logger.info(f"成功创建文档: {final_doc_path.name}")
#
#         except Exception as e:
#             logger.warning(f"复制媒体文件失败: {e}，回退到不含媒体的版本")
#             import traceback
#             logger.warning(traceback.format_exc())
#             # 回退：直接使用临时文档
#             if temp_doc_path.exists() and not final_doc_path.exists():
#                 shutil.copy2(temp_doc_path, final_doc_path)
#
#     def _update_content_types(self, extract_dir: Path, image_files: set):
#         """更新[Content_Types].xml以包含media文件的类型声明"""
#         try:
#             import xml.etree.ElementTree as ET
#
#             content_types_file = extract_dir / "[Content_Types].xml"
#             if not content_types_file.exists():
#                 logger.warning("未找到[Content_Types].xml")
#                 return
#
#             # 解析现有文件
#             tree = ET.parse(content_types_file)
#             root = tree.getroot()
#
#             ns = {'ct': 'http://schemas.openxmlformats.org/package/2006/content-types'}
#             ET.register_namespace('', ns['ct'])
#
#             # 检查已有的扩展名声明
#             existing_extensions = set()
#             for default in root.findall('{' + ns['ct'] + '}Default'):
#                 ext = default.get('Extension')
#                 if ext:
#                     existing_extensions.add(ext.lower())
#
#             # 需要的图片扩展名及其MIME类型
#             image_types = {
#                 'png': 'image/png',
#                 'jpg': 'image/jpeg',
#                 'jpeg': 'image/jpeg',
#                 'gif': 'image/gif',
#                 'bmp': 'image/bmp',
#                 'tiff': 'image/tiff',
#                 'emf': 'image/x-emf',
#                 'wmf': 'image/x-wmf'
#             }
#
#             # 从image_files中提取需要的扩展名
#             needed_extensions = set()
#             for img_file in image_files:
#                 ext = Path(img_file).suffix.lstrip('.').lower()
#                 if ext:
#                     needed_extensions.add(ext)
#
#             # 添加缺失的扩展名声明
#             for ext in needed_extensions:
#                 if ext not in existing_extensions and ext in image_types:
#                     default_elem = ET.Element(
#                         '{' + ns['ct'] + '}Default',
#                         Extension=ext,
#                         ContentType=image_types[ext]
#                     )
#                     root.append(default_elem)
#                     logger.info(f"添加Content Type: .{ext} -> {image_types[ext]}")
#
#             # 保存更新后的文件
#             tree.write(content_types_file, encoding='utf-8', xml_declaration=True)
#
#         except Exception as e:
#             logger.warning(f"更新Content Types失败: {e}")
#             import traceback
#             logger.warning(traceback.format_exc())