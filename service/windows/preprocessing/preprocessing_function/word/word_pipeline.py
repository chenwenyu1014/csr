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

    # 匹配开始/结束标记，如 {{Table_1_Start}} 或 {{TemplateTable_1_Start}}
    START_PATTERN = re.compile(r'^\{\{(TemplateTable|Table|Image)_(\d+)(?:_(\d+))?_Start\}\}$')
    END_PATTERN = re.compile(r'^\{\{(TemplateTable|Table|Image)_(\d+)(?:_(\d+))?_End\}\}$')


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
                import traceback
                traceback.print_exc()
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

        # 递归查找所有标记段落（包括嵌套在表格单元格内的）
        all_markers = self._find_all_markers_recursive(doc._element.body)

        regions: List[Dict] = []
        # 使用栈结构处理嵌套区域
        region_stack: List[Dict] = []

        for marker_info in all_markers:
            text = marker_info['text']
            elem = marker_info['element']
            is_in_cell = marker_info['is_in_cell']
            cell_path = marker_info['cell_path']

            start_match = self.START_PATTERN.match(text)
            if start_match:
                marker_type = start_match.group(1)
                primary_idx = int(start_match.group(2))
                secondary_idx = int(start_match.group(3)) if start_match.group(3) else None

                # 判断是否为嵌套区域：通过是否有第二个索引判断
                # Table_1 或 Image_1 是顶层，Table_1_1 或 Image_1_1 是嵌套
                is_nested = secondary_idx is not None

                if is_nested:
                    # 嵌套区域使用元素引用
                    name = f"{marker_type}_{primary_idx}_{secondary_idx}"
                    new_region = {
                        'name':       name,
                        'type':       marker_type.lower(),
                        'start_idx':  None,  # 嵌套区域不使用索引
                        'end_idx':    None,
                        'is_nested':  True,
                        'start_elem': elem,
                        'end_elem':   None,
                        'cell_path':  cell_path,
                    }
                else:
                    # 顶层区域使用索引
                    name = f"{marker_type}_{primary_idx}"
                    # 查找标记段落在 elements 列表中的索引
                    idx = self._find_element_index(elements, elem)
                    new_region = {
                        'name':       name,
                        'type':       marker_type.lower(),
                        'start_idx':  idx + 1,   # 内容从下一个元素开始
                        'end_idx':    None,
                        'is_nested':  False,
                        'start_elem': elem,
                        'end_elem':   None,
                    }

                # Push 到栈
                region_stack.append(new_region)
                continue

            end_match = self.END_PATTERN.match(text)
            if end_match and region_stack:
                marker_type = end_match.group(1)
                primary_idx = int(end_match.group(2))
                secondary_idx = int(end_match.group(3)) if end_match.group(3) else None

                # 检查栈顶是否匹配
                top_region = region_stack[-1]

                if top_region['is_nested']:
                    expected_name = f"{marker_type}_{primary_idx}_{secondary_idx}"
                else:
                    expected_name = f"{marker_type}_{primary_idx}"

                if top_region['name'] == expected_name:
                    # 匹配成功，Pop 并完成区域
                    top_region['end_elem'] = elem
                    if not top_region['is_nested']:
                        # 顶层区域需要计算结束索引
                        idx = self._find_element_index(elements, elem)
                        top_region['end_idx'] = idx   # 不含结束标记本身

                    region_stack.pop()
                    regions.append(top_region)
                else:
                    logger.warning(
                        f"结束标记 {expected_name} 与栈顶区域 {top_region['name']} 不匹配，忽略"
                    )

        # 检查未闭合的区域
        for unfinished in region_stack:
            logger.warning(f"区域 {unfinished['name']} 有开始标记但没有结束标记，已忽略")

        return regions

    def _find_element_index(self, elements: List[Tuple[str, etree._Element]],
                             target_elem: etree._Element) -> int:
        """在元素列表中查找目标元素的索引"""
        for idx, (elem_type, elem) in enumerate(elements):
            if elem is target_elem:
                return idx
        return -1

    def _find_all_markers_recursive(self, root_element: etree._Element) -> List[Dict]:
        """
        递归遍历文档树，查找所有标记段落

        包括嵌套在表格单元格内的标记段落

        Args:
            root_element: 根元素（通常是 body）

        Returns:
            标记信息列表，每个包含: text, element, is_in_cell, cell_path
        """
        markers: List[Dict] = []

        def traverse(elem: etree._Element, in_cell: bool = False, cell_path: List = None):
            """递归遍历元素树"""
            if cell_path is None:
                cell_path = []

            local_name = etree.QName(elem.tag).localname

            # 如果是段落，检查是否为标记段落
            if local_name == 'p':
                text = self._get_element_text(elem)
                if self.START_PATTERN.match(text) or self.END_PATTERN.match(text):
                    markers.append({
                        'text':       text,
                        'element':    elem,
                        'is_in_cell': in_cell,
                        'cell_path':  cell_path.copy(),
                    })

            # 如果是表格单元格，标记进入单元格
            elif local_name == 'tc':
                new_path = cell_path + [elem]
                for child in elem:
                    traverse(child, in_cell=True, cell_path=new_path)

            # 其他元素继续递归遍历
            else:
                for child in elem:
                    traverse(child, in_cell=in_cell, cell_path=cell_path)

        traverse(root_element)
        return markers

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
        解析 document.xml，只保留目标区域内的元素。

        处理两种情况：
        - 顶层区域：保留 body 直接子元素中索引在范围内的内容
        - 嵌套区域：提取单元格内的内容，创建包含该内容的最小表格结构

        保留规则：
          - sectPr（页面/节属性）始终保留，放在最后
        """
        parser = etree.XMLParser(remove_blank_text=False)
        tree   = etree.parse(str(doc_xml_path), parser)
        body   = tree.find(f'.//{WNS_P}body')

        if body is None:
            raise ValueError("document.xml 中找不到 <w:body>")

        # 单独摘出 sectPr（可能不存在）
        sect_pr = body.find(f'{WNS_P}sectPr')

        if region.get('is_nested', False):
            # 处理嵌套区域
            self._trim_nested_region(body, region, sect_pr)
        else:
            # 处理顶层区域（原有逻辑）
            self._trim_top_level_region(body, region, sect_pr)

        # 写回文件
        tree.write(
            str(doc_xml_path),
            xml_declaration=True,
            encoding='UTF-8',
            standalone=True,
        )

    def _trim_top_level_region(self, body: etree._Element, region: Dict,
                                sect_pr: Optional[etree._Element]) -> None:
        """
        处理顶层区域的裁剪

        Args:
            body: body 元素
            region: 区域信息字典
            sect_pr: sectPr 元素（可选）
        """
        # 取出所有直接子元素（含 sectPr）
        all_children = list(body)

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
            # 清理元素内部的标记段落（嵌套表格和单元格内图片的标记）
            self._remove_internal_markers(elem)
            body.append(elem)

        # sectPr 始终放最后（保持页面设置），但需要删除页眉页脚引用
        if sect_pr is not None:
            # 删除 headerReference 和 footerReference，避免导出文档带有页眉页脚
            for ref in sect_pr.findall(f'{WNS_P}headerReference'):
                sect_pr.remove(ref)
            for ref in sect_pr.findall(f'{WNS_P}footerReference'):
                sect_pr.remove(ref)
            body.append(sect_pr)

    def _remove_internal_markers(self, elem: etree._Element) -> None:
        """
        清理元素内部的标记段落

        删除表格单元格内的 {{Table_N_M_Start/End}} 和 {{Image_N_M_Start/End}} 标记

        Args:
            elem: 要清理的元素（通常是表格）
        """
        # 只处理表格元素
        if not elem.tag.endswith('tbl'):
            return

        # 遍历表格结构：tbl -> tr -> tc -> p
        for tr in elem:
            if not tr.tag.endswith('tr'):
                continue
            for tc in tr:
                if not tc.tag.endswith('tc'):
                    continue

                # 收集需要删除的标记段落
                markers_to_remove = []
                for child in tc:
                    if child.tag.endswith('p'):
                        text = self._get_element_text(child)
                        # 匹配嵌套标记：{{Table_N_M_Start/End}} 或 {{Image_N_M_Start/End}}
                        if self._is_internal_marker(text):
                            markers_to_remove.append(child)

                # 删除标记段落
                for marker in markers_to_remove:
                    tc.remove(marker)

    def _is_internal_marker(self, text: str) -> bool:
        """
        判断文本是否为内部标记

        内部标记格式：{{Table_N_M_Start/End}} 或 {{Image_N_M_Start/End}}
        特点：有两个数字索引（N_M）

        Args:
            text: 段落文本

        Returns:
            是否为内部标记
        """
        if not text.startswith('{{') or not text.endswith('}}'):
            return False

        # 移除 {{ 和 }}
        content = text[2:-2]

        # 检查格式：Table_N_M_Start/End 或 Image_N_M_Start/End
        parts = content.split('_')
        if len(parts) != 4:
            return False

        type_part, num1, num2, start_end = parts
        if type_part not in ('Table', 'Image'):
            return False

        if start_end not in ('Start', 'End'):
            return False

        try:
            int(num1)
            int(num2)
            return True
        except ValueError:
            return False

    def _trim_nested_region(self, body: etree._Element, region: Dict,
                            sect_pr: Optional[etree._Element]) -> None:
        """
        处理嵌套区域的裁剪

        对于嵌套在表格单元格内的内容：
        - 嵌套表格 (NestedTable): 直接输出表格本身
        - 单元格内图片 (CellImage): 创建简单的表格结构包含图片段落

        Args:
            body: body 元素
            region: 区域信息字典
            sect_pr: sectPr 元素（可选）
        """
        import copy

        # 找到开始和结束标记之间的内容
        start_elem = region.get('start_elem')
        end_elem = region.get('end_elem')
        cell_path = region.get('cell_path', [])
        region_type = region.get('type', '')

        if start_elem is None or end_elem is None:
            raise ValueError(f"嵌套区域 {region['name']} 缺少标记元素引用")

        # 获取标记所在的单元格
        container = cell_path[-1] if cell_path else None

        if container is None:
            raise ValueError(f"嵌套区域 {region['name']} 缺少单元格路径")

        # 找到开始标记和结束标记之间的内容元素
        content_elements = self._get_elements_between_markers(
            start_elem, end_elem, container
        )

        # 清空 body
        for child in list(body):
            body.remove(child)

        # 根据区域类型决定输出方式
        if region_type == 'table':
            # 嵌套表格：直接输出表格元素
            for elem in content_elements:
                # 深拷贝以保留完整结构
                tbl_elem = copy.deepcopy(elem)
                # 确保表格有必需的 tblGrid 元素
                self._ensure_tbl_grid(tbl_elem)
                body.append(tbl_elem)
        else:
            # 单元格内图片等其他内容：创建简化的表格结构
            simple_tbl = self._create_simple_table_for_nested(content_elements)
            body.append(simple_tbl)

        # sectPr 始终放最后，但需要删除页眉页脚引用
        if sect_pr is not None:
            # 删除 headerReference 和 footerReference，避免导出文档带有页眉页脚
            for ref in sect_pr.findall(f'{WNS_P}headerReference'):
                sect_pr.remove(ref)
            for ref in sect_pr.findall(f'{WNS_P}footerReference'):
                sect_pr.remove(ref)
            body.append(sect_pr)

    def _ensure_tbl_grid(self, tbl_elem: etree._Element) -> None:
        """
        确保表格元素有必需的 tblGrid 子元素

        Word 要求每个表格都有 tblGrid 元素来定义列宽

        Args:
            tbl_elem: 表格 XML 元素
        """
        # 检查是否已有 tblGrid
        tblGrid = tbl_elem.find(f'{WNS_P}tblGrid')
        if tblGrid is not None:
            return  # 已存在，无需添加

        # 计算列数（通过第一行的单元格数）
        col_count = 0
        for tr in tbl_elem:
            if etree.QName(tr.tag).localname == 'tr':
                for tc in tr:
                    if etree.QName(tc.tag).localname == 'tc':
                        col_count += 1
                break  # 只检查第一行

        if col_count == 0:
            col_count = 1  # 默认至少1列

        # 创建 tblGrid 元素
        tblGrid = etree.Element(f'{WNS_P}tblGrid')
        for _ in range(col_count):
            gridCol = etree.SubElement(tblGrid, f'{WNS_P}gridCol')
            gridCol.set(f'{WNS_P}w', '3000')  # 默认列宽

        # 找到合适的位置插入 tblGrid
        # tblGrid 应该在 tblPr 之后，如果 tblPr 存在
        tblPr = tbl_elem.find(f'{WNS_P}tblPr')
        if tblPr is not None:
            # 在 tblPr 之后插入
            tblPr_index = list(tbl_elem).index(tblPr)
            tbl_elem.insert(tblPr_index + 1, tblGrid)
        else:
            # 在第一个位置插入 tblPr 和 tblGrid
            new_tblPr = etree.Element(f'{WNS_P}tblPr')
            tbl_elem.insert(0, new_tblPr)
            tbl_elem.insert(1, tblGrid)

    def _get_elements_between_markers(self, start_marker: etree._Element,
                                       end_marker: etree._Element,
                                       container: Optional[etree._Element]) -> List[etree._Element]:
        """
        获取开始标记和结束标记之间的内容元素

        Args:
            start_marker: 开始标记段落元素
            end_marker: 结束标记段落元素
            container: 包含这些标记的容器元素（通常是单元格）

        Returns:
            内容元素列表（不含标记段落本身）
        """
        content_elements: List[etree._Element] = []

        if container is not None:
            # 在容器内查找
            found_start = False
            for child in container:
                if child is start_marker:
                    found_start = True
                    continue
                if child is end_marker:
                    break
                if found_start:
                    content_elements.append(child)

        return content_elements

    def _create_simple_table_for_nested(self, content_elements: List[etree._Element]) -> etree._Element:
        """
        为嵌套内容创建简化的表格结构

        创建一个单行单列的表格，包含给定的内容元素
        使用深拷贝的方式保留原有元素的命名空间

        Args:
            content_elements: 要包含的内容元素列表

        Returns:
            表格 XML 元素 (w:tbl)
        """
        import copy

        # 创建表格元素（使用命名空间）
        tbl = etree.Element(f'{WNS_P}tbl', nsmap={None: WNS})

        # 创建表格属性
        tblPr = etree.SubElement(tbl, f'{WNS_P}tblPr')

        # 创建 tblGrid（必需元素，定义列宽）
        tblGrid = etree.SubElement(tbl, f'{WNS_P}tblGrid')
        gridCol = etree.SubElement(tblGrid, f'{WNS_P}gridCol')
        gridCol.set(f'{WNS_P}w', '5000')  # 默认列宽

        # 创建表格行
        tr = etree.SubElement(tbl, f'{WNS_P}tr')

        # 创建表格单元格
        tc = etree.SubElement(tr, f'{WNS_P}tc')

        # 添加内容元素到单元格（深拷贝以保留完整结构）
        for elem in content_elements:
            # 深拷贝元素，保留所有子元素和属性
            tc.append(copy.deepcopy(elem))

        return tbl

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
        """提取段落元素的纯文本（拼接所有 w:t 子节点）

        注意：不使用 itertext()，因为它在某些情况下会返回重复文本。
        改用 findall 精确查找所有 w:t 元素。
        """
        try:
            # 使用 findall 精确查找所有 w:t 元素，避免重复
            t_elements = p_element.findall(f'.//{WNS_P}t')
            texts = [t.text for t in t_elements if t.text]
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