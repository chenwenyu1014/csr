#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Word Content Control 内容插入服务（完整版）
基于Content Control的智能插入，支持：
1. 检测文件方向（Word/RTF/Excel）
2. 自动分类纵向/横向占位符
3. 在控件内插入分节符和切换页面方向
4. Excel使用COM粘贴保留格式
"""

import logging
import re
import time
from dataclasses import dataclass, field
import win32com.client as win32
import pywintypes
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

# 从独立模块导入数据类（不依赖win32com，可在Linux上导入）

logger = logging.getLogger(__name__)

# COM错误码
RPC_E_CALL_REJECTED = -2147418111  # 被呼叫方拒绝接收呼叫
RPC_E_SERVERCALL_RETRYLATER = -2147417846  # 服务器繁忙，稍后重试

@dataclass
class ResourceMapping:
    """资源映射"""
    placeholder: str  # 占位符（如 {{Table_1_Start}}）
    path: str  # 资源路径
    type: str  # 资源类型（table/image/excel）
    source_file: str  # 来源文件
    description: Optional[str] = None  # 描述
    orientation: Optional[str] = None  # 🆕 纸张方向（portrait/landscape），由Windows端检测后填充


@dataclass
class ContentInsertResult:
    """内容插入结果"""
    success: bool
    message: str
    output_file: Optional[str] = None
    inserted_controls: List[str] = field(default_factory=list)
    inserted_resources: List[str] = field(default_factory=list)
    error: Optional[str] = None
    # 🆕 资源方向信息（Windows端检测后返回给Linux）
    resource_orientations: Optional[dict] = None  # {占位符: "portrait"/"landscape"}

def com_retry(func, max_retries=5, delay=0.5, *args, **kwargs):
    """
    COM 操作重试辅助函数
    
    处理 Word/Excel COM 操作中常见的 RPC_E_CALL_REJECTED 错误
    当 COM 服务器繁忙时自动重试
    
    Args:
        func: 要执行的函数或 lambda
        max_retries: 最大重试次数
        delay: 重试间隔（秒），每次重试会递增
        *args, **kwargs: 传递给 func 的参数
    
    Returns:
        func 的返回值
    
    Raises:
        最后一次重试的异常
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except pywintypes.com_error as e:
            error_code = e.args[0] if e.args else None
            # 检查是否是可重试的 COM 错误
            if error_code in (RPC_E_CALL_REJECTED, RPC_E_SERVERCALL_RETRYLATER):
                last_error = e
                wait_time = delay * (attempt + 1)  # 递增等待时间
                logger.warning(f"⚠️ COM操作被拒绝，{wait_time:.1f}秒后重试 ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                # 其他 COM 错误直接抛出
                raise
        except Exception as e:
            # 非 COM 错误直接抛出
            raise
    
    # 所有重试都失败，抛出最后一个错误
    raise last_error

# Word常量
wdCollapseEnd = 0
wdCollapseStart = 1
wdSectionBreakNextPage = 2
wdOrientLandscape = 1
wdOrientPortrait = 0


class WordControlContentInserter:
    """
    基于Content Control的智能内容插入器
    
    功能：
    1. 检测Word/RTF/Excel文件的纸张方向
    2. 自动分类纵向/横向占位符
    3. 在控件内插入分节符和切换页面方向
    4. Word/RTF使用InsertFile保留格式
    5. Excel使用COM粘贴保留格式
    """
    
    def __init__(self):
        """初始化插入器"""
        self.word = None
        self.excel = None
        logger.info("WordControlContentInserter初始化完成")
    
    def _connect_word(self):
        """连接Word应用"""
        # 先清理旧的 Word 实例（如果有）
        if self.word is not None:
            try:
                self.word.Quit()
            except:
                pass
            self.word = None
        
        # 多线程环境需要初始化COM
        import pythoncom
        try:
            pythoncom.CoInitialize()
        except:
            pass
        
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                # 直接使用标准Dispatch（EnsureDispatch在某些环境有问题）
                from utils.windows_com import safe_dispatch
                self.word = safe_dispatch("Word.Application", use_ex=False, logger=logger)
                logger.info(f"✅ 连接Word应用（标准模式，尝试{attempt + 1}/{max_retries}）")
                
                # 验证连接是否有效
                _ = self.word.Version
                
                self.word.Visible = False
                self.word.DisplayAlerts = 0
                return  # 成功连接
                
            except Exception as e:
                last_error = e
                logger.warning(f"⚠️ Word连接失败（尝试{attempt + 1}/{max_retries}）: {e}")
                
                # 清理当前实例并重试
                try:
                    if self.word:
                        self.word.Quit()
                except:
                    pass
                self.word = None
                
                import time
                time.sleep(1)  # 等待1秒再重试
        
        raise RuntimeError(f"无法连接Word应用（重试{max_retries}次后失败）: {last_error}")
    
    def _connect_excel(self):
        """连接Excel应用"""
        if self.excel is None:
            # Flask多线程环境需要初始化COM
            import pythoncom
            try:
                pythoncom.CoInitialize()
            except:
                pass
            
            try:
                # 直接使用动态Dispatch
                self.excel = win32.dynamic.Dispatch("Excel.Application")
                logger.info("✅ 连接Excel应用（动态模式）")
            except:
                # 备用：标准Dispatch
                from utils.windows_com import safe_dispatch
                self.excel = safe_dispatch("Excel.Application", use_ex=False, logger=logger)
                logger.info("✅ 连接Excel应用")
            
            self.excel.Visible = False
            self.excel.DisplayAlerts = False
    
    def _cleanup(self):
        """清理资源"""
        # 清理Excel
        try:
            if self.excel:
                self.excel.Quit()
        except Exception as e:
            logger.warning(f"Excel清理异常: {e}")
        finally:
            self.excel = None
        
        # 清理Word
        try:
            if self.word:
                self.word.Quit()
        except Exception as e:
            logger.warning(f"Word清理异常: {e}")
        finally:
            self.word = None
        
        # 🆕 强制垃圾回收，确保 COM 对象被释放
        try:
            import gc
            gc.collect()
        except:
            pass
        
        # 释放COM资源
        try:
            import pythoncom
            pythoncom.CoUninitialize()
        except:
            pass
    
    @staticmethod
    def _clean_text_for_word(self, text: str) -> str:
        """
        清理文本中可能导致 Word COM 报错的字符

        Word COM 的 InsertAfter / Range.Text 对以下字符零容忍：
        - 不间断空格 \u00A0
        - 零宽字符 \u200B \u200C \u200D \uFEFF
        - 软连字符 \u00AD
        - 行尾不可见空格（Markdown two-space line break）
        - 其他 Unicode 特殊空白和控制字符
        """
        if not text:
            return ""
        original_text = text
        import re

        # 1. 统一换行符
        text = text.replace('\r\n', '\n').replace('\r', '\n')

        # 2. 替换各种特殊空白为普通空格
        special_spaces = [
            '\u00A0',  # 不间断空格 (Non-Breaking Space)
            '\u2002',  # En Space
            '\u2003',  # Em Space
            '\u2004',  # Three-Per-Em Space
            '\u2005',  # Four-Per-Em Space
            '\u2006',  # Six-Per-Em Space
            '\u2007',  # Figure Space
            '\u2008',  # Punctuation Space
            '\u2009',  # Thin Space
            '\u200A',  # Hair Space
            '\u202F',  # Narrow No-Break Space
            '\u205F',  # Medium Mathematical Space
            '\u3000',  # 全角空格 (Ideographic Space)
        ]
        for sp in special_spaces:
            text = text.replace(sp, ' ')

        # 3. 删除零宽字符和不可见字符
        invisible_chars = [
            '\u200B',  # 零宽空格 (Zero Width Space)
            '\u200C',  # 零宽不连字 (Zero Width Non-Joiner)
            '\u200D',  # 零宽连字 (Zero Width Joiner)
            '\u200E',  # 从左到右标记
            '\u200F',  # 从右到左标记
            '\uFEFF',  # BOM / 零宽不间断空格
            '\u00AD',  # 软连字符 (Soft Hyphen)
            '\u2028',  # 行分隔符 (Line Separator)
            '\u2029',  # 段分隔符 (Paragraph Separator)
        ]
        for ch in invisible_chars:
            text = text.replace(ch, '')

        # 4. 清理 Markdown 行尾两个空格（LLM 常见输出，会产生不可见尾部空格）
        #    "内容  \n" -> "内容\n"
        text = re.sub(r'  +\n', '\n', text)
        text = re.sub(r' +\n', '\n', text)

        # 5. 逐字符过滤残余控制字符
        #    保留：普通可打印字符、\n（换行）、\t（制表符）
        #    替换：其他 ASCII 控制字符（\x00-\x1F 中除 \t \n 外）
        cleaned = []
        for ch in text:
            code = ord(ch)
            if ch in ('\n', '\t'):
                cleaned.append(ch)
            elif code < 0x20:
                # 其他控制字符用空格替代（不直接删除，避免词语粘连）
                cleaned.append(' ')
            elif code == 0xFFFD:
                # Unicode 替换字符
                cleaned.append(' ')
            else:
                cleaned.append(ch)
        text = ''.join(cleaned)

        # 6. 压缩连续空行（超过2个换行压缩为2个）
        text = re.sub(r'\n{3,}', '\n\n', text)

        # 7. 去掉首尾空白
        text = text.strip()

        if text != original_text:
            logger.debug(f"  文本清洗: {len(original_text)} -> {len(text)} 字符")

        return text
    
    def detect_file_orientation(self, file_path: str) -> str:
        """
        检测文件的纸张方向
        
        Args:
            file_path: 文件路径
            
        Returns:
            "landscape" 或 "portrait"
        """
        file_ext = Path(file_path).suffix.lower()
        
        try:
            # Word文件：不需要检测方向（InsertFile会保留原格式）
            if file_ext in ['.docx', '.doc']:
                logger.info(f"  Word文件跳过方向检测: {Path(file_path).name}")
                return "portrait"
            
            # RTF文件：通过读取文件内容判断方向（不需要打开 Word）
            # RTF 格式中：
            # 1. \landscape 控制字表示横向
            # 2. 如果没有 \landscape，通过 \paperw 和 \paperh 判断（宽度 > 高度 = 横向）
            elif file_ext == '.rtf':
                logger.info(f"  🔍 检测RTF方向（文件内容扫描）: {Path(file_path).name}")
                try:
                    landscape_found = False
                    paper_width = None
                    paper_height = None
                    max_lines = 10  # 只读取前10行
                    
                    # 逐行读取文件的前几行
                    with open(file_path, 'r', encoding='latin1', errors='ignore') as file:
                        for i, line in enumerate(file):
                            if i >= max_lines:  # 只读取前max_lines行
                                break
                            
                            # 检查是否有 \landscape
                            if '\\landscape' in line:
                                landscape_found = True
                                break  # 找到横向后，直接跳出循环
                            
                            # 查找纸张宽度和高度
                            width_match = re.search(r'\\paperw(\d+)', line)
                            height_match = re.search(r'\\paperh(\d+)', line)
                            
                            if width_match:
                                paper_width = int(width_match.group(1))
                            if height_match:
                                paper_height = int(height_match.group(1))
                            
                            # 如果同时找到了宽度和高度，可以提前结束循环
                            if paper_width and paper_height:
                                break
                    
                    # 判断纸张方向
                    if landscape_found:
                        logger.info(f"    ✅ RTF方向: landscape (检测到 \\landscape)")
                        return "landscape"
                    
                    if paper_width is not None and paper_height is not None:
                        if paper_width > paper_height:
                            logger.info(f"    ✅ RTF方向: landscape (宽度{paper_width} > 高度{paper_height})")
                            return "landscape"
                        else:
                            logger.info(f"    ✅ RTF方向: portrait (宽度{paper_width} ≤ 高度{paper_height})")
                            return "portrait"
                    
                    # 如果都没有找到，默认横向（RTF 通常是数据表格）
                    logger.info(f"    ✅ RTF方向: landscape (默认，RTF通常是数据表格)")
                    return "landscape"
                    
                except Exception as e:
                    logger.warning(f"    ⚠️ RTF内容读取失败: {e}，默认横向")
                    return "landscape"
            
            # Excel文件：通过表格形状判断
            elif file_ext in ['.xlsx', '.xls']:
                logger.info(f"  🔍 开始检测Excel方向: {Path(file_path).name}")
                try:
                    import openpyxl
                    wb = openpyxl.load_workbook(file_path, read_only=True)
                    ws = wb.active
                    
                    max_row = ws.max_row
                    max_col = ws.max_column
                    wb.close()
                    
                    logger.info(f"    Excel尺寸: {max_row}行 × {max_col}列")
                    
                    # 列数 > 行数 → 横向
                    if max_col > max_row:
                        logger.info(f"    ✅ Excel方向: landscape (列数{max_col} > 行数{max_row})")
                        return "landscape"
                    else:
                        logger.info(f"    ✅ Excel方向: portrait (列数{max_col} ≤ 行数{max_row})")
                        return "portrait"
                except Exception as e:
                    logger.warning(f"    ⚠️ Excel方向检测失败: {e}，默认横向")
                    return "landscape"  # 默认横向
            
            # 其他文件：默认纵向
            else:
                return "portrait"
                
        except Exception as e:
            logger.error(f"检测失败 {Path(file_path).name}: {e}")
            return "portrait"
    
    def classify_placeholders_by_orientation(
        self,
        placeholders: List[str],
        resource_mappings: Dict[str, ResourceMapping]
    ) -> Tuple[List[str], List[str]]:
        """
        根据文件纸张方向分类占位符
        
        🆕 优化：将检测结果缓存到 mapping.orientation，便于后续使用
        
        Args:
            placeholders: 占位符列表
            resource_mappings: 资源映射
            
        Returns:
            (portrait_placeholders, landscape_placeholders)
            纵向占位符在前，横向占位符在后，便于插入时只用一对分节符
        """
        portrait = []
        landscape = []
        
        for placeholder in placeholders:
            if placeholder not in resource_mappings:
                logger.warning(f"占位符无映射: {placeholder}")
                portrait.append(placeholder)
                continue
            
            mapping = resource_mappings[placeholder]
            file_path = mapping.path
            
            if not Path(file_path).exists():
                logger.warning(f"文件不存在: {file_path}")
                portrait.append(placeholder)
                continue
            
            # 检测方向
            orientation = self.detect_file_orientation(file_path)
            
            # 🆕 缓存方向信息到 mapping（如果支持动态属性）
            try:
                mapping.orientation = orientation
            except:
                pass
            
            if orientation == "landscape":
                landscape.append(placeholder)
                logger.info(f"  📐 横向: {placeholder} <- {Path(file_path).name}")
            else:
                portrait.append(placeholder)
                logger.info(f"  📄 纵向: {placeholder} <- {Path(file_path).name}")
        
        # 🆕 日志汇总
        logger.info(f"  📊 方向分类完成: 纵向{len(portrait)}个, 横向{len(landscape)}个")
        
        return portrait, landscape
    
    def insert_to_control(
        self,
        doc,
        control,
        generated_text: str,
        portrait_placeholders: List[str],
        landscape_placeholders: List[str]
    ):
        """
        在控件内插入内容
        
        核心策略（参考原始设计）：
        - 纵向内容（包含纵向占位符）直接插入控件
        - 所有横向占位符集中到一起，用一对分节符包裹（纵向→横向→纵向）
        - 这样只有2个分节符，而不是每个横向文件各2个
        
        Args:
            doc: Word文档对象
            control: Content Control对象
            generated_text: 生成的文本
            portrait_placeholders: 纵向占位符列表
            landscape_placeholders: 横向占位符列表
        """
        # 获取外部格式
        try:
            outside_range = control.Range.Previous()
            font_name = outside_range.Font.Name
            font_size = outside_range.Font.Size
        except:
            font_name = "宋体"
            font_size = 12
        
        # 重新排列内容：从文本中移除横向占位符（它们会被集中到横向节）
        portrait_content = generated_text
        landscape_content = ""
        
        for p in landscape_placeholders:
            portrait_content = portrait_content.replace(p, "")  # 移除横向占位符
            landscape_content += f"{p}\n"  # 收集横向占位符
        
        # 清理可能导致 COM 错误的字符
        portrait_content = self._clean_text_for_word(portrait_content)
        
        # 清空控件并插入纵向内容（按照原始设计）
        # control.Range.Text = ""
        control.Range.Delete()
        control.Range.Style = "正文"
        control.Range.InsertAfter(portrait_content)
        
        # 应用格式
        control.Range.Font.Name = font_name
        control.Range.Font.Size = font_size
        
        logger.info(f"  ✅ 插入纵向内容 ({len(portrait_content)} 字符)")
        
        # 如果有横向占位符，用一对分节符包裹所有横向占位符
        if landscape_placeholders:
            logger.info(f"  处理 {len(landscape_placeholders)} 个横向占位符")
            
            # 在控件末尾插入第一个分节符（切换到横向）
            rng = control.Range
            rng.Collapse(wdCollapseEnd)
            rng.InsertBreak(Type=wdSectionBreakNextPage)
            logger.info("  ✅ 插入分节符（纵向→横向）")
            
            # 设置新节为横向
            new_section = doc.Sections(doc.Sections.Count)
            new_section.PageSetup.Orientation = wdOrientLandscape
            logger.info("  ✅ 设置横向页面")
            
            # 插入所有横向占位符（集中在一起）
            newrng = doc.Range(rng.Start, rng.Start)
            newrng.InsertAfter(landscape_content)
            newrng.Font.Name = font_name
            newrng.Font.Size = font_size
            logger.info(f"  ✅ 插入 {len(landscape_placeholders)} 个横向占位符")
            
            # 在横向区域末尾插入第二个分节符（恢复纵向）
            newrng.Collapse(wdCollapseEnd)
            newrng.InsertBreak(Type=wdSectionBreakNextPage)
            logger.info("  ✅ 插入分节符（横向→纵向）")
            
            # 恢复纵向
            final_section = doc.Sections(doc.Sections.Count)
            final_section.PageSetup.Orientation = wdOrientPortrait
            logger.info("  ✅ 恢复纵向页面")

    def _get_range_by_scanning(self, ws, max_scan_rows=100, max_scan_cols=100):
        """
        通过扫描单元格获取有效范围
        """
        logger.info("扫描单元格获取有效范围...")
        # 确定第一个和最后一个有内容的单元格
        first_row = None
        first_col = None
        last_row = None
        last_col = None

        # 扫描前max_scan_rows行和max_scan_cols列
        for row in range(1, max_scan_rows + 1):
            row_has_content = False

            for col in range(1, max_scan_cols + 1):
                try:
                    cell = ws.Cells(row, col)
                    # 检查单元格是否有内容（值或公式）
                    has_value = cell.Value is not None
                    has_formula = False

                    try:
                        formula = cell.Formula
                        has_formula = formula is not None and formula != ''
                    except:
                        pass

                    # 如果有内容
                    if has_value or has_formula:
                        row_has_content = True

                        # 更新边界
                        if first_row is None:
                            first_row = row
                        if first_col is None or col < first_col:
                            first_col = col

                        last_row = row
                        if last_col is None or col > last_col:
                            last_col = col

                except Exception as e:
                    continue

            # 如果连续5行都没有内容，提前结束扫描
            if not row_has_content:
                if row > 50:  # 至少扫描100行
                    empty_count = 0
                    # 检查后面几行
                    for next_row in range(row + 1, min(row + 5, max_scan_rows + 1)):
                        next_row_empty = True
                        for col in range(1, max_scan_cols + 1):
                            try:
                                cell = ws.Cells(next_row, col)
                                if cell.Value is not None:
                                    next_row_empty = False
                                    break
                            except:
                                pass
                        if next_row_empty:
                            empty_count += 1
                        else:
                            empty_count = 0
                            break

                    if empty_count >= 5:  # 连续5行空行，认为已经结束
                        logger.info(f"检测到连续空行，在第{row}行停止扫描")
                        break
        # 设置默认值
        if first_row is None:
            first_row = 1
        if first_col is None:
            first_col = 1
        if last_row is None:
            last_row = first_row
        if last_col is None:
            last_col = first_col
        # 确保范围有效
        if last_row < first_row:
            last_row = first_row
        if last_col < first_col:
            last_col = first_col
        # 创建范围
        true_range = ws.Range(
            ws.Cells(first_row, first_col),
            ws.Cells(last_row, last_col)
        )

        logger.info(f"扫描结果: {true_range.Address} ({last_row - first_row + 1}行×{last_col - first_col + 1}列)")

        return true_range

    def replace_placeholder_with_file(
        self,
        doc,
        placeholder: str,
        mapping: ResourceMapping
    ):
        """
        替换占位符为实际文件内容
        
        注意：不在这里处理分节符！
        横向占位符已经在 insert_to_control 中被移到横向节里了，
        这里只需要简单地替换占位符为文件内容。
        
        Args:
            doc: Word文档对象
            placeholder: 占位符
            mapping: 资源映射
            
        Raises:
            Exception: 当发生严重 COM 错误时（如 RPC 服务器不可用）向上抛出
        """
        file_path = mapping.path
        file_ext = Path(file_path).suffix.lower()
        
        try:
            # 查找占位符
            find_range = doc.Range()
            find_range.Find.ClearFormatting()
            find_range.Find.Text = placeholder
            
            if not find_range.Find.Execute():
                logger.warning(f"  ⚠️ 未找到占位符: {placeholder}")
                return
            
            logger.info(f"  替换: {placeholder}")
            logger.info(f"    <- {Path(file_path).name}")
            
            # 清除占位符
            # find_range.Text = ""
            find_range.Delete()
            
            # 根据文件类型插入
            file_abs_path = str(Path(file_path).absolute())
            
            if file_ext in ['.docx', '.doc', '.rtf']:
                # Word/RTF文件：InsertFile（保留格式）
                find_range.InsertFile(file_abs_path)
                logger.info(f"    ✅ InsertFile插入")
            
            elif file_ext in ['.xlsx', '.xls']:
                # Excel文件：COM粘贴（保留格式）
                self._connect_excel()
                
                workbook = self.excel.Workbooks.Open(file_abs_path,ReadOnly=True)
                sheet = workbook.Sheets(1)

                # 复制Excel内容
                # sheet.UsedRange.Copy()
                import openpyxl
                wb = openpyxl.load_workbook(file_path, read_only=True)
                ws = wb.active
                max_row = ws.max_row
                max_col = ws.max_column
                wb.close()
                rng = self._get_range_by_scanning(sheet,max_row+10,max_col+10)
                if rng is None:
                    raise RuntimeError(
                        f"Sheet 没找到有效内容范围。"
                        f"可尝试 top_left='A1' 或切换 look_in_formulas。"
                    )

                rng.Copy()  # 放入剪贴板

                
                # 粘贴到Word
                find_range.Paste()
                
                # 关闭Excel文件
                workbook.Close(False)
                logger.info(f"    ✅ Excel COM粘贴插入")
            
            else:
                logger.warning(f"    ⚠️ 不支持的文件类型: {file_ext}")
        
        except Exception as e:
            error_str = str(e)
            logger.error(f"  ❌ 替换失败 {placeholder}: {e}")
            
            # 检查是否是严重的 COM 错误（RPC 服务器不可用等）
            # 错误码 -2147023174 (0x800706BA) = RPC 服务器不可用
            # 错误码 -2147417848 (0x80010108) = 对象已断开连接
            is_fatal_com_error = (
                '-2147023174' in error_str or  # RPC 服务器不可用
                '-2147417848' in error_str or  # 对象已断开连接
                'RPC' in error_str.upper() or
                '服务器不可用' in error_str or
                'disconnected' in error_str.lower()
            )
            
            if is_fatal_com_error:
                logger.error(f"  ❌ 检测到严重 COM 错误，向上抛出")
                raise  # 重新抛出异常，让调用方知道 Word 可能已经不可用
    
    def insert_to_template(
        self,
        template_file: str,
        generation_results: List[Dict[str, Any]],
        resource_mappings: Dict[str, ResourceMapping],
        output_file: str
    ) -> ContentInsertResult:
        """
        将生成结果插入到Word模板
        
        Args:
            template_file: Word模板文件路径
            generation_results: 生成结果列表 [
                {
                    "paragraph_id": "study_population",
                    "control_title": "study_population",
                    "generated_content": "文本内容\n{{Table_1}}\n更多文本",
                    "status": "success"
                }
            ]
            resource_mappings: 资源映射 {
                "{{Table_1}}": ResourceMapping(...)
            }
            output_file: 输出文件路径
            
        Returns:
            ContentInsertResult
        """
        try:
            logger.info("=" * 70)
            logger.info("开始插入内容到Word模板")
            logger.info("=" * 70)
            
            # 连接Word
            self._connect_word()
            
            # 验证模板文件存在
            template_path = Path(template_file).absolute()
            if not template_path.exists():
                raise FileNotFoundError(f"模板文件不存在: {template_path}")
            
            # 打开模板
            logger.info(f"📄 打开模板文件: {template_path}")
            try:
                # 使用 com_retry 处理 Word 繁忙的情况
                doc = com_retry(
                    lambda: self.word.Documents.Open(str(template_path)),
                    max_retries=5,
                    delay=0.5
                )
                
                # 验证文档对象
                if doc is None:
                    raise RuntimeError(f"Word打开文档失败，返回None")
                
                # 使用 com_retry 访问 doc.Name，防止 COM 繁忙错误
                doc_name = com_retry(lambda: doc.Name, max_retries=5, delay=0.3)
                logger.info(f"✅ 打开模板: {doc_name}")
                
            except AttributeError as e:
                # 如果仍然出错，说明 EnsureDispatch 也没有生效
                logger.error(f"❌ doc 对象类型: {type(doc)}")
                raise RuntimeError(f"Word返回的不是有效的文档对象. 错误: {e}")
            except pywintypes.com_error as e:
                error_code = e.args[0] if e.args else None
                logger.error(f"❌ Word COM 错误 (代码: {error_code}): {e}")
                raise RuntimeError(f"Word打开文档失败(COM错误): {e}")
            except Exception as e:
                raise RuntimeError(f"Word打开文档失败: {e}")
            
            inserted_controls = []
            inserted_resources = []
            resource_orientations = {}  # 🆕 收集方向信息
            
            # ===== 第一步：插入文本内容（包括占位符作为文本） =====
            logger.info("\n" + "=" * 70)
            logger.info("第一步：插入文本内容和占位符")
            logger.info("=" * 70)
            
            # 处理每个段落
            for result in generation_results:
                if result.get("status") != "success":
                    continue
                
                control_title = result.get("control_title") or result.get("paragraph_id")
                generated_content = result.get("generated_content", "")
                
                logger.info(f"处理控件: {control_title}")
                
                # 查找控件
                cc_collection = doc.SelectContentControlsByTitle(control_title)
                if cc_collection.Count < 1:
                    logger.warning(f"  ⚠️ 未找到控件: {control_title}")
                    continue
                else:
                    logger.info(f"  ⚠️ 找到控件: {control_title},共{cc_collection.Count}个")

                controls = cc_collection
                logger.info(f"  ✅ 找到控件")
                
                # 提取占位符
                placeholders = re.findall(r'\{\{[^}]+\}\}', generated_content)
                logger.info(f"  发现 {len(placeholders)} 个占位符")
                
                # 过滤有效的占位符
                valid_placeholders = [p for p in placeholders if p in resource_mappings]
                
                if not valid_placeholders:
                    # 没有占位符，直接插入文本
                    for control in controls:
                        try:
                            # 清理内容中可能导致 COM 错误的字符
                            cleaned_content = self._clean_text_for_word(generated_content)
                            control.Range.Text = cleaned_content
                            logger.info(f"  ✅ 文本插入成功 ({len(cleaned_content)} 字符)")
                            inserted_controls.append(control_title)
                        except Exception as e:
                            logger.error(f"  ❌ 文本插入失败: {e}")
                            logger.error(f"  内容长度: {len(generated_content)} 字符")
                            logger.error(f"  内容前200字符: {generated_content[:200]}")
                            raise
                    continue
                
                # 检测方向并分类
                logger.info("  检测文件方向...")
                portrait_list, landscape_list = self.classify_placeholders_by_orientation(
                    valid_placeholders,
                    resource_mappings
                )
                
                # 🆕 收集方向信息（用于返回给Linux）
                for p in portrait_list:
                    resource_orientations[p] = "portrait"
                for p in landscape_list:
                    resource_orientations[p] = "landscape"
                
                logger.info(f"  分类结果: 纵向{len(portrait_list)}个, 横向{len(landscape_list)}个")
                
                # 在控件内插入内容（占位符作为文本插入）
                for control in controls:
                    self.insert_to_control(
                        doc,
                        control,
                        generated_content,
                        portrait_list,
                        landscape_list
                    )
                
                inserted_controls.append(control_title)
            
            # 保存并关闭文档（第一步完成）
            temp_output = str(Path(output_file).absolute())
            doc.SaveAs(temp_output)
            logger.info(f"\n✅ 第一步完成，保存文档: {Path(output_file).name}")
            
            # 关闭文档
            try:
                doc.Close(False)
                logger.info("✅ 文档已关闭")
            except Exception as close_err:
                logger.warning(f"关闭文档时出错: {close_err}")
            
            # 清理 COM 对象
            doc = None
            self._cleanup()
            
            # 🆕 等待 Word 进程完全退出，避免 RPC 连接到僵尸进程
            import time
            time.sleep(2)
            logger.info("⏳ 等待 Word 进程完全退出...")
            
            # ===== 第二步：重新打开文档，替换占位符为实际文件 =====
            # 只替换实际被使用的占位符（在 resource_orientations 中记录的）
            used_placeholders = list(resource_orientations.keys())
            
            if used_placeholders:
                logger.info("=" * 70)
                logger.info(f"第二步：替换 {len(used_placeholders)} 个占位符为实际文件")
                logger.info("=" * 70)
                
                # 🆕 重新连接 Word，带重试机制
                max_connect_retries = 3
                for connect_attempt in range(max_connect_retries):
                    try:
                        self._connect_word()
                        # 重新打开文档
                        doc = self.word.Documents.Open(temp_output)
                        logger.info(f"✅ 重新打开文档: {Path(output_file).name}")
                        break
                    except Exception as connect_err:
                        logger.warning(f"⚠️ 第二步连接 Word 失败 (尝试 {connect_attempt + 1}/{max_connect_retries}): {connect_err}")
                        if connect_attempt < max_connect_retries - 1:
                            self._cleanup()
                            import time
                            time.sleep(3)  # 等待更长时间再重试
                        else:
                            logger.error("❌ 无法重新连接 Word，跳过占位符替换")
                            # 即使第二步失败，第一步的文件已保存
                            return ContentInsertResult(
                                success=True,
                                message="文本内容已插入，但占位符替换失败",
                                output_file=output_file,
                                inserted_controls=inserted_controls,
                                inserted_resources=[],
                                resource_orientations=resource_orientations,
                                error=f"第二步失败: {connect_err}"
                            )
                
                # 只替换实际使用的占位符
                replace_errors = []
                word_reconnect_needed = False
                
                for placeholder in used_placeholders:
                    if placeholder not in resource_mappings:
                        logger.warning(f"  ⚠️ 占位符无映射: {placeholder}")
                        continue
                    
                    mapping = resource_mappings[placeholder]
                    if not Path(mapping.path).exists():
                        logger.warning(f"  ⚠️ 文件不存在: {mapping.path}")
                        continue
                    
                    # 🆕 如果之前检测到 Word 连接断开，尝试重新连接
                    if word_reconnect_needed:
                        logger.info("  🔄 尝试重新连接 Word...")
                        try:
                            self._cleanup()
                            import time
                            time.sleep(2)
                            self._connect_word()
                            doc = self.word.Documents.Open(temp_output)
                            word_reconnect_needed = False
                            logger.info("  ✅ Word 重新连接成功")
                        except Exception as reconnect_err:
                            logger.error(f"  ❌ 重新连接失败: {reconnect_err}，跳过剩余占位符")
                            break
                    
                    # 直接替换（分节符已在第一步处理）
                    try:
                        self.replace_placeholder_with_file(doc, placeholder, mapping)
                        inserted_resources.append(placeholder)
                    except Exception as replace_err:
                        error_msg = f"替换 {placeholder} 失败: {replace_err}"
                        logger.error(f"  ❌ {error_msg}")
                        replace_errors.append(error_msg)
                        
                        # 检查 Word 连接是否仍然有效
                        try:
                            _ = self.word.Version
                        except Exception:
                            logger.warning("  ⚠️ Word 连接已断开，将尝试重新连接")
                            word_reconnect_needed = True
                
                # 保存并关闭文档（第二步完成）
                try:
                    doc.SaveAs(temp_output)
                    logger.info(f"✅ 第二步完成，保存文档: {Path(output_file).name}")
                except Exception as save_err:
                    # 保存失败时，记录错误但不立即失败
                    # 因为第一步的文件可能已经保存成功
                    logger.error(f"  ❌ 第二步保存失败: {save_err}")
                    logger.info("  ℹ️ 第一步的文件已保存，但占位符替换可能不完整")
                    # 不抛出异常，让函数继续返回部分成功的结果
                
                try:
                    doc.Close(False)
                    logger.info("✅ 文档已关闭")
                except Exception as close_err:
                    logger.warning(f"关闭文档时出错: {close_err}")
            else:
                logger.info("\n✅ 没有占位符需要替换，跳过第二步")
            
            logger.info("=" * 70)
            logger.info("✅ 插入完成")
            logger.info("=" * 70)
            
            return ContentInsertResult(
                success=True,
                message="内容插入成功",
                output_file=output_file,
                inserted_controls=inserted_controls,
                inserted_resources=inserted_resources,
                resource_orientations=resource_orientations  # 🆕 返回方向信息给Linux
            )
        
        except Exception as e:
            logger.error(f"❌ 插入失败: {e}")
            import traceback
            traceback.print_exc()
            # ✅ 出错时也要尝试关闭文档，否则文件会被锁定
            try:
                if 'doc' in dir() or 'doc' in locals():
                    doc.Close(SaveChanges=False)  # False = 不保存，直接关闭
                    logger.info("🔒 异常退出：文档已关闭（不保存）")
            except Exception as close_err:
                logger.warning(f"异常退出时关闭文档失败: {close_err}")
            
            return ContentInsertResult(
                success=False,
                message="内容插入失败",
                error=str(e)
            )
        
        finally:
            self._cleanup()
    
    @staticmethod
    def build_resource_mappings_from_extraction(
        extracted_data: Dict[str, Any]
    ) -> Dict[str, ResourceMapping]:
        """
        从提取数据构建资源映射
        
        Args:
            extracted_data: 提取数据，包含 extracted_items，其中有 tfl_insert_mappings
            
        Returns:
            Dict[str, ResourceMapping]: 占位符到资源的映射
        """
        mappings = {}
        
        items = extracted_data.get('extracted_items', [])
        for item in items:
            tfl_mappings = item.get('tfl_insert_mappings', [])
            for mapping in tfl_mappings:
                placeholder = mapping.get('Placeholder')
                path = mapping.get('Path')
                source = mapping.get('Source', '')
                
                if placeholder and path:
                    # 判断类型
                    file_ext = Path(path).suffix.lower()
                    if file_ext in ['.xlsx', '.xls']:
                        resource_type = 'excel'
                    elif file_ext in ['.docx', '.doc', '.rtf']:
                        resource_type = 'table'
                    else:
                        resource_type = 'unknown'
                    
                    mappings[placeholder] = ResourceMapping(
                        placeholder=placeholder,
                        path=path,
                        type=resource_type,
                        source_file=source
                    )
        
        return mappings
