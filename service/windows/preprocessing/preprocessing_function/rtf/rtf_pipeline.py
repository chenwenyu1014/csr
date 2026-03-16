# -*- coding: utf-8 -*-
from __future__ import annotations
import re
import logging
import subprocess
from shutil import which
from pathlib import Path
from typing import Dict, Any,Tuple, List, Optional

from utils.timing import Timer
from service.windows.preprocessing.file_processor import ContentType

logger = logging.getLogger(__name__)


# 主入口（兼容旧接口）
def run(rtf_path: Path | str, work_dir: Path | str) -> Dict[str, Any]:
    rtf_path = Path(rtf_path)
    work_dir = Path(work_dir)
    try:
        # 目录对齐旧结构
        sheets_dir = work_dir / "sheets"
        md_dir = sheets_dir / "markdown"
        md_dir.mkdir(parents=True, exist_ok=True)

        # Step1: RTF → DOCX → 单MD
        docx_path = rtf_to_docx(rtf_path, work_dir)
        raw_md = work_dir / f"{rtf_path.stem}.md"
        docx_to_md(docx_path, raw_md)

        # Step2: 简化MD文件
        text = raw_md.read_text(encoding="utf-8")
        clean_text = simplify_markdown_table(text)
        logger.info(
            f"简化完成，原始始字符数: {len(text)}，简化后: {len(clean_text)},字符数减少: {len(text) - len(clean_text)} (约{((len(text) - len(clean_text)) / len(text) * 100):.1f}%)")

        clean_md = work_dir / f"{rtf_path.stem}_clean.md"
        clean_md.write_text(clean_text, encoding="utf-8")
        logger.info(f"保存简化结果到 {clean_md}")

        # Step3: 多表拆分
        logger.info("开始拆分")
        table_files = split_markdown_by_tables(clean_md, md_dir)
        logger.info(f"拆分完成，共生成 {len(table_files)} 个文件")
    except Exception as e:
        logger.error(f"RTF_pipeline转换出问题：{e}")
        raise

    # ==== 构造兼容之前处理方式的返回内容====
    excel_regions = []
    excel_markdown_regions = []

    for md in table_files:
        name = md.stem
        excel_regions.append({
            "sheet_name": name,
            "file_path": str(md)  # 兼容占位
        })
        excel_markdown_regions.append({
            "sheet_name": name,
            "markdown_file": str(md)
        })

    return {
        'content': '',
        'content_type': ContentType.STRUCTURED,
        'text': '',
        'excel_regions': excel_regions,
        'excel_markdown_regions': excel_markdown_regions,
        'metadata': {
            'conversion_method': 'rtf_pipeline',
            'steps': [
                'rtf_to_docx',
                'docx_to_markdown',
                'clean_markdown',
                'split_tables'
            ],
            'sheets_total': len(table_files),
            'excel_sheets': excel_regions,
            'excel_markdown_regions': excel_markdown_regions,
            'out_dir': str(sheets_dir.resolve()),
        }
    }


# ===转换部分：RTF → DOCX → MD===
def rtf_to_docx(rtf_path: Path, output_dir: Path) -> Path:
    """
    RTF 转换为 DOCX
    """
    if not rtf_path.exists():
        logger.error(f"输入的 RTF 文件不存在: {rtf_path}")
        raise FileNotFoundError(f"输入的 RTF 文件不存在: {rtf_path}")

    soffice = _check_tool_available("soffice")
    if not soffice:
        logger.error("未找到 LibreOffice soffice, 请检查是否已安装并添加到环境变量中")
        raise RuntimeError("未找到 LibreOffice soffice")

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"正在将 RTF 转换为 DOCX，文件位置:{rtf_path}")
    timer = Timer('RTF 转 DOCX', parent='RTF转MD')
    cmd = [
        str(soffice),
        "--headless",
        "--convert-to", "docx",
        "--outdir", str(output_dir),
        str(rtf_path),
    ]
    try:
        timer.start()
        _run_cmd(cmd)
        timer.stop()
    except (FileNotFoundError, RuntimeError) as e:
        logger.error(f"RTF 转 DOCX 失败: {e}")
        raise RuntimeError(f"无法将 {rtf_path} 转换为 DOCX：{e}") from e

    docx_path = output_dir / (rtf_path.stem + ".docx")
    if not docx_path.exists():
        raise RuntimeError(
            f"LibreOffice 命令执行成功，但未生成预期的 DOCX 文件: {docx_path}"
        )

    return docx_path


def docx_to_md(docx_path: Path, md_path: Path) -> None:
    """
    DOCX 转换为 MD
    """
    pandoc = _check_tool_available("pandoc")
    if not pandoc:
        raise RuntimeError("未找到 Pandoc, 请检查是否已安装并添加到环境变量中")
    if not docx_path.exists():
        raise FileNotFoundError(f"输入的 DOCX 文件不存在: {docx_path}")

    md_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"正在将 DOCX 转换为 MD，文件位置: {docx_path}")
    timer = Timer('DOCX 转 MD', parent='RTF转MD')
    cmd = [
        str(pandoc),
        str(docx_path),
        "-f", "docx",
        "-t", "markdown",
        "-o", str(md_path),
        "--wrap=none"
    ]

    try:
        timer.start()
        _run_cmd(cmd)
        timer.stop()
    except (FileNotFoundError, RuntimeError) as e:
        logger.error(f"DOCX 转 Markdown 失败: {e}")
        raise RuntimeError(f"无法将 {docx_path} 转换为 Markdown：{e}") from e
    if not md_path.exists():
        raise RuntimeError(f"Pandoc 命令执行成功，但未生成预期的 Markdown 文件: {md_path}")


def _check_tool_available(name: str) -> Optional[Path]:
    p = which(name)
    return Path(p) if p else None


def _run_cmd(cmd: list[str]) -> None:
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("命令执行超时")
    except subprocess.CalledProcessError as e:
        logger.error(f"命令执行失败: {e}")
    if result.returncode != 0:
        raise RuntimeError(
            f"Command 命令执行失败:\n{' '.join(cmd)}\n\nSTDERR:\n{result.stderr[:500]}"
        )


# ===简化 Markdown部分===

def simplify_markdown_table(md_content):
    """
    将复杂的ASCII表格转换为简洁的Markdown表格
    """
    lines = md_content.split('\n')
    simplified_lines = []
    in_table = False
    table_rows = []

    for line in lines:
        # 检测表格边框线（包含+和-的行）
        if re.match(r'^[\+\-\|: ]+$', line):
            if '+' in line:  # 表格边框
                if table_rows:  # 处理已收集的表格行
                    simplified_lines.extend(convert_table_rows(table_rows))
                    table_rows = []
                continue
            elif '|' in line and '---' in line:  # 标准Markdown表格分隔线
                simplified_lines.append(line)
                continue
        elif '|' in line:  # 表格数据行
            # 清理多余的空白
            clean_line = '|' + '|'.join(
                _clean_cell(cell) for cell in line.strip('|').split('|')
            ) + '|'
            table_rows.append(clean_line)
        else:
            if table_rows:
                simplified_lines.extend(convert_table_rows(table_rows))
                table_rows = []
            simplified_lines.append(line)

    if table_rows:
        simplified_lines.extend(convert_table_rows(table_rows))

    return '\n'.join(simplified_lines)


def _clean_cell(text: str) -> str:
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\(\s+', '(', text)
    text = re.sub(r'\s+\)', ')', text)
    return text

def convert_table_rows(rows):
    """
    将收集的表格行转换为标准格式
    """
    if len(rows) < 2:
        return rows

    # 确定列数
    col_count = rows[0].count('|') - 1

    # 创建表头分隔线
    separator = '|' + ' | '.join(['---'] * col_count) + '|'

    return [rows[0], separator] + rows[1:]


# ===拆分为多个表部分===
def split_markdown_by_tables(input_path: Path, output_directory: Path) -> List[Path]:
    lines = input_path.read_text(encoding="utf-8").splitlines(True)
    base_name = input_path.stem.rstrip('_clean')
    output_directory.mkdir(parents=True, exist_ok=True)

    current_header_lines: List[str] = []
    current_table_lines: List[str] = []
    table_count = 0
    generated: List[Path] = []

    for line in lines:
        if _is_table_line(line):
            current_table_lines.append(line)
        else:
            if current_table_lines:
                # 表格结束，准备写出
                table_count += 1  # 更新编号
                output_file = _flush_current_table(
                    current_header_lines,
                    current_table_lines,
                    table_count,
                    base_name,
                    output_directory,
                    generated
                )
                generated.append(output_file)
                # 重置状态
                current_header_lines = [line]
                current_table_lines = []
            else:
                current_header_lines.append(line)

    # 处理结尾残留的表格
    if current_table_lines:
        table_count += 1
        output_file = _flush_current_table(
            current_header_lines,
            current_table_lines,
            table_count,
            base_name,
            output_directory,
            generated
        )
        generated.append(output_file)

    return generated
def _flush_current_table(
        current_header_lines: List[str],
        current_table_lines: List[str],
        table_index: int,  # 当前要写第几个表格”的编号
        base_name: str,
        output_directory: Path,
        generated: List[Path]
) -> Path:
    """
    将当前表格内容写入文件，并返回生成的文件路径。
    """
    if not current_table_lines:
        raise ValueError("不应 flush 空表格")
    table_name = _extract_table_name(current_header_lines, current_table_lines)
    output_file = output_directory / f"{base_name}_{table_name}_{table_index}.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        if current_header_lines:
            f.write(''.join(current_header_lines))
        f.write(''.join(current_table_lines))
        logger.info(f"已生成第{table_index}个文件：{output_file}")

    return output_file

def _is_table_line(line: str) -> bool:
    """
    判断是否为表格行
    """
    s = line.strip()
    return s.startswith('|') and s.endswith('|') and len(s) >= 3


def _extract_table_name(header_lines, table_lines) -> str:
    """
    从表头和表体中提取表名
    """
    all_lines = header_lines + table_lines
    for line in all_lines:
        stripped = line.strip()
        if not stripped:
            continue
        if not _is_table_line(stripped):
            text = re.sub(r"^[#\s*]+", "", stripped)
            text = re.sub(r"\*\*", "", text)
            text = re.sub(r'[\\/:*?"<>|]', '_', text)
            return text[:40]
    return "sheet"

# 原RTF处理逻辑
# from service.windows.preprocessing.preprocessing_function.rtf.rtf_to_excel import rtf_to_xlsx_native
# from service.windows.preprocessing.preprocessing_function.excel.excel_pipeline import run as excel_run
#
#
# def run(rtf_path: Path | str, work_dir: Path | str) -> Dict[str, Any]:
#     rtf_path = Path(rtf_path)
#     work_dir = Path(work_dir)
#
#     # 1) RTF -> Excel (native)
#     temp_excel = work_dir / f"{rtf_path.stem}_rtf2excel.xlsx"
#     info = rtf_to_xlsx_native(str(rtf_path), str(temp_excel), sheet_prefix=rtf_path.stem[:20] or "RTFTable")
#
#     # 2) Excel pipeline
#     excel_result = excel_run(temp_excel, work_dir)
#
#     # 3) Merge metadata
#     meta = excel_result.get('metadata', {})
#     steps = list(dict.fromkeys(['rtf_to_excel'] + meta.get('steps', [])))
#     meta['steps'] = steps
#     meta['rtf_intermediate_excel'] = str(temp_excel)
#     excel_result['metadata'] = meta
#
#     return excel_result
