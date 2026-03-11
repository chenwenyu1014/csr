#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Markdown Chunker

按段落优先、句子不被切断的原则，将 Markdown 文本切分为若干块；
支持少量块间重叠，并在每块元信息中保留标题层级路径。

默认参数（可通过调用处传入）：
  - max_chars: 1200
  - min_chars: 600
  - overlap_sentences: 1
  - respect_heading_level: 2

返回：
  chunks: List[Dict]，每项包含：
    - text: 分块内容（包含重叠）
    - title_path: [H1, H2, H3, ...]
    - overlap_from_prev: int（重叠的句子数）
    - meta: 可选元信息（原子块计数等）
"""

from __future__ import annotations

import re
from typing import List, Dict, Any, Tuple


_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")
_CODE_FENCE_RE = re.compile(r"^```+")
_IMAGE_LINE_RE = re.compile(r"!\[[^\]]*\]\([^\)]+\)")
# 自定义区域标签，例如 {{Table_1_Start}} ... {{Table_1_End}}
# 兼容可选空格与连字符，大小写不敏感
_REGION_START_RE = re.compile(r"\{\{\s*([A-Za-z0-9_\-]+)\s*_Start\s*\}\}", re.IGNORECASE)
_REGION_END_TPL = r"\{\{\s*%s\s*_End\s*\}\}"


def _extract_region_segments(md: str) -> List[Dict[str, Any]]:
    """
    在原始 Markdown 上先行提取区域段（{{Name_Start}} ... {{Name_End}}），
    返回按顺序的段列表：[{type: 'region'|'text', 'text': str, 'name'?: str}]
    这样可以保证区域不被后续分块逻辑打断。
    """
    segs: List[Dict[str, Any]] = []
    i = 0
    n = len(md)
    while i < n:
        m = _REGION_START_RE.search(md, i)
        if not m:
            # 余下为普通文本
            tail = md[i:]
            if tail:
                segs.append({"type": "text", "text": tail})
            break
        # 普通文本片段
        if m.start() > i:
            segs.append({"type": "text", "text": md[i:m.start()]})
        name = m.group(1)
        end_re = re.compile(_REGION_END_TPL % re.escape(name), re.IGNORECASE)
        mend = end_re.search(md, m.end())
        if mend:
            segs.append({
                "type": "region",
                "name": name,
                "text": md[m.start(): mend.end()],
            })
            i = mend.end()
        else:
            # 未找到配对 End，当作普通文本处理，避免死循环
            segs.append({"type": "text", "text": md[m.start():]})
            break
    return segs


def _split_sentences(text: str) -> List[str]:
    """按中英文常见句末标点切分，保留标点，不产生空句。"""
    if not text:
        return []
    # 将换行折叠为空格，避免误切
    s = re.sub(r"\s+", " ", text).strip()
    if not s:
        return []
    # 句子边界：中文「。！？；……」与英文「.!?;」
    pattern = r"[^。！？!\?；;…]+(?:[。！？!；;…]+|$)"
    sentences = re.findall(pattern, s)
    out: List[str] = []
    for seg in sentences:
        t = seg.strip()
        if t:
            out.append(t)
    return out


def _is_table_line(line: str) -> bool:
    # 简单启发：包含管道且不是代码围栏
    if _CODE_FENCE_RE.match(line):
        return False
    if "|" in line:
        return True
    return False


def _parse_blocks(md: str) -> List[Dict[str, Any]]:
    """将 Markdown 粗粒度解析为块：heading/paragraph/code/table/image/other。"""
    lines = md.splitlines()
    blocks: List[Dict[str, Any]] = []
    i = 0
    in_code = False
    code_lines: List[str] = []
    para_lines: List[str] = []
    table_lines: List[str] = []

    def flush_paragraph():
        nonlocal para_lines
        if para_lines:
            text = "\n".join(para_lines).strip()
            if text:
                blocks.append({"type": "paragraph", "text": text})
            para_lines = []

    def flush_table():
        nonlocal table_lines
        if table_lines:
            text = "\n".join(table_lines)
            blocks.append({"type": "table", "text": text})
            table_lines = []

    while i < len(lines):
        line = lines[i]

        # 自定义区域标签：原子块提取（避免在 Start/End 之间切分）
        mreg = _REGION_START_RE.search(line)
        if mreg:
            # 刷新现有缓冲
            flush_paragraph()
            flush_table()
            name = mreg.group(1)
            end_pat = re.compile(_REGION_END_TPL % re.escape(name))
            region_lines: List[str] = [line]
            i += 1
            # 收集直到对应的 End
            while i < len(lines):
                region_lines.append(lines[i])
                if end_pat.search(lines[i]):
                    i += 1
                    break
                i += 1
            blocks.append({"type": "region", "name": name, "text": "\n".join(region_lines)})
            continue

        # 代码块
        if _CODE_FENCE_RE.match(line):
            if not in_code:
                # 进入代码块：先冲掉段落与表格
                flush_paragraph()
                flush_table()
                in_code = True
                code_lines = [line]
            else:
                code_lines.append(line)
                blocks.append({"type": "code", "text": "\n".join(code_lines)})
                code_lines = []
                in_code = False
            i += 1
            continue

        if in_code:
            code_lines.append(line)
            i += 1
            continue

        # 空行：结束段落/表
        if not line.strip():
            flush_paragraph()
            flush_table()
            i += 1
            continue

        # 标题
        m = _HEADING_RE.match(line)
        if m:
            flush_paragraph()
            flush_table()
            level = len(m.group(1))
            text = m.group(2).strip()
            blocks.append({"type": "heading", "level": level, "text": text})
            i += 1
            continue

        # 表格行（连续 group）
        if _is_table_line(line):
            flush_paragraph()
            table_lines.append(line)
            i += 1
            # 连续表格行
            while i < len(lines) and _is_table_line(lines[i]) and not _CODE_FENCE_RE.match(lines[i]):
                table_lines.append(lines[i])
                i += 1
            # 下轮循环会触发 flush_table 或其他
            continue

        # 图片（独立行）
        if _IMAGE_LINE_RE.search(line.strip()):
            flush_paragraph()
            flush_table()
            blocks.append({"type": "image", "text": line.strip()})
            i += 1
            continue

        # 普通段落行
        para_lines.append(line)
        i += 1

    # 收尾
    if in_code and code_lines:
        blocks.append({"type": "code", "text": "\n".join(code_lines)})
    flush_paragraph()
    flush_table()

    return blocks


def chunk_markdown(md: str,
                   max_chars: int = 1200,
                   min_chars: int = 600,
                   overlap_sentences: int = 1,
                   respect_heading_level: int = 2) -> List[Dict[str, Any]]:
    """将 Markdown 切分为若干块，返回包含文本与标题路径的列表。"""
    # 先做区域分段，区域作为原子块；其余文本再按块解析
    raw_segs = _extract_region_segments(md)
    blocks: List[Dict[str, Any]] = []
    for seg in raw_segs:
        if seg["type"] == "region":
            blocks.append({"type": "region", "name": seg.get("name"), "text": seg["text"]})
        else:
            blocks.extend(_parse_blocks(seg["text"]))

    title_path: List[str] = []  # 1-based heading level
    chunks: List[Dict[str, Any]] = []

    current_parts: List[str] = []
    current_len: int = 0
    last_overlap: List[str] = []
    last_chunk_sentences: List[str] = []

    def update_title(level: int, text: str):
        nonlocal title_path
        # 标题路径长度至少为 level
        if len(title_path) < level:
            title_path += [""] * (level - len(title_path))
        # 设置当前层级文本，截断更深层
        title_path[level - 1] = text
        del title_path[level:]

    def title_path_copy() -> List[str]:
        return [t for t in title_path if t]

    def finalize_chunk():
        nonlocal current_parts, current_len, last_overlap, last_chunk_sentences
        if not current_parts:
            return
        text = "\n\n".join([p for p in current_parts if p.strip()])
        chunk = {
            "text": text,
            "title_path": title_path_copy(),
            "overlap_from_prev": len(last_overlap),
        }
        chunks.append(chunk)
        # 计算下个块的重叠句子
        if last_chunk_sentences and overlap_sentences > 0:
            last_overlap = last_chunk_sentences[-overlap_sentences:]
        else:
            last_overlap = []
        # 重置
        current_parts = []
        current_len = 0
        last_chunk_sentences = []

    def append_overlap_if_any():
        nonlocal current_parts, current_len
        if last_overlap:
            ov_text = "".join(last_overlap)
            if ov_text:
                current_parts.append(ov_text)
                current_len += len(ov_text)

    for blk in blocks:
        btype = blk.get("type")
        if btype == "heading":
            level = int(blk.get("level") or 1)
            text = blk.get("text") or ""
            update_title(level, text)
            continue

        if btype in ("code", "table", "image", "region"):
            block_text = blk.get("text") or ""
            if btype == "region":
                # 区域块：必须独占一个 chunk，且不与前后合并/叠加
                if current_len > 0:
                    finalize_chunk()
                # 发出独立 chunk（无重叠）
                chunks.append({
                    "text": block_text,
                    "title_path": title_path_copy(),
                    "overlap_from_prev": 0,
                })
                # 清空状态，避免与后续合并
                current_parts = []
                current_len = 0
                last_overlap = []
                last_chunk_sentences = []
                continue
            # 非区域原子块：作为块内单元，必要时在边界处收束
            if not current_parts:
                append_overlap_if_any()
            if current_len > 0 and current_len + len(block_text) > max_chars:
                finalize_chunk()
                append_overlap_if_any()
            current_parts.append(block_text)
            current_len += len(block_text)
            if current_len >= max_chars or len(block_text) >= max_chars:
                finalize_chunk()
            continue

        if btype == "paragraph":
            # 段落视为原子块，避免将段落切断
            para = blk.get("text") or ""
            if not para.strip():
                continue
            if not current_parts:
                append_overlap_if_any()
            # 若当前块已有内容且加入段落会超限，则先收束再加入该段落
            if current_len > 0 and current_len + len(para) > max_chars:
                finalize_chunk()
                append_overlap_if_any()
            # 加入整段；如单段本身超限，允许该块超限以保证段落完整性
            current_parts.append(para)
            current_len += len(para)
            # 收束过长的单段为一块
            if current_len >= max_chars:
                finalize_chunk()
            continue

        # 其他类型：当作段落文本处理
        text = (blk.get("text") or "").strip()
        if not text:
            continue
        if not current_parts:
            append_overlap_if_any()
        if current_len > 0 and current_len + len(text) > max_chars:
            finalize_chunk()
            append_overlap_if_any()
        current_parts.append(text)
        current_len += len(text)

    # 收尾
    finalize_chunk()

    # 如果最后一个块过短并且有前块，尝试同层级合并
    if len(chunks) >= 2 and len(chunks[-1]["text"]) < min_chars:
        tail = chunks.pop()
        prev = chunks[-1]
        merged_text = prev["text"] + "\n\n" + tail["text"]
        prev["text"] = merged_text

    return chunks








