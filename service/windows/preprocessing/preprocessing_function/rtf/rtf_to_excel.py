# # -*- coding: utf-8 -*-
# """
# RTF → Excel （Office 原生端到端；支持复杂合并；表头/脚注稳健）
# 依赖: pywin32
# 环境: Windows + Microsoft Word/Excel
# """
# import os
# import re
# import uuid
# import shutil
# import tempfile
# import time
# import logging
# from typing import Dict
# from threading import Lock
#
# logger = logging.getLogger(__name__)
#
# def _sanitize_sheet_name(name: str) -> str:
#     name = re.sub(r"[\[\]\:\*\?\/\\]", "_", name).strip()
#     return name[:31] or "Sheet"
#
#
# def _nl(s: str) -> str:
#     if s is None:
#         return ""
#     return s.replace("\r\n", "\n").replace("\r", "\n")
#
#
# def _extract_header_footer_texts(doc):
#     headers, footers = [], []
#     n = doc.Tables.Count
#     doc_start = doc.Content.Start
#     doc_end = doc.Content.End
#     starts = [doc.Tables(i).Range.Start for i in range(1, n + 1)]
#     ends = [doc.Tables(i).Range.End for i in range(1, n + 1)]
#     for i in range(n):
#         prev_end = ends[i - 1] if i > 0 else doc_start
#         this_start = starts[i]
#         this_end = ends[i]
#         next_start = starts[i + 1] if i < n - 1 else doc_end
#         head_text = _nl(doc.Range(Start=prev_end, End=this_start).Text or "")
#         foot_text = _nl(doc.Range(Start=this_end, End=next_start).Text or "")
#         headers.append(head_text)
#         footers.append(foot_text)
#     return headers, footers
#
#
# def _first_nonempty_line(text: str) -> str:
#     """
#     取文本中的首个非空行（保留原样），用于Sheet命名兜底。
#     """
#     if text is None:
#         return ""
#     for ln in _nl(text).split("\n"):
#         if ln.strip():
#             return ln.strip()
#     return ""
#
#
# def _insert_1x1_blocks_for_lines(wdoc, text: str, align_center=False) -> int:
#     """
#     将文本按行拆分，每行插入为一个 1x1 表格
#
#     Returns:
#         插入的行数（只计算非空行）
#     """
#     if text is None:
#         text = ""
#
#     lines = _nl(text).split("\n")
#
#     # 过滤掉空行，保留有实际内容的行
#     non_empty_lines = [ln.strip() for ln in lines if ln.strip()]
#
#     if not non_empty_lines:
#         return 0
#
#     cnt = 0
#     for ln in non_empty_lines:
#         try:
#             rng = wdoc.Range(wdoc.Content.End - 1, wdoc.Content.End - 1)
#             tbl = wdoc.Tables.Add(Range=rng, NumRows=1, NumColumns=1)
#             cell = tbl.Cell(1, 1).Range
#             cell.Text = ln
#             # 0: 左对齐, 1: 居中
#             try:
#                 tbl.Range.ParagraphFormat.Alignment = 1 if align_center else 0
#             except Exception:
#                 pass
#             cnt += 1
#             logger.debug(f"已插入标题行 {cnt}: {ln[:50]}...")  # 记录前50个字符
#         except Exception as e:
#             logger.warning(f"插入标题行失败: {ln[:50]}... - {e}")
#
#     if cnt > 0:
#         logger.info(f"✅ 共插入 {cnt} 行标题/脚注")
#
#     return cnt
#
#
# _RTF_TO_XLSX_LOCK = Lock()
#
# def rtf_to_xlsx_native(rtf_path: str, xlsx_out: str, sheet_prefix: str = "RTFTable") -> Dict:
#     """
#     将 RTF 文档中的表格（含表头/脚注）以 Office 原生方式导出为 Excel。
#     返回: { 'tables': int, 'output': xlsx_path }
#     """
#     assert os.path.exists(rtf_path), f"RTF 文件不存在：{rtf_path}"
#     logger.info(f"rtf_to_xlsx_native start | rtf={os.path.abspath(rtf_path)} | out={os.path.abspath(xlsx_out)} | prefix={sheet_prefix}")
#     try:
#         import pythoncom  # type: ignore
#         import win32com.client as win32  # type: ignore
#     except Exception as e:
#         raise RuntimeError("缺少 pywin32 依赖，无法执行 RTF→Excel 原生转换。请安装: pip install pywin32") from e
#     # 延迟导入：避免非Windows环境导入失败
#     from utils.windows_com import safe_dispatch
#
#     # Excel 对齐常量（仅少量用到）
#     XL_LEFT = -4131
#     XL_TOP = -4160
#
#     lock_acquired = False
#     try:
#         _RTF_TO_XLSX_LOCK.acquire()
#         lock_acquired = True
#     except Exception:
#         pass
#
#     pythoncom.CoInitialize()
#     logger.debug("CoInitialize done")
#     word = excel = None
#     src_doc = None
#     dst_wb = None
#     # 支持通过环境变量指定临时目录，避免落在系统盘缓存
#     tmp_base = os.getenv("WINDOWS_TMP_DIR", "").strip()
#     if tmp_base:
#         os.makedirs(tmp_base, exist_ok=True)
#         tmp_root = tempfile.mkdtemp(prefix="rtf2xlsx_native_", dir=tmp_base)
#     else:
#         tmp_root = tempfile.mkdtemp(prefix="rtf2xlsx_native_")
#     logger.debug(f"tmp_root={tmp_root}")
#
#     # Word 保存常量
#     wdDoNotSaveChanges = 0
#     wdFormatFilteredHTML = 10
#     msoEncodingUTF8 = 65001
#
#     try:
#         # 打开 RTF
#         word = safe_dispatch("Word.Application", use_ex=True, logger=logger)
#         word.Visible = False
#         word.DisplayAlerts = 0
#         src_doc = word.Documents.Open(os.path.abspath(rtf_path))
#         try:
#             src_doc.Repaginate()
#         except Exception:
#             pass
#         max_retries = int(os.getenv("RTF_TABLE_DETECT_RETRIES", "3"))
#         interval = float(os.getenv("RTF_TABLE_DETECT_INTERVAL", "2"))
#         tcount = 0
#         for _ in range(max_retries):
#             try:
#                 tcount = int(src_doc.Tables.Count)
#             except Exception:
#                 tcount = 0
#             if tcount > 0:
#                 break
#             try:
#                 pythoncom.PumpWaitingMessages()
#             except Exception:
#                 pass
#             time.sleep(interval)
#         if tcount == 0:
#             raise RuntimeError("文件中未检出任何 Word 表格。请先在 Word 中转换为表格。")
#         logger.info(f"word opened | tables={tcount}")
#
#         headers, footers = _extract_header_footer_texts(src_doc)
#
#         # 目标 Excel 工作簿
#         excel = safe_dispatch("Excel.Application", use_ex=True, logger=logger)
#         excel.Visible = False
#         excel.DisplayAlerts = False
#         dst_wb = excel.Workbooks.Add()
#         while dst_wb.Worksheets.Count > 1:
#             dst_wb.Worksheets(dst_wb.Worksheets.Count).Delete()
#
#         # 逐表处理
#         for i in range(1, tcount + 1):
#             logger.info(f"table {i}/{tcount} begin")
#             tbl = src_doc.Tables(i)
#
#             # 临时 Word 文档：表头 → 原表 → 脚注
#             tdoc = word.Documents.Add()
#             try:
#                 head_lines = _insert_1x1_blocks_for_lines(tdoc, headers[i - 1], align_center=True)
#                 # 使用 FormattedText 直接复制，避免系统剪贴板
#                 dest = tdoc.Range(tdoc.Content.End - 1, tdoc.Content.End - 1)
#                 dest.FormattedText = tbl.Range.FormattedText
#                 foot_lines = _insert_1x1_blocks_for_lines(tdoc, footers[i - 1], align_center=False)
#
#                 # 导出 HTML（UTF-8）
#                 html_dir = os.path.join(tmp_root, f"t{i}")
#                 os.makedirs(html_dir, exist_ok=True)
#                 html_path = os.path.join(html_dir, f"t{i}.html")
#                 tdoc.WebOptions.Encoding = msoEncodingUTF8
#                 tdoc.SaveAs2(os.path.abspath(html_path), FileFormat=wdFormatFilteredHTML)
#                 logger.debug(f"exported html | path={html_path}")
#             finally:
#                 try:
#                     tdoc.Close(SaveChanges=wdDoNotSaveChanges)
#                 finally:
#                     try:
#                         dest = None
#                         tbl = None
#                         tdoc = None
#                     except Exception:
#                         pass
#
#             # Excel 打开 HTML 并复制到目标工作簿
#             hwb = excel.Workbooks.Open(os.path.abspath(html_path))
#             try:
#                 hws = hwb.Worksheets(1)
#                 dws = dst_wb.Worksheets(1) if i == 1 else dst_wb.Worksheets.Add(After=dst_wb.Worksheets(dst_wb.Worksheets.Count))
#
#                 # 优先使用表头的首行作为Sheet名；兜底前缀+序号
#                 header_title = _first_nonempty_line(headers[i - 1])
#                 base_title = header_title if header_title else f"{sheet_prefix}_{i}"
#                 base = _sanitize_sheet_name(base_title)
#                 if not base:
#                     base = _sanitize_sheet_name(f"{sheet_prefix}_{i}")
#                 name, k = base, 1
#                 while True:
#                     try:
#                         dws.Name = name
#                         break
#                     except Exception:
#                         k += 1
#                         name = _sanitize_sheet_name(f"{base}_{k}")
#
#                 # 避免使用系统剪贴板，直接复制到目标区域
#                 hws.UsedRange.Copy(Destination=dws.Range("A1"))
#                 try:
#                     excel.CutCopyMode = False
#                 except Exception:
#                     pass
#
#                 # 基础格式
#                 dws.Cells.WrapText = True
#                 dws.Cells.HorizontalAlignment = XL_LEFT
#                 dws.Cells.VerticalAlignment = XL_TOP
#
#                 # 合并表头/脚注行到整表宽度
#                 used = dws.UsedRange
#                 total_rows = used.Rows.Count
#                 total_cols = used.Columns.Count
#                 logger.debug(f"sheet ready | name={dws.Name} | rows={total_rows} | cols={total_cols} | head_merge={head_lines} | foot_merge={foot_lines}")
#                 r = 1
#                 for _ in range(head_lines):
#                     dws.Range(dws.Cells(r, 1), dws.Cells(r, total_cols)).Merge()
#                     r += 1
#                 if foot_lines > 0:
#                     start_foot = total_rows - foot_lines + 1
#                     for rr in range(start_foot, total_rows + 1):
#                         dws.Range(dws.Cells(rr, 1), dws.Cells(rr, total_cols)).Merge()
#
#                 dws.UsedRange.Columns.AutoFit()
#                 dws.UsedRange.Rows.AutoFit()
#             finally:
#                 try:
#                     hwb.Close(SaveChanges=False)
#                 finally:
#                     try:
#                         hws = None
#                         dws = None
#                         hwb = None
#                         import pythoncom as _pc
#                         _pc.CoFreeUnusedLibraries()
#                         import gc as _gc
#                         _gc.collect()
#                         logger.debug("excel per-table cleanup done")
#                     except Exception:
#                         pass
#             logger.info(f"table {i}/{tcount} end")
#
#         os.makedirs(os.path.dirname(os.path.abspath(xlsx_out)) or ".", exist_ok=True)
#         dst_wb.SaveAs(os.path.abspath(xlsx_out))
#         logger.info(f"saved xlsx | path={os.path.abspath(xlsx_out)}")
#         return {"tables": tcount, "output": os.path.abspath(xlsx_out)}
#
#     finally:
#         try:
#             if src_doc is not None:
#                 src_doc.Close(SaveChanges=0)
#         except Exception:
#             pass
#         try:
#             if word is not None:
#                 word.Quit()
#         except Exception:
#             pass
#         logger.debug("word quit done, sleep 0.2s")
#         time.sleep(0.2)
#         try:
#             if dst_wb is not None:
#                 dst_wb.Close(SaveChanges=True)
#         except Exception:
#             pass
#         try:
#             if excel is not None:
#                 excel.Quit()
#         except Exception:
#             pass
#         logger.debug("excel quit done, sleep 0.2s")
#         time.sleep(0.2)
#         try:
#             shutil.rmtree(tmp_root, ignore_errors=True)
#         except Exception:
#             pass
#         logger.debug("tmp_root removed")
#         try:
#             import pythoncom  # type: ignore
#             pythoncom.CoUninitialize()
#         except Exception:
#             pass
#         logger.debug("CoUninitialize done")
#         try:
#             # 释放锁并触发 GC，帮助尽快清理 COM 引用
#             if lock_acquired:
#                 _RTF_TO_XLSX_LOCK.release()
#         except Exception:
#             pass
#         try:
#             src_doc = None
#             dst_wb = None
#             word = None
#             excel = None
#             import gc
#             gc.collect()
#         except Exception:
#             pass
#         logger.info("rtf_to_xlsx_native end")
