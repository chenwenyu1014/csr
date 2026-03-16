# # pip install pywin32
# import os
# import sys
# import traceback
# import time
# import logging
# import win32com.client as win32
#
# # 配置日志记录
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)
#
# def rtf_to_txt_with_word(
#     in_path: str,
#     out_path: str | None = None,
#     encoding: int = 65001,  # 65001=UTF-8, 936=GBK
#     replace_if_exists: bool = True,
#     visible: bool = False,
# ) -> str:
#     """
#     用 Word 的 SaveAs2，把 RTF 转成 TXT（尽量复刻"另存为txt"的效果）
#     返回 out_path；如果失败会抛异常（不会悄悄失败）。
#
#     安全警告：处理来自不受信任来源的 RTF 文件可能存在风险，
#     因为 Word 可能会执行嵌入的宏或其他潜在的有害内容。
#     """
#     in_path = os.path.abspath(in_path)
#     if not os.path.isfile(in_path):
#         raise FileNotFoundError(f"输入文件不存在: {in_path}")
#
#     if out_path is None:
#         out_path = os.path.splitext(in_path)[0] + ".txt"
#     out_path = os.path.abspath(out_path)
#
#     out_dir = os.path.dirname(out_path)
#     os.makedirs(out_dir, exist_ok=True)
#
#     if os.path.exists(out_path):
#         if replace_if_exists:
#             try:
#                 os.remove(out_path)
#             except PermissionError as e:
#                 raise PermissionError(f"权限不足，无法删除已存在的目标文件: {out_path}") from e
#             except OSError as e:
#                 raise OSError(f"删除已存在的目标文件时发生错误: {out_path}") from e
#         else:
#             raise FileExistsError(f"目标文件已存在: {out_path}")
#
#     # Word 常量
#     wdFormatText = 2              # 纯文本 .txt (本地代码页)
#     wdFormatUnicodeText = 7       # Unicode 文本 .txt (UTF-16LE)
#     wdCRLF = 0                    # Windows 风格换行
#     wdDoNotSaveChanges = 0
#
#     # 有些机器第一次启动 COM 比较慢；用 DispatchEx 更干净
#     from utils.windows_com import safe_dispatch
#     word = safe_dispatch("Word.Application", use_ex=True, logger=logger)
#     word.Visible = visible
#     # 禁用弹窗（例如“文件已存在是否覆盖”、“转换器选项”等）
#     word.DisplayAlerts = 0
#
#     doc = None
#     try:
#         # 关键：把 ReadOnly/ConfirmConversions 关掉，避免弹对话框卡住
#         logger.info(f"打开：{in_path}")
#         doc = word.Documents.Open(
#             in_path,
#             ConfirmConversions=False,
#             ReadOnly=True,
#             AddToRecentFiles=False
#         )
#
#         # 很关键：用“命名参数”调用 SaveAs2，避免位置参数错位
#         logger.info(f"另存为 TXT：{out_path} (Unicode UTF-16)")
#         # 使用 Unicode 文本格式，避免本地代码页导致的乱码
#         doc.SaveAs2(
#             FileName=out_path,
#             FileFormat=wdFormatUnicodeText,
#             LineEnding=wdCRLF,
#             LockComments=False,
#             AddToRecentFiles=False
#         )
#
#         # Word 有时异步写盘，等待并检查文件是否已完全写入
#         timeout = 5  # 最大等待时间（秒）
#         check_interval = 0.1  # 检查间隔（秒）
#         elapsed_time = 0
#
#         while not os.path.exists(out_path) and elapsed_time < timeout:
#             time.sleep(check_interval)
#             elapsed_time += check_interval
#
#         if not os.path.exists(out_path):
#             raise RuntimeError("SaveAs2 执行后未发现输出文件（可能被杀软拦截或权限问题）。")
#
#         logger.info(f"已生成：{out_path}")
#         return out_path
#
#     except Exception as e:
#         # 把 Word 的 COM 异常完整抛出
#         tb = traceback.format_exc()
#         raise RuntimeError(f"RTF→TXT 失败：{e}\n{tb}") from e
#
#     finally:
#         try:
#             if doc is not None:
#                 doc.Close(SaveChanges=wdDoNotSaveChanges)
#         except Exception as e:
#             logging.warning(f"关闭文档时发生错误: {e}")
#         try:
#             word.Quit()
#         except Exception as e:
#             logging.warning(f"退出Word时发生错误: {e}")
#
#
# if __name__ == "__main__":
#     # 用法：python rtf2txt_word.py input.rtf [output.txt]
#     if len(sys.argv) < 2:
#         print("用法: python rtf2txt_word.py input.rtf [output.txt]")
#         sys.exit(1)
#     inp = sys.argv[1]
#     outp = sys.argv[2] if len(sys.argv) >= 3 else None
#     rtf_to_txt_with_word(inp, outp)
