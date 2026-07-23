#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Windows Bridge Service (FastAPI)

端点：
- GET  /healthz                                  健康检查
- GET  /version                                  版本信息
- POST /api/v1/rtf/insert_head_section_break     为RTF在文首插入“下一页分节符”，返回处理后的RTF

实现策略（优先级）：
1) Spire.Doc：加载RTF -> 文首插入空段落 -> 该段后插入 SectionBreakType.NewPage -> 返回RTF
2) Word COM：打开 -> 在Range(0,0)插入 wdSectionBreakNextPage(2) -> 保存RTF -> 返回
3) 失败回退：返回原始RTF
"""

from __future__ import annotations  # 兼容前向引用的类型注解

import os  # 标准库：环境变量与路径
import sys  # 标准库：系统路径
import tempfile  # 标准库：临时目录/文件
import shutil  # 标准库：文件复制
import threading
from contextlib import contextmanager

# 无论从哪里启动，都能正确导入
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
import uuid  # 标准库：请求ID/随机名
import logging  # 标准库：日志记录
import time  # 标准库：耗时统计
import json  # 标准库：JSON 读写
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import JSONResponse
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# 预加载环境变量，确保 setup_logging 能读取到 ENVIRONMENT 配置
try:
    _env_path = find_dotenv(usecwd=True)
    if not _env_path:
        _env_path = str(Path(__file__).resolve().parents[2] / ".env")
    load_dotenv(_env_path, override=False, encoding="utf-8")
except Exception:
    pass

# 使用统一日志配置（控制台 + 文件双输出）
from utils.logging_config import setup_logging 
setup_logging(service_name="Bridge")

# 设置本模块的logger
logger = logging.getLogger(__name__)
app = FastAPI(title="Windows Bridge Service", version="1.0.0")
# ============================================================
# Windows 串行任务锁（防止 win32/Office COM 并发导致崩溃）
#
# FastAPI 的同步 def 会在线程池中并发执行；但 Word/Excel COM
# 在同一进程内并发非常脆弱，因此这里对“会触发 Office/COM 的路由”
# 统一做全局串行化。
#
# 环境变量：
# - WINDOWS_BRIDGE_SERIAL_MODE: "wait"(默认) | "reject"
# - WINDOWS_BRIDGE_SERIAL_TIMEOUT: 秒；0/空=无限等待（仅 wait 模式有效）
# - WINDOWS_BRIDGE_SERIAL_IPC_LOCK: "1"(默认) 开启跨进程文件锁；"0" 关闭
# - WINDOWS_BRIDGE_SERIAL_LOCK_FILE: 锁文件路径（默认 AAA/.windows_bridge.lock）
# ============================================================

from utils.windows_com import _COM_SERIAL_LOCK as _WIN_BRIDGE_TASK_LOCK


def _get_request_id_from_request(request: Request) -> str:
    try:
        rid = request.headers.get("X-Request-Id") or request.headers.get("x-request-id")
        return (rid or "-").strip() or "-"
    except Exception:
        return "-"


@contextmanager
def _windows_serial_task_guard(task_name: str, request: Request):
    """
    串行化 Windows 端重任务（尤其是 Word/Excel COM）。
    - wait 模式：排队等待锁
    - reject 模式：锁被占用则直接返回 429
    """
    mode = (os.getenv("WINDOWS_BRIDGE_SERIAL_MODE", "wait") or "wait").strip().lower()
    timeout_raw = (os.getenv("WINDOWS_BRIDGE_SERIAL_TIMEOUT", "0") or "0").strip()
    try:
        timeout_s = float(timeout_raw)
    except Exception:
        timeout_s = 0.0

    rid = _get_request_id_from_request(request)

    # 1) 线程级互斥（同进程内串行）
    started_wait = time.perf_counter()
    acquired = False
    if mode == "reject":
        acquired = _WIN_BRIDGE_TASK_LOCK.acquire(blocking=False)
    else:
        if timeout_s and timeout_s > 0:
            acquired = _WIN_BRIDGE_TASK_LOCK.acquire(timeout=timeout_s)
        else:
            _WIN_BRIDGE_TASK_LOCK.acquire()
            acquired = True

    if not acquired:
        raise HTTPException(
            status_code=429,
            detail=f"windows-bridge busy: another task is running (task={task_name}, rid={rid})",
        )

    wait_ms = int((time.perf_counter() - started_wait) * 1000)
    if wait_ms >= 50:
        logger.info(f"⏳ 等待Windows串行锁 {wait_ms}ms (task={task_name}, rid={rid})")

    # 2) 可选：跨进程文件锁（防止误用多 worker / 多实例）
    ipc_enabled = (os.getenv("WINDOWS_BRIDGE_SERIAL_IPC_LOCK", "1") or "1").strip()
    lock_f = None
    try:
        if ipc_enabled not in ("0", "false", "False", "no", "NO"):
            lock_path = (os.getenv("WINDOWS_BRIDGE_SERIAL_LOCK_FILE", "AAA/.windows_bridge.lock") or "").strip()
            if not lock_path:
                lock_path = "AAA/.windows_bridge.lock"
            lp = Path(lock_path)
            try:
                lp.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

            try:
                import msvcrt  # Windows only
                lock_f = open(lp, "a+b")
                try:
                    # 确保文件至少有 1 字节，并将锁定区间固定在 offset=0
                    lock_f.seek(0, os.SEEK_END)
                    if lock_f.tell() == 0:
                        lock_f.write(b"\0")
                        lock_f.flush()
                    lock_f.seek(0)
                except Exception:
                    try:
                        lock_f.seek(0)
                    except Exception:
                        pass
                # 锁 1 字节区间；阻塞等待即可（线程锁已经串行，跨进程才会竞争）
                msvcrt.locking(lock_f.fileno(), msvcrt.LK_LOCK, 1)
            except Exception:
                # 文件锁为 best-effort：失败不阻断业务，但会失去跨进程保护
                try:
                    if lock_f:
                        lock_f.close()
                except Exception:
                    pass
                lock_f = None

        yield
    finally:
        # 释放文件锁
        if lock_f is not None:
            try:
                import msvcrt  # type: ignore
                try:
                    lock_f.seek(0)
                except Exception:
                    pass
                try:
                    msvcrt.locking(lock_f.fileno(), msvcrt.LK_UNLCK, 1)
                except Exception:
                    pass
            finally:
                try:
                    lock_f.close()
                except Exception:
                    pass
        # 释放线程锁
        try:
            _WIN_BRIDGE_TASK_LOCK.release()
        except Exception:
            pass

# ========== Content Control插入器（完整版，支持横竖方向检测和分节符）==========

# Word常量
wdCollapseEnd = 0
wdCollapseStart = 1
wdSectionBreakNextPage = 2
wdOrientLandscape = 1
wdOrientPortrait = 0


# ========== 导入核心插入模块 ==========

from pathlib import Path
from service.windows.insertion.word_control_content_inserter import WordControlContentInserter

# 异步任务管理
task_storage: Dict[str, Dict[str, Any]] = {}
task_results: Dict[str, Any] = {}


def _auth_ok(request: Request) -> bool:
    token = (os.getenv("WINDOWS_BRIDGE_TOKEN") or "").strip()
    if not token:
        return True
    auth = request.headers.get("Authorization") or request.headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        return False
    return auth.split(" ", 1)[1].strip() == token


# 请求链路ID上下文（独立于主服务）
try:
    from contextvars import ContextVar
    request_id_ctx = ContextVar("request_id", default="-")
except Exception:
    request_id_ctx = None  # type: ignore


@app.middleware("http")
async def _with_request_id(request: Request, call_next):
    rid = request.headers.get("X-Request-Id") or ("req_" + uuid.uuid4().hex)
    token = None
    if request_id_ctx is not None:
        try:
            token = request_id_ctx.set(rid)
        except Exception:
            token = None
    started = time.perf_counter()
    response = None
    try:
        response = await call_next(request)
        return response
    finally:
        try:
            dur_ms = int((time.perf_counter() - started) * 1000)
            status = getattr(response, "status_code", 0) if response is not None else 0
            ua = request.headers.get("user-agent") or request.headers.get("User-Agent")
            ref = request.headers.get("referer") or request.headers.get("Referer")
            clen = request.headers.get("content-length") or request.headers.get("Content-Length")
            try:
                clen_val = int(clen) if clen else None
            except Exception:
                clen_val = None
            route_path = None
            try:
                route = request.scope.get("route")
                route_path = getattr(route, "path", None)
            except Exception:
                route_path = None
            logging.getLogger("bridge.access").info(
                "request.done",
                extra={
                    "event": "request.done",
                    "path": request.url.path if hasattr(request, "url") else None,
                    "route": route_path,
                    "method": getattr(request, "method", None),
                    "status": status,
                    "duration_ms": dur_ms,
                    "client": getattr(getattr(request, "client", None), "host", None),
                    "remote_port": getattr(getattr(request, "client", None), "port", None),
                    "user_agent": ua,
                    "referer": ref,
                    "request_size": clen_val,
                }
            )
        except Exception:
            pass
        try:
            if response is not None:
                response.headers["X-Request-Id"] = rid
        except Exception:
            pass
        if token is not None:
            try:
                request_id_ctx.reset(token)  # type: ignore
            except Exception:
                pass

@app.get("/healthz")
def healthz() -> JSONResponse:
    info = {
        "spire_available": _probe_spire_available(),
        "win32_available": _probe_win32_available(),
    }
    return JSONResponse({"status": "ok", "info": info})


@app.get("/version")
def version() -> JSONResponse:
    return JSONResponse({"service": "windows-bridge", "version": "1.0.0"})


def _probe_spire_available() -> bool:
    try:
        import spire.doc  # type: ignore
        return True
    except Exception:
        return False


def _probe_win32_available() -> bool:
    try:
        import win32com.client  # type: ignore
        return True
    except Exception:
        return False


@app.post("/ky/sys/ai/insert_direct")
def content_control_insert_direct(
    request: Request,
    template_file: str = Form(...),  # 模板文件路径（相对于AAA）
    data_json: str = Form(...)       # JSON数据
):
    """
    直接从共享文件夹插入内容（无需zip）
    
    注意：此函数故意使用同步def而非async def，因为内部有阻塞的COM操作。
    FastAPI会自动在线程池中执行同步函数，避免阻塞事件循环。
    
    Args:
        template_file: 模板文件路径，相对于AAA目录
        data_json: JSON字符串，包含generation_results和resource_mappings
    """
    if not _auth_ok(request):
        raise HTTPException(status_code=401, detail="unauthorized")
    
    with _windows_serial_task_guard("insert_direct", request):
        return _content_control_insert_direct_impl(template_file=template_file, data_json=data_json, request=request)


def _content_control_insert_direct_impl(*, template_file: str, data_json: str, request: Request):
    logger.info("=" * 70)
    logger.info("Content Control插入服务（直接模式）")
    logger.info("=" * 70)
    
    try:
        # ✅ 显示原始参数（用于调试）
        logger.info(f"📄 接收到的template_file: {template_file}")
        logger.info(f"📄 接收到的data_json长度: {len(data_json)} 字符")
        # logger.info(f"📄 data_json前500字符: {data_json[:500]}")
        
        # 解析JSON数据
        data = json.loads(data_json)
        
        # ✅ 兼容两种字段名：generation_results（新）和 paragraphs（旧）
        generation_results = data.get('generation_results') or data.get('paragraphs', [])
        
        # ✅ 标准化字段名：paragraph_id -> control_title
        #    同时确保有generated_content字段
        for item in generation_results:
            if 'paragraph_id' in item and 'control_title' not in item:
                item['control_title'] = item['paragraph_id']
            # 确保有generated_content字段（兼容content字段）
            if 'generated_content' not in item and 'content' in item:
                item['generated_content'] = item['content']
            # 确保有status字段
            if 'status' not in item:
                item['status'] = 'success'

            # 兼容 generated_content 为 dict 对象的情况（表格 JSON）
            # 如果是 dict 类型，转为 JSON 字符串，以便下游 _is_table_json() 检测
            gc = item.get('generated_content')
            if isinstance(gc, dict):
                item['generated_content'] = json.dumps(gc, ensure_ascii=False)
                logger.info(f"  🔄 generated_content 从 dict 转为 JSON 字符串（段落: {item.get('control_title')}）")

            # 清理段落级 resource_mappings 的路径
            paragraph_mappings = item.get('resource_mappings', {})
            if paragraph_mappings:
                cleaned_mappings = {}
                for placeholder, mapping in paragraph_mappings.items():
                    rel_path = mapping.get('path', '') if isinstance(mapping, dict) else str(mapping)

                    # 统一路径分隔符
                    clean_path = rel_path.replace("\\", "/")

                    # 处理混合路径格式（如 /home/xxx/AAA/project_data/...）
                    aaa_idx = clean_path.lower().find("/aaa/")
                    if aaa_idx != -1:
                        clean_path = clean_path[aaa_idx + 5:]  # 跳过 /AAA/
                    elif clean_path.lower().startswith("aaa/"):
                        clean_path = clean_path[4:]  # 跳过 AAA/
                    elif clean_path.startswith("/"):
                        clean_path = clean_path[1:]  # 去掉开头的 /

                    # 转换为本地相对路径
                    resource_path = f"../AAA/{clean_path}"
                    if not Path(resource_path).exists():
                        resource_path = f"AAA/{clean_path}"

                    # 保留原有映射结构
                    if isinstance(mapping, dict):
                        cleaned_mappings[placeholder] = {**mapping, 'path': resource_path}
                    else:
                        cleaned_mappings[placeholder] = {'path': resource_path}

                    logger.info(f"  段落资源路径清理: {placeholder} -> {Path(resource_path).name}")

                item['resource_mappings'] = cleaned_mappings

        # ✅ 清理模板路径：确保是相对路径
        # 先去除首尾的空白和引号
        clean_template = template_file.strip().strip('"').strip("'")
        
        # 处理各种路径前缀
        if clean_template.startswith('//'):
            # 处理 //project_data/... 格式，转换为 project_data/...
            clean_template = clean_template[2:]
        elif clean_template.startswith('/AAA/'):
            clean_template = clean_template[5:]
        elif clean_template.startswith('AAA/'):
            clean_template = clean_template[4:]
        elif clean_template.startswith('/'):
            clean_template = clean_template[1:]
        
        # 记录清理后的路径
        logger.info(f"📁 清理后的模板路径: {clean_template}")
        
        # 直接使用相对路径（相对于AAA）
        # 从父目录查找AAA（因为在windows_bridge目录下运行）
        template_path = f"../AAA/{clean_template}"
        logger.info(f"📁 尝试路径1: {template_path} (存在: {Path(template_path).exists()})")
        
        if not Path(template_path).exists():
            # 也尝试当前目录
            template_path = f"AAA/{clean_template}"
            logger.info(f"📁 尝试路径2: {template_path} (存在: {Path(template_path).exists()})")
            
            if not Path(template_path).exists():
                # 也尝试直接路径（如果已经是完整路径）
                template_path_direct = clean_template
                logger.info(f"📁 尝试路径3: {template_path_direct} (存在: {Path(template_path_direct).exists()})")
                
                if not Path(template_path_direct).exists():
                    error_msg = f"模板文件不存在。尝试的路径:\n  1. ../AAA/{clean_template}\n  2. AAA/{clean_template}\n  3. {clean_template}\n原始template_file参数: {template_file}"
                    logger.error(error_msg)
                    raise FileNotFoundError(error_msg)
                else:
                    template_path = template_path_direct
        
        logger.info(f"模板文件: {template_path}")
        logger.info(f"段落数: {len(generation_results)}")
        
        # 输出文件路径（相对路径）
        output_dir = "../AAA/output"
        if not Path("../AAA").exists():
            output_dir = "AAA/output"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        import time
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = f"{output_dir}/result_{timestamp}.docx"
        
        logger.info(f"输出文件: {output_file}")
        
        # 执行插入
        inserter = WordControlContentInserter()
        result = inserter.insert_to_template(
            template_file=template_path,
            generation_results=generation_results,
            output_file=output_file
        )
        
        if result.success:
            logger.info("✅ 插入成功")
            logger.info(f"   - 插入控件: {len(result.inserted_controls)} 个")
            logger.info(f"   - 插入资源: {len(result.inserted_resources)} 个")
            logger.info(f"   - 输出: {output_file}")
            
            # 返回文件路径
            return JSONResponse({
                "success": True,
                "output_file": output_file,
                "inserted_controls": len(result.inserted_controls),
                "inserted_resources": len(result.inserted_resources)
            })
        else:
            raise HTTPException(status_code=500, detail=f"插入失败: {result.error}")

    except HTTPException as e:
        logger.error(f"❌ 处理失败: {e.detail}")
        raise e
    except FileNotFoundError as e:
        error_msg = f"文件不存在: {str(e)}"
        logger.error(f"❌ {error_msg}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=error_msg)
    except Exception as e:
        import traceback
        traceback.print_exc()
        error_msg = str(e) if str(e) else f"未知错误: {type(e).__name__}"
        logger.error(f"❌ 处理失败: {error_msg}")
        import traceback
        tb_str = traceback.format_exc()
        logger.error(f"❌ 堆栈:\n{tb_str}")
        raise HTTPException(status_code=500, detail=error_msg)


def _snapshot_word_pids() -> set:
    """获取当前所有 WINWORD.EXE 的 PID。

    用于结束时只 kill 本次新增的进程，不误杀用户原本开着的 Word。
    """
    try:
        import subprocess
        out = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq WINWORD.EXE", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=10,
        ).stdout
        pids = set()
        for line in out.splitlines():
            parts = [p.strip().strip('"') for p in line.split(",")]
            if len(parts) >= 2 and parts[0].upper().startswith("WINWORD"):
                try:
                    pids.add(int(parts[1]))
                except ValueError:
                    pass
        return pids
    except Exception:
        return set()


def _kill_word_pids(pids):
    """taskkill /F 指定 PID（连子进程），单个失败不影响其他。"""
    import subprocess
    for pid in pids:
        try:
            subprocess.run(["taskkill", "/F", "/PID", str(pid), "/T"],
                           capture_output=True, timeout=10)
            logger.warning(f"强制终止残留的 WINWORD 进程 pid={pid}")
        except Exception as e:
            logger.warning(f"终止 WINWORD 进程 pid={pid} 失败: {e}")


def _ensure_word_quit(word, *, pre_pids):
    """确保 Word 进程被关闭：先正常 Quit()，失败则按 PID 差集兜底 kill。

    无论正常退出还是异常退出都调用，保证不残留 WINWORD.EXE。
    pre_pids 为进入 COM 前的 WINWORD PID 快照，只杀本次新增的。
    """
    if word is not None:
        try:
            word.Quit()
        except Exception as e:
            logger.warning(f"word.Quit() 失败, 将退回 taskkill: {e}")
    try:
        leftover = _snapshot_word_pids() - pre_pids
        if leftover:
            _kill_word_pids(leftover)
    except Exception as e:
        logger.warning(f"残留的 WINWORD 清理失败: {e}")


# ---------- .doc -> .docx（Word COM 无损转换） ----------

@app.post("/api/v1/document/doc-to-docx")
def doc_to_docx(
    request: Request,
    doc_path: str = Form(..., description=".doc 或 .docx 文件路径（相对于AAA目录）"),
):
    """
    通过 Word COM 将文档重存为过渡格式(Transitional).docx（无损保真）。

    - .doc 输入：在源文件同目录产出同名 .docx（源 .doc 保留）。
    - .docx 输入：原地规范化（严格格式(Strict)OOXML → 过渡格式）。

    Args:
        doc_path: .doc 或 .docx 文件路径，相对于 AAA 目录

    Returns:
        JSON: {"success": True, "docx_path": "<相对AAA路径>", "docx_abs_path": "<绝对路径>"}
              失败时 {"success": False, "error": "..."}
    """
    if not _auth_ok(request):
        raise HTTPException(status_code=401, detail="unauthorized")

    with _windows_serial_task_guard("doc_to_docx", request):
        return _doc_to_docx_impl(request=request, doc_path=doc_path)


def _doc_to_docx_impl(*, request: Request, doc_path: str):
    logger.info("=" * 70)
    logger.info(".doc -> .docx 转换（Word COM）")
    logger.info("=" * 70)

    result: Dict[str, Any] = {"success": False, "error": None}

    try:
        full_path = _resolve_aaa_path(doc_path)

        suffix = full_path.suffix.lower()
        if suffix not in (".doc", ".docx"):
            raise ValueError(f"输入文件不是 .doc/.docx: {full_path}")

        # .doc 输入：同目录产出同名 .docx（源 .doc 保留）
        # .docx 输入：原地规范化（严格格式 → 过渡格式），先存临时文件再覆盖
        in_place = (suffix == ".docx")
        if in_place:
            out_path = full_path
            tmp_path = full_path.with_name(f"{full_path.stem}_transitional_tmp.docx")
            logger.info(f"📄 输入 .docx（原地规范化 严格→过渡）: {full_path}")
        else:
            out_path = full_path.with_suffix(".docx")
            tmp_path = None
            logger.info(f"📄 输入 .doc（转为 .docx文档）: {full_path}")

        # 实际 SaveAs2 的目标：.docx 原地场景先写临时文件，避免与正在打开的源文件冲突
        save_path = tmp_path if tmp_path is not None else out_path
        # 残留的临时文件先清掉，避免 SaveAs2 受干扰
        if tmp_path is not None and tmp_path.exists():
            try:
                os.remove(tmp_path)
            except Exception:
                pass

        try:
            import win32com.client  # type: ignore
        except Exception as e:
            raise RuntimeError(f"win32com 不可用: {e}")

        from utils.windows_com import safe_dispatch

        word = None
        doc = None
        com_inited = False
        pre_pids = set()
        try:
            import pythoncom  # type: ignore
            try:
                pythoncom.CoInitialize()
                com_inited = True
            except Exception:
                com_inited = False

            # Dispatch 前快照 WINWORD PID，用于退出时精确兜底清理（只杀本次新增）
            pre_pids = _snapshot_word_pids()

            word = safe_dispatch("Word.Application", use_ex=False, logger=logger)
            try:
                word.Visible = False
                word.DisplayAlerts = 0
            except Exception:
                pass

            doc = word.Documents.Open(str(full_path), ReadOnly=False)

            # 16 = wdFormatXMLDocument (.docx, 过渡格式)；优先 SaveAs2，旧版 Word 回退 SaveAs
            try:
                doc.SaveAs2(str(save_path), FileFormat=16)
            except Exception:
                doc.SaveAs(str(save_path), FileFormat=16)

            # 先关闭文档释放句柄，再做原地覆盖（.docx 规范化场景）
            try:
                doc.Close(SaveChanges=0)
                doc = None
            except Exception:
                pass

            if tmp_path is not None:
                # 临时文件就绪，原位覆盖源文件
                os.replace(tmp_path, full_path)
                logger.info(f"✅ 规范化成功(原地覆盖): {full_path}")
            else:
                logger.info(f"✅ 转换成功: {out_path}")

            # 返回相对 AAA 的路径，便于调用方定位
            try:
                rel = str(out_path)
                marker = os.sep + "AAA" + os.sep
                idx = rel.find(marker)
                if idx != -1:
                    rel = rel[idx + len(marker):]
                else:
                    sep_aaa = "AAA" + os.sep
                    if rel.startswith(sep_aaa):
                        rel = rel[len(sep_aaa):]
            except Exception:
                rel = str(out_path)

            result = {
                "success": True,
                "docx_path": rel.replace("\\", "/"),
                "docx_abs_path": str(out_path),
            }
            return result
        finally:
            try:
                if doc is not None:
                    doc.Close(SaveChanges=0)
            except Exception:
                pass
            # 正常/异常退出都保证 Word 被关掉：Quit 失败则 taskkill 兜底
            _ensure_word_quit(word, pre_pids=pre_pids)
            try:
                if com_inited:
                    import pythoncom  # type: ignore
                    pythoncom.CoUninitialize()
            except Exception:
                pass
            # 失败时清理残留的临时文件（仅 .docx 原地规范化场景）
            if tmp_path is not None and tmp_path.exists():
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
    except FileNotFoundError as e:
        logger.error(f"文件不存在: {e}")
        result["error"] = f"文件不存在: {e}"
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f".doc -> .docx 转换失败: {e}", exc_info=True)
        result["error"] = str(e)

    return result


def _resolve_aaa_path(rel_path: str) -> Path:
    """
    将“相对于 AAA 目录”的路径解析为本地绝对路径。

    多 base 试探策略：
    依次尝试 ../AAA、AAA、WINDOWS_AAA_ROOT、cwd/AAA、cwd.parent/AAA。

    Args:
        rel_path: 相对 AAA 的路径，可带 AAA/ /AAA/ // / 等前缀

    Returns:
        解析后的绝对 Path

    Raises:
        FileNotFoundError: 所有候选 base 都不存在该文件
    """
    clean = rel_path.strip().strip('"').strip("'").replace("\\", "/")
    if clean.startswith("//"):
        clean = clean[2:]
    elif clean.startswith("/AAA/"):
        clean = clean[5:]
    elif clean.startswith("AAA/"):
        clean = clean[4:]
    elif clean.startswith("/"):
        clean = clean[1:]

    aaa_root = Path(os.getenv("WINDOWS_AAA_ROOT", "AAA")).absolute()
    base_paths = [
        Path("../AAA").absolute(),
        Path("AAA").absolute(),
        aaa_root,
        Path.cwd() / "AAA",
        Path.cwd().parent / "AAA",
    ]

    tried = []
    for base in base_paths:
        test_path = base / clean
        tried.append(str(test_path))
        if test_path.exists():
            return test_path

    raise FileNotFoundError(
        f"文件不存在: {clean}\n尝试过的路径:\n" + "\n".join(f"  - {p}" for p in tried)
    )


# ========== Linux转发的预处理接口（新增）==========

@app.post("/api/v1/preprocessing/process")
def preprocessing_process(
    request: Request,
    file_path: str = Form(..., description="文件相对路径（相对于AAA/project_data）"),
    folder_path: str = Form(..., description="项目文件夹路径"),
    filename: str = Form(..., description="文件名"),
    file_id: str = Form(None, description="文件ID"),
    force_ocr: bool = Form(False, description="是否强制OCR"),
    extract_regions: bool = Form(True, description="是否提取表格图片"),
    extract_assets: bool = Form(True, description="是否提取资产"),
    chunking_enabled: bool = Form(True, description="是否启用分块"),
    chunking_mode: str = Form("heading", description="分块模式"),
):
    """
    Linux转发的预处理接口（同步执行，避免阻塞事件循环）
    处理Word/RTF/Excel等需要Windows环境的文件
    从共享目录AAA读取文件
    """
    if not _auth_ok(request):
        raise HTTPException(status_code=401, detail="unauthorized")

    return _preprocessing_process_impl(
        request=request,
        file_path=file_path,
        folder_path=folder_path,
        filename=filename,
        file_id=file_id,
        force_ocr=force_ocr,
        extract_regions=extract_regions,
        extract_assets=extract_assets,
        chunking_enabled=chunking_enabled,
        chunking_mode=chunking_mode,
    )


def _preprocessing_process_impl(
    *,
    request: Request,
    file_path: str,
    folder_path: str,
    filename: str,
    file_id: str | None,
    force_ocr: bool,
    extract_regions: bool,
    extract_assets: bool,
    chunking_enabled: bool,
    chunking_mode: str,
):
    try:
        logger.info(f"📥 收到预处理请求: {filename}")
        logger.info(f"   文件路径: {file_path}")
        
        # ✅ 延迟导入PreprocessingService（恢复原来的方式）
        from service.windows.preprocessing.service import PreprocessingService

        # ✅ 从共享目录读取文件
        AAA_ROOT = Path(os.getenv("WINDOWS_AAA_ROOT", "AAA"))
        full_file_path = AAA_ROOT / "project_data" / file_path
        
        if not full_file_path.exists():
            raise FileNotFoundError(f"文件不存在: {full_file_path}")
        
        logger.info(f"📁 找到文件: {full_file_path}")
        
        # 构建输出路径（共享目录）
        output_dir = AAA_ROOT / "Preprocessing" / folder_path / Path(filename).stem
        
        logger.info(f"🔄 开始预处理: {filename} → {output_dir}")
    
        # 调用预处理服务
        preprocessing_svc = PreprocessingService()
        extra_info = {"file_id": file_id} if file_id else {}
        
        result = preprocessing_svc.preprocess(
            file_path=full_file_path,
            force_ocr=force_ocr,
            extract_regions=extract_regions,
            extract_assets=extract_assets,
            chunking_enabled=chunking_enabled,
            chunking_mode=chunking_mode,
            output_dir=output_dir,
            extra_info=extra_info
        )
        
        # 构建返回路径（相对于AAA的路径，供Linux访问）
        preprocessed_file = str(result.work_dir / "preprocessed.json") if result.work_dir else None
        chunks_file = result.processing_info.get('structured_chunks_file', None)
        preprocessed_dir = str(result.work_dir) if result.work_dir else None
        
        # 转换为Linux可访问的AAA相对路径
        if preprocessed_file:
            p = Path(preprocessed_file)
            try:
                rel = p.relative_to(AAA_ROOT)
                preprocessed_file = "AAA/" + rel.as_posix()
            except Exception:
                preprocessed_file = p.as_posix()
        if chunks_file:
            p2 = Path(chunks_file)
            try:
                rel2 = p2.relative_to(AAA_ROOT)
                chunks_file = "AAA/" + rel2.as_posix()
            except Exception:
                chunks_file = p2.as_posix()
        if preprocessed_dir:
            pd = Path(preprocessed_dir)
            try:
                rel3 = pd.relative_to(AAA_ROOT)
                preprocessed_dir = "AAA/" + rel3.as_posix()
            except Exception:
                preprocessed_dir = pd.as_posix()
        
        logger.info(f"✅ 预处理成功: {filename}")
        
        return JSONResponse({
            "success": True,
            "id": file_id or "",
            "status": "success",
            "file_name": filename,
            "file_type": result.file_type.value if hasattr(result.file_type, 'value') else str(result.file_type),
            "preprocessed_json": preprocessed_file,
            "preprocessed_dir": preprocessed_dir,
            "chunks_file": chunks_file,
            "regions_count": len(result.regions) if result.regions else 0,
            "processing_method": "windows_server"
        })
        
    except Exception as e:
        logger.error(f"❌ 预处理失败: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "id": file_id or "",
                "status": "fail",
                "file_name": filename,
                "error_message": str(e)
            }
        )


if __name__ == "__main__":
    import uvicorn

    
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8081"))
    
    logger.info("=" * 70)
    logger.info("Windows Bridge Service 启动")
    logger.info(f"监听地址: {host}:{port}")
    logger.info("=" * 70)
    
    # 直接传 app 对象，避免 import path 解析失败
    uvicorn.run(app, host=host, port=port, reload=False, log_level="info")


