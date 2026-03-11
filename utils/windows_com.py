# -*- coding: utf-8 -*-
"""
Windows COM helpers (pywin32)

Goal: make Word/Excel automation more robust by auto-healing common pywin32
`win32com.gen_py` cache corruption / version mismatch issues.

This module is safe to import on non-Windows: it does not import pywin32
until the functions are called.
"""

from __future__ import annotations

import os
import shutil
import threading
from typing import Any, Optional


_GENPY_REPAIR_LOCK = threading.Lock()


def _str_exc(e: BaseException) -> str:
    try:
        return f"{type(e).__name__}: {e}"
    except Exception:
        return type(e).__name__


def _looks_like_genpy_cache_error(e: BaseException) -> bool:
    """
    Detect errors caused by broken `win32com.gen_py` cache, e.g.
    `... has no attribute 'CLSIDToPackageMap'`.
    """
    msg = str(e) or ""
    if "CLSIDToPackageMap" in msg:
        return True
    # Some environments raise different wrapper/load errors but still point to gen_py
    if "win32com.gen_py" in msg or "gen_py" in msg:
        return True
    return False


def repair_win32com_gen_py_cache(*, logger: Optional[Any] = None) -> bool:
    """
    Best-effort cleanup + rebuild of pywin32 generated wrapper cache.

    Returns True if we *attempted* a repair (even if some steps fail),
    False if repair is disabled by env var.
    """
    enabled = str(os.getenv("WIN32COM_AUTO_REPAIR", "1")).strip().lower() not in ("0", "false", "no", "off")
    if not enabled:
        return False

    with _GENPY_REPAIR_LOCK:
        try:
            import win32com.client.gencache as gencache  # type: ignore
        except Exception as e:
            if logger:
                logger.warning(f"WIN32COM auto-repair skipped: cannot import gencache ({_str_exc(e)})")
            return False

        # Ensure cache is writable (some deployments mark it read-only)
        try:
            gencache.is_readonly = False
        except Exception:
            pass

        gen_path = None
        try:
            gen_path = gencache.GetGeneratePath()
        except Exception:
            gen_path = None

        if logger:
            logger.warning(f"WIN32COM auto-repair: rebuilding gencache (gen_path={gen_path!r})")

        # 1) Delete generated cache directory
        if gen_path:
            try:
                shutil.rmtree(gen_path, ignore_errors=True)
            except Exception as e:
                if logger:
                    logger.warning(f"WIN32COM auto-repair: failed to remove gen_py ({_str_exc(e)})")

        # 2) Rebuild gencache index
        try:
            gencache.Rebuild()
        except Exception as e:
            if logger:
                logger.warning(f"WIN32COM auto-repair: gencache.Rebuild failed ({_str_exc(e)})")

        # 3) Hint COM to free unused libraries (optional)
        try:
            import pythoncom  # type: ignore
            pythoncom.CoFreeUnusedLibraries()
        except Exception:
            pass

        return True


def safe_dispatch(
    prog_id: str,
    *,
    use_ex: bool = False,
    logger: Optional[Any] = None,
) -> Any:
    """
    Dispatch a COM automation object with one auto-repair retry if we detect
    a `gen_py` cache problem.

    Notes:
    - Caller is still responsible for pythoncom.CoInitialize/CoUninitialize per thread.
    - `use_ex=True` uses DispatchEx (separate instance).
    """
    import win32com.client as win32  # type: ignore

    dispatch_fn = win32.DispatchEx if use_ex else win32.Dispatch
    try:
        return dispatch_fn(prog_id)
    except Exception as e:
        # Only retry for known gen_py issues
        if not _looks_like_genpy_cache_error(e):
            raise
        if logger:
            logger.warning(f"WIN32COM Dispatch failed ({prog_id}, use_ex={use_ex}): {_str_exc(e)}; attempting auto-repair and retry")
        repair_win32com_gen_py_cache(logger=logger)
        return dispatch_fn(prog_id)


