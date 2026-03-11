#!/usr/bin/env python3
"""
路径与目录工具
"""

from pathlib import Path
from typing import Union


def ensure_dir(dir_path: Union[str, Path]) -> Path:
    """确保目录存在，返回 Path 对象。"""
    p = Path(dir_path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def ensure_parent(file_path: Union[str, Path]) -> Path:
    """确保文件的父目录存在，返回文件 Path 对象。"""
    fp = Path(file_path)
    if fp.parent:
        fp.parent.mkdir(parents=True, exist_ok=True)
    return fp


