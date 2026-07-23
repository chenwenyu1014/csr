#!/usr/bin/env python3
"""
统一的输出管理工具

职责：
- 提供便捷的保存方法（JSON/Text），保存时自动确保父目录存在
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def ensure_parent(file_path) -> Path:
    """确保文件父目录存在，返回该文件 Path。"""
    fp = Path(file_path)
    fp.parent.mkdir(parents=True, exist_ok=True)
    return fp


def save_json(file_path: Path, data: Any) -> Path:
    """将数据保存为JSON文件，确保父目录存在。"""
    fp = ensure_parent(file_path)
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return fp


def save_text(file_path: Path, text: str) -> Path:
    """将文本保存为文件，确保父目录存在。"""
    fp = ensure_parent(file_path)
    with open(fp, "w", encoding="utf-8") as f:
        f.write(text)
    return fp