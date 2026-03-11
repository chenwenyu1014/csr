#!/usr/bin/env python3
"""
统一的输出管理工具

职责：
- 统一确定输出根目录
- 提供便捷的保存方法（JSON/Text）
- 生成规范化的文件路径（每次运行一个汇总文件）
"""

from __future__ import annotations

import os
import json
import time
from pathlib import Path
from typing import Any, Optional

from utils.paths import ensure_parent
from utils.context_manager import get_current_output_dir


def generate_timestamp() -> str:
    """返回 YYYYMMDD_HHMMSS 格式的时间戳。"""
    return time.strftime("%Y%m%d_%H%M%S")


def get_output_root(config_output_dir: Optional[str] = None) -> Path:
    """统一获取输出根目录（使用线程安全的方式）。

    优先级：
    1) 显式传入的配置值
    2) 线程本地存储 / 环境变量 CURRENT_OUTPUT_DIR
    3) 默认 "output"
    """
    root = (config_output_dir or get_current_output_dir(default="output")).strip()
    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_outputs_dir(output_root: Path) -> Path:
    """返回 outputs 子目录并确保存在。"""
    outputs_dir = output_root / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    return outputs_dir


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


def build_run_output_path(output_root: Path, timestamp: Optional[str] = None) -> Path:
    """构建每次运行的汇总输出文件路径（JSON）。"""
    ts = timestamp or generate_timestamp()
    return get_outputs_dir(output_root) / f"run_output_{ts}.json"


def build_run_dir(output_root: Path, timestamp: Optional[str] = None) -> Path:
    """构建并返回按时间分层的单次运行目录路径：
    output/runs/YYYYMMDD/HHMMSS/
    """
    ts = timestamp or generate_timestamp()
    # 解析时间戳为日期与时间两段
    if "_" in ts:
        date_part, time_part = ts.split("_", 1)
    else:
        date_part, time_part = ts[:8], ts[8:]
    run_dir = output_root / "runs" / date_part / time_part
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def ensure_run_subdirs(run_dir: Path) -> dict:
    """确保运行目录下常用子目录存在，返回路径字典。"""
    subdirs = {
        "prompts": run_dir / "prompts",
        "outputs": run_dir / "outputs",
        "logs": run_dir / "logs",
        "steps": run_dir / "steps",
        "debug": run_dir / "debug",
    }
    for p in subdirs.values():
        p.mkdir(parents=True, exist_ok=True)
    return subdirs


