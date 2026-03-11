# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

# Reuse robust implementations from FileProcessor to avoid duplication
from service.windows.preprocessing.file_processor import FileProcessor


def run(pdf_path: Path | str, work_dir: Path | str, scanned: bool = False) -> Dict[str, Any]:
    fp = FileProcessor()
    pdf_path = Path(pdf_path)
    work_dir = Path(work_dir)
    # Both normal and scanned currently go through the same robust path in FileProcessor
    return fp._pdf_to_markdown_direct(pdf_path, work_dir)  # type: ignore
