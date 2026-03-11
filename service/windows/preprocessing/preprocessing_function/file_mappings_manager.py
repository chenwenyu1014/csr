#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件映射管理器
管理预处理文件的映射关系（文件名 → preprocessed.json路径）
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

# 映射文件路径
MAPPING_FILE = Path('AAA/Preprocessing/file_mappings.json')


def _load_mappings() -> Dict[str, Any]:
    """加载映射文件"""
    if MAPPING_FILE.exists():
        try:
            with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载映射文件失败: {e}")
            return _create_empty_mappings()
    else:
        return _create_empty_mappings()


def _create_empty_mappings() -> Dict[str, Any]:
    """创建空的映射结构"""
    return {
        "version": "1.0",
        "last_updated": datetime.now().isoformat(),
        "projects": {}
    }


def _save_mappings(mappings: Dict[str, Any]) -> None:
    """保存映射文件"""
    try:
        MAPPING_FILE.parent.mkdir(parents=True, exist_ok=True)
        mappings['last_updated'] = datetime.now().isoformat()
        
        with open(MAPPING_FILE, 'w', encoding='utf-8') as f:
            json.dump(mappings, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✓ 映射文件已保存: {MAPPING_FILE}")
    except Exception as e:
        logger.error(f"保存映射文件失败: {e}")


def add_file_mapping(
    project_name: str,
    file_name: str,
    file_id: str,
    preprocessed_json_path: str,
    preprocessed_dir: str,
    status: str = "success",
    error_message: str = None
) -> None:
    """
    添加或更新文件映射
    
    Args:
        project_name: 项目名称
        file_name: 文件名
        file_id: 文件ID
        preprocessed_json_path: preprocessed.json的路径
        preprocessed_dir: 预处理目录
        status: 状态（success/fail）
        error_message: 错误信息（如果失败）
    """
    mappings = _load_mappings()
    
    # 确保项目存在
    if project_name not in mappings['projects']:
        mappings['projects'][project_name] = {"files": {}}
    
    # 构建文件映射
    file_mapping = {
        "file_id": file_id,
        "status": status,
        "processed_at": datetime.now().isoformat()
    }
    
    if status == "success":
        file_mapping.update({
            "preprocessed_json": preprocessed_json_path,
            "preprocessed_dir": preprocessed_dir
        })
    else:
        file_mapping["error_message"] = error_message or "处理失败"
    
    # 添加到映射
    mappings['projects'][project_name]['files'][file_name] = file_mapping
    
    # 保存
    _save_mappings(mappings)
    
    logger.info(f"✓ 文件映射已更新: {project_name}/{file_name} ({status})")


def get_file_mapping(project_name: str, file_name: str) -> Optional[Dict[str, Any]]:
    """
    获取文件映射
    
    Args:
        project_name: 项目名称
        file_name: 文件名
        
    Returns:
        文件映射信息，如果不存在返回None
    """
    mappings = _load_mappings()
    
    project = mappings.get('projects', {}).get(project_name, {})
    return project.get('files', {}).get(file_name)


def find_file_mapping(file_name: str) -> Optional[Dict[str, Any]]:
    """
    在所有项目中查找文件映射
    
    Args:
        file_name: 文件名
        
    Returns:
        文件映射信息，如果不存在返回None
    """
    mappings = _load_mappings()
    
    # 遍历所有项目
    for project_name, project_data in mappings.get('projects', {}).items():
        file_info = project_data.get('files', {}).get(file_name)
        if file_info:
            return file_info
    
    return None


def get_preprocessed_json_path(file_name: str, project_name: str = None) -> Optional[str]:
    """
    获取文件的 preprocessed.json 路径
    
    Args:
        file_name: 文件名
        project_name: 项目名称（可选，如果不提供则搜索所有项目）
        
    Returns:
        preprocessed.json 的路径，如果不存在返回None
    """
    if project_name:
        file_info = get_file_mapping(project_name, file_name)
    else:
        file_info = find_file_mapping(file_name)
    
    if file_info and file_info.get('status') == 'success':
        return file_info.get('preprocessed_json')
    
    return None


def remove_file_mapping(project_name: str, file_name: str) -> None:
    """
    删除文件映射
    
    Args:
        project_name: 项目名称
        file_name: 文件名
    """
    mappings = _load_mappings()
    
    if project_name in mappings.get('projects', {}):
        project = mappings['projects'][project_name]
        if file_name in project.get('files', {}):
            del project['files'][file_name]
            _save_mappings(mappings)
            logger.info(f"✓ 文件映射已删除: {project_name}/{file_name}")


def get_project_files(project_name: str) -> Dict[str, Any]:
    """
    获取项目的所有文件映射
    
    Args:
        project_name: 项目名称
        
    Returns:
        文件映射字典
    """
    mappings = _load_mappings()
    project = mappings.get('projects', {}).get(project_name, {})
    return project.get('files', {})


def list_all_projects() -> list:
    """
    列出所有项目
    
    Returns:
        项目名称列表
    """
    mappings = _load_mappings()
    return list(mappings.get('projects', {}).keys())


def get_mapping_stats() -> Dict[str, Any]:
    """
    获取映射统计信息
    
    Returns:
        统计信息字典
    """
    mappings = _load_mappings()
    
    total_projects = len(mappings.get('projects', {}))
    total_files = 0
    success_files = 0
    failed_files = 0
    
    for project_data in mappings.get('projects', {}).values():
        files = project_data.get('files', {})
        total_files += len(files)
        
        for file_info in files.values():
            if file_info.get('status') == 'success':
                success_files += 1
            else:
                failed_files += 1
    
    return {
        "total_projects": total_projects,
        "total_files": total_files,
        "success_files": success_files,
        "failed_files": failed_files,
        "last_updated": mappings.get('last_updated')
    }
