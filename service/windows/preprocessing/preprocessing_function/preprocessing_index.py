"""
预处理索引管理器
维护全局的文件 → 预处理结果映射关系
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from threading import Lock

logger = logging.getLogger(__name__)


class PreprocessingIndex:
    """预处理索引管理器"""
    
    # 全局索引文件路径
    INDEX_FILE = Path("AAA/Preprocessing/preprocessing_index.json")
    
    # 线程锁（保证并发安全）
    _lock = Lock()

    @staticmethod
    def _to_aaa_relative(p: str | None) -> str | None:
        """将任意路径规范为以 AAA/ 开头的相对路径（若可能）。
        - 支持 Windows/Unix 分隔符
        - 若包含 AAA 段，则截取 AAA/ 之后部分
        - 若为绝对路径且位于当前工程 AAA 下，则转为 AAA 相对
        - 若无法规范，则原样返回
        """
        if not p:
            return p
        try:
            s = str(p)
            s_norm = s.replace('\\', '/')
            low = s_norm.lower()
            idx = low.find('aaa/')
            if idx != -1:
                rel = s_norm[idx + 4:].lstrip('/')
                return str(Path('AAA') / Path(rel))
            # 尝试基于实际磁盘路径相对化
            try:
                aaa_base = Path.cwd() / 'AAA'
                relpath = Path(s).expanduser().resolve().relative_to(aaa_base)
                return str(Path('AAA') / relpath)
            except Exception:
                return s
        except Exception:
            return p

    @classmethod
    def _normalize_info(cls, info: dict) -> dict:
        """返回一个拷贝，规范其中涉及路径的字段为 AAA 相对。"""
        if not isinstance(info, dict):
            return info
        out = dict(info)
        for k in [
            'original_path', 'preprocessing_dir', 'preprocessed_json',
            'chunks_file', 'regions_dir']:
            if k in out:
                out[k] = cls._to_aaa_relative(out.get(k))
        return out
    
    @classmethod
    def _load_index(cls) -> Dict[str, Any]:
        """加载索引文件"""
        if not cls.INDEX_FILE.exists():
            return {
                "version": "1.0",
                "last_updated": datetime.now().isoformat(),
                "files": {}
            }
        
        try:
            with open(cls.INDEX_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载索引文件失败: {e}")
            return {
                "version": "1.0",
                "last_updated": datetime.now().isoformat(),
                "files": {}
            }
    
    @classmethod
    def _save_index(cls, index_data: Dict[str, Any]) -> bool:
        """保存索引文件"""
        try:
            # 确保目录存在
            cls.INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
            
            # 更新时间
            index_data["last_updated"] = datetime.now().isoformat()
            
            # 保存
            with open(cls.INDEX_FILE, 'w', encoding='utf-8') as f:
                json.dump(index_data, f, ensure_ascii=False, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"保存索引文件失败: {e}")
            return False
    
    @classmethod
    def add_file(cls,
                 file_name: str,
                 original_path: str,
                 preprocessing_dir: str,
                 preprocessed_json: str,
                 file_type: str,
                 chunks_file: Optional[str] = None,
                 regions_dir: Optional[str] = None,
                 status: str = "success",
                 **extra_info) -> bool:
        """
        添加或更新文件映射
        
        Args:
            file_name: 文件名（作为key）
            original_path: 原始文件路径
            preprocessing_dir: 预处理目录
            preprocessed_json: preprocessed.json路径
            file_type: 文件类型 (word/pdf/excel/rtf)
            chunks_file: 分块文件路径（可选）
            regions_dir: regions目录路径（可选）
            status: 处理状态 (success/failed)
            **extra_info: 其他额外信息
        
        Returns:
            bool: 是否成功
        """
        with cls._lock:
            # 加载索引
            index = cls._load_index()
            
            # 构建文件信息
            file_info = {
                "original_path": cls._to_aaa_relative(original_path) or str(original_path),
                "preprocessing_dir": cls._to_aaa_relative(preprocessing_dir) or str(preprocessing_dir),
                "preprocessed_json": cls._to_aaa_relative(preprocessed_json) or str(preprocessed_json),
                "file_type": file_type,
                "processed_at": datetime.now().isoformat(),
                "status": status
            }
            
            # 添加可选字段
            if chunks_file:
                file_info["chunks_file"] = cls._to_aaa_relative(chunks_file) or str(chunks_file)
            if regions_dir:
                file_info["regions_dir"] = cls._to_aaa_relative(regions_dir) or str(regions_dir)
            
            # 合并额外信息
            file_info.update(extra_info)
            
            # 更新索引
            index["files"][file_name] = file_info
            
            # 保存
            success = cls._save_index(index)
            
            if success:
                logger.info(f"索引已更新: {file_name}")
            
            return success
    
    @classmethod
    def find_file(cls, file_name: str) -> Optional[Dict[str, Any]]:
        """
        查找文件的预处理信息
        
        Args:
            file_name: 文件名
        
        Returns:
            Dict 或 None: 文件信息
        """
        with cls._lock:
            index = cls._load_index()
            info = index["files"].get(file_name)
            return cls._normalize_info(info) if info else None
    
    @classmethod
    def find_by_pattern(cls, pattern: str) -> Dict[str, Dict[str, Any]]:
        """
        模糊查找文件
        
        Args:
            pattern: 文件名模式（子串匹配）
        
        Returns:
            Dict: 匹配的文件信息
        """
        with cls._lock:
            index = cls._load_index()
            results = {}
            
            for file_name, file_info in index["files"].items():
                if pattern.lower() in file_name.lower():
                    results[file_name] = file_info
            
            return results
    
    @classmethod
    def get_all_files(cls) -> Dict[str, Dict[str, Any]]:
        """获取所有文件映射"""
        with cls._lock:
            index = cls._load_index()
            return index["files"]
    
    @classmethod
    def remove_file(cls, file_name: str) -> bool:
        """删除文件映射"""
        with cls._lock:
            index = cls._load_index()
            
            if file_name in index["files"]:
                del index["files"][file_name]
                return cls._save_index(index)
            
            return False
    
    @classmethod
    def get_stats(cls) -> Dict[str, Any]:
        """获取统计信息"""
        with cls._lock:
            index = cls._load_index()
            files = index["files"]
            
            stats = {
                "total_files": len(files),
                "by_type": {},
                "by_status": {},
                "last_updated": index.get("last_updated")
            }
            
            # 按类型统计
            for file_info in files.values():
                file_type = file_info.get("file_type", "unknown")
                stats["by_type"][file_type] = stats["by_type"].get(file_type, 0) + 1
                
                status = file_info.get("status", "unknown")
                stats["by_status"][status] = stats["by_status"].get(status, 0) + 1
            
            return stats


# 便捷函数
def add_preprocessing_result(file_name: str, **kwargs) -> bool:
    """添加预处理结果到索引"""
    return PreprocessingIndex.add_file(file_name, **kwargs)


def find_preprocessing_result(file_name: str) -> Optional[Dict[str, Any]]:
    """查找预处理结果"""
    return PreprocessingIndex.find_file(file_name)


def search_preprocessing_results(pattern: str) -> Dict[str, Dict[str, Any]]:
    """搜索预处理结果"""
    return PreprocessingIndex.find_by_pattern(pattern)
