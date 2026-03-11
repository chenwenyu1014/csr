"""
数据分配服务模块

功能说明：
- 封装数据源分配（文件匹配）的业务逻辑
- 使用 LLM 进行智能匹配
- 支持批量处理多个分组

主要类：
- AllocationService: 数据分配服务类
"""

# ========== 标准库导入 ==========
import logging
import threading
from typing import Any, Dict, List, Optional

# ========== 本地导入 ==========
from config import get_settings

# ========== 模块配置 ==========
logger = logging.getLogger(__name__)
settings = get_settings()


class AllocationService:
    """
    数据分配服务类
    
    封装了数据源分配（文件匹配）的核心业务逻辑。
    """
    
    def __init__(self):
        """初始化数据分配服务"""
        self.settings = settings
    
    # ============================================================
    # 公开方法
    # ============================================================
    
    def allocate_batch(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        批量处理数据分配
        
        Args:
            data: 分组数据列表，格式为 [{"tagId": "...", "data": [...]}]
        
        Returns:
            处理后的分组数据列表，每个数据项增加 matched_files 字段
        """
        from service.linux.allocation.data_source_validator import DataSourceValidator
        
        validator = DataSourceValidator()
        result_groups = []

        for group in data:
            if not isinstance(group, dict):
                continue
            
            tag_id = group.get("tagId") or group.get("tag_id") or "unknown"
            items = group.get("data", [])
            if not isinstance(items, list):
                items = []

            logger.info(f"开始处理分组 tagId={tag_id}, items={len(items)}")
            new_items = []

            for item in items:
                new_item = self._process_item(item, tag_id, validator)
                new_items.append(new_item)

            result_groups.append({
                "tagId": tag_id,
                "data": new_items
            })

        return result_groups
    
    # ============================================================
    # 私有辅助方法
    # ============================================================
    
    def _process_item(
        self, 
        item: Dict[str, Any], 
        tag_id: str,
        validator
    ) -> Dict[str, Any]:
        """
        处理单个数据项
        
        Args:
            item: 数据项
            tag_id: 分组标签ID
            validator: 数据源验证器
        
        Returns:
            处理后的数据项，增加 matched_files 字段
        """
        if not isinstance(item, dict):
            return item
        
        rules = str(item.get("first_match_logic", "") or "").strip()
        sff = item.get("source_full_file", [])

        # 提取文件名列表
        file_names = self._extract_file_names(sff)
        matched_files = []

        if not rules or not file_names:
            logger.warning(
                f"跳过数据项（缺少first_match_logic或source_full_file）: "
                f"{item.get('id', 'unknown')}"
            )
        else:
            logger.info(
                f"数据匹配请求 ({item.get('id', 'unknown')}): "
                f"first_match_logic='{rules}', 候选文件数={len(file_names)}"
            )
            
            try:
                spec = {"rules": rules, "files": file_names}
                task_name = f"{tag_id}_{item.get('id', 'default')}"
                res = validator.match(spec, task_name=str(task_name))

                if isinstance(res, dict) and res.get("success") and isinstance(res.get("result"), dict):
                    parsed = res.get("result", {})
                    comp = parsed.get("Compliant")
                    # 严格只读顶层 Compliant 字段
                    tmp = [f for f in comp] if isinstance(comp, list) else []
                    # 仅保留在候选文件中的名称
                    tmp = [f for f in tmp if f in file_names]
                    # 去重并按原 files 顺序返回
                    seen = set()
                    matched_files = [
                        f for f in file_names 
                        if (f in tmp) and not (f in seen or seen.add(f))
                    ]
                    logger.info(
                        f"匹配完成 ({item.get('id', 'unknown')}): "
                        f"{len(matched_files)}/{len(file_names)}"
                    )
                else:
                    matched_files = []
            except Exception as e:
                logger.error(f"匹配执行失败 ({item.get('id', 'unknown')}): {e}")
                matched_files = []

        # 返回新对象，添加 matched_files 字段
        return {**item, "matched_files": matched_files}
    
    def _extract_file_names(self, source_full_file: Any) -> List[str]:
        """
        从 source_full_file 中提取文件名列表
        
        Args:
            source_full_file: 可能是列表或字典或字符串
        
        Returns:
            文件名列表
        """
        file_names = []
        if isinstance(source_full_file, list):
            for f in source_full_file:
                if isinstance(f, dict):
                    fn = f.get("fileName")
                    if isinstance(fn, str) and fn.strip():
                        file_names.append(fn)
                elif isinstance(f, str) and f.strip():
                    file_names.append(f)
        return file_names


# ============================================================
# 全局单例（线程安全）
# ============================================================

_allocation_service: Optional[AllocationService] = None
_allocation_service_lock = threading.Lock()


def get_allocation_service() -> AllocationService:
    """获取数据分配服务单例（线程安全）"""
    global _allocation_service
    if _allocation_service is None:
        with _allocation_service_lock:
            if _allocation_service is None:
                _allocation_service = AllocationService()
    return _allocation_service
