from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class DataItem:
    """数据项配置（通用版）"""
    extract: Optional[str] = None  # 提取提示词
    datas: Optional[List] = None  # 文件列表（可以是字符串列表或字典列表）
    original_mode: bool = False  # 原文模式是否使用原始数据
    insert_original: bool = False  # 是否插入表格/图片等（针对单个数据项）
    quote: Optional[str] = None  # 引用编号标签（用于在生成时标注来源）


@dataclass
class Paragraph:
    """段落配置"""
    id: str  # 段落ID
    data: List[DataItem]  # 数据项列表
    generate: str  # 生成提示词
    insert_original: bool = False  # 表示整个段落是否有需要插入的图表,由内部数据项自动推导得出 (任何一项为True则此处为True)
    example: Optional[str] = None  # 生成模板/示例


class ConfigParser:
    """配置文件解析器"""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
    
    def parse(self) -> List[Paragraph]:
        """解析配置文件，返回段落列表"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        
        with self.config_path.open("r", encoding="utf-8") as f:
            config_data = json.load(f)
        
        paragraphs = []
        for para_data in config_data.get("paragraphs", []):
            paragraph = self._parse_paragraph(para_data)
            paragraphs.append(paragraph)
        
        logger.info(f"成功解析 {len(paragraphs)} 个段落配置")
        return paragraphs

    @staticmethod
    def _parse_bool(value: Any, default: bool = False) -> bool:
        """辅助函数：统一处理布尔值解析（兼容 JSON bool 和 字符串 'True'/'False'）"""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() == "true"
        return default

    def _parse_paragraph(self, para_data: Dict[str, Any]) -> Paragraph:
        """解析单个段落配置"""
        # 解析数据项
        data_items = []
        for data_item in para_data.get("data", []):
            # 处理 original_mode 字段（兼容旧的 options 字段）
            original_mode = False
            if "original_mode" in data_item:
                original_mode = self._parse_bool(data_item.get("original_mode"), False)
            elif "options" in data_item:
                # 兼容旧格式：options="原文" 转换为 original_mode=True
                options_val = data_item.get("options", "")
                original_mode = (str(options_val).strip() == "原文")

            item = DataItem(
                extract=data_item.get("extract"),
                datas=data_item.get("datas"),
                original_mode=original_mode,
                insert_original=self._parse_bool(data_item.get("insert_original"), False),
                quote=data_item.get("quote")  # 读取quote字段
            )
            data_items.append(item)

        # 根据各个数据项级别insert_original的计算段落级 insert_original
        # 逻辑：只要 data_items 中有任何一个 item.insert_original 为 True，则外层为 True
        has_any_insert = any(item.insert_original for item in data_items)

        # 创建段落对象
        paragraph = Paragraph(
            id=para_data.get("id", ""),
            data=data_items,
            generate=para_data.get("generate", ""),
            insert_original=has_any_insert,
            example=para_data.get("example")
        )
        
        return paragraph
    

#
#     def get_raw_data_items(self) -> List[Dict[str, Any]]:
#         """返回所有原始数据项，不做任何处理"""
#         paragraphs = self.parse()
#         all_items = []
#
#         for para in paragraphs:
#             for item in para.data:
#                 raw_item = {
#                     "paragraph_id": para.id,
#                     "extract": item.extract,
#                     "datas": item.datas,
#                     "insert_original": item.insert_original
#                 }
#                 all_items.append(raw_item)
#
#         return all_items
#
#     def get_data_by_type_structured(self) -> Dict[str, List[Dict[str, Any]]]:
#         """按数据类型分组返回原始数据（已废弃：无type字段）"""
#         # 由于DataItem已移除type字段，此方法仅返回空字典
#         return {}
#
#     def get_simple_config(self) -> Dict[str, Any]:
#         """返回简化的配置结构，方便直接使用"""
#         paragraphs = self.parse()
#
#         # 构建简化的配置结构
#         config = {
#             "paragraphs": [],
#             "data_items": [],
#             "data_by_type": {},
#             "paragraph_count": len(paragraphs),
#             "total_data_items": 0
#         }
#
#         for idx, para in enumerate(paragraphs):
#             # 段落信息
#             para_info = {
#                 "id": para.id,
#                 "generate": para.generate,
#                 "example": para.example,
#                 "data_count": len(para.data),
#                 "data": []
#             }
#
#             # 处理段落中的数据项
#             for item in para.data:
#                 # 构建数据项信息
#                 data_item = {
#                     "index": idx + 1
#                 }
#
#                 # 只添加非空字段
#                 if item.extract:
#                     data_item["extract"] = item.extract
#                 if item.datas:
#                     data_item["datas"] = item.datas
#                 if item.insert_original:
#                     data_item["insert_original"] = item.insert_original
#
#                 para_info["data"].append(data_item)
#                 config["data_items"].append(data_item)
#
#             config["paragraphs"].append(para_info)
#
#         config["total_data_items"] = len(config["data_items"])
#
#         return config
#
#
#
#
# def main():
#     """测试解析功能"""
#     # config_path = "../configs/paragraphs-v1.1(1).json"  # 已废弃
#     config_path = None  # 需要从API接口传入配置
#
#     try:
#         parser = ConfigParser(config_path)
#
#         print("=== 原始数据项解析 ===")
#         raw_items = parser.get_raw_data_items()
#         print(f"总共解析出 {len(raw_items)} 个数据项:")
#
#         for i, item in enumerate(raw_items, 1):
#             print(f"\n数据项 {i} (段落: {item['paragraph_id']}):")
#             # 只显示非空字段
#             for key, value in item.items():
#                 if key not in ['paragraph_id'] and value is not None:
#                     print(f"  {key}: {value}")
#
#         print("\n=== 按类型分组 ===")
#         type_groups = parser.get_data_by_type_structured()
#         for data_type, items in type_groups.items():
#             print(f"\n{data_type} 类型: {len(items)} 个数据项")
#
#         print("\n=== JSON格式输出 ===")
#         import json
#         print(json.dumps(raw_items, ensure_ascii=False, indent=2))
#
#     except Exception as e:
#         print(f"解析失败: {e}")
#
#
# if __name__ == "__main__":
#     main()
