#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Markdown文档标记与拆分工具

功能：
1. 在Markdown的表格和图片前后自动插入标签
2. 按标签拆分为独立的Markdown片段
3. 每个片段提取Base64图片并保存

优势：
- 比Word处理简单（纯文本操作，无需COM）
- 跨平台（不依赖Windows/Office）
- 易于调试（可以直接看Markdown内容）
"""

import re
import logging
from pathlib import Path
from typing import List, Dict, Tuple
from service.windows.preprocessing.preprocessing_function.markdown.markdown_image_extractor import extract_base64_images

logger = logging.getLogger(__name__)


def mark_tables_and_images_in_markdown(markdown_text: str) -> str:
    """
    在Markdown的表格和图片前后插入标签
    
    标签格式：
    - 表格：{{Table_1_Start}} ... {{Table_1_End}}
    - 图片：{{Image_1_Start}} ... {{Image_1_End}}
    
    Args:
        markdown_text: 原始Markdown文本
        
    Returns:
        标记后的Markdown文本
    """
    lines = markdown_text.split('\n')
    result_lines = []
    
    table_counter = 0
    image_counter = 0
    in_table = False
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # 检测Markdown表格（至少包含 | 和 -）
        is_table_header = '|' in line and i + 1 < len(lines) and re.match(r'^\s*\|?\s*[-:]+\s*\|', lines[i + 1])
        is_table_row = in_table or (is_table_header)
        
        if is_table_header and not in_table:
            # 表格开始
            table_counter += 1
            in_table = True
            result_lines.append(f"\n{{{{Table_{table_counter}_Start}}}}\n")
            result_lines.append(line)
            
        elif in_table:
            # 判断表格是否结束
            if not line.strip() or not ('|' in line or re.match(r'^\s*\|?\s*[-:]+', line)):
                # 表格结束
                result_lines.append(f"\n{{{{Table_{table_counter}_End}}}}\n")
                in_table = False
                result_lines.append(line)
            else:
                result_lines.append(line)
                
        # 检测图片 ![alt](url)
        elif re.search(r'!\[([^\]]*)\]\(([^)]+)\)', line):
            # 图片行
            image_counter += 1
            result_lines.append(f"\n{{{{Image_{image_counter}_Start}}}}")
            result_lines.append(line)
            result_lines.append(f"{{{{Image_{image_counter}_End}}}}\n")
            
        else:
            result_lines.append(line)
        
        i += 1
    
    marked_text = '\n'.join(result_lines)
    logger.info(f"标记完成: {table_counter} 个表格, {image_counter} 张图片")
    
    return marked_text


def scan_markdown_regions(markdown_text: str) -> List[Dict[str, any]]:
    """
    扫描Markdown中的标签区域
    
    Returns:
        [
            {"type": "table", "index": 1, "start_pos": 123, "end_pos": 456, "name": "Table_1"},
            {"type": "image", "index": 1, "start_pos": 567, "end_pos": 789, "name": "Image_1"},
            ...
        ]
    """
    regions = []
    
    # 匹配标签：{{Table_1_Start}}, {{Table_1_End}}, {{Image_1_Start}}, {{Image_1_End}}
    pattern = r'\{\{(Table|Image)_(\d+)_(Start|End)\}\}'
    
    starts = {}  # {name: position}
    
    for match in re.finditer(pattern, markdown_text):
        obj_type = match.group(1).lower()  # table/image
        index = int(match.group(2))
        tag_type = match.group(3).lower()  # start/end
        position = match.start()
        
        name = f"{match.group(1)}_{index}"
        
        if tag_type == 'start':
            starts[name] = position
        elif tag_type == 'end' and name in starts:
            regions.append({
                "type": obj_type,
                "index": index,
                "start_pos": starts[name],
                "end_pos": match.end(),
                "name": name,
                "start_tag": f"{{{{{name}_Start}}}}",
                "end_tag": f"{{{{{name}_End}}}}"
            })
    
    logger.info(f"扫描到 {len(regions)} 个标记区域")
    return regions


def extract_markdown_region(markdown_text: str, region: Dict[str, any]) -> str:
    """
    提取单个标记区域的Markdown内容（不含标签）
    
    Args:
        markdown_text: 完整的Markdown文本
        region: scan_markdown_regions()返回的区域信息
        
    Returns:
        提取的Markdown片段
    """
    start_pos = region['start_pos']
    end_pos = region['end_pos']
    
    # 提取区域内容（包含标签）
    full_content = markdown_text[start_pos:end_pos]
    
    # 移除起止标签
    content = full_content.replace(region['start_tag'], '').replace(region['end_tag'], '').strip()
    
    return content


def split_markdown_by_regions(markdown_text: str, output_dir: str | Path) -> List[Dict[str, str]]:
    """
    按标记区域拆分Markdown，并为每个片段提取图片
    
    Args:
        markdown_text: 标记后的Markdown文本
        output_dir: 输出目录
        
    Returns:
        [
            {
                "name": "Table_1",
                "type": "table",
                "markdown_file": "path/to/Table_1.md",
                "images": [...],  # 提取的图片列表
                "content_preview": "前100字符..."
            },
            ...
        ]
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 扫描标记区域
    regions = scan_markdown_regions(markdown_text)
    
    results = []
    
    for region in regions:
        # 提取Markdown片段
        content = extract_markdown_region(markdown_text, region)
        
        if not content.strip():
            logger.warning(f"区域 {region['name']} 内容为空，跳过")
            continue
        
        # 为每个片段创建子目录
        region_dir = output_dir / region['name']
        region_dir.mkdir(exist_ok=True)
        
        # 提取Base64图片（如果有）
        cleaned_content, images = extract_base64_images(
            markdown_text=content,
            output_dir=region_dir,
            image_subdir="images"
        )
        
        # 保存Markdown文件
        md_file = region_dir / f"{region['name']}.md"
        md_file.write_text(cleaned_content, encoding='utf-8')
        
        results.append({
            "name": region['name'],
            "type": region['type'],
            "index": region['index'],
            "markdown_file": str(md_file),
            "images": images,
            "content_preview": cleaned_content[:100] + "..." if len(cleaned_content) > 100 else cleaned_content,
            "region_dir": str(region_dir)
        })
        
        logger.info(f"导出 {region['name']}: {md_file} (提取 {len(images)} 张图片)")
    
    return results


def export_all_tables_and_images(markdown_text: str, output_dir: str | Path) -> Dict[str, any]:
    """
    完整流程：标记 → 拆分 → 提取图片
    
    Returns:
        {
            "marked_markdown": "标记后的完整Markdown",
            "regions": [...],  # 拆分结果列表
            "summary": {
                "total_tables": 5,
                "total_images": 3,
                "total_extracted_images": 8  # Base64图片数量
            }
        }
    """
    logger.info("开始Markdown标记与拆分流程")
    
    # 1. 标记
    marked_md = mark_tables_and_images_in_markdown(markdown_text)
    
    # 2. 拆分并提取图片
    regions = split_markdown_by_regions(marked_md, output_dir)
    
    # 3. 统计
    total_tables = sum(1 for r in regions if r['type'] == 'table')
    total_images = sum(1 for r in regions if r['type'] == 'image')
    total_extracted_images = sum(len(r['images']) for r in regions)
    
    summary = {
        "total_tables": total_tables,
        "total_images": total_images,
        "total_regions": len(regions),
        "total_extracted_images": total_extracted_images
    }
    
    logger.info(f"拆分完成: {summary}")
    
    return {
        "marked_markdown": marked_md,
        "regions": regions,
        "summary": summary
    }


# 使用示例
if __name__ == "__main__":
    # 测试Markdown
    test_md = """
# 临床试验报告

## 受试者分布

| 组别 | 人数 | 年龄(岁) |
|------|------|----------|
| 试验组 | 50 | 45.3±5.2 |
| 对照组 | 48 | 44.8±6.1 |

## 流程图

下图展示了试验流程：

![试验流程](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==)

## 安全性数据

| 不良反应 | 发生率 |
|----------|--------|
| 头痛 | 5.2% |
| 恶心 | 3.1% |

结束。
"""
    
    logging.basicConfig(level=logging.INFO)
    
    # 完整流程
    result = export_all_tables_and_images(test_md, "test_output_md")
    
    print("\n=== 拆分结果 ===")
    for region in result['regions']:
        print(f"\n{region['name']} ({region['type']})")
        print(f"  文件: {region['markdown_file']}")
        print(f"  图片: {len(region['images'])} 张")
        print(f"  预览: {region['content_preview']}")
    
    print(f"\n=== 统计 ===")
    print(result['summary'])

























