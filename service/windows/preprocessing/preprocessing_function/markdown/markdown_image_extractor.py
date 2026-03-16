#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Markdown图片提取工具

功能：
1. 从Markdown中提取Base64编码的图片
2. 保存为独立文件（PNG/JPEG等）
3. 替换Markdown中的引用为相对路径
"""

import re
import base64
import hashlib
import logging
from pathlib import Path
from typing import Tuple, List, Dict

logger = logging.getLogger(__name__)


def extract_base64_images(markdown_text: str, output_dir: str | Path, 
                         image_subdir: str = "images") -> Tuple[str, List[Dict[str, str]]]:
    """
    从Markdown中提取Base64图片并保存为独立文件
    
    Args:
        markdown_text: 包含Base64图片的Markdown文本
        output_dir: 输出目录
        image_subdir: 图片子目录名称（默认"images"）
        
    Returns:
        (cleaned_markdown, extracted_images)
        - cleaned_markdown: 替换后的Markdown文本（图片引用改为相对路径）
        - extracted_images: 提取的图片列表，每项包含 {name, path, format, size}
    """
    output_dir = Path(output_dir)
    img_dir = output_dir / image_subdir
    img_dir.mkdir(parents=True, exist_ok=True)
    
    # 匹配Base64图片的正则表达式
    # 格式: ![alt](data:image/png;base64,iVBORw0KG...)
    pattern = r'!\[([^\]]*)\]\(data:image/([^;,]+);base64,([^)]+)\)'
    
    extracted_images: List[Dict[str, str]] = []
    
    def replace_base64(match: re.Match) -> str:
        """替换单个Base64图片引用"""
        alt_text = match.group(1) or "image"
        img_format = match.group(2).lower()  # png, jpeg, gif, webp等
        b64_data = match.group(3).strip()
        
        try:
            # 解码Base64
            img_bytes = base64.b64decode(b64_data)
            
            # 生成文件名（使用内容哈希避免重复）
            content_hash = hashlib.md5(img_bytes).hexdigest()[:12]
            # 标准化格式后缀
            ext = _normalize_image_ext(img_format)
            img_name = f"image_{content_hash}.{ext}"
            img_path = img_dir / img_name
            
            # 保存图片（如果已存在则跳过）
            if not img_path.exists():
                img_path.write_bytes(img_bytes)
                logger.info(f"提取图片: {img_name} ({len(img_bytes)} bytes)")
            else:
                logger.debug(f"图片已存在，跳过: {img_name}")
            
            # 记录提取信息
            extracted_images.append({
                "name": img_name,
                "path": str(img_path),
                "format": ext,
                "size": len(img_bytes),
                "alt_text": alt_text
            })
            
            # 返回新的Markdown引用（相对路径）
            return f'![{alt_text}](./{image_subdir}/{img_name})'
            
        except Exception as e:
            logger.error(f"解码Base64图片失败: {e}")
            # 失败时保留原引用
            return match.group(0)
    
    # 替换所有Base64图片
    cleaned_markdown = re.sub(pattern, replace_base64, markdown_text)
    
    logger.info(f"从Markdown中提取了 {len(extracted_images)} 张图片到 {img_dir}")
    
    return cleaned_markdown, extracted_images


def _normalize_image_ext(format_str: str) -> str:
    """标准化图片格式后缀"""
    format_map = {
        "jpeg": "jpg",
        "jpg": "jpg",
        "png": "png",
        "gif": "gif",
        "webp": "webp",
        "bmp": "bmp",
        "tiff": "tiff",
        "svg+xml": "svg",
    }
    return format_map.get(format_str.lower(), "png")


def strip_all_images(markdown_text: str) -> str:
    """
    从Markdown中移除所有图片标记（保留alt文本）
    
    Args:
        markdown_text: Markdown文本
        
    Returns:
        移除图片后的Markdown文本
    """
    # 匹配所有图片引用: ![alt](url) 或 ![alt](data:...)
    pattern = r'!\[([^\]]*)\]\([^)]+\)'
    
    def keep_alt_only(match: re.Match) -> str:
        alt_text = match.group(1)
        if alt_text:
            return f"[图片: {alt_text}]"
        return "[图片]"
    
    return re.sub(pattern, keep_alt_only, markdown_text)


def count_base64_images(markdown_text: str) -> int:
    """统计Markdown中Base64图片的数量"""
    pattern = r'!\[([^\]]*)\]\(data:image/[^;,]+;base64,[^)]+\)'
    return len(re.findall(pattern, markdown_text))


def estimate_markdown_size_reduction(markdown_text: str) -> Dict[str, int]:
    """
    估算提取图片后Markdown文件的大小变化
    
    Returns:
        {
            "original_size": 原始大小(bytes),
            "estimated_cleaned_size": 提取后大小(bytes),
            "reduction_bytes": 减少的字节数,
            "reduction_percent": 减少的百分比
        }
    """
    original_size = len(markdown_text.encode('utf-8'))
    pattern = r'!\[([^\]]*)\]\(data:image/([^;,]+);base64,([^)]+)\)'
    
    total_base64_chars = 0
    for match in re.finditer(pattern, markdown_text):
        total_base64_chars += len(match.group(0))
    
    # 假设每个图片引用替换为相对路径（约50字符）
    num_images = len(re.findall(pattern, markdown_text))
    estimated_cleaned_size = original_size - total_base64_chars + (num_images * 50)
    
    reduction = original_size - estimated_cleaned_size
    reduction_percent = (reduction / original_size * 100) if original_size > 0 else 0
    
    return {
        "original_size": original_size,
        "estimated_cleaned_size": estimated_cleaned_size,
        "reduction_bytes": reduction,
        "reduction_percent": round(reduction_percent, 2),
        "num_images": num_images
    }


# 使用示例
if __name__ == "__main__":
    # 测试用例
    test_md = """
# 测试文档

这是一段文字。

![测试图片](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==)

再来一张图片：

![另一张](data:image/jpeg;base64,/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJCQwLDBgNDRgyIRwhMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjL/wAARCAABAAEDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAv/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAAAAX/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIRAxEAPwCwAA8//9k=)

结束。
"""
    
    logging.basicConfig(level=logging.INFO)
    
    # 统计
    stats = estimate_markdown_size_reduction(test_md)
    print(f"原始大小: {stats['original_size']} bytes")
    print(f"图片数量: {stats['num_images']}")
    print(f"预计减少: {stats['reduction_percent']}%")
    
    # 提取
    cleaned_md, images = extract_base64_images(test_md, "test_output")
    print(f"\n提取的图片: {len(images)}")
    for img in images:
        print(f"  - {img['name']}: {img['size']} bytes")
    
    print(f"\n清理后的Markdown:\n{cleaned_md}")

























