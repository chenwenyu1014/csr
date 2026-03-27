#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基于一级标题的分块器
按一级标题分块，保护表格完整性，生成JSON结构化存储，并为每个分块生成AI摘要
"""

import re
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from service.prompts.system_prompt_manager import SystemPromptManager
from config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

# 正则表达式
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")
_TABLE_START_RE = re.compile(r"\{\{\s*([A-Za-z0-9_\-]+)\s*_Start\s*\}\}", re.IGNORECASE)
_TABLE_END_RE = re.compile(r"\{\{\s*([A-Za-z0-9_\-]+)\s*_End\s*\}\}", re.IGNORECASE)


class HeadingBasedChunker:
    """基于一级标题的分块器"""
    
    def __init__(self, llm_service=None):
        """
        初始化分块器
        
        Args:
            llm_service: LLM服务实例，用于生成摘要
        """
        self.llm_service = llm_service
    
    def chunk_by_h1_headings(self, markdown_content: str, file_name: str = "") -> Dict[str, Any]:
        """
        按一级标题分块，保护表格完整性
        
        Args:
            markdown_content: Markdown内容
            file_name: 文件名
            
        Returns:
            Dict: 结构化的分块结果
        """
        logger.info(f"开始按一级标题分块: {file_name}")
        
        # 解析Markdown内容
        sections = self._parse_h1_sections(markdown_content)
        
        # 构建结构化结果
        result = {
            "file_name": file_name,
            "total_sections": len(sections),
            "sections": []
        }
        
        # 处理每个一级标题区域（并发生成摘要，每次5个）
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def process_section(i, section):
            """处理单个section并生成摘要"""
            logger.info(f"处理第 {i+1}/{len(sections)} 个区域: {section['title'][:50]}...")
            
            # 生成摘要
            summary = self._generate_summary(section['content'], section['title'])
            
            return {
                "index": i,
                "section_data": {
                    "section_id": f"h1_{i+1}",
                    "title": section['title'],
                    "content": section['content'],
                    "summary": summary,
                    "word_count": len(section['content']),
                    "has_tables": section['has_tables'],
                    "table_count": section['table_count']
                }
            }
        
        # 使用线程池并发处理
        max_workers = settings.max_summary_workers
        section_results = [None] * len(sections)  # 预分配列表，保持顺序
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_index = {
                executor.submit(process_section, i, section): i 
                for i, section in enumerate(sections)
            }
            
            # 收集结果
            completed = 0
            for future in as_completed(future_to_index):
                try:
                    processed = future.result()
                    section_results[processed["index"]] = processed["section_data"]
                    completed += 1
                    logger.info(f"✅ 已完成 {completed}/{len(sections)} 个区域的摘要生成")
                except Exception as e:
                    index = future_to_index[future]
                    logger.error(f"❌ 处理第 {index+1} 个区域时出错: {e}")
                    # 即使出错也创建一个基本的数据结构
                    section_results[index] = {
                        "section_id": f"h1_{index+1}",
                        "title": sections[index]['title'],
                        "content": sections[index]['content'],
                        "summary": "",  # 摘要生成失败，使用空摘要
                        "word_count": len(sections[index]['content']),
                        "has_tables": sections[index]['has_tables'],
                        "table_count": sections[index]['table_count']
                    }
        
        # 按顺序添加到结果中
        for section_data in section_results:
            if section_data:
                result["sections"].append(section_data)
        
        logger.info(f"分块完成，共 {len(sections)} 个区域")
        return result
    
    def _parse_h1_sections(self, markdown_content: str) -> List[Dict[str, Any]]:
        """
        解析一级标题区域，保护表格完整性
        
        改进：
        - 先扫描所有表格标签，建立表格范围映射
        - 然后按一级标题分块，但如果标题在表格内部，则不分块
        - 这样既能正确分块，又能保护表格完整性
        
        Args:
            markdown_content: Markdown内容
            
        Returns:
            List[Dict]: 一级标题区域列表
        """
        lines = markdown_content.split('\n')
        
        # 第一步：扫描所有表格范围
        table_ranges = []  # [(start_line, end_line, table_name), ...]
        table_stack = []  # [(table_name, start_line), ...]
        
        for line_num, line in enumerate(lines):
            table_start_match = _TABLE_START_RE.search(line)
            table_end_match = _TABLE_END_RE.search(line)
            
            if table_start_match:
                table_name = table_start_match.group(1)
                table_stack.append((table_name, line_num))
                logger.debug(f"表格开始: {table_name} (行 {line_num+1})")
            
            if table_end_match:
                table_name = table_end_match.group(1)
                # 找到匹配的开始标签
                for i in range(len(table_stack) - 1, -1, -1):
                    if table_stack[i][0] == table_name:
                        start_line = table_stack[i][1]
                        table_ranges.append((start_line, line_num, table_name))
                        table_stack.pop(i)
                        logger.debug(f"表格结束: {table_name} (行 {line_num+1})")
                        break
        
        # 第二步：判断一行是否在表格内部
        def is_in_table(line_num: int) -> bool:
            for start, end, _ in table_ranges:
                if start <= line_num <= end:
                    return True
            return False
        
        # 第三步：按一级标题分块，但保护表格完整性
        sections = []
        current_section = None
        
        for line_num, line in enumerate(lines):
            table_start_match = _TABLE_START_RE.search(line)
            
            # 检查一级标题
            heading_match = _HEADING_RE.match(line.strip())
            if heading_match and heading_match.group(1) == '#' and not is_in_table(line_num):
                # 这是一级标题，且不在表格内部
                title = heading_match.group(2).strip()
                
                # 保存前一个区域
                if current_section is not None:
                    sections.append(current_section)
                
                # 开始新区域
                current_section = {
                    'title': title,
                    'content': line + '\n',
                    'has_tables': False,
                    'table_count': 0,
                    'start_line': line_num + 1
                }
                logger.debug(f"发现一级标题: {title} (行 {line_num+1})")
            else:
                # 添加到当前区域
                if current_section is not None:
                    current_section['content'] += line + '\n'
                    
                    # 检查是否包含表格标签
                    if table_start_match:
                        current_section['has_tables'] = True
                        current_section['table_count'] += 1
                else:
                    # 文档开头没有一级标题的内容，创建默认区域
                    if not sections:
                        current_section = {
                            'title': '文档开头',
                            'content': line + '\n',
                            'has_tables': False,
                            'table_count': 0,
                            'start_line': 1
                        }
                        if table_start_match:
                            current_section['has_tables'] = True
                            current_section['table_count'] += 1
        
        # 保存最后一个区域
        if current_section is not None:
            sections.append(current_section)
        
        # 清理内容（去除首尾空行）
        for section in sections:
            section['content'] = section['content'].strip()
        
        logger.info(f"共识别 {len(sections)} 个一级标题区域，{len(table_ranges)} 个表格范围")
        return sections
    
    def _generate_summary(self, content: str, title: str) -> str:
        """
        为内容生成摘要 - 调用LLM模型生成智能摘要
        
        Args:
            content: 内容文本
            title: 标题
            
        Returns:
            str: 生成的摘要
        """
        if not self.llm_service:
            # 如果没有LLM服务，生成简单摘要
            logger.info(f"LLM服务不可用，使用简单摘要: {title}")
            return self._generate_simple_summary(content, title)
        
        try:
            # 清理内容，移除过多的换行和特殊字符
            clean_content = re.sub(r'\n\s*\n', '\n', content)  # 合并多个空行
            clean_content = re.sub(r'\{\{[^}]+\}\}', '', clean_content)  # 移除表格标签
            clean_content = clean_content.strip()
            
            # 限制内容长度，避免token超限
            max_content_length = 50000
            if len(clean_content) > max_content_length:
                clean_content = clean_content[:max_content_length] + "..."
            
            
            prompt = ""
            try:
                spm = SystemPromptManager()
                prompt = spm.build_prompt("chunk_section_summary", {"title": title, "content": clean_content})
            except Exception:
                prompt = ""

            
            if not prompt:
                try:
                    base_dir = Path(__file__).resolve().parents[3]  # service 目录
                    md_path = base_dir / "prompts" / "summary" / "chunk_section_summary.md"
                    if md_path.exists():
                        text = md_path.read_text(encoding="utf-8")
                        prompt = text.replace("{{title}}", title).replace("{{content}}", clean_content)
                except Exception:
                    prompt = ""

            
            if not prompt:
                prompt = f"""请为以下CSR临床研究方案的章节内容生成一个简洁准确的摘要：

【章节标题】
{title}

【章节内容】
{clean_content}

【摘要要求】
1. 摘要长度控制在100-200字
2. 提取该章节的核心要点和关键信息
3. 保持客观准确，使用专业术语
4. 突出重要的数据、标准、流程等
5. 语言简洁明了，便于快速理解

【输出格式】
直接输出摘要内容，不需要额外的标题或格式。

摘要："""

            logger.info(f"调用LLM生成摘要: {title}")
            
            # 调用LLM生成摘要
            summary = self.llm_service.generate(prompt)
            
            if summary and len(summary.strip()) > 0:
                # 清理生成的摘要
                summary = summary.strip()
                
                # 移除可能的前缀
                if summary.startswith("摘要："):
                    summary = summary[3:].strip()
                
                logger.info(f"LLM摘要生成成功: {title} ({len(summary)}字)")
                return summary
            else:
                logger.warning(f"LLM返回空摘要，使用简单摘要: {title}")
                return self._generate_simple_summary(content, title)
                
        except Exception as e:
            logger.warning(f"LLM摘要生成失败: {e}，使用简单摘要")
            return self._generate_simple_summary(content, title)
    
    def _generate_simple_summary(self, content: str, title: str) -> str:
        """
        生成简单的基于规则的摘要
        
        Args:
            content: 内容文本
            title: 标题
            
        Returns:
            str: 简单摘要
        """
        # 移除Markdown标记
        clean_content = re.sub(r'[#*`\[\]()]', '', content)
        clean_content = re.sub(r'\{\{[^}]+\}\}', '', clean_content)  # 移除表格标签
        
        # 按句子分割
        sentences = re.split(r'[。！？.!?]', clean_content)
        sentences = [s.strip() for s in sentences if s.strip() and len(s.strip()) > 10]
        
        # 取前3句作为摘要
        summary_sentences = sentences[:3]
        summary = '。'.join(summary_sentences)
        
        if len(summary) > 200:
            summary = summary[:200] + "..."
        
        return f"本节主要内容：{summary}。" if summary else f"本节为「{title}」相关内容。"
    
    def save_chunks_to_json(self, chunks_data: Dict[str, Any], output_path: str) -> bool:
        """
        保存分块结果到JSON文件
        
        Args:
            chunks_data: 分块数据
            output_path: 输出文件路径
            
        Returns:
            bool: 是否保存成功
        """
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(chunks_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"分块结果已保存到: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"保存分块结果失败: {e}")
            return False
    
    def load_chunks_from_json(self, json_path: str) -> Optional[Dict[str, Any]]:
        """
        从JSON文件加载分块结果
        
        Args:
            json_path: JSON文件路径
            
        Returns:
            Dict: 分块数据，失败返回None
        """
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"成功加载分块结果: {json_path}")
            return data
            
        except Exception as e:
            logger.error(f"加载分块结果失败: {e}")
            return None


def chunk_markdown_by_headings(markdown_content: str, 
                              file_name: str = "",
                              llm_service=None,
                              save_to: Optional[str] = None) -> Dict[str, Any]:
    """
    便捷函数：按一级标题分块Markdown内容
    
    Args:
        markdown_content: Markdown内容
        file_name: 文件名
        llm_service: LLM服务实例
        save_to: 保存路径（可选）
        
    Returns:
        Dict: 分块结果
    """
    chunker = HeadingBasedChunker(llm_service=llm_service)
    result = chunker.chunk_by_h1_headings(markdown_content, file_name)
    
    if save_to:
        chunker.save_chunks_to_json(result, save_to)
    
    return result


if __name__ == "__main__":
    # 测试代码
    test_markdown = """# 试验概述

这是试验的基本信息。

{{Table_1_Start}}
| 项目 | 值 |
|------|-----|
| 试验名称 | 测试试验 |
| 试验编号 | ABC-001 |
{{Table_1_End}}

## 试验目的

主要目的是评估药物的安全性。

# 试验人群

## 入选标准

1. 年龄18-65岁
2. 身体健康

{{Table_2_Start}}
| 标准 | 描述 |
|------|-----|
| 年龄 | 18-65岁 |
| 性别 | 不限 |
{{Table_2_End}}

## 排除标准

1. 孕妇
2. 哺乳期妇女

# 统计分析

采用描述性统计方法。
"""
    
    result = chunk_markdown_by_headings(test_markdown, "test.md")
    print(json.dumps(result, ensure_ascii=False, indent=2))
