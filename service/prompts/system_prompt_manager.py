#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
系统提示词管理器
专门管理系统提示词和用户提示词模板的分离
"""

import json
import os
import time
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import requests
import logging

# 导入耗时记录工具
try:
    from utils.timing import Timer, generation_timer, log_timing
except ImportError:
    # 如果导入失败，提供空实现
    class Timer:
        def __init__(self, *args, **kwargs): pass
        def start(self): return self
        def stop(self): return 0
        def __enter__(self): return self
        def __exit__(self, *args): pass
        @property
        def duration(self): return 0
        @property
        def duration_str(self): return "0ms"
    generation_timer = None
    def log_timing(*args, **kwargs): pass

logger = logging.getLogger(__name__)

class SystemPromptManager:
    """系统提示词管理器"""
    
    def __init__(self, system_config_file: str = "service/prompts/system/system_prompts.json"):
        # system_config_file = Path(__file__).parent.parent.parent / system_config_file
        self.system_config_file = Path(system_config_file)
        self.system_config = self._load_system_config()
    
    def _load_system_config(self) -> Dict[str, Any]:
        """加载系统提示词配置"""
        if not self.system_config_file.exists():
            raise FileNotFoundError(f"系统提示词配置文件不存在: {self.system_config_file}")
        
        try:
            with open(self.system_config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"加载系统提示词配置失败: {str(e)}")
    
    def get_prompt_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """获取提示词模板"""
        template_data = self.system_config.get("prompt_templates", {}).get(template_id, {})
        if template_data:
            return template_data
        return None
    
    # def get_system_prompt_info(self, prompt_id: str = "default") -> Dict[str, Any]:
    #     """获取系统提示词详细信息"""
    #     prompt_data = self.system_config.get("system_prompts", {}).get(prompt_id, {})
    #     if isinstance(prompt_data, dict):
    #         return {
    #             "id": prompt_id,
    #             "content": prompt_data.get("content", ""),
    #             "usage": prompt_data.get("usage", ""),
    #             "location": prompt_data.get("location", ""),
    #             "used_by": prompt_data.get("used_by", [])
    #         }
    #     else:
    #         # 兼容旧格式
    #         return {
    #             "id": prompt_id,
    #             "content": prompt_data,
    #             "usage": "旧格式提示词",
    #             "location": "未知",
    #             "used_by": []
    #         }
    #
    # def get_user_prompt_template(self, template_id: str) -> Optional[Dict[str, Any]]:
    #     """获取用户提示词模板"""
    #     template_data = self.system_config.get("user_prompt_templates", {}).get(template_id, {})
    #     if template_data:
    #         return template_data
    #     return None
    
    def build_prompt(self, template_id: str, variables: Dict[str, Any]) -> str:
        """构建完整的提示词"""
        # 开始计时
        build_timer = Timer(f"构建提示词({template_id})", parent="提示词")
        build_timer.start()
        
        template = self.get_prompt_template(template_id)
        if not template:
            build_timer.stop()
            return ""
        
        template_parts = template.get("template", {})
        if not template_parts:
            template_parts = {}
        
        # 安全格式化：占位符缺失时以空串兜底，避免整段失败
        class _SafeDict(dict):
            def __missing__(self, key):
                return ""

        def _render_double_brace(text: str, vars_dict: Dict[str, Any]) -> str:
            # 将 {{var}} 替换为变量值；仅匹配简单变量名，避免误伤 JSON 的 {{"key":...}}
            if not text:
                return ""
            try:
                import re as _re
                pattern = _re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")
                return pattern.sub(lambda m: str((vars_dict or {}).get(m.group(1), "")), text)
            except Exception:
                return text

        def _safe_format(text: str, vars_dict: Dict[str, Any]) -> str:
            # 先渲染 {{var}}，再兼容 {var} 的格式化（缺失占位符忽略）
            try:
                pre = _render_double_brace(text or "", vars_dict)
                try:
                    return (pre or "").format_map(_SafeDict(vars_dict or {}))
                except Exception:
                    # 如果单花格式化失败（例如内容中包含花括号），保留已完成的双花替换结果
                    return pre or ""
            except Exception:
                return text or ""

        # 注入项目背景变量（若缺失则从环境变量CURRENT_PROJECT_DESC回退）
        try:
            if not (isinstance((variables or {}).get("project_desc", ""), str) and (variables or {}).get("project_desc", "").strip()):
                variables = dict(variables or {})
                variables["project_desc"] = os.getenv("CURRENT_PROJECT_DESC", "") or ""
        except Exception:
            pass

        def _with_project_desc(text: str, vars_dict: Dict[str, Any]) -> str:
            """保持兼容但不再前置项目背景，由模板内 {{project_desc}} 占位符自行决定位置。"""
            try:
                return text
            except Exception:
                return text

        # 优先从远端提示词服务获取模板（通过 combinationId + usedBy）
        try:
            combination_id = str(os.getenv("CURRENT_COMBINATION_ID", "")).strip()
        except Exception:
            combination_id = ""
        if not combination_id:
            try:
                logger.debug(f"[PromptFetch] CURRENT_COMBINATION_ID not set; using local template for {template_id}")
            except Exception:
                pass
        # if combination_id:
        #     # 远程提示词获取计时
        #     remote_timer = Timer(f"远程获取提示词({template_id})", parent="提示词")
        #     remote_timer.start()
        #     try:
        #         service_url = os.getenv(
        #             "PROMPT_SERVICE_URL",
        #             "http://192.168.3.32:8088/ky/sys/projectPromptDetailTable/findByCombinationAndUsedBy",
        #         )
        #         # 先尝试 POST（表单），失败或空则回退 GET
        #         try:
        #             logger.info(
        #                 f"[PromptFetch] POST requesting usedBy={template_id} combinationId={combination_id} url={service_url}"
        #             )
        #             post_timer = Timer("POST请求提示词", parent="远程提示词")
        #             post_timer.start()
        #             resp_post = requests.post(
        #                 service_url,
        #                 data={
        #                     "combinationId": combination_id,
        #                     "usedBy": template_id,
        #                 },
        #                 timeout=10,
        #             )
        #             post_timer.stop()
        #             logger.info(f"[PromptFetch] POST response status={resp_post.status_code} [耗时: {post_timer.duration_str}]")
        #             if resp_post.status_code == 200:
        #                 data = resp_post.json() if hasattr(resp_post, "json") else None
        #                 if isinstance(data, dict):
        #                     result = data.get("result") or {}
        #                     content = result.get("promptContent")
        #                     try:
        #                         logger.info(
        #                             f"[PromptFetch] remote promptContent length={len(content) if isinstance(content, str) else 0}"
        #                         )
        #                     except Exception:
        #                         pass
        #                     if isinstance(content, str) and content.strip():
        #                         remote_timer.stop()
        #                         build_timer.stop()
        #                         if generation_timer:
        #                             generation_timer.record(f"提示词构建(远程POST)-{template_id}", build_timer.duration, parent="提示词")
        #                         logger.info(f"✅ 提示词构建完成(远程POST) [模板: {template_id}, 耗时: {build_timer.duration_str}]")
        #                         return _with_project_desc(_safe_format(content, variables), variables)
        #                     else:
        #                         try:
        #                             logger.info(
        #                                 f"[PromptFetch] POST returned empty promptContent, try GET fallback for {template_id}"
        #                             )
        #                         except Exception:
        #                             pass
        #             else:
        #                 try:
        #                     logger.warning(
        #                         f"[PromptFetch] POST non-200 status: {resp_post.status_code}; try GET fallback"
        #                     )
        #                 except Exception:
        #                     pass
        #         except Exception as e_post:
        #             try:
        #                 logger.warning(
        #                     f"[PromptFetch] POST request failed: {e_post}; try GET fallback", exc_info=True
        #                 )
        #             except Exception:
        #                 pass
        #
        #         # GET 回退
        #         try:
        #             logger.info(
        #                 f"[PromptFetch] GET requesting usedBy={template_id} combinationId={combination_id} url={service_url}"
        #             )
        #             get_timer = Timer("GET请求提示词", parent="远程提示词")
        #             get_timer.start()
        #             resp_get = requests.get(
        #                 service_url,
        #                 params={
        #                     "combinationId": combination_id,
        #                     "usedBy": template_id,
        #                 },
        #                 timeout=10,
        #             )
        #             get_timer.stop()
        #             logger.info(f"[PromptFetch] GET response status={resp_get.status_code} [耗时: {get_timer.duration_str}]")
        #             if resp_get.status_code == 200:
        #                 data = resp_get.json() if hasattr(resp_get, "json") else None
        #                 if isinstance(data, dict):
        #                     result = data.get("result") or {}
        #                     content = result.get("promptContent")
        #                     try:
        #                         logger.info(
        #                             f"[PromptFetch] remote promptContent length={len(content) if isinstance(content, str) else 0}"
        #                         )
        #                     except Exception:
        #                         pass
        #                     if isinstance(content, str) and content.strip():
        #                         remote_timer.stop()
        #                         build_timer.stop()
        #                         if generation_timer:
        #                             generation_timer.record(f"提示词构建(远程GET)-{template_id}", build_timer.duration, parent="提示词")
        #                         logger.info(f"✅ 提示词构建完成(远程GET) [模板: {template_id}, 耗时: {build_timer.duration_str}]")
        #                         return _with_project_desc(_safe_format(content, variables), variables)
        #                     else:
        #                         try:
        #                             logger.info(
        #                                 f"[PromptFetch] empty promptContent, will fallback to local for {template_id}"
        #                             )
        #                         except Exception:
        #                             pass
        #             else:
        #                 try:
        #                     logger.warning(
        #                         f"[PromptFetch] GET non-200 status: {resp_get.status_code}; will fallback to local"
        #                     )
        #                 except Exception:
        #                     pass
        #         except Exception as e_get:
        #             try:
        #                 logger.warning(
        #                     f"[PromptFetch] GET request failed: {e_get}; falling back to local", exc_info=True
        #                 )
        #             except Exception:
        #                 pass
        #         remote_timer.stop()
        #     except Exception as e:
        #         # 忽略远端异常，回退到本地模板
        #         remote_timer.stop()
        #         try:
        #             logger.warning(f"[PromptFetch] remote fetch failed: {e}; falling back to local", exc_info=True)
        #         except Exception:
        #             pass
        #         pass

        # 若配置了 md_file，则优先读取并渲染整份MD
        md_file = template.get("md_file") or template_parts.get("md_file")
        if md_file:
            try:
                base_dir = self.system_config_file.parent
                from pathlib import Path as _Path
                md_path = (base_dir / md_file).resolve()
                try:
                    logger.info(f"[PromptFetch] using local md template: {md_path}")
                except Exception:
                    pass
                
                # 读取本地MD模板计时
                with Timer("读取本地MD模板", parent="提示词") as read_timer:
                    text = md_path.read_text(encoding="utf-8")
                
                # 渲染模板计时
                with Timer("渲染模板变量", parent="提示词") as render_timer:
                    result = _with_project_desc(_safe_format(text, variables), variables)
                
                build_timer.stop()
                if generation_timer:
                    generation_timer.record(f"提示词构建(本地MD)-{template_id}", build_timer.duration, parent="提示词")
                logger.info(f"✅ 提示词构建完成(本地MD) [模板: {template_id}, 耗时: {build_timer.duration_str}, 长度: {len(result)}字符]")
                return result
            except Exception as e:
                # 失败则退回到分段模板渲染
                try:
                    logger.warning(f"[PromptFetch] reading local md template failed: {e}; will fallback to structured parts")
                except Exception:
                    pass
                pass

        # 构建提示词（分段拼接）
        with Timer("拼接提示词分段", parent="提示词") as concat_timer:
            prompt_parts = []

            # 添加头部（如果有）
            if "header" in template_parts:
                header = _safe_format(template_parts["header"], variables)
                if header:
                    prompt_parts.append(header)

            # 添加选择要求/提取要求/总结要求（如果有）
            for key in ["selection_requirements", "extraction_requirements", "summary_requirements"]:
                if key in template_parts:
                    requirements = _safe_format(template_parts[key], variables)
                    if requirements:
                        prompt_parts.append(requirements)

            # 添加文件列表头部（如果有）
            if "file_list_header" in template_parts:
                flh = template_parts["file_list_header"]
                if flh:
                    prompt_parts.append(flh)

            # 添加文件列表（如果有）
            if "file_list" in variables:
                file_list = variables["file_list"]
                if file_list:
                    prompt_parts.append(file_list)

            # 添加内容头部和内容（如果有）
            for key in ["content_header", "content"]:
                if key in template_parts:
                    content_part = _safe_format(template_parts[key], variables)
                    if content_part:
                        prompt_parts.append(content_part)

            # 添加尾部（如果有）
            if "footer" in template_parts:
                footer = template_parts["footer"]
                if footer:
                    prompt_parts.append(footer)

            result = _with_project_desc("\n\n".join(prompt_parts), variables)

        build_timer.stop()
        if generation_timer:
            generation_timer.record(f"提示词构建(本地分段)-{template_id}", build_timer.duration, parent="提示词")
        logger.info(f"✅ 提示词构建完成(本地分段) [模板: {template_id}, 耗时: {build_timer.duration_str}, 长度: {len(result)}字符]")
        return result
    
    # def build_file_list(self, template_id: str, index_rows: List[Dict[str, Any]]) -> str:
    #     """构建文件列表"""
    #     template = self.get_prompt_template(template_id)
    #     if not template:
    #         return ""
    #
    #     template_parts = template.get("template", {})
    #     file_list_item_template = template_parts.get("file_list_item", "{{index}}. {{filename}} - {{file_title}}")
    #
    #     def _render_double_brace_item(text: str, vars_dict: Dict[str, Any]) -> str:
    #         try:
    #             import re as _re
    #             pattern = _re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")
    #             return pattern.sub(lambda m: str((vars_dict or {}).get(m.group(1), "")), text or "")
    #         except Exception:
    #             return text or ""
    #
    #     file_list_items = []
    #     for i, row in enumerate(index_rows, 1):
    #         filename = row.get("filename", row.get("file", row.get("name", "")))
    #         file_title = row.get("title", "")
    #         output_type = row.get("output_type", "")
    #
    #         # 如果模板未指定，默认显示为：序号. 文件名 (类型) - 标题
    #         if not template_parts.get("file_list_item"):
    #             display = f"{i}. {filename}"
    #             if output_type:
    #                 display += f" ({output_type})"
    #             if file_title:
    #                 display += f" - {file_title}"
    #             file_list_items.append(display)
    #             continue
    #
    #         file_list_item = _render_double_brace_item(file_list_item_template, {
    #             "index": i,
    #             "filename": filename,
    #             "file_title": file_title,
    #             "output_type": output_type
    #         })
    #         file_list_items.append(file_list_item)
    #
    #     return "\n".join(file_list_items)
    #
    # def get_available_system_prompts(self) -> List[Dict[str, str]]:
    #     """获取所有可用的系统提示词"""
    #     system_prompts = []
    #     for prompt_id, prompt_data in self.system_config.get("system_prompts", {}).items():
    #         if isinstance(prompt_data, dict):
    #             system_prompts.append({
    #                 "id": prompt_id,
    #                 "content": prompt_data.get("content", ""),
    #                 "usage": prompt_data.get("usage", ""),
    #                 "location": prompt_data.get("location", ""),
    #                 "used_by": prompt_data.get("used_by", []),
    #                 "length": len(prompt_data.get("content", ""))
    #             })
    #         else:
    #             # 兼容旧格式
    #             system_prompts.append({
    #                 "id": prompt_id,
    #                 "content": prompt_data,
    #                 "usage": "",
    #                 "location": "",
    #                 "used_by": [],
    #                 "length": len(prompt_data)
    #             })
    #     return system_prompts
    #
    # def get_available_user_templates(self) -> List[Dict[str, str]]:
    #     """获取所有可用的用户提示词模板"""
    #     user_templates = []
    #     for template_id, template_data in self.system_config.get("user_prompt_templates", {}).items():
    #         if isinstance(template_data, dict):
    #             user_templates.append({
    #                 "id": template_id,
    #                 "content": template_data.get("content", ""),
    #                 "usage": template_data.get("usage", ""),
    #                 "variables": template_data.get("variables", []),
    #                 "length": len(template_data.get("content", ""))
    #             })
    #     return user_templates
    #
    # def get_template_variables(self, template_id: str) -> List[str]:
    #     """获取模板的变量列表"""
    #     template = self.get_user_prompt_template(template_id)
    #     if template:
    #         return template.get("variables", [])
    #     return []
    #
    # def add_system_prompt(self, prompt_id: str, content: str, usage: str = "", location: str = "", used_by: List[str] = None) -> bool:
    #     """添加系统提示词"""
    #     try:
    #         if "system_prompts" not in self.system_config:
    #             self.system_config["system_prompts"] = {}
    #
    #         self.system_config["system_prompts"][prompt_id] = {
    #             "content": content,
    #             "usage": usage,
    #             "location": location,
    #             "used_by": used_by or []
    #         }
    #
    #         # 保存到文件
    #         with open(self.system_config_file, 'w', encoding='utf-8') as f:
    #             json.dump(self.system_config, f, ensure_ascii=False, indent=2)
    #
    #         return True
    #     except Exception as e:
    #         print(f"添加系统提示词失败: {e}")
    #         return False
    #
    # def add_user_template(self, template_id: str, content: str, usage: str = "", variables: List[str] = None) -> bool:
    #     """添加用户提示词模板"""
    #     try:
    #         if "user_prompt_templates" not in self.system_config:
    #             self.system_config["user_prompt_templates"] = {}
    #
    #         self.system_config["user_prompt_templates"][template_id] = {
    #             "content": content,
    #             "usage": usage,
    #             "variables": variables or []
    #         }
    #
    #         # 保存到文件
    #         with open(self.system_config_file, 'w', encoding='utf-8') as f:
    #             json.dump(self.system_config, f, ensure_ascii=False, indent=2)
    #
    #         return True
    #     except Exception as e:
    #         print(f"添加用户提示词模板失败: {e}")
    #         return False
    #
    # def get_config_summary(self) -> Dict[str, Any]:
    #     """获取配置摘要"""
    #     return {
    #         "system_prompts_count": len(self.system_config.get("system_prompts", {})),
    #         "user_templates_count": len(self.system_config.get("user_prompt_templates", {})),
    #         "config_file": str(self.system_config_file),
    #         "last_modified": self.system_config_file.stat().st_mtime if self.system_config_file.exists() else 0
    #     }
    #
    # def print_summary(self):
    #     """打印配置摘要"""
    #     summary = self.get_config_summary()
    #     print("📋 系统提示词配置摘要:")
    #     print(f"  系统提示词: {summary['system_prompts_count']} 个")
    #     print(f"  用户提示词模板: {summary['user_templates_count']} 个")
    #     print(f"  配置文件: {summary['config_file']}")

# 创建全局实例
system_prompt_manager = SystemPromptManager()
