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
from utils.timing import Timer, generation_timer
from utils.context_manager import get_project_desc, get_combination_id

logger = logging.getLogger(__name__)

class SystemPromptManager:
    """系统提示词管理器"""
    
    def __init__(self, system_config_file: str = "service/prompts/system/system_prompts.json"):
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
            import traceback
            traceback.print_exc()
            raise Exception(f"加载系统提示词配置失败: {str(e)}")
    
    def get_prompt_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """获取提示词模板"""
        template_data = self.system_config.get("prompt_templates", {}).get(template_id, {})
        if template_data:
            return template_data
        return None
    
    
    # 系统/用户提示词分隔标记：标记之上为可缓存的静态系统指令，之下为每次变化的用户数据
    SYSTEM_USER_DELIMITER = "===USER_DATA==="

    def _split_system_user(self, content: str) -> tuple:
        """按分隔标记把渲染后的提示词拆成 (system, user)。无标记时 system 为空、user 为整段。"""
        if not content or self.SYSTEM_USER_DELIMITER not in content:
            return "", content
        system, _, user = content.partition(self.SYSTEM_USER_DELIMITER)
        return system.rstrip("\n").rstrip(), user.lstrip("\n").rstrip()

    def build_messages(self, template_id: str, variables: Dict[str, Any]) -> Dict[str, str]:
        """构建 system / user 双段提示词。

        返回 {"system": str, "user": str}：system 为分隔标记之上的静态指令（可缓存前缀），
        user 为之下的动态数据。模板未使用分隔标记时 system 为空、user 为整段渲染结果。
        """
        system, user = self._split_system_user(self.build_prompt(template_id, variables))
        return {"system": system, "user": user}

    def build_prompt(self, template_id: str, variables: Dict[str, Any]) -> str:
        """构建完整的提示词"""
        # 开始计时
        build_timer = Timer(f"构建提示词({template_id})", parent="提示词")
        build_timer.start()
        
        template = self.get_prompt_template(template_id)
        if not template:
            build_timer.stop()
            return ""

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

        # 注入项目背景变量（若缺失则从上下文回退）
        try:
            if not (isinstance((variables or {}).get("project_desc", ""), str) and (variables or {}).get("project_desc", "").strip()):
                variables = dict(variables or {})
                variables["project_desc"] = get_project_desc()
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
            combination_id = str(get_combination_id()).strip()
        except Exception:
            combination_id = ""
        if not combination_id:
            try:
                logger.debug(f"[提示词获取] 未设置 CURRENT_COMBINATION_ID，使用本地模板: {template_id}")
            except Exception:
                pass
        if combination_id:
            # 远程提示词获取计时
            remote_timer = Timer(f"远程获取提示词({template_id})", parent="提示词")
            remote_timer.start()
            try:
                base_url = os.getenv("CALLBACK_BASE_URL")
                service_url = base_url +"/ky/sys/projectPromptDetailTable/findByCombinationAndUsedBy"
                # 先尝试 POST（表单），失败或空则回退 GET
                try:
                    logger.info(
                        f"[提示词获取] POST 请求中 usedBy={template_id} combinationId={combination_id} url={service_url}"
                    )
                    post_timer = Timer("POST请求提示词", parent="远程提示词")
                    post_timer.start()
                    resp_post = requests.post(
                        service_url,
                        data={
                            "combinationId": combination_id,
                            "usedBy": template_id,
                        },
                        timeout=10,
                    )
                    post_timer.stop()
                    logger.info(f"[提示词获取] POST 响应状态码={resp_post.status_code} [耗时: {post_timer.duration_str}]")
                    if resp_post.status_code == 200:
                        data = resp_post.json() if hasattr(resp_post, "json") else None
                        if isinstance(data, dict):
                            result = data.get("result") or {}
                            content = result.get("promptContent")
                            try:
                                logger.info(
                                    f"[提示词获取] 远程 promptContent 长度={len(content) if isinstance(content, str) else 0}"
                                )
                            except Exception:
                                pass
                            if isinstance(content, str) and content.strip():
                                remote_timer.stop()
                                build_timer.stop()
                                if generation_timer:
                                    generation_timer.record(f"提示词构建(远程POST)-{template_id}", build_timer.duration, parent="提示词")
                                logger.info(f"✅ 提示词构建完成(远程POST) [模板: {template_id}, 耗时: {build_timer.duration_str}]")
                                return _with_project_desc(_safe_format(content, variables), variables)
                            else:
                                try:
                                    logger.info(
                                        f"[提示词获取] POST 返回空 promptContent，尝试 GET 回退: {template_id}"
                                    )
                                except Exception:
                                    pass
                    else:
                        try:
                            logger.warning(
                                f"[提示词获取] POST 非 200 状态码: {resp_post.status_code}；尝试 GET 回退"
                            )
                        except Exception:
                            pass
                except Exception as e_post:
                    try:
                        logger.warning(
                            f"[提示词获取] POST 请求失败: {e_post}；尝试 GET 回退", exc_info=True
                        )
                    except Exception:
                        pass

                # GET 回退
                try:
                    logger.info(
                        f"[提示词获取] GET 请求中 usedBy={template_id} combinationId={combination_id} url={service_url}"
                    )
                    get_timer = Timer("GET请求提示词", parent="远程提示词")
                    get_timer.start()
                    resp_get = requests.get(
                        service_url,
                        params={
                            "combinationId": combination_id,
                            "usedBy": template_id,
                        },
                        timeout=10,
                    )
                    get_timer.stop()
                    logger.info(f"[提示词获取] GET 响应状态码={resp_get.status_code} [耗时: {get_timer.duration_str}]")
                    if resp_get.status_code == 200:
                        data = resp_get.json() if hasattr(resp_get, "json") else None
                        if isinstance(data, dict):
                            result = data.get("result") or {}
                            content = result.get("promptContent")
                            try:
                                logger.info(
                                    f"[提示词获取] 远程 promptContent 长度={len(content) if isinstance(content, str) else 0}"
                                )
                            except Exception:
                                pass
                            if isinstance(content, str) and content.strip():
                                remote_timer.stop()
                                build_timer.stop()
                                if generation_timer:
                                    generation_timer.record(f"提示词构建(远程GET)-{template_id}", build_timer.duration, parent="提示词")
                                logger.info(f"✅ 提示词构建完成(远程GET) [模板: {template_id}, 耗时: {build_timer.duration_str}]")
                                return _with_project_desc(_safe_format(content, variables), variables)
                            else:
                                try:
                                    logger.info(
                                        f"[提示词获取] promptContent 为空，将回退到本地模板: {template_id}"
                                    )
                                except Exception:
                                    pass
                    else:
                        try:
                            logger.warning(
                                f"[提示词获取] GET 非 200 状态码: {resp_get.status_code}；将回退到本地模板"
                            )
                        except Exception:
                            pass
                except Exception as e_get:
                    try:
                        logger.warning(
                            f"[提示词获取] GET 请求失败: {e_get}；回退到本地模板", exc_info=True
                        )
                    except Exception:
                        pass
                remote_timer.stop()
            except Exception as e:
                # 忽略远端异常，回退到本地模板
                remote_timer.stop()
                try:
                    logger.warning(f"[提示词获取] 远程获取失败: {e}；回退到本地模板", exc_info=True)
                except Exception:
                    pass
                pass

        # 若配置了 md_file，则优先读取并渲染整份MD
        md_file = (template.get("template") or {}).get("md_file")
        if md_file:
            try:
                base_dir = self.system_config_file.parent
                from pathlib import Path as _Path
                md_path = (base_dir / md_file).resolve()
                try:
                    logger.info(f"[提示词获取] 使用本地 MD 模板: {md_path}")
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
            except OSError as e:
                # 本地 MD 读取失败：当前所有模板均以 md_file 为唯一来源，无分段兜底，
                # 此处返回空串（与历史行为一致），由上层感知并处理。
                try:
                    logger.error(f"[提示词获取] 读取本地 MD 模板失败: {e}；返回空串", exc_info=True)
                except Exception:
                    pass
                build_timer.stop()
                return ""

        # 未配置 md_file：无可用模板来源，返回空串
        build_timer.stop()
        logger.warning(f"[提示词获取] 模板未配置 md_file，返回空串: {template_id}")
        return ""
    

# 创建全局实例
system_prompt_manager = SystemPromptManager()
