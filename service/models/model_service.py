"""
模型服务模块

功能说明：
- 提供统一的LLM（大语言模型）和视觉模型服务接口
- 实现请求限流和错误重试机制
- 支持DashScope（阿里云）兼容OpenAI格式的API
- 提供并发控制和QPS限制

主要特性：
1. 单例模式管理配置，避免重复初始化
2. 智能限流：支持并发数和QPS双重限制
3. 自动重试：处理429（限流）和网络错误
4. 配置优先级：环境变量 > 配置文件 > 默认值

技术实现：
- 使用requests库进行HTTP调用
- 使用信号量控制并发数
- 使用时间窗口队列控制QPS
- 支持流式输出（Server-Sent Events）
"""

from __future__ import annotations

import os
import base64
import json
import logging
import time
import threading
import asyncio
import functools
import random
import uuid
from pathlib import Path
from collections import deque
from typing import Any, Dict, Optional, List, Union

import requests
from requests.exceptions import RequestException, ProxyError, SSLError, ConnectionError, ReadTimeout, HTTPError

# 导入耗时记录工具
from utils.timing import Timer, model_timer
from utils.output_manager import save_json


# ========== 单例配置管理 ==========
class _Singleton:
    """
    单例配置类
    
    使用类变量存储全局配置，避免重复初始化。
    包含LLM和视觉模型的配置信息。
    """
    llm_cfg: Dict[str, Any] = {}      # LLM服务配置
    vision_cfg: Dict[str, Any] = {}   # 视觉模型服务配置
    initialized: bool = False          # 是否已初始化


def _init_once() -> None:
    """
    初始化配置（单例模式，只执行一次）
    
    配置优先级：
    1. 环境变量（QWEN_API_BASE, DASHSCOPE_API_KEY等）
    2. 配置文件（通过get_settings获取）
    3. 默认值
    
    支持的LLM服务：
    - DashScope（阿里云通义千问），兼容OpenAI API格式
    - 默认模型：qwen3.6-flash
    """
    if _Singleton.initialized:
        return
    
    # ========== LLM配置初始化 ==========
    # Qwen (DashScope 兼容 OpenAI 接口)
    try:
        from config import get_settings
        _cfg_default = get_settings()
    except Exception:
        _cfg_default = None

    # API基础URL（优先级：环境变量 > 配置 > 默认值）
    api_base = (
        os.getenv("QWEN_API_BASE")
        or os.getenv("DASHSCOPE_API_BASE")
        or (getattr(_cfg_default, "llm_api_base", None) or "https://dashscope.aliyuncs.com/compatible-mode/v1")
    )
    # API密钥（优先级：环境变量 > 配置 > 空）
    api_key = (
        os.getenv("DASHSCOPE_API_KEY")
        or (getattr(_cfg_default, "llm_api_key", None) or "")
    )
    # 模型名称（优先级：环境变量 > 配置 > 默认值）
    model_name = (
        os.getenv("QWEN_MODEL")
        or (getattr(_cfg_default, "llm_model", None) or "qwen3.6-flash")
    )
    # 超时时间（默认300秒）
    try:
        timeout = int(os.getenv("QWEN_TIMEOUT", str(getattr(_cfg_default, "llm_timeout", 300))))
    except Exception:
        timeout = 300

    # 保存LLM配置
    _Singleton.llm_cfg = {
        "api_base": api_base,
        "api_key": api_key,
        "model": model_name,
        "timeout": timeout,
    }
    
    # ========== 视觉模型配置初始化 ==========
    # 视觉 HTTP 服务（本地部署）
    _Singleton.vision_cfg = {
        "endpoint": os.getenv("VISION_HTTP_ENDPOINT", "http://120.195.112.10:8001"),
        "api_key": os.getenv("VISION_HTTP_KEY", ""),
        "timeout": int(os.getenv("VISION_TIMEOUT", "120")),
    }
    _Singleton.initialized = True


# ========== 请求限流器 ==========
# 预防性限流（并发/QPS + 429退避）
class _RateLimiter:
    """
    请求限流器
    
    功能：
    1. 控制并发请求数（通过信号量）
    2. 控制每秒请求数（QPS，通过时间窗口队列）
    3. 防止API限流错误（429）
    
    配置方式：
    - LLM_MAX_CONCURRENCY: 最大并发数（默认4）
    - LLM_MAX_QPS: 最大QPS（默认3.0）
    """
    def __init__(self, max_concurrency: Optional[int] = None, max_qps: Optional[float] = None) -> None:
        """初始化限流器

        Args:
            max_concurrency: 最大并发数，None时读全局环境变量LLM_MAX_CONCURRENCY
            max_qps: 最大QPS，None时读全局环境变量LLM_MAX_QPS
        """
        # 可配置并发与QPS（支持传入自定义值，用于按模型限流）
        if max_concurrency is not None:
            self.max_concurrency = max(1, max_concurrency)
        else:
            try:
                self.max_concurrency = max(1, int(os.getenv("LLM_MAX_CONCURRENCY", "4")))
            except Exception:
                self.max_concurrency = 4
        if max_qps is not None:
            self.max_qps = float(max_qps)
        else:
            try:
                self.max_qps = float(os.getenv("LLM_MAX_QPS", "3"))
            except Exception:
                self.max_qps = 3.0

        # 并发控制：使用信号量限制同时进行的请求数
        self._sem = threading.Semaphore(self.max_concurrency)
        # QPS控制：使用锁保护时间戳队列
        self._lock = threading.Lock()
        # 最近1秒内的请求时间戳队列（用于QPS控制）
        self._recent: deque[float] = deque()

    def _acquire_qps(self) -> None:
        """
        获取QPS许可
        
        使用滑动时间窗口算法：
        1. 维护最近1秒内的请求时间戳队列
        2. 如果队列长度小于max_qps，允许请求
        3. 否则等待最早的请求过期（1秒后）
        """
        # max_qps <= 0 视为不限制
        if self.max_qps <= 0:
            return
        while True:
            with self._lock:
                now = time.time()
                # 清理超过1秒的时间戳
                while self._recent and (now - self._recent[0]) >= 1.0:
                    self._recent.popleft()
                # 如果当前QPS未超限，允许请求
                if len(self._recent) < int(self.max_qps + 1e-6):
                    self._recent.append(now)
                    return
                # 需要等待至最早一条记录过期
                earliest = self._recent[0]
                wait = max(0.0, 1.0 - (now - earliest))
            # 等待时间不超过0.25秒，避免长时间阻塞
            time.sleep(min(wait, 0.25))

    def acquire(self) -> None:
        """
        获取请求许可（先QPS，后并发）
        
        流程：
        1. 先检查QPS限制
        2. 再检查并发限制
        """
        # 先确保QPS，再抢占并发位
        self._acquire_qps()
        self._sem.acquire()

    def release(self) -> None:
        """
        释放请求许可
        
        释放信号量，允许下一个等待的请求继续。
        """
        try:
            self._sem.release()
        except Exception:
            # 如果信号量已满，忽略错误
            pass


# ========== 按模型独立配置加载 ==========
_MODEL_CONFIGS: Optional[Dict[str, Dict]] = None
_MODEL_CONFIGS_LOCK = threading.Lock()


def _load_model_configs() -> Dict[str, Dict]:
    """加载按模型的独立配置（限流/思考模式/超时/重试等）

    优先级：环境变量 LLM_MODEL_CONFIGS_FILE > config settings > 内联JSON
    """
    import logging
    _logger = logging.getLogger(__name__)

    # 1. 优先：独立配置文件
    file_path = os.getenv("LLM_MODEL_CONFIGS_FILE", "")
    if not file_path:
        try:
            from config import get_settings
            file_path = getattr(get_settings(), "llm_model_configs_file", "")
        except Exception:
            file_path = ""
    if file_path:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                configs = json.load(f)
            _logger.info(f"✅ 加载模型配置文件: {file_path}")
            return configs if isinstance(configs, dict) else {}
        except FileNotFoundError:
            _logger.warning(f"⚠️ 模型配置文件不存在: {file_path}，回退到默认")
        except json.JSONDecodeError as e:
            _logger.error(f"❌ 模型配置文件JSON解析失败: {file_path}: {e}")

    # 2. 回退：内联JSON环境变量
    raw = os.getenv("LLM_MODEL_CONFIGS", "")
    if not raw:
        try:
            from config import get_settings
            raw = getattr(get_settings(), "llm_model_configs", "")
        except Exception:
            raw = ""
    if not raw:
        return {}
    try:
        configs = json.loads(raw)
        return configs if isinstance(configs, dict) else {}
    except Exception:
        _logger.warning("⚠️ LLM_MODEL_CONFIGS 解析失败，回退到全局默认")
        return {}


def _model_config(model_name: str) -> Dict[str, Any]:
    """获取指定模型的完整配置（未配置的模型返回空dict，调用方回退到全局默认）"""
    global _MODEL_CONFIGS
    if _MODEL_CONFIGS is None:
        with _MODEL_CONFIGS_LOCK:
            if _MODEL_CONFIGS is None:
                _MODEL_CONFIGS = _load_model_configs()
    return _MODEL_CONFIGS.get(model_name, {})


# ========== 按模型限流管理器 ==========
class _ModelRateLimiterManager:
    """按模型名称管理独立的限流器实例"""

    def __init__(self) -> None:
        self._limiters: Dict[str, _RateLimiter] = {}
        self._lock = threading.Lock()

    def get(self, model_name: str) -> _RateLimiter:
        """获取指定模型的限流器（懒创建，缓存复用）"""
        if model_name in self._limiters:
            return self._limiters[model_name]

        with self._lock:
            if model_name in self._limiters:
                return self._limiters[model_name]

            # 从模型配置中提取限流参数
            cfg = _model_config(model_name)
            rl_cfg = cfg.get("rate_limit", {}) if cfg else {}
            qps = rl_cfg.get("qps") if isinstance(rl_cfg, dict) else None
            concurrency = rl_cfg.get("concurrency") if isinstance(rl_cfg, dict) else None

            if qps is not None or concurrency is not None:
                limiter = _RateLimiter(max_concurrency=concurrency, max_qps=qps)
                logging.getLogger(__name__).info(
                    f"创建模型专属限流器: {model_name} "
                    f"(QPS={limiter.max_qps}, 并发={limiter.max_concurrency})"
                )
            else:
                # 回退到全局默认值（读LLM_MAX_QPS/LLM_MAX_CONCURRENCY）
                limiter = _RateLimiter()
                logging.getLogger(__name__).info(
                    f"创建默认限流器: {model_name} "
                    f"(QPS={limiter.max_qps}, 并发={limiter.max_concurrency})"
                )

            self._limiters[model_name] = limiter
            return limiter

    def stats(self) -> Dict[str, Any]:
        """返回所有限流器配置信息（用于调试/监控）"""
        result = {}
        for name, limiter in self._limiters.items():
            result[name] = {
                "qps": limiter.max_qps,
                "concurrency": limiter.max_concurrency,
            }
        return result


_RATE_LIMITER_MANAGER: Optional[_ModelRateLimiterManager] = None


def _rate_limiter_for(model_name: str) -> _RateLimiter:
    """获取指定模型的限流器实例（按模型独立限流）"""
    global _RATE_LIMITER_MANAGER
    if _RATE_LIMITER_MANAGER is None:
        _RATE_LIMITER_MANAGER = _ModelRateLimiterManager()
    return _RATE_LIMITER_MANAGER.get(model_name)


def _rate_limiter_stats() -> Dict[str, Any]:
    """返回限流器统计信息"""
    global _RATE_LIMITER_MANAGER
    if _RATE_LIMITER_MANAGER is None:
        _RATE_LIMITER_MANAGER = _ModelRateLimiterManager()
    return _RATE_LIMITER_MANAGER.stats()


# ========== 模型配置应用辅助函数 ==========
def _apply_model_config(model_name: str,
                        temperature: Optional[float],
                        max_tokens: Optional[int],
                        extra: Optional[Dict[str, Any]],
                        timeout: Optional[int]) -> Dict[str, Any]:
    """应用模型配置默认值（调用方显式传参优先）

    合并规则：调用方显式参数 > 模型配置默认值

    Returns:
        dict: 包含合并后的 temperature, max_tokens, extra, timeout
    """
    cfg = _model_config(model_name)
    if not cfg:
        return {
            "temperature": temperature,
            "max_tokens": max_tokens,
            "extra": extra or {},
            "timeout": timeout,
        }

    # temperature: 调用方未传时用配置默认
    final_temp = temperature if temperature is not None else cfg.get("temperature")

    # max_tokens: 调用方未传时用配置默认（null表示不限制）
    if max_tokens is not None:
        final_max_tokens = max_tokens
    else:
        final_max_tokens = cfg.get("max_tokens")

    # timeout: 调用方未传时用配置默认
    final_timeout = timeout if timeout is not None else cfg.get("timeout")

    # extra: 合并配置中的 enable_thinking 等（调用方extra优先，不覆盖）
    final_extra: Dict[str, Any] = {}
    if "enable_thinking" in cfg:
        final_extra["enable_thinking"] = cfg["enable_thinking"]
    if extra:
        final_extra.update(extra)  # 调用方的extra覆盖配置默认

    return {
        "temperature": final_temp,
        "max_tokens": final_max_tokens,
        "extra": final_extra,
        "timeout": final_timeout,
    }


def _get_model_retry_config(model_name: str, default_retries: int, default_backoff: float) -> tuple:
    """获取模型的重试配置（环境变量优先 > 模型配置 > 默认值）"""
    cfg = _model_config(model_name)
    retry_cfg = cfg.get("retry", {}) if cfg else {}
    if not isinstance(retry_cfg, dict):
        retry_cfg = {}

    # 环境变量优先
    try:
        max_retries = int(os.getenv("LLM_RETRY_MAX", str(retry_cfg.get("max_retries", default_retries))))
    except Exception:
        max_retries = default_retries
    try:
        backoff = float(os.getenv("LLM_RETRY_BACKOFF", str(retry_cfg.get("backoff", default_backoff))))
    except Exception:
        backoff = default_backoff

    return max_retries, backoff


def ensure_ready() -> Dict[str, Any]:
    """返回当前服务配置与可用性信息。"""
    _init_once()
    # 确保模型配置已加载
    global _MODEL_CONFIGS
    if _MODEL_CONFIGS is None:
        with _MODEL_CONFIGS_LOCK:
            if _MODEL_CONFIGS is None:
                _MODEL_CONFIGS = _load_model_configs()
    return {
        "llm_ready": bool(_Singleton.llm_cfg.get("api_key")),
        "vision_ready": bool(_Singleton.vision_cfg.get("endpoint")),
        "llm_cfg": {k: ("***" if k == "api_key" and v else v) for k, v in _Singleton.llm_cfg.items()},
        "vision_cfg": {k: ("***" if k == "api_key" and v else v) for k, v in _Singleton.vision_cfg.items()},
        "model_configs_loaded": bool(_MODEL_CONFIGS),
        "rate_limiter_stats": _rate_limiter_stats(),
    }


def set_llm_model(model_name: str) -> None:
    """切换当前LLM模型名称（仅影响后续调用）。"""
    _init_once()
    if model_name:
        _Singleton.llm_cfg["model"] = model_name


def generate_raw(prompt: str,
             system: Optional[str] = None,
             messages: Optional[List[Dict[str, str]]] = None,
             temperature: Optional[float] = None,
             max_tokens: Optional[int] = None,
             extra: Optional[Dict[str, Any]] = None,
             model: Optional[str] = None,
             skip_rate_limit: Optional[bool] = None,
             rate_limit_category: Optional[str] = None) -> Dict[str, Any]:
    """调用文本模型并返回原始响应(JSON)。"""
    _init_once()
    cfg = _Singleton.llm_cfg
    if not cfg.get("api_key"):
        return {"error": "LLM未配置: 请设置 DASHSCOPE_API_KEY"}

    url = f"{cfg['api_base'].rstrip('/')}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cfg['api_key']}"
    }

    msgs: List[Dict[str, str]] = []
    if system:
        msgs.append({"role": "system", "content": system})
    if messages:
        msgs.extend(messages)
    else:
        msgs.append({"role": "user", "content": prompt})

    payload: Dict[str, Any] = {
        "model": (model or cfg["model"]),
        "messages": msgs,
    }
    if temperature is not None:
        payload["temperature"] = temperature
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    if extra:
        payload.update(extra)

    import logging
    _logger = logging.getLogger(__name__)
    
    # 开始计时
    model_name = model or cfg["model"]
    prompt_len = len(prompt)
    api_timer = Timer(f"LLM API调用({model_name})", parent="模型生成")
    api_timer.start()
    
    # 应用模型配置默认值（超时等，调用方传参优先）
    _resolved_timeout = _apply_model_config(model_name, None, None, None, None)["timeout"] or cfg["timeout"]

    def _post_with_retry(max_retries: int = 3, backoff: float = 1.5):
        last_err: Optional[Exception] = None
        # 重试策略：环境变量 > 模型配置 > 默认值
        max_retries, backoff = _get_model_retry_config(model_name, max_retries, backoff)

        for attempt in range(1, max_retries + 1):
            rl = _rate_limiter_for(model_name)
            _skip_rl = bool(skip_rate_limit)
            
            # 等待限流计时
            wait_timer = Timer("等待限流", parent="模型生成")
            wait_timer.start()
            if not _skip_rl:
                rl.acquire()
            wait_timer.stop()
            if wait_timer.duration > 0.1:  # 超过100ms才记录
                _logger.info(f"⏱️ 限流等待: {wait_timer.duration_str}")
            
            try:
                # HTTP请求计时
                http_timer = Timer("HTTP请求", parent="模型生成")
                http_timer.start()
                r = requests.post(url, headers=headers, json=payload, timeout=_resolved_timeout)
                http_timer.stop()
                
                # 特判429，遵循 Retry-After
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    try:
                        wait_sec = float(retry_after) if retry_after is not None else (backoff ** attempt)
                    except Exception:
                        wait_sec = backoff ** attempt
                    wait_sec = min(wait_sec, 10.0)  # 上限10秒防止过长阻塞
                    _logger.warning(f"⏱️ 遇到429限流，等待 {wait_sec:.2f}s 后重试 (attempt {attempt})")
                    # 释放并发名额后再等待
                    if not _skip_rl:
                        rl.release()
                    time.sleep(wait_sec + random.uniform(0, 0.25))
                    # 继续下一次尝试
                    last_err = HTTPError(f"429 Too Many Requests (attempt {attempt})")
                    continue

                # 记录 4xx/5xx 错误的详细响应内容
                if r.status_code >= 400:
                    try:
                        error_body = r.text[:1000]
                        _logger.error(f"API 错误响应 [{r.status_code}]: {error_body}")
                        _logger.error(f"请求 payload 大小: prompt约{len(prompt)}字符, max_tokens={max_tokens}, model={model or cfg['model']}")
                    except Exception:
                        pass

                r.raise_for_status()
                
                # 记录成功的HTTP耗时
                _logger.info(f"⏱️ HTTP请求完成: {http_timer.duration_str}, prompt约{prompt_len}字符")
                
                return r.json()
            except (ProxyError, SSLError, ConnectionError, ReadTimeout, RequestException) as e:
                last_err = e
                _logger.warning(f"⏱️ 请求失败(attempt {attempt}): {type(e).__name__}")
                if attempt >= max_retries:
                    raise
                # 常规退避（带轻微抖动）
                sleep_s = (backoff ** attempt) + random.uniform(0, 0.25)
                time.sleep(min(sleep_s, 10.0))
            finally:
                # 正常响应或异常都会释放并发位（若启用限流）
                if not _skip_rl:
                    try:
                        rl.release()
                    except Exception:
                        pass
        # 理论上不会到这
        if last_err:
            raise last_err

    try:
        result = _post_with_retry()
        api_timer.stop()
        
        # 记录到全局计时器
        if model_timer:
            model_timer.record(f"LLM生成({model_name})", api_timer.duration, parent="模型调用", 
                              metadata={"prompt_len": prompt_len, "max_tokens": max_tokens})
        
        _logger.info(f"✅ LLM API调用完成 [模型: {model_name}, 耗时: {api_timer.duration_str}, prompt: {prompt_len}字符]")
        return result
    except Exception as e:
        import traceback
        traceback.print_exc()
        api_timer.stop()
        _logger.error(f"❌ LLM API调用失败 [模型: {model_name}, 耗时: {api_timer.duration_str}]: {e}", exc_info=True)
        raise


def generate(prompt: str,
             system: Optional[str] = None,
             messages: Optional[List[Dict[str, str]]] = None,
             temperature: Optional[float] = None,
             max_tokens: Optional[int] = None,
             extra: Optional[Dict[str, Any]] = None,
             model: Optional[str] = None,
             skip_rate_limit: Optional[bool] = None,
             rate_limit_category: Optional[str] = None,
             timeout: Optional[int] = None) -> str:
    """调用 Qwen 文本模型（DashScope 兼容 OpenAI Chat Completions）。"""
    _init_once()
    cfg = _Singleton.llm_cfg
    if not cfg.get("api_key"):
        return "[LLM未配置: 请设置 DASHSCOPE_API_KEY]"

    import logging
    _logger = logging.getLogger(__name__)
    # 原始响应存储路径
    output_dir =  Path(os.getenv("OUTPUT_DIR"))
    RAW_RESPONSE_SAVE_DIR: Optional[str] = output_dir / "Model_response"
    task_id = uuid.uuid4().hex[:8]
    # 开始计时
    model_name = model or cfg["model"]
    # 应用模型配置默认值（超时/思考模式/温度等，调用方传参优先）
    _resolved = _apply_model_config(model_name, temperature, max_tokens, extra, timeout)
    request_timeout = _resolved["timeout"] or cfg["timeout"]
    prompt_len = len(prompt)
    gen_timer = Timer(f"文本生成({model_name})", parent="模型生成")
    gen_timer.start()

    url = f"{cfg['api_base'].rstrip('/')}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cfg['api_key']}"
    }

    # 构造 messages
    msgs: List[Dict[str, str]] = []
    if system:
        msgs.append({"role": "system", "content": system})
    if messages:
        msgs.extend(messages)
    else:
        msgs.append({"role": "user", "content": prompt})

    payload: Dict[str, Any] = {
        "model": (model or cfg["model"]),
        "messages": msgs,
    }
    # 应用模型配置默认值（调用方显式参数已在_apply_model_config中保留优先）
    if _resolved["temperature"] is not None:
        payload["temperature"] = _resolved["temperature"]
    if _resolved["max_tokens"] is not None:
        payload["max_tokens"] = _resolved["max_tokens"]
    if _resolved["extra"]:
        payload.update(_resolved["extra"])
    raw_payload = payload.copy()
    def _post_with_retry(max_retries: int = 3, backoff: float = 1.5):
        last_err: Optional[Exception] = None
        # 重试策略：环境变量 > 模型配置 > 默认值
        max_retries, backoff = _get_model_retry_config(model_name, max_retries, backoff)

        for attempt in range(1, max_retries + 1):
            rl = _rate_limiter_for(model_name)
            _skip_rl = bool(skip_rate_limit)
            if not _skip_rl:
                rl.acquire()
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=request_timeout)
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    try:
                        wait_sec = float(retry_after) if retry_after is not None else (backoff ** attempt)
                    except Exception:
                        wait_sec = backoff ** attempt
                    wait_sec = min(wait_sec, 10.0)
                    _logger.warning(f"⏱️ 遇到429限流，等待 {wait_sec:.2f}s 后重试 (attempt {attempt})")
                    if not _skip_rl:
                        rl.release()
                    time.sleep(wait_sec + random.uniform(0, 0.25))
                    last_err = HTTPError(f"429 Too Many Requests (attempt {attempt})")
                    continue

                # 记录 4xx/5xx 错误的详细响应内容
                if r.status_code >= 400:
                    try:
                        error_body = r.text[:1000]  # 限制长度避免日志过大
                        _logger.error(f"API 错误响应 [{r.status_code}]: {error_body}")
                        _logger.error(f"请求 payload 大小: prompt约{len(prompt)}字符, max_tokens={max_tokens}, model={model or cfg['model']}")
                    except Exception:
                        pass
                
                r.raise_for_status()
                return r.json()
            except (ProxyError, SSLError, ConnectionError, ReadTimeout, RequestException) as e:
                last_err = e
                _logger.warning(f"⏱️ 请求失败(attempt {attempt}): {type(e).__name__}")
                if attempt >= max_retries:
                    raise
                sleep_s = (backoff ** attempt) + random.uniform(0, 0.25)
                time.sleep(min(sleep_s, 10.0))
            finally:
                if not _skip_rl:
                    try:
                        rl.release()
                    except Exception:
                        pass
        if last_err:
            raise last_err

    # 保存原始响应到本地的辅助函数
    def _save_raw_response(data: dict, turn: int) -> None:
        if not RAW_RESPONSE_SAVE_DIR:
            return
        try:
            filename = f"llm_raw_{task_id}_turn{turn}.json"
            filepath = os.path.join(RAW_RESPONSE_SAVE_DIR, filename)
            save_json(filepath, data)
            _logger.debug(f"💾 原始响应已保存: {filepath}")
        except Exception as save_err:
            _logger.warning(f"⚠️ 原始响应保存失败: {save_err}")

    try:
        accumulated_content: str = ""
        turn = 0
        data: dict = {}

        while True:
            turn += 1
            data = _post_with_retry()

            # 保存本次原始响应
            _save_raw_response(data, turn)

            choice = data["choices"][0]
            chunk: str = choice["message"]["content"]
            finish_reason: str = choice.get("finish_reason")

            accumulated_content += chunk

            if finish_reason in ("length", None):
                if not chunk.strip():
                    _logger.warning("⚠️ content 为空，跳过续写")
                    break

                _logger.info(f"第{turn}轮因长度中断，启动续写 (已累计 {len(accumulated_content)} 字符)")

                payload["messages"] = raw_payload["messages"] + [
                    {
                        "role": "assistant",
                        "content": accumulated_content,
                        "partial": True
                    }
                ]

                # 关闭思考模式
                if turn >= 1:
                    payload["enable_thinking"] = False

                continue

            # finish_reason == "stop"（或其他终止原因）→ 退出循环
            break

        gen_timer.stop()
        
        # 记录到全局计时器
        if model_timer:
            model_timer.record(
                f"文本生成({model_name})", gen_timer.duration, parent="模型调用",
                metadata={"prompt_len": prompt_len, "max_tokens": max_tokens, "turns": turn}
            )

        content_len = len(accumulated_content)
        _logger.info(
            f"✅ 文本生成完成 [模型: {model_name}, 耗时: {gen_timer.duration_str}, "
            f"轮次: {turn}, 输入: {prompt_len}字符, 输出: {content_len}字符]"
        )
        return accumulated_content

    except Exception as e:
        import traceback
        traceback.print_exc()
        gen_timer.stop()
        _logger.error(f"❌ 文本生成失败 [模型: {model_name}, 耗时: {gen_timer.duration_str}]: {e}")
        if isinstance(e, KeyError):
            return json.dumps(data, ensure_ascii=False)
        raise


def stream_generate(prompt: str,
                    system: Optional[str] = None,
                    messages: Optional[List[Dict[str, str]]] = None,
                    temperature: Optional[float] = None,
                    max_tokens: Optional[int] = None,
                    extra: Optional[Dict[str, Any]] = None,
                    model: Optional[str] = None,
                    skip_rate_limit: Optional[bool] = None,
                    rate_limit_category: Optional[str] = None):
    """流式调用文本模型，逐块产出内容（生成器）。

    兼容 OpenAI Chat Completions 风格：payload.stream=true，响应为 SSE（data: ...）。
    每个 data JSON 中优先解析 choices[].delta.content，其次 choices[].message.content，再兜底 output_text/text/content。
    """
    _init_once()
    cfg = _Singleton.llm_cfg
    if not cfg.get("api_key"):
        # 未配置时直接结束（避免抛错中断主流程）
        return

    model_name = model or cfg["model"]
    # 应用模型配置默认值（超时/思考模式/温度等，调用方传参优先）
    _resolved = _apply_model_config(model_name, temperature, max_tokens, extra, None)
    _resolved_timeout = _resolved["timeout"] or cfg["timeout"]

    url = f"{cfg['api_base'].rstrip('/')}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cfg['api_key']}"
    }

    msgs: List[Dict[str, str]] = []
    if system:
        msgs.append({"role": "system", "content": system})
    if messages:
        msgs.extend(messages)
    else:
        msgs.append({"role": "user", "content": prompt})

    payload: Dict[str, Any] = {
        "model": (model or cfg["model"]),
        "messages": msgs,
        "stream": True,
    }
    if _resolved["temperature"] is not None:
        payload["temperature"] = _resolved["temperature"]
    if _resolved["max_tokens"] is not None:
        payload["max_tokens"] = _resolved["max_tokens"]
    if _resolved["extra"]:
        payload.update(_resolved["extra"])

    rl = _rate_limiter_for(model_name)
    _skip_rl = bool(skip_rate_limit)
    if not _skip_rl:
        rl.acquire()
    try:
        with requests.post(url, headers=headers, json=payload, timeout=_resolved_timeout, stream=True) as r:
            r.raise_for_status()
            for raw_line in r.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue
                line = raw_line.strip()
                if not line:
                    continue
                if line.startswith("data:"):
                    line = line[5:].strip()
                if not line or line == "[DONE]":
                    if line == "[DONE]":
                        break
                    continue
                # 解析 JSON
                try:
                    obj = json.loads(line)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    # 非JSON行忽略（保持健壮）
                    continue
                chunk_parts: List[str] = []
                try:
                    choices = obj.get("choices") or []
                    for ch in choices:
                        if not isinstance(ch, dict):
                            continue
                        delta = ch.get("delta") or ch.get("message") or {}
                        if isinstance(delta, dict):
                            ct = delta.get("content")
                            if isinstance(ct, str) and ct:
                                chunk_parts.append(ct)
                        elif isinstance(delta, str) and delta:
                            chunk_parts.append(delta)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    pass
                if not chunk_parts:
                    # 兜底字段
                    for key in ("output_text", "text", "content"):
                        v = obj.get(key)
                        if isinstance(v, str) and v:
                            chunk_parts.append(v)
                if chunk_parts:
                    yield "".join(chunk_parts)
    finally:
        if not _skip_rl:
            try:
                rl.release()
            except Exception as e:
                import traceback
                traceback.print_exc()
                pass


async def generate_raw_async(prompt: str,
                         system: Optional[str] = None,
                         messages: Optional[List[Dict[str, str]]] = None,
                         temperature: Optional[float] = None,
                         max_tokens: Optional[int] = None,
                         extra: Optional[Dict[str, Any]] = None,
                         model: Optional[str] = None,
                         skip_rate_limit: Optional[bool] = None,
                         rate_limit_category: Optional[str] = None) -> Dict[str, Any]:
    """异步版本：调用文本模型并返回原始响应(JSON)。
    通过线程池复用同步实现，配合全局限流，便于在 asyncio 下批量并发。
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        functools.partial(
            generate_raw,
            prompt=prompt,
            system=system,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            extra=extra,
            model=model,
            skip_rate_limit=skip_rate_limit,
            rate_limit_category=rate_limit_category,
        ),
    )


async def generate_async(prompt: str,
                     system: Optional[str] = None,
                     messages: Optional[List[Dict[str, str]]] = None,
                     temperature: Optional[float] = None,
                     max_tokens: Optional[int] = None,
                     extra: Optional[Dict[str, Any]] = None,
                     model: Optional[str] = None,
                     skip_rate_limit: Optional[bool] = None,
                     rate_limit_category: Optional[str] = None) -> str:
    """异步版本：调用 Qwen 文本模型，返回字符串内容。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        functools.partial(
            generate,
            prompt=prompt,
            system=system,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            extra=extra,
            model=model,
            skip_rate_limit=skip_rate_limit,
            rate_limit_category=rate_limit_category,
        ),
    )


def vision_infer(image: Union[str, bytes],
                 prompt: str = "",
                 options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """调用本地视觉HTTP服务。image 支持文件路径或二进制。"""
    import logging
    _logger = logging.getLogger(__name__)
    
    _init_once()
    cfg = _Singleton.vision_cfg
    
    # 开始计时
    vision_timer = Timer("视觉模型推理", parent="模型生成")
    vision_timer.start()
    
    url = cfg["endpoint"].rstrip("/")
    headers = {"Content-Type": "application/json"}
    if cfg.get("api_key"):
        headers["Authorization"] = f"Bearer {cfg['api_key']}"

    # 图片预处理计时
    prep_timer = Timer("图片预处理", parent="视觉模型")
    prep_timer.start()
    if isinstance(image, (bytes, bytearray)):
        img_b64 = base64.b64encode(image).decode()
        img_payload = f"data:application/octet-stream;base64,{img_b64}"
        image_size = len(image)
    else:
        # 传路径则让服务端读取，或也可改为本地转base64
        img_payload = image
        image_size = 0
    prep_timer.stop()

    payload: Dict[str, Any] = {"image": img_payload}
    if prompt:
        payload["prompt"] = prompt
    if options:
        payload["options"] = options

    try:
        # HTTP请求计时
        http_timer = Timer("Vision HTTP请求", parent="视觉模型")
        http_timer.start()
        r = requests.post(url, headers=headers, json=payload, timeout=cfg["timeout"])
        http_timer.stop()
        
        r.raise_for_status()
        data = r.json()
        
        vision_timer.stop()
        
        # 记录到全局计时器
        if model_timer:
            model_timer.record("视觉模型推理", vision_timer.duration, parent="模型调用",
                              metadata={"image_size": image_size, "has_prompt": bool(prompt)})
        
        _logger.info(f"✅ 视觉模型推理完成 [耗时: {vision_timer.duration_str}, 图片大小: {image_size}bytes]")
        return data
    except Exception as e:
        import traceback
        traceback.print_exc()
        vision_timer.stop()
        _logger.error(f"❌ 视觉模型推理失败 [耗时: {vision_timer.duration_str}]: {e}")
        raise


