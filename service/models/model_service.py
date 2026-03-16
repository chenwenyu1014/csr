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
import time
import threading
import asyncio
import functools
import random
from collections import deque
from typing import Any, Dict, Optional, List, Union

import requests
from requests.exceptions import RequestException, ProxyError, SSLError, ConnectionError, ReadTimeout, HTTPError

# 导入耗时记录工具
try:
    from utils.timing import Timer, model_timer, log_timing
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
    model_timer = None
    def log_timing(*args, **kwargs): pass


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
    - 默认模型：qwen3-max
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
        or (getattr(_cfg_default, "dashscope_api_key", None) or "")
    )
    # 模型名称（优先级：环境变量 > 配置 > 默认值）
    model_name = (
        os.getenv("QWEN_MODEL")
        or (getattr(_cfg_default, "llm_model_name", None) or "qwen3-max")
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
    def __init__(self) -> None:
        """初始化限流器"""
        # 可配置并发与QPS
        try:
            self.max_concurrency = max(1, int(os.getenv("LLM_MAX_CONCURRENCY", "4")))
        except Exception:
            self.max_concurrency = 4
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


_RATE_LIMITER: Optional[_RateLimiter] = None


def _rate_limiter() -> _RateLimiter:
    global _RATE_LIMITER
    if _RATE_LIMITER is None:
        _RATE_LIMITER = _RateLimiter()
    return _RATE_LIMITER


def ensure_ready() -> Dict[str, Any]:
    """返回当前服务配置与可用性信息。"""
    _init_once()
    return {
        "llm_ready": bool(_Singleton.llm_cfg.get("api_key")),
        "vision_ready": bool(_Singleton.vision_cfg.get("endpoint")),
        "llm_cfg": {k: ("***" if k == "api_key" and v else v) for k, v in _Singleton.llm_cfg.items()},
        "vision_cfg": {k: ("***" if k == "api_key" and v else v) for k, v in _Singleton.vision_cfg.items()},
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
    
    def _post_with_retry(max_retries: int = 3, backoff: float = 1.5):
        last_err: Optional[Exception] = None
        # 环境变量可覆盖重试策略
        try:
            max_retries = int(os.getenv("LLM_RETRY_MAX", str(max_retries)))
        except Exception:
            pass
        try:
            backoff = float(os.getenv("LLM_RETRY_BACKOFF", str(backoff)))
        except Exception:
            pass

        for attempt in range(1, max_retries + 1):
            rl = _rate_limiter()
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
                r = requests.post(url, headers=headers, json=payload, timeout=cfg["timeout"])
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
             rate_limit_category: Optional[str] = None) -> str:
    """调用 Qwen 文本模型（DashScope 兼容 OpenAI Chat Completions）。"""
    _init_once()
    cfg = _Singleton.llm_cfg
    if not cfg.get("api_key"):
        return "[LLM未配置: 请设置 DASHSCOPE_API_KEY]"

    import logging
    _logger = logging.getLogger(__name__)
    
    # 开始计时
    model_name = model or cfg["model"]
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
    if temperature is not None:
        payload["temperature"] = temperature
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    if extra:
        payload.update(extra)

    def _post_with_retry(max_retries: int = 3, backoff: float = 1.5):
        last_err: Optional[Exception] = None
        # 环境变量可覆盖重试策略
        try:
            max_retries = int(os.getenv("LLM_RETRY_MAX", str(max_retries)))
        except Exception:
            pass
        try:
            backoff = float(os.getenv("LLM_RETRY_BACKOFF", str(backoff)))
        except Exception:
            pass
        
        for attempt in range(1, max_retries + 1):
            rl = _rate_limiter()
            _skip_rl = bool(skip_rate_limit)
            if not _skip_rl:
                rl.acquire()
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=cfg["timeout"])
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

    try:
        data = _post_with_retry()
        gen_timer.stop()
        
        # 记录到全局计时器
        if model_timer:
            model_timer.record(f"文本生成({model_name})", gen_timer.duration, parent="模型调用",
                              metadata={"prompt_len": prompt_len, "max_tokens": max_tokens})
        
        content = data["choices"][0]["message"]["content"]
        content_len = len(content) if content else 0
        _logger.info(f"✅ 文本生成完成 [模型: {model_name}, 耗时: {gen_timer.duration_str}, 输入: {prompt_len}字符, 输出: {content_len}字符]")
        return content
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
    if temperature is not None:
        payload["temperature"] = temperature
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    if extra:
        payload.update(extra)

    rl = _rate_limiter()
    _skip_rl = bool(skip_rate_limit)
    if not _skip_rl:
        rl.acquire()
    try:
        with requests.post(url, headers=headers, json=payload, timeout=cfg["timeout"], stream=True) as r:
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


