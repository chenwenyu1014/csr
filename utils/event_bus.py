"""
事件总线
用于流式返回进度事件
"""

import threading
from queue import Queue
from typing import Dict, Any, Optional


class EventBus:
    """事件总线，支持请求级别的事件订阅"""
    
    def __init__(self):
        self._queues: Dict[str, Queue] = {}
        self._lock = threading.Lock()
    
    def register(self, request_id: str) -> Queue:
        """注册一个请求的事件队列"""
        with self._lock:
            q = Queue()
            self._queues[request_id] = q
            return q
    
    def emit(self, request_id: str, event: Dict[str, Any]):
        """发送事件到指定请求的队列"""
        with self._lock:
            if request_id in self._queues:
                self._queues[request_id].put(event)
    
    def complete(self, request_id: str):
        """标记请求完成，发送结束事件并清理"""
        with self._lock:
            if request_id in self._queues:
                self._queues[request_id].put({"type": "_complete"})
                # 不立即删除，让消费者有机会读取完所有事件
    
    def unregister(self, request_id: str):
        """注销请求的事件队列"""
        with self._lock:
            if request_id in self._queues:
                del self._queues[request_id]


# 全局事件总线实例
event_bus = EventBus()
