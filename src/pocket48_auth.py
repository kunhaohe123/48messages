import json
import logging
import os
import threading
import time
from typing import Any, Dict, Optional

import requests

from pocket48_config import (
    DEFAULT_TOKEN_PATH,
    DEFAULT_TOKEN_REFRESH_TTL_SECONDS,
    DEFAULT_TOKEN_TTL_SECONDS,
    resolve_project_path,
)


logger = logging.getLogger(__name__)


class ServerChanNotifier:
    """发送微信告警，并在故障恢复前避免重复提醒。"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        notify_config = config or {}
        self.sendkey = str(notify_config.get("sendkey") or "").strip()
        self.enabled = bool(notify_config.get("enabled") and self.sendkey)
        self.timeout = self._safe_int(notify_config.get("timeout", 10), 10)
        self._active_events = set()
        self._lock = threading.Lock()

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        try:
            return max(int(value), 1)
        except (TypeError, ValueError):
            return default

    def send_problem(self, event_key: str, title: str, desp: str):
        if not self.enabled:
            return

        with self._lock:
            if event_key in self._active_events:
                logger.info("Skip duplicate active alert: %s", event_key)
                return
            self._active_events.add(event_key)

        try:
            response = requests.post(
                f"https://sctapi.ftqq.com/{self.sendkey}.send",
                data={"title": title, "desp": desp},
                timeout=self.timeout,
            )
            response.raise_for_status()
            logger.info("ServerChan alert sent: %s", event_key)
        except Exception as exc:
            logger.error("ServerChan alert failed %s: %s", event_key, exc)

    def send_recovery(self, event_key: str, title: str, desp: str):
        if not self.enabled:
            return

        with self._lock:
            if event_key not in self._active_events:
                return
            self._active_events.remove(event_key)

        try:
            response = requests.post(
                f"https://sctapi.ftqq.com/{self.sendkey}.send",
                data={"title": title, "desp": desp},
                timeout=self.timeout,
            )
            response.raise_for_status()
            logger.info("ServerChan recovery sent: %s", event_key)
        except Exception as exc:
            logger.error("ServerChan recovery failed %s: %s", event_key, exc)


class Pocket48ScraperError(RuntimeError):
    """抓取器运行时错误基类。"""


class AuthenticationUnavailableError(Pocket48ScraperError):
    """当前没有可用认证信息，但服务应继续等待人工恢复。"""


class FetchMessagesError(Pocket48ScraperError):
    """房间消息接口请求失败。"""


class TokenManager:
    """负责本地缓存 token，并维护本地过期时间。"""

    def __init__(self, token_file: str = DEFAULT_TOKEN_PATH):
        self.token_file = resolve_project_path(token_file)
        self._lock = threading.RLock()
        self.token_data = self._load_token()

    def _load_token(self) -> Dict[str, Any]:
        path = self.token_file
        if not path.exists():
            return {}
        try:
            with open(path, "r", encoding="utf-8") as file:
                token_data = json.load(file)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Token cache ignored: %s", exc)
            return {}
        if not isinstance(token_data, dict):
            logger.warning(
                "Token cache ignored: expected object, got %s",
                type(token_data).__name__,
            )
            return {}
        return token_data

    def _save_token(self):
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        temp_file = self.token_file.with_name(f".{self.token_file.name}.tmp")
        with open(temp_file, "w", encoding="utf-8") as file:
            json.dump(self.token_data, file, ensure_ascii=False, indent=2)
        try:
            os.chmod(temp_file, 0o600)
        except OSError as exc:
            logger.warning("Failed to chmod token temp file: %s", exc)
        os.replace(temp_file, self.token_file)
        try:
            os.chmod(self.token_file, 0o600)
        except OSError as exc:
            logger.warning("Failed to chmod token file: %s", exc)

    def reload(self):
        with self._lock:
            self.token_data = self._load_token()

    def set_token(self, access_token: str, expires_in: int = DEFAULT_TOKEN_TTL_SECONDS):
        with self._lock:
            now = time.time()
            self.token_data = {
                "access_token": access_token,
                "expires_at": now + expires_in,
                "acquired_at": now,
            }
            self._save_token()
        logger.info("Token saved")

    def has_token(self) -> bool:
        with self._lock:
            return bool(self.token_data.get("access_token"))

    def is_expired(self) -> bool:
        with self._lock:
            if not self.has_token():
                return True
            return time.time() >= self.token_data.get("expires_at", 0)

    def get_token(self, allow_expired: bool = False) -> Optional[str]:
        with self._lock:
            if not self.token_data:
                return None
            if not allow_expired and self.is_expired():
                logger.warning("Token expired")
                return None
            return self.token_data.get("access_token")

    def refresh_expiry(
        self,
        expires_in: int = DEFAULT_TOKEN_REFRESH_TTL_SECONDS,
        log_refresh: bool = True,
    ) -> bool:
        with self._lock:
            if not self.has_token():
                return False
            now = time.time()
            old_expires_at = self.token_data.get("expires_at", 0)
            remaining_seconds = max(old_expires_at - now, 0)
            refresh_when_below = max(int(expires_in // 3), 600)
            if remaining_seconds > refresh_when_below:
                return False

            new_expires_at = now + expires_in
            old_expires_at = self.token_data.get("expires_at", 0)
            if new_expires_at <= old_expires_at:
                return False
            self.token_data["expires_at"] = new_expires_at
            self._save_token()
        if log_refresh:
            logger.info("Token expiry refreshed")
        return True

    def clear(self):
        with self._lock:
            self.token_data = {}
            path = self.token_file
            if path.exists():
                path.unlink()
        logger.info("Token cleared")
