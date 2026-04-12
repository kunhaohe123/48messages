"""
口袋48成员本人消息抓取工具
用于抓取成员房间中的成员本人消息并保存到数据库。

注意：此工具仅供学习研究使用，请遵守口袋48用户协议。
"""

import json
import time
import logging
import logging.handlers
import threading
import random
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from pathlib import Path

import requests

from message_storage import (
    MessageStorage,
    create_storage,
    _parse_json_like,
    _parse_member_role_from_json,
)

log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "logs")
os.makedirs(log_dir, exist_ok=True)
DEFAULT_LOG_FILE = os.path.join(log_dir, "scraper.log")
ONCE_LOG_FILE = os.path.join(log_dir, "scraper_once.log")
logger = logging.getLogger(__name__)


def _configure_logging(for_once: bool = False) -> None:
    if for_once:
        file_handler = logging.FileHandler(
            ONCE_LOG_FILE,
            encoding="utf-8",
            mode="w",
        )
    else:
        file_handler = logging.handlers.TimedRotatingFileHandler(
            DEFAULT_LOG_FILE,
            when="midnight",
            interval=1,
            backupCount=14,
            encoding="utf-8",
        )
        file_handler.suffix = "%Y-%m-%d"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            file_handler,
            logging.StreamHandler(),
        ],
        force=True,
    )


def _setup_console_encoding():
    """尽量避免 Windows 终端下的中文输出乱码。"""
    if os.name != "nt":
        return

    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8")
        except ValueError:
            # 某些重定向场景下流对象不允许 reconfigure，直接跳过即可。
            pass


_setup_console_encoding()


DEFAULT_CONFIG_PATH = "config/config.json"
DEFAULT_TOKEN_PATH = "data/runtime/token.json"
DEFAULT_MEMBERS_FILENAME = "members.json"
DEFAULT_SINCE_DAYS_MAX_PAGES = 20
DEFAULT_TOKEN_TTL_SECONDS = 86400
DEFAULT_TOKEN_REFRESH_TTL_SECONDS = 6 * 60 * 60
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def resolve_project_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _normalize_member_config(member: Any, index: int) -> Dict[str, Any]:
    if not isinstance(member, dict):
        raise ValueError(f"成员配置第 {index} 项必须是对象")

    normalized = dict(member)

    if normalized.get("memberId") is None and normalized.get("id") is not None:
        normalized["memberId"] = normalized.get("id")

    return normalized


def _member_display_name(member: Dict[str, Any]) -> str:
    return str(
        member.get("ownerName")
        or member.get("memberName")
        or member.get("nickname")
        or member.get("channelId")
        or "-"
    )


def _format_time_ms(timestamp_ms: Optional[int]) -> str:
    if timestamp_ms is None:
        return "N/A"
    from datetime import datetime

    return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def _format_delay(delay_seconds: float) -> str:
    if delay_seconds == 0:
        return "0"
    return f"{delay_seconds:.1f}".rstrip("0").rstrip(".")


def load_config(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    with open(path, "r", encoding="utf-8") as file:
        config = json.load(file)

    members_path = path.parent / DEFAULT_MEMBERS_FILENAME
    if not members_path.exists():
        raise FileNotFoundError(f"成员配置文件不存在: {members_path}")
    with open(members_path, "r", encoding="utf-8") as file:
        raw_members = json.load(file)

    if not isinstance(raw_members, list):
        raise ValueError(f"成员配置必须是数组: {members_path}")

    config["members"] = [
        _normalize_member_config(member, index + 1)
        for index, member in enumerate(raw_members)
    ]

    return config


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


class AuthenticationUnavailableError(RuntimeError):
    """当前没有可用认证信息，但服务应继续等待人工恢复。"""


class FetchMessagesError(RuntimeError):
    """房间消息接口请求失败。"""


class TokenManager:
    """负责本地缓存 token，并维护本地过期时间。"""

    def __init__(self, token_file: str = DEFAULT_TOKEN_PATH):
        self.token_file = resolve_project_path(token_file)
        self.token_data = self._load_token()

    def _load_token(self) -> Dict[str, Any]:
        path = self.token_file
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)

    def _save_token(self):
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.token_file, "w", encoding="utf-8") as file:
            json.dump(self.token_data, file, ensure_ascii=False, indent=2)

    def reload(self):
        self.token_data = self._load_token()

    def set_token(self, access_token: str, expires_in: int = DEFAULT_TOKEN_TTL_SECONDS):
        now = time.time()
        self.token_data = {
            "access_token": access_token,
            "expires_at": now + expires_in,
            "acquired_at": now,
        }
        self._save_token()
        logger.info("Token saved")

    def has_token(self) -> bool:
        return bool(self.token_data.get("access_token"))

    def is_expired(self) -> bool:
        if not self.has_token():
            return True
        return time.time() >= self.token_data.get("expires_at", 0)

    def get_token(self, allow_expired: bool = False) -> Optional[str]:
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
        self.token_data = {}
        path = self.token_file
        if path.exists():
            path.unlink()
        logger.info("Token cleared")


class Pocket48Client:
    """封装配置加载、登录、消息抓取和存储访问。"""

    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config_path = config_path
        self.config = load_config(config_path)
        self._thread_local = threading.local()
        self.storage = self._init_storage()
        self.storage.sync_members(self.config.get("members", []))
        self.notifier = ServerChanNotifier(self.config.get("notify", {}))
        self.password_login_blocked_reason: Optional[str] = None
        token_file = self.config.get("storage", {}).get(
            "token_file", DEFAULT_TOKEN_PATH
        )
        self.token_manager = TokenManager(token_file)
        configured_token = self.config.get("pocket48", {}).get("token")
        if configured_token and not self.token_manager.has_token():
            self.token_manager.set_token(configured_token)

    def _get_session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            self._setup_session(session)
            self._thread_local.session = session
        return session

    def _init_storage(self) -> MessageStorage:
        return create_storage(self.config)

    def _api_config(self) -> Dict[str, Any]:
        return self.config.get("api", {})

    def _pocket48_config(self) -> Dict[str, Any]:
        return self.config.get("pocket48", {})

    def _build_app_info(self) -> str:
        app_info = self._pocket48_config().get("appInfo", {})
        return json.dumps(app_info, ensure_ascii=False, separators=(",", ":"))

    def _setup_session(self, session: requests.Session):
        # 这些请求头来自抓包结果，初始化后整个 Session 复用同一套指纹。
        pocket48_config = self._pocket48_config()
        headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-Hans-CN;q=1, zh-Hant-CN;q=0.9, en-CN;q=0.7",
            "Content-Type": "application/json;charset=utf-8",
            "P-Sign-Type": pocket48_config.get("pSignType", "V0"),
            "User-Agent": pocket48_config.get("userAgent", ""),
            "appInfo": self._build_app_info(),
            "pa": pocket48_config.get("pa", ""),
        }
        session.headers.update({key: value for key, value in headers.items() if value})

    def _get_url(self, path_key: str, default_path: str) -> str:
        api_config = self._api_config()
        base_url = api_config.get("base_url", "https://pocketapi.48.cn").rstrip("/")
        path = api_config.get(path_key, default_path)
        return f"{base_url}{path}"

    def _get_authenticated_headers(self) -> Dict[str, str]:
        token = self.token_manager.get_token(allow_expired=True)
        if not token:
            raise RuntimeError("未登录或缺少有效 token")
        return {"token": token}

    def _token_refresh_ttl_seconds(self) -> int:
        configured = self.config.get("storage", {}).get(
            "token_refresh_ttl_seconds", DEFAULT_TOKEN_REFRESH_TTL_SECONDS
        )
        try:
            return max(int(configured), 600)
        except (TypeError, ValueError):
            return DEFAULT_TOKEN_REFRESH_TTL_SECONDS

    def _token_retry_interval_seconds(self) -> int:
        configured = self.config.get("monitor", {}).get("token_retry_interval", 60)
        try:
            return max(int(configured), 10)
        except (TypeError, ValueError):
            return 60

    def _success_heartbeat_every(self) -> int:
        configured = self.config.get("monitor", {}).get("success_heartbeat_every", 10)
        try:
            return max(int(configured), 1)
        except (TypeError, ValueError):
            return 10

    def _api_retry_times(self) -> int:
        configured = self._api_config().get("retry_times", 3)
        try:
            return max(int(configured), 1)
        except (TypeError, ValueError):
            return 3

    def _api_retry_delay_seconds(self) -> float:
        configured = self._api_config().get("retry_delay", 5)
        try:
            return max(float(configured), 0.0)
        except (TypeError, ValueError):
            return 5.0

    def reload_auth_state(self):
        self.config = load_config(str(resolve_project_path(self.config_path)))
        self.notifier = ServerChanNotifier(self.config.get("notify", {}))
        self.token_manager.reload()
        self._setup_session(self._get_session())
        configured_token = self.config.get("pocket48", {}).get("token")
        if configured_token and not self.token_manager.has_token():
            self.token_manager.set_token(configured_token)

    def _block_password_login(self, reason: str):
        self.password_login_blocked_reason = reason
        logger.warning("Password login blocked until manual token refresh: %s", reason)
        self.notifier.send_problem(
            event_key="password-login-blocked",
            title="48messages 告警：需要手动更新 token",
            desp=(f"自动密码登录已被禁用，请更新抓包 token。\n\n> 原因: {reason}"),
        )

    def _begin_fetch_round(self):
        self._thread_local.token_refresh_logged = False

    def _mark_token_accepted(self):
        self.password_login_blocked_reason = None
        already_logged = getattr(self._thread_local, "token_refresh_logged", False)
        self.token_manager.refresh_expiry(
            self._token_refresh_ttl_seconds(), log_refresh=not already_logged
        )
        self._thread_local.token_refresh_logged = True
        self.notifier.send_recovery(
            event_key="saved-token-rejected",
            title="48messages 恢复：token 已恢复",
            desp="服务端重新接受了当前 token，抓取已恢复。",
        )
        self.notifier.send_recovery(
            event_key="automatic-password-login-disabled",
            title="48messages 恢复：认证已恢复",
            desp="检测到可用 token，自动等待状态已解除，抓取已恢复。",
        )
        self.notifier.send_recovery(
            event_key="password-login-blocked",
            title="48messages 恢复：token 已更新",
            desp="检测到新的可用 token，手动更新流程已完成，抓取已恢复。",
        )

    def _is_message_newer_than_local(
        self, message: Dict[str, Any], latest_local: Optional[Dict[str, Any]]
    ) -> bool:
        if not latest_local:
            return True

        latest_timestamp = latest_local.get("timestamp") or 0
        message_timestamp = message.get("timestamp") or 0
        if message_timestamp > latest_timestamp:
            return True
        if message_timestamp < latest_timestamp:
            return False

        latest_message_id = str(latest_local.get("message_id") or "")
        message_id = str(message.get("message_id") or "")
        if not latest_message_id or not message_id:
            return False
        return message_id != latest_message_id

    def _is_same_message_as_local_boundary(
        self, message: Dict[str, Any], latest_local: Optional[Dict[str, Any]]
    ) -> bool:
        if not latest_local:
            return False

        latest_message_id = str(latest_local.get("message_id") or "")
        message_id = str(message.get("message_id") or "")
        if latest_message_id and message_id:
            return message_id == latest_message_id

        latest_timestamp = latest_local.get("timestamp") or 0
        message_timestamp = message.get("timestamp") or 0
        return latest_timestamp > 0 and latest_timestamp == message_timestamp

    def _extract_user_from_ext(self, ext_info: Any) -> Dict[str, Any]:
        if not ext_info:
            return {}
        parsed = _parse_json_like(ext_info)
        return parsed.get("user", {}) if isinstance(parsed, dict) else {}

    def _is_member_message(self, ext_info: Any) -> bool:
        if not ext_info:
            return False
        parsed = _parse_json_like(ext_info)
        return _parse_member_role_from_json(parsed)

    def login(self) -> bool:
        if self.token_manager.get_token():
            logger.debug("Using saved token")
            return True

        stale_token = self.token_manager.get_token(allow_expired=True)
        if stale_token:
            logger.warning(
                "Local token expiry reached, but saved token still exists; "
                "continuing to use it until the server rejects it"
            )
            return True

        if self.password_login_blocked_reason:
            self.notifier.send_problem(
                event_key="automatic-password-login-disabled",
                title="48messages 告警：自动密码登录已禁用",
                desp=(
                    "当前没有可用 token，且不会再自动尝试密码登录。\n\n"
                    f"> 原因: {self.password_login_blocked_reason}"
                ),
            )
            logger.error(
                "No usable token and automatic password login is disabled: %s",
                self.password_login_blocked_reason,
            )
            return False

        pocket48_config = self._pocket48_config()
        mobile = pocket48_config.get("mobile")
        encrypted_password = pocket48_config.get("encryptedPassword")

        if not mobile or not encrypted_password:
            logger.error("Missing mobile or encryptedPassword, cannot login")
            return False

        # 登录接口要求的主体基本固定，真正会变化的是配置里的账号和抓包字段。
        payload = {
            "deviceToken": pocket48_config.get("deviceToken", ""),
            "loginType": "MOBILE_PWD",
            "loginMobile": {
                "mobile": mobile,
                "pwd": encrypted_password,
            },
        }

        try:
            logger.info("Starting login...")
            response = self._get_session().post(
                self._get_url("login_path", "/user/api/v2/login/app/app_login"),
                json=payload,
                timeout=self._api_config().get("timeout", 30),
            )
            response.raise_for_status()

            data = response.json()
            if data.get("status") != 200 or not data.get("success"):
                logger.error("Login failed: %s", data.get("message"))
                self._block_password_login(
                    data.get("message") or "password login rejected by server"
                )
                return False

            content = data.get("content", {})
            token = content.get("token") or content.get("userInfo", {}).get("token")
            if not token:
                logger.error("Login success but no token returned")
                return False

            valid_time_minutes = content.get("userInfo", {}).get("validTime", 40)
            expires_in = max(int(valid_time_minutes) * 60, 600)
            self.token_manager.set_token(token, expires_in)
            self.password_login_blocked_reason = None

            logger.info(
                "登录成功: userId=%s", content.get("userInfo", {}).get("userId")
            )
            return True

        except Exception as exc:
            logger.error("Login error: %s", exc)
            return False

    def ensure_authenticated(self) -> None:
        if not self.login():
            raise AuthenticationUnavailableError(
                self.password_login_blocked_reason or "未登录或缺少有效 token"
            )

    def get_room_messages(
        self, member: Dict[str, Any], limit: int = 100, next_time: int = 0
    ) -> Dict[str, Any]:
        server_id = member.get("serverId")
        channel_id = member.get("channelId")
        if server_id is None or channel_id is None:
            raise ValueError(
                f"成员配置缺少 serverId 或 channelId: {_member_display_name(member)}"
            )

        payload = {
            "limit": limit,
            "serverId": server_id,
            "channelId": channel_id,
            "nextTime": next_time,
        }

        try:
            logger.debug(
                "Fetching room messages: %s(%s)",
                _member_display_name(member),
                channel_id,
            )
            response = self._get_session().post(
                self._get_url("message_list_path", "/im/api/v1/team/message/list/all"),
                json=payload,
                headers=self._get_authenticated_headers(),
                timeout=self._api_config().get("timeout", 30),
            )
            response.raise_for_status()

            data = response.json()
            if data.get("status") != 200 or not data.get("success"):
                message = data.get("message") or "unknown error"
                if "token" in message.lower() or "登录" in message or "认证" in message:
                    self.notifier.send_problem(
                        event_key="saved-token-rejected",
                        title="48messages 告警：saved token 已失效",
                        desp=(
                            "服务器拒绝了当前保存的 token，需要重新抓包更新。\n\n"
                            f"> 返回信息: {message}"
                        ),
                    )
                    self.token_manager.clear()
                    self._block_password_login(
                        f"saved token rejected by server: {message}"
                    )
                    raise AuthenticationUnavailableError(
                        self.password_login_blocked_reason
                        or f"saved token rejected by server: {message}"
                    )
                raise FetchMessagesError(f"Fetch failed: {message}")

            content = data.get("content", {})
            messages = content.get("message", [])
            normalized_messages = []
            room_id = str(channel_id)
            oldest_raw_timestamp = None
            newest_raw_timestamp = None
            # 统一整理成存储层可直接消费的结构，避免数据库实现感知接口细节。
            for msg in messages:
                message_timestamp = msg.get("msgTime")
                if message_timestamp is not None:
                    if (
                        newest_raw_timestamp is None
                        or message_timestamp > newest_raw_timestamp
                    ):
                        newest_raw_timestamp = message_timestamp
                    if (
                        oldest_raw_timestamp is None
                        or message_timestamp < oldest_raw_timestamp
                    ):
                        oldest_raw_timestamp = message_timestamp
                ext_info = msg.get("extInfo", "")
                parsed_ext_info = _parse_json_like(ext_info)
                if not self._is_member_message(parsed_ext_info):
                    continue
                if str(msg.get("msgType") or "") != "TEXT":
                    continue
                user_info = self._extract_user_from_ext(parsed_ext_info)
                normalized_messages.append(
                    {
                        "room_id": room_id,
                        "server_id": server_id,
                        "channel_id": channel_id,
                        "owner_member_id": server_id,
                        "member_name": _member_display_name(member),
                        "message_id": msg.get("msgIdServer") or msg.get("msgIdClient"),
                        "user_id": user_info.get("userId"),
                        "username": user_info.get("nickName"),
                        "content": msg.get("bodys"),
                        "msg_type": msg.get("msgType"),
                        "ext_info": ext_info,
                        "timestamp": msg.get("msgTime"),
                    }
                )

            self._mark_token_accepted()
            logger.debug("Fetched %s member TEXT messages", len(normalized_messages))
            return {
                "messages": normalized_messages,
                "next_time": content.get("nextTime", next_time),
                "raw_count": len(messages),
                "newest_raw_timestamp": newest_raw_timestamp,
                "oldest_raw_timestamp": oldest_raw_timestamp,
            }

        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in {401, 403}:
                self.notifier.send_problem(
                    event_key="saved-token-rejected",
                    title="48messages 告警：saved token 已失效",
                    desp=(
                        "服务器以 HTTP 401/403 拒绝了当前保存的 token，需要重新抓包更新。\n\n"
                        f"> HTTP 状态码: {exc.response.status_code}"
                    ),
                )
                self.token_manager.clear()
                self._block_password_login(
                    f"saved token rejected by server with HTTP {exc.response.status_code}"
                )
                raise AuthenticationUnavailableError(
                    self.password_login_blocked_reason
                    or f"saved token rejected by server with HTTP {exc.response.status_code}"
                ) from exc
            raise FetchMessagesError(f"Fetch message HTTP error: {exc}") from exc
        except requests.RequestException as exc:
            raise FetchMessagesError(f"Fetch message request error: {exc}") from exc
        except Exception as exc:
            raise FetchMessagesError(f"Fetch message error: {exc}") from exc

    def _retry_sleep_seconds(self, retry_delay: float, attempt_index: int) -> float:
        if retry_delay <= 0:
            return 0.0
        return retry_delay * attempt_index

    def _get_room_messages_with_retry(
        self, member: Dict[str, Any], limit: int, next_time: int
    ) -> tuple[Dict[str, Any], int]:
        max_attempts = self._api_retry_times()
        retry_delay = self._api_retry_delay_seconds()
        room_name = _member_display_name(member)
        room_id = member.get("channelId")
        last_error: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                return self.get_room_messages(
                    member, limit=limit, next_time=next_time
                ), (attempt - 1)
            except AuthenticationUnavailableError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt >= max_attempts:
                    break
                sleep_seconds = self._retry_sleep_seconds(retry_delay, attempt)
                logger.warning(
                    "Fetch retry %s(%s) page nextTime=%s attempt %s/%s failed: %s",
                    room_name,
                    room_id,
                    next_time,
                    attempt,
                    max_attempts,
                    exc,
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

        assert last_error is not None
        raise FetchMessagesError(
            f"failed after {max_attempts} attempts: {last_error}"
        ) from last_error

    def save_messages(self, messages: List[Dict[str, Any]]) -> int:
        return self.storage.save_messages(messages)

    def _get_latest_local_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        return self.storage.get_latest_message(room_id)

    def _filter_new_messages(
        self, room_id: str, messages: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        latest_local = self._get_latest_local_message(room_id)
        return [
            msg
            for msg in messages
            if self._is_message_newer_than_local(msg, latest_local)
        ]

    def _history_target_already_covered(
        self, checkpoint: Optional[Dict[str, Any]], target_time_ms: int
    ) -> bool:
        if not checkpoint:
            return False
        oldest_covered_time_ms = checkpoint.get("oldest_covered_time_ms")
        return (
            oldest_covered_time_ms is not None
            and int(oldest_covered_time_ms) <= target_time_ms
        )

    def _resolve_history_fetch_start(
        self, checkpoint: Optional[Dict[str, Any]]
    ) -> tuple[int, bool, bool]:
        if not checkpoint or not checkpoint.get("resume_next_time"):
            return 0, False, False

        resume_next_time = int(checkpoint["resume_next_time"])
        if checkpoint.get("cursor_verified"):
            return resume_next_time, True, False
        return resume_next_time, False, True

    def _verify_history_cursor_page(
        self,
        checkpoint: Optional[Dict[str, Any]],
        result: Dict[str, Any],
        requested_next_time: int,
    ) -> bool:
        if requested_next_time <= 0:
            return False
        if result.get("raw_count", 0) <= 0:
            return False
        if result.get("next_time") == requested_next_time:
            return False

        if checkpoint is None:
            return True

        expected_oldest_time_ms = checkpoint.get("oldest_covered_time_ms")
        newest_raw_timestamp = result.get("newest_raw_timestamp")
        if (
            expected_oldest_time_ms is not None
            and newest_raw_timestamp is not None
            and newest_raw_timestamp > int(expected_oldest_time_ms) + 6 * 60 * 60 * 1000
        ):
            return False
        return True

    def _persist_history_progress_if_needed(
        self,
        member: Dict[str, Any],
        page_count: int,
        oldest_covered_message_id: Optional[str],
        oldest_covered_time_ms: Optional[int],
        resume_next_time: Optional[int],
        cursor_verified: Optional[bool] = None,
        force: bool = False,
    ) -> None:
        if not force and page_count % 5 != 0:
            return

        server_id = member.get("serverId")
        channel_id = member.get("channelId")
        if server_id is None or channel_id is None:
            return

        self.storage.update_history_checkpoint_progress(
            server_id=int(server_id),
            channel_id=int(channel_id),
            oldest_covered_message_id=oldest_covered_message_id,
            oldest_covered_time_ms=oldest_covered_time_ms,
            resume_next_time=resume_next_time,
            last_page_count=page_count,
            cursor_verified=cursor_verified,
        )

    def _get_oldest_message_info(
        self, messages: List[Dict[str, Any]]
    ) -> tuple[Optional[str], Optional[int]]:
        if not messages:
            return None, None
        oldest_message = min(messages, key=lambda item: item.get("timestamp") or 0)
        return oldest_message.get("message_id"), oldest_message.get("timestamp") or 0

    def _flush_history_buffer(
        self,
        member: Dict[str, Any],
        buffered_messages: List[Dict[str, Any]],
        persisted_oldest_message_id: Optional[str],
        persisted_oldest_time_ms: Optional[int],
        persisted_boundary_time_ms: Optional[int],
        resume_next_time: Optional[int],
        page_count: int,
        cursor_verified: bool,
    ) -> tuple[int, Optional[str], Optional[int]]:
        if not buffered_messages and persisted_boundary_time_ms is None:
            return 0, persisted_oldest_message_id, persisted_oldest_time_ms

        saved_count = self.save_messages(buffered_messages) if buffered_messages else 0
        batch_oldest_message_id, batch_oldest_time_ms = (None, None)
        if buffered_messages:
            batch_oldest_message_id, batch_oldest_time_ms = (
                self._get_oldest_message_info(buffered_messages)
            )
        if batch_oldest_time_ms is not None and (
            persisted_oldest_message_id is None
            or batch_oldest_time_ms
            < (persisted_oldest_time_ms or batch_oldest_time_ms + 1)
        ):
            persisted_oldest_message_id = batch_oldest_message_id
        if persisted_boundary_time_ms is not None and (
            persisted_oldest_time_ms is None
            or persisted_boundary_time_ms < persisted_oldest_time_ms
        ):
            persisted_oldest_time_ms = persisted_boundary_time_ms

        self._persist_history_progress_if_needed(
            member=member,
            page_count=page_count,
            oldest_covered_message_id=persisted_oldest_message_id,
            oldest_covered_time_ms=persisted_oldest_time_ms,
            resume_next_time=resume_next_time,
            cursor_verified=cursor_verified,
            force=True,
        )
        buffered_messages.clear()
        return saved_count, persisted_oldest_message_id, persisted_oldest_time_ms

    def _get_adaptive_page_delay(
        self,
        page_count: int,
        explicit_page_delay: Optional[float],
        consecutive_errors: int = 0,
    ) -> float:
        if explicit_page_delay is not None:
            return explicit_page_delay

        if consecutive_errors >= 3:
            return 2.0
        if consecutive_errors == 2:
            return 1.0
        if consecutive_errors == 1:
            return 0.5

        if page_count <= 20:
            return 0.0
        if page_count <= 100:
            return 0.1
        return 0.3

    def fetch_latest_incremental_messages(
        self,
        member: Dict[str, Any],
        limit: int = 100,
        max_pages: Optional[int] = None,
        page_delay: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        self.ensure_authenticated()
        self._begin_fetch_round()
        room_id = str(member.get("channelId"))
        room_name = _member_display_name(member)
        latest_local = self._get_latest_local_message(room_id)
        since_time_ms = None
        if latest_local is None:
            # 首次抓取没有本地边界时，默认只回溯最近 30 天，避免无限追历史。
            since_time_ms = int((time.time() - 30 * 24 * 60 * 60) * 1000)
        next_time = 0
        seen_message_ids = set()
        collected_messages: List[Dict[str, Any]] = []
        page_count = 0
        consecutive_errors = 0
        last_logged_delay: Optional[float] = None
        start_time = time.time()
        from datetime import datetime

        while True:
            if max_pages is not None and page_count >= max_pages:
                logger.info(
                    "房间 %s(%s) 达到最大分页数 %s，停止继续翻页",
                    room_name,
                    room_id,
                    max_pages,
                )
                break
            result, retry_count = self._get_room_messages_with_retry(
                member, limit=limit, next_time=next_time
            )
            consecutive_errors = retry_count
            page_messages = result["messages"]
            raw_count = result.get("raw_count", 0)
            newest_raw_timestamp = result.get("newest_raw_timestamp")
            oldest_raw_timestamp = result.get("oldest_raw_timestamp")
            page_count += 1

            if raw_count == 0:
                break

            if (
                since_time_ms is not None
                and newest_raw_timestamp is not None
                and newest_raw_timestamp < since_time_ms
            ):
                break

            should_stop = False
            for message in page_messages:
                message_id = message.get("message_id")
                message_timestamp = message.get("timestamp") or 0
                if message_id and message_id in seen_message_ids:
                    continue

                if since_time_ms is not None and message_timestamp < since_time_ms:
                    should_stop = True
                    continue

                if self._is_same_message_as_local_boundary(message, latest_local):
                    should_stop = True
                    continue

                if self._is_message_newer_than_local(message, latest_local):
                    collected_messages.append(message)
                    if message_id:
                        seen_message_ids.add(message_id)
                    continue

                should_stop = True

            new_next_time = result["next_time"]
            elapsed = time.time() - start_time
            oldest_str = (
                datetime.fromtimestamp(oldest_raw_timestamp / 1000).strftime(
                    "%m-%d %H:%M"
                )
                if oldest_raw_timestamp
                else "N/A"
            )
            logger.debug(
                "[Page %s] [collected %s] [elapsed %.0fs] [oldest %s]",
                page_count,
                len(collected_messages),
                elapsed,
                oldest_str,
            )
            if should_stop:
                logger.debug("[%s] Stop: local latest boundary reached", room_name)
                break
            if (
                since_time_ms is not None
                and oldest_raw_timestamp is not None
                and oldest_raw_timestamp <= since_time_ms
            ):
                logger.debug(
                    "[%s] Stop: reached initial 30-day protection boundary", room_name
                )
                break
            if not new_next_time:
                logger.debug("[%s] Stop: no more messages", room_name)
                break
            if new_next_time == next_time:
                logger.debug("[%s] Stop: pagination ended", room_name)
                break
            next_time = new_next_time
            effective_page_delay = self._get_adaptive_page_delay(
                page_count=page_count,
                explicit_page_delay=page_delay,
                consecutive_errors=consecutive_errors,
            )
            if last_logged_delay != effective_page_delay:
                logger.debug(
                    "[%s] Page delay -> %ss",
                    room_name,
                    _format_delay(effective_page_delay),
                )
                last_logged_delay = effective_page_delay
            if effective_page_delay > 0:
                time.sleep(effective_page_delay)

        total_time = time.time() - start_time
        logger.info(
            "[%s] Done: %spages %smessages %.1fs",
            room_name,
            page_count,
            len(collected_messages),
            total_time,
        )
        return collected_messages

    def fetch_history_messages(
        self,
        member: Dict[str, Any],
        target_time_ms: int,
        limit: int = 100,
        max_pages: Optional[int] = None,
        page_delay: Optional[float] = None,
    ) -> Dict[str, Any]:
        self.ensure_authenticated()
        self._begin_fetch_round()
        room_id = member.get("channelId")
        server_id = member.get("serverId")
        room_name = _member_display_name(member)
        if room_id is None or server_id is None:
            raise ValueError("历史抓取需要 serverId 和 channelId")

        checkpoint = self.storage.get_history_checkpoint(int(server_id), int(room_id))
        if self._history_target_already_covered(checkpoint, target_time_ms):
            logger.info(
                "[%s] Skip history fetch: already covered to target %s (covered_to %s)",
                room_name,
                _format_time_ms(target_time_ms),
                _format_time_ms(checkpoint.get("oldest_covered_time_ms"))
                if checkpoint
                else "N/A",
            )
            return {
                "messages": [],
                "page_count": 0,
                "fetched_count": 0,
                "saved_count": 0,
                "oldest_covered_message_id": checkpoint.get("oldest_covered_message_id")
                if checkpoint
                else None,
                "oldest_covered_time_ms": checkpoint.get("oldest_covered_time_ms")
                if checkpoint
                else None,
                "resume_next_time": checkpoint.get("resume_next_time")
                if checkpoint
                else None,
                "reached_target": True,
                "cursor_verified": bool(
                    checkpoint and checkpoint.get("cursor_verified")
                ),
                "cursor_invalid": False,
            }

        self.storage.start_history_fetch(
            server_id=int(server_id),
            channel_id=int(room_id),
            target_time_ms=target_time_ms,
        )
        logger.info(
            "[%s] Start history fetch: target %s",
            room_name,
            _format_time_ms(target_time_ms),
        )
        checkpoint = self.storage.get_history_checkpoint(int(server_id), int(room_id))

        next_time, using_resume_cursor, probing_resume_cursor = (
            self._resolve_history_fetch_start(checkpoint)
        )
        seen_message_ids = set()
        buffered_messages: List[Dict[str, Any]] = []
        page_count = 0
        persisted_oldest_time_ms = (
            checkpoint.get("oldest_covered_time_ms") if checkpoint else None
        )
        persisted_oldest_message_id = (
            checkpoint.get("oldest_covered_message_id") if checkpoint else None
        )
        cursor_verified = bool(checkpoint and checkpoint.get("cursor_verified"))
        cursor_invalid = False
        completed_successfully = False
        failure_status = "interrupted"
        failure_message = "history fetch stopped before reaching target"
        consecutive_errors = 0
        last_logged_delay: Optional[float] = None
        total_fetched_count = 0
        total_saved_count = 0
        persisted_boundary_time_ms = persisted_oldest_time_ms
        start_time = time.time()
        from datetime import datetime

        while True:
            if max_pages is not None and page_count >= max_pages:
                logger.info(
                    "房间 %s(%s) 达到历史抓取最大分页数 %s，停止继续翻页",
                    room_name,
                    room_id,
                    max_pages,
                )
                failure_message = f"reached max_pages={max_pages} before target"
                break

            requested_next_time = next_time
            result, retry_count = self._get_room_messages_with_retry(
                member, limit=limit, next_time=requested_next_time
            )
            consecutive_errors = retry_count

            if using_resume_cursor or probing_resume_cursor:
                if not self._verify_history_cursor_page(
                    checkpoint, result, requested_next_time
                ):
                    cursor_invalid = True
                    logger.warning(
                        "[%s] Saved history cursor became invalid, fallback to latest page",
                        room_name,
                    )
                    self.storage.finish_history_fetch_failed(
                        server_id=int(server_id),
                        channel_id=int(room_id),
                        status="invalid_cursor",
                        error_message="saved history cursor became invalid",
                        resume_next_time=requested_next_time,
                        last_page_count=page_count,
                    )
                    self.storage.start_history_fetch(
                        server_id=int(server_id),
                        channel_id=int(room_id),
                        target_time_ms=target_time_ms,
                    )
                    checkpoint = self.storage.get_history_checkpoint(
                        int(server_id), int(room_id)
                    )
                    next_time = 0
                    using_resume_cursor = False
                    probing_resume_cursor = False
                    cursor_verified = False
                    continue

                if probing_resume_cursor:
                    logger.debug(
                        "[%s] Resume history cursor verified and promoted", room_name
                    )
                else:
                    logger.debug("[%s] Resume history cursor accepted", room_name)

            page_messages = result["messages"]
            raw_count = result.get("raw_count", 0)
            oldest_raw_timestamp = result.get("oldest_raw_timestamp")
            page_count += 1

            if raw_count == 0:
                completed_successfully = True
                break

            reached_target = False
            for message in page_messages:
                message_id = message.get("message_id")
                message_timestamp = message.get("timestamp") or 0
                if message_id and message_id in seen_message_ids:
                    continue
                if message_timestamp < target_time_ms:
                    reached_target = True
                    continue
                buffered_messages.append(message)
                total_fetched_count += 1
                if message_id:
                    seen_message_ids.add(message_id)

            new_next_time = result["next_time"]
            elapsed = time.time() - start_time
            oldest_str = (
                datetime.fromtimestamp(oldest_raw_timestamp / 1000).strftime(
                    "%m-%d %H:%M"
                )
                if oldest_raw_timestamp
                else "N/A"
            )
            logger.debug(
                "[History %s] [collected %s] [elapsed %.0fs] [oldest %s]",
                page_count,
                total_fetched_count,
                elapsed,
                oldest_str,
            )

            if using_resume_cursor or probing_resume_cursor:
                cursor_verified = True
                using_resume_cursor = False
                probing_resume_cursor = False

            if oldest_raw_timestamp is not None:
                persisted_boundary_time_ms = oldest_raw_timestamp

            if buffered_messages and (
                page_count % 5 == 0 or len(buffered_messages) >= 200
            ):
                saved_count, persisted_oldest_message_id, persisted_oldest_time_ms = (
                    self._flush_history_buffer(
                        member=member,
                        buffered_messages=buffered_messages,
                        persisted_oldest_message_id=persisted_oldest_message_id,
                        persisted_oldest_time_ms=persisted_oldest_time_ms,
                        persisted_boundary_time_ms=persisted_boundary_time_ms,
                        resume_next_time=new_next_time,
                        page_count=page_count,
                        cursor_verified=cursor_verified,
                    )
                )
                total_saved_count += saved_count

            if reached_target:
                completed_successfully = True
                logger.debug(
                    "[%s] Stop: history target reached in page body", room_name
                )
                next_time = new_next_time
                break
            if (
                oldest_raw_timestamp is not None
                and oldest_raw_timestamp <= target_time_ms
            ):
                completed_successfully = True
                logger.debug(
                    "[%s] Stop: history target reached by raw boundary", room_name
                )
                next_time = new_next_time
                break
            if not new_next_time:
                completed_successfully = True
                logger.debug("[%s] Stop: no more messages", room_name)
                next_time = new_next_time
                break
            if new_next_time == requested_next_time:
                completed_successfully = True
                logger.debug("[%s] Stop: pagination ended", room_name)
                next_time = new_next_time
                break

            next_time = new_next_time
            effective_page_delay = self._get_adaptive_page_delay(
                page_count=page_count,
                explicit_page_delay=page_delay,
                consecutive_errors=consecutive_errors,
            )
            if last_logged_delay != effective_page_delay:
                logger.debug(
                    "[%s] Page delay -> %ss",
                    room_name,
                    _format_delay(effective_page_delay),
                )
                last_logged_delay = effective_page_delay
            if effective_page_delay > 0:
                time.sleep(effective_page_delay)

        total_time = time.time() - start_time
        if buffered_messages:
            saved_count, persisted_oldest_message_id, persisted_oldest_time_ms = (
                self._flush_history_buffer(
                    member=member,
                    buffered_messages=buffered_messages,
                    persisted_oldest_message_id=persisted_oldest_message_id,
                    persisted_oldest_time_ms=persisted_oldest_time_ms,
                    persisted_boundary_time_ms=persisted_boundary_time_ms,
                    resume_next_time=next_time,
                    page_count=page_count,
                    cursor_verified=cursor_verified,
                )
            )
            total_saved_count += saved_count
        elif persisted_boundary_time_ms is not None:
            _, persisted_oldest_message_id, persisted_oldest_time_ms = (
                self._flush_history_buffer(
                    member=member,
                    buffered_messages=buffered_messages,
                    persisted_oldest_message_id=persisted_oldest_message_id,
                    persisted_oldest_time_ms=persisted_oldest_time_ms,
                    persisted_boundary_time_ms=persisted_boundary_time_ms,
                    resume_next_time=next_time,
                    page_count=page_count,
                    cursor_verified=cursor_verified,
                )
            )
        logger.info(
            "[%s] History done: %spages fetched %s saved %s %.1fs [target %s] [covered_to %s]",
            room_name,
            page_count,
            total_fetched_count,
            total_saved_count,
            total_time,
            _format_time_ms(target_time_ms),
            _format_time_ms(persisted_oldest_time_ms),
        )
        if completed_successfully:
            self.storage.finish_history_fetch_success(
                server_id=int(server_id),
                channel_id=int(room_id),
                target_time_ms=target_time_ms,
                oldest_covered_message_id=persisted_oldest_message_id,
                oldest_covered_time_ms=persisted_oldest_time_ms,
                resume_next_time=next_time,
                last_page_count=page_count,
                cursor_verified=cursor_verified,
            )
        else:
            self.storage.finish_history_fetch_failed(
                server_id=int(server_id),
                channel_id=int(room_id),
                status=failure_status,
                error_message=failure_message,
                resume_next_time=next_time,
                last_page_count=page_count,
            )
        return {
            "messages": [],
            "page_count": page_count,
            "fetched_count": total_fetched_count,
            "saved_count": total_saved_count,
            "oldest_covered_message_id": persisted_oldest_message_id,
            "oldest_covered_time_ms": persisted_oldest_time_ms,
            "resume_next_time": next_time,
            "reached_target": (
                persisted_oldest_time_ms is not None
                and persisted_oldest_time_ms <= target_time_ms
            ),
            "cursor_verified": cursor_verified,
            "cursor_invalid": cursor_invalid,
        }

    def fetch_incremental_messages(
        self,
        member: Dict[str, Any],
        limit: int = 100,
        since_time_ms: Optional[int] = None,
        max_pages: Optional[int] = None,
        page_delay: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        if since_time_ms is None:
            return self.fetch_latest_incremental_messages(
                member,
                limit=limit,
                max_pages=max_pages,
                page_delay=page_delay,
            )

        history_result = self.fetch_history_messages(
            member,
            target_time_ms=since_time_ms,
            limit=limit,
            max_pages=max_pages,
            page_delay=page_delay,
        )
        return history_result["messages"]

    def monitor_room_once(
        self,
        member: Dict[str, Any],
        limit: int,
        max_pages: Optional[int],
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        room_id = str(member.get("channelId"))
        server_id = member.get("serverId")
        room_name = _member_display_name(member)
        token_retry_interval = self._token_retry_interval_seconds()
        success_heartbeat_every = self._success_heartbeat_every()
        try:
            messages = self.fetch_incremental_messages(
                member, limit=limit, max_pages=max_pages
            )

            if messages:
                saved = self.save_messages(messages)
                latest_message = max(
                    messages, key=lambda item: item.get("timestamp") or 0
                )
                self.storage.record_fetch(
                    room_id=room_id,
                    messages_count=saved,
                    status="success",
                    last_message_id=latest_message.get("message_id"),
                    last_message_time_ms=latest_message.get("timestamp"),
                    server_id=server_id,
                    channel_id=int(room_id),
                )
                logger.info(
                    "Room %s(%s) saved %s new messages", room_name, room_id, saved
                )
                state["consecutive_failures"] = 0
                state["idle_success_count"] = 0
            else:
                idle_success_count = int(state.get("idle_success_count", 0)) + 1
                if idle_success_count >= success_heartbeat_every:
                    self.storage.record_fetch(
                        room_id=room_id,
                        messages_count=0,
                        status="success",
                        server_id=server_id,
                        channel_id=int(room_id),
                    )
                    idle_success_count = 0
                state["idle_success_count"] = idle_success_count
                state["consecutive_failures"] = 0
            state["last_error"] = None
            state["next_delay_seconds"] = 0
            return state
        except AuthenticationUnavailableError as exc:
            logger.warning(
                "Auth unavailable for room %s(%s), retrying in %s seconds: %s",
                room_name,
                room_id,
                token_retry_interval,
                exc,
            )
            self.reload_auth_state()
            state["consecutive_failures"] = 0
            state["last_error"] = str(exc)
            state["next_delay_seconds"] = token_retry_interval
            return state
        except Exception as exc:
            consecutive_failures = int(state.get("consecutive_failures", 0)) + 1
            logger.error(
                "Monitor error %s(%s) [consecutive failures %s]: %s",
                room_name,
                room_id,
                consecutive_failures,
                exc,
            )
            self.storage.record_fetch(
                room_id=room_id,
                messages_count=0,
                status="failed",
                error_message=str(exc),
                server_id=server_id,
                channel_id=int(room_id),
            )
            state["consecutive_failures"] = consecutive_failures
            state["last_error"] = str(exc)
            state["next_delay_seconds"] = 0
            return state


class MessageScraper:
    """面向 CLI 的高层调度器。"""

    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.client = Pocket48Client(config_path)
        self.config = self.client.config

    def _monitor_worker_count(self, members_count: int) -> int:
        configured = self.config.get("monitor", {}).get("workers")
        default_workers = min(max(members_count, 1), 8)
        try:
            requested = int(configured) if configured is not None else default_workers
        except (TypeError, ValueError):
            requested = default_workers
        return max(1, min(requested, members_count))

    def _monitor_jitter_seconds(self) -> float:
        configured = self.config.get("monitor", {}).get("jitter_seconds", 3)
        try:
            return max(float(configured), 0.0)
        except (TypeError, ValueError):
            return 3.0

    def _select_members(
        self, member_names: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        members = self.config.get("members", [])
        if not member_names:
            return members

        # CLI 允许重复传入 --member，这里统一按名称过滤并提示缺失项。
        selected = [
            member
            for member in members
            if _member_display_name(member) in set(member_names)
        ]
        missing_names = [
            name
            for name in member_names
            if name not in {_member_display_name(member) for member in selected}
        ]
        for name in missing_names:
            logger.warning("Member config not found: %s", name)
        return selected

    def run(self, member_names: Optional[List[str]] = None):
        members = self._select_members(member_names)
        monitor_config = self.config.get("monitor", {})
        if not members:
            logger.warning("No members configured for monitoring")
            return

        logger.info("Starting monitoring %s members", len(members))
        interval = monitor_config.get("interval", 60)
        limit = monitor_config.get("limit", 100)
        max_pages = monitor_config.get("max_pages")
        max_retries = monitor_config.get("max_retries", 5)
        worker_count = self._monitor_worker_count(len(members))
        jitter_seconds = self._monitor_jitter_seconds()
        now = time.time()
        states: Dict[str, Dict[str, Any]] = {}
        active_members: Dict[str, Dict[str, Any]] = {}

        for member in members:
            if member.get("channelId") is None or member.get("serverId") is None:
                logger.warning(
                    "Skipping member config missing serverId/channelId: %s", member
                )
                continue
            room_id = str(member.get("channelId"))
            active_members[room_id] = member
            states[room_id] = {
                "consecutive_failures": 0,
                "idle_success_count": 0,
                "running": False,
                "next_run_at": now + random.uniform(0, jitter_seconds),
                "last_error": None,
                "next_delay_seconds": 0,
            }
            logger.info(
                "Scheduled monitoring room %s(%s), interval %s seconds",
                _member_display_name(member),
                room_id,
                interval,
            )

        logger.info(
            "Started monitoring %s rooms with %s workers",
            len(active_members),
            worker_count,
        )

        try:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures: Dict[Any, str] = {}
                while active_members or futures:
                    current_time = time.time()

                    completed_futures = [
                        future for future in list(futures) if future.done()
                    ]
                    for future in completed_futures:
                        room_id = futures.pop(future)
                        member = active_members.get(room_id)
                        if member is None or room_id not in states:
                            continue

                        updated_state = future.result()
                        updated_state["running"] = False
                        states[room_id] = updated_state
                        next_delay_seconds = (
                            updated_state.get("next_delay_seconds") or 0
                        )
                        delay_seconds = next_delay_seconds or interval
                        updated_state["next_run_at"] = (
                            time.time()
                            + delay_seconds
                            + random.uniform(0, jitter_seconds)
                        )

                        consecutive_failures = int(
                            updated_state.get("consecutive_failures", 0)
                        )
                        if consecutive_failures >= max_retries:
                            room_name = _member_display_name(member)
                            last_error = (
                                updated_state.get("last_error") or "unknown error"
                            )
                            self.client.notifier.send_problem(
                                event_key=f"room-monitor-failed:{room_id}",
                                title=f"48messages 告警：{room_name} 连续抓取失败",
                                desp=(
                                    f"房间 {room_name}({room_id}) 连续失败已达到 {consecutive_failures}/{max_retries}。\n\n"
                                    f"> 最近错误: {last_error}"
                                ),
                            )
                            logger.error(
                                "Room %s(%s) consecutive failures reached limit, stopping monitor",
                                room_name,
                                room_id,
                            )
                            active_members.pop(room_id, None)
                            states.pop(room_id, None)

                    available_slots = worker_count - len(futures)
                    if available_slots > 0:
                        due_room_ids = [
                            room_id
                            for room_id, state in states.items()
                            if not state.get("running")
                            and state.get("next_run_at", 0) <= current_time
                            and room_id in active_members
                        ]
                        due_room_ids.sort(
                            key=lambda room_id: states[room_id]["next_run_at"]
                        )

                        for room_id in due_room_ids[:available_slots]:
                            member = active_members[room_id]
                            state = states[room_id]
                            state["running"] = True
                            futures[
                                executor.submit(
                                    self.client.monitor_room_once,
                                    member,
                                    limit,
                                    max_pages,
                                    dict(state),
                                )
                            ] = room_id

                    time.sleep(0.5)
        except KeyboardInterrupt:
            logger.info("Stop monitoring")

    def _run_member_once(
        self,
        member: Dict[str, Any],
        fetch_limit: int,
        since_time_ms: Optional[int] = None,
        max_pages: Optional[int] = None,
        page_delay: Optional[float] = None,
    ):
        if since_time_ms is None:
            self._run_member_once_latest(
                member,
                fetch_limit=fetch_limit,
                max_pages=max_pages,
                page_delay=page_delay,
            )
            return
        self._run_member_once_history(
            member,
            fetch_limit=fetch_limit,
            target_time_ms=since_time_ms,
            max_pages=max_pages,
            page_delay=page_delay,
        )

    def _run_member_once_latest(
        self,
        member: Dict[str, Any],
        fetch_limit: int,
        max_pages: Optional[int] = None,
        page_delay: Optional[float] = None,
    ):
        room_id = member.get("channelId")
        server_id = member.get("serverId")
        room_name = _member_display_name(member)
        if room_id is None or server_id is None:
            logger.warning(
                "Skipping member config missing serverId/channelId: %s", member
            )
            return

        try:
            messages = self.client.fetch_latest_incremental_messages(
                member,
                limit=fetch_limit,
                max_pages=max_pages,
                page_delay=page_delay,
            )
            saved = self.client.save_messages(messages) if messages else 0

            if messages:
                latest_message = max(
                    messages, key=lambda item: item.get("timestamp") or 0
                )
                self.client.storage.record_fetch(
                    room_id=str(room_id),
                    messages_count=saved,
                    status="success",
                    last_message_id=latest_message.get("message_id"),
                    last_message_time_ms=latest_message.get("timestamp"),
                    server_id=server_id,
                    channel_id=room_id,
                )
            else:
                self.client.storage.record_fetch(
                    room_id=str(room_id),
                    messages_count=0,
                    status="success",
                    server_id=server_id,
                    channel_id=room_id,
                )

            logger.info(
                "One-time fetch %s(%s): fetched %s, saved %s",
                room_name,
                room_id,
                len(messages),
                saved,
            )
        except Exception as exc:
            self.client.storage.record_fetch(
                room_id=str(room_id),
                messages_count=0,
                status="failed",
                error_message=str(exc),
                server_id=server_id,
                channel_id=room_id,
            )
            logger.error("One-time fetch error %s(%s): %s", room_name, room_id, exc)

    def _run_member_once_history(
        self,
        member: Dict[str, Any],
        fetch_limit: int,
        target_time_ms: int,
        max_pages: Optional[int] = None,
        page_delay: Optional[float] = None,
    ):
        room_id = member.get("channelId")
        server_id = member.get("serverId")
        room_name = _member_display_name(member)
        if room_id is None or server_id is None:
            logger.warning(
                "Skipping member config missing serverId/channelId: %s", member
            )
            return

        try:
            history_result = self.client.fetch_history_messages(
                member,
                target_time_ms=target_time_ms,
                limit=fetch_limit,
                max_pages=max_pages,
                page_delay=page_delay,
            )
            self.client.storage.record_fetch(
                room_id=str(room_id),
                messages_count=history_result["saved_count"],
                status="success",
                server_id=server_id,
                channel_id=room_id,
            )
            logger.info(
                "One-time history fetch %s(%s): fetched %s, saved %s, pages %s, target %s, covered_to %s",
                room_name,
                room_id,
                history_result["fetched_count"],
                history_result["saved_count"],
                history_result["page_count"],
                _format_time_ms(target_time_ms),
                _format_time_ms(history_result["oldest_covered_time_ms"]),
            )
        except KeyboardInterrupt:
            self.client.storage.finish_history_fetch_failed(
                server_id=int(server_id),
                channel_id=int(room_id),
                status="interrupted",
                error_message="history fetch interrupted by user",
                resume_next_time=None,
                last_page_count=0,
            )
            raise
        except Exception as exc:
            self.client.storage.finish_history_fetch_failed(
                server_id=int(server_id),
                channel_id=int(room_id),
                status="failed",
                error_message=str(exc),
                resume_next_time=None,
                last_page_count=0,
            )
            self.client.storage.record_fetch(
                room_id=str(room_id),
                messages_count=0,
                status="failed",
                error_message=str(exc),
                server_id=server_id,
                channel_id=room_id,
            )
            logger.error(
                "One-time history fetch error %s(%s): %s", room_name, room_id, exc
            )

    def run_once(
        self,
        limit: Optional[int] = None,
        member_names: Optional[List[str]] = None,
        max_workers: Optional[int] = None,
        since_days: Optional[int] = None,
        max_pages: Optional[int] = None,
        page_delay: Optional[float] = None,
    ):
        if not self.client.login():
            logger.error("Login failed, exiting")
            return

        members = self._select_members(member_names)
        monitor_config = self.config.get("monitor", {})
        if not members:
            logger.warning("No members configured")
            return

        fetch_limit = limit or monitor_config.get("limit", 100)
        worker_count = max_workers or len(members)
        worker_count = max(1, min(worker_count, len(members)))
        since_time_ms = None
        explicit_page_delay = page_delay
        if since_days is not None:
            since_time_ms = int((time.time() - since_days * 24 * 60 * 60) * 1000)
        if explicit_page_delay is None:
            logger.info(
                "开始单次抓取 %s 个成员，并发 %s，翻页间隔自适应（1-20页=0s，21-100页=0.1s，100页后=0.3s）",
                len(members),
                worker_count,
            )
        else:
            logger.info(
                "开始单次抓取 %s 个成员，并发 %s，翻页间隔 %.1f 秒",
                len(members),
                worker_count,
                explicit_page_delay,
            )

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(
                    self._run_member_once,
                    member,
                    fetch_limit,
                    since_time_ms,
                    max_pages,
                    explicit_page_delay,
                )
                for member in members
            ]
            for future in as_completed(futures):
                future.result()

    def export(
        self,
        output_path: str,
        output_format: str,
        room_id: Optional[str],
        limit: Optional[int],
    ):
        count = self.client.storage.export_messages(
            output_path=output_path,
            room_id=room_id,
            limit=limit,
            output_format=output_format,
        )
        logger.info("Exported %s messages to %s", count, output_path)

    def get_statistics(self) -> Dict[str, Any]:
        return self.client.storage.get_statistics()


def print_statistics(stats: Dict[str, Any]):
    """Print formatted statistics to terminal."""
    print("\n=== Scrape Statistics ===")
    print(f"Total messages: {stats['total_messages']}")
    print(f"Total rooms: {stats['total_rooms']}")
    print(f"Successful fetches: {stats['successful_fetches']}")
    print("\nTop 10 rooms by message count:")
    for room, count in stats["top_rooms"]:
        print(f"  {room}: {count}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Pocket48 member message scraper")
    parser.add_argument(
        "-c", "--config", default=DEFAULT_CONFIG_PATH, help="Config file path"
    )
    parser.add_argument(
        "--export-format", choices=["json", "csv"], help="Export messages from database"
    )
    parser.add_argument("--output", help="Export file path")
    parser.add_argument("--room-id", help="Export messages for specific room only")
    parser.add_argument("--limit", type=int, help="Export message limit")
    parser.add_argument(
        "--once", action="store_true", help="Fetch once for each member then exit"
    )
    parser.add_argument(
        "--member", action="append", help="Fetch specific member only, can repeat"
    )
    parser.add_argument(
        "--workers", type=int, help="Max concurrent members for one-time fetch"
    )
    parser.add_argument(
        "--since-days", type=int, help="Fetch messages from last N days"
    )
    parser.add_argument("--max-pages", type=int, help="Max pages for one-time fetch")
    parser.add_argument(
        "--page-delay",
        type=float,
        help="Delay between pages in seconds (default: 0 for <=30 days, otherwise 0.3)",
    )
    parser.add_argument("--stats", action="store_true", help="Show scrape statistics")
    args = parser.parse_args()
    _configure_logging(for_once=args.once)

    try:
        scraper = MessageScraper(args.config)
        if args.stats:
            print_statistics(scraper.get_statistics())
            return
        if args.export_format:
            if not args.output:
                raise ValueError("Must provide --output when using export")
            scraper.export(
                output_path=args.output,
                output_format=args.export_format,
                room_id=args.room_id,
                limit=args.limit,
            )
            return
        if args.once:
            scraper.run_once(
                limit=args.limit,
                member_names=args.member,
                max_workers=args.workers,
                since_days=args.since_days,
                max_pages=args.max_pages,
                page_delay=args.page_delay,
            )
            return
        scraper.run(member_names=args.member)
    except Exception as exc:
        logger.error("Program error: %s", exc)
        raise


if __name__ == "__main__":
    main()
