import base64
import hashlib
import json
import uuid
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

    def set_token(self, access_token: str, expires_in: int = DEFAULT_TOKEN_TTL_SECONDS,
                  metadata: Optional[Dict[str, Any]] = None):
        with self._lock:
            now = time.time()
            self.token_data = {
                "access_token": access_token,
                "expires_at": now + expires_in,
                "acquired_at": now,
            }
            self.token_data.update(metadata or {})
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
            metadata = {key: self.token_data[key] for key in ("auth_method", "ignored_config_token")
                        if key in self.token_data}
            self.token_data = metadata
            path = self.token_file
            if metadata:
                self._save_token()
            elif path.exists():
                path.unlink()
        logger.info("Token cleared")


def generate_pa() -> str:
    """按客户端格式生成每次请求独立的 pa。"""
    prefix = f"{int(time.time() * 1000)}{uuid.uuid4().hex}"
    digest = hashlib.md5((prefix + 'ejrw9u345h32u@#&%^#G').encode()).hexdigest()
    return base64.b64encode(f"{prefix}{digest}2021060901".encode()).decode()


def configured_token_digest(config) -> str:
    token = str(config.get("pocket48", {}).get("token") or "").strip()
    return hashlib.sha256(token.encode()).hexdigest()


def device_headers(token_manager) -> Dict[str, str]:
    # 和 token 分开保存，token 失效或被清除后设备身份仍保持不变。
    path = token_manager.token_file.with_name("device.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        device_id = json.loads(path.read_text(encoding="utf-8"))["deviceId"]
        if not isinstance(device_id, str) or len(device_id) != 16:
            raise ValueError("设备 ID 无效")
    except FileNotFoundError:
        device_id = uuid.uuid4().hex[:16]
        try:
            with path.open("x", encoding="utf-8") as file:
                json.dump({"deviceId": device_id}, file)
            path.chmod(0o600)
        except FileExistsError:
            return device_headers(token_manager)
    app_info = {
        "IMEI": device_id, "deviceId": device_id, "appName": "pocket48",
        "appVersion": "7.1.45", "appBuild": "26102701", "deviceName": "SM-G9730",
        "osType": "android", "osVersion": "12", "phoneName": "SM-G9730",
        "phoneSystemVersion": "12", "vendor": "Samsung",
    }
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "User-Agent": "PocketFans201807/7.1.45_26102701 (SM-G9730:Android 12;Samsung V417IR release-keys)",
        "Accept-Language": "zh-Hans-CN;q=1",
        "appInfo": json.dumps(app_info, separators=(",", ":")),
    }


class SmsLogin:
    """显式交互登录；后台抓取不会自动发送短信。"""

    def __init__(self, config):
        self.config = config
        self.manager = TokenManager(config.get("storage", {}).get("token_file", DEFAULT_TOKEN_PATH))
        self.session = requests.Session()
        self.session.headers.update(device_headers(self.manager))
        self.session.headers["P-Sign-Type"] = "V0"

    def request(self, path, payload, token=None):
        base = self.config.get("api", {}).get("base_url", "https://pocketapi.48.cn").rstrip("/")
        response = self.session.post(
            base + path, json=payload,
            headers={"pa": generate_pa(), **({"token": token} if token else {})},
            timeout=self.config.get("api", {}).get("timeout", 30),
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise AuthenticationUnavailableError("登录接口返回格式无效")
        return data

    def login(self, input_fn=input):
        pocket = self.config.get("pocket48", {})
        mobile = str(pocket.get("mobile") or "").strip()
        if not mobile.isdigit():
            mobile = input_fn("手机号：").strip()
        area = str(pocket.get("area") or "86").lstrip("+")
        if not mobile.isdigit() or not 6 <= len(mobile) <= 20 or not area.isdigit():
            raise ValueError("手机号或区号格式不正确")
        payload = {"mobile": mobile, "area": area, "businessCode": 1, "intelligenceToken": ""}
        data = self.request("/user/api/v2/sms/send_sms", payload)
        if data.get("status") == 2001:
            try:
                verification = json.loads(data.get("message", ""))
                print("口袋48验证问题：", verification["question"])
                print("可选答案：", verification["answer"])
            except (ValueError, KeyError, TypeError):
                raise AuthenticationUnavailableError("验证问题格式无法识别，请稍后重试") from None
            answer = input_fn("请输入验证答案（按上面原文填写）：").strip()
            data = self.request("/user/api/v2/sms/send_sms", {"mobile": mobile, "area": area, "answer": answer})
        self.check_result(data, "发送短信失败")
        code = input_fn("短信已发送，请输入验证码：").strip()
        if not code.isdigit() or not 4 <= len(code) <= 10:
            raise ValueError("验证码格式不正确")
        data = self.request("/user/api/v2/login/app/app_login", {
            "mobileCodeLogin": {"mobile": mobile, "code": code, "area": area},
            "loginType": "MOBILE_SMS_CODE", "deviceToken": "",
        })
        self.check_result(data, "验证码登录失败")
        token = (data.get("content") or {}).get("token")
        if not isinstance(token, str) or not token.strip():
            raise AuthenticationUnavailableError("登录接口未返回 token，原有缓存已保留")
        # 保存独立恢复缓存，切换出错后可重试，避免再次发送短信。
        pending = TokenManager(str(self.manager.token_file.with_name("pending_login.json")))
        pending.set_token(token, metadata={"auth_method": "sms"})
        self.select_account(token, input_fn=input_fn)
        pending.clear()

    def switch_account(self, input_fn=input):
        pending = TokenManager(str(self.manager.token_file.with_name("pending_login.json")))
        token = pending.get_token(allow_expired=True) or self.manager.get_token(allow_expired=True)
        if not token:
            token = str(self.config.get("pocket48", {}).get("token") or "").strip()
        if not token:
            raise AuthenticationUnavailableError("没有已保存的登录，请先运行 --login")
        self.select_account(token, input_fn=input_fn)
        pending.clear()

    def account_info(self, token):
        data = self.request("/user/api/v1/user/info/reload", {"from": "appstart"}, token=token)
        self.check_result(data, "获取账号资料失败，请运行 --login 重新登录")
        content = data.get("content")
        if not isinstance(content, dict):
            raise AuthenticationUnavailableError("账号资料格式无效，原有缓存已保留")
        user = content.get("userInfo", content)
        if not isinstance(user, dict) or not str(user.get("userId") or "").strip():
            raise AuthenticationUnavailableError("账号资料缺少用户 ID，原有缓存已保留")
        big_small = content.get("bigSmallInfo") or user.get("bigSmallInfo") or {}
        if not isinstance(big_small, dict):
            raise AuthenticationUnavailableError("大小号资料格式无效，原有缓存已保留")
        return user, big_small

    def select_account(self, token, input_fn=input):
        user, big_small = self.account_info(token)
        current_id = str(user["userId"])
        main = big_small.get("bigUserInfo")
        small = big_small.get("smallUserInfo") or []
        if not isinstance(small, list):
            raise AuthenticationUnavailableError("小号列表格式无效，原有缓存已保留")
        small_ids = {str(item.get("userId")) for item in small if isinstance(item, dict)}
        main_id = str(main.get("userId")) if isinstance(main, dict) else ""
        accounts = []
        seen = set()
        for candidate in [user, main, *small]:
            if not isinstance(candidate, dict) or not candidate.get("userId"):
                continue
            uid = str(candidate["userId"])
            if uid in seen:
                continue
            seen.add(uid)
            label = "大号" if uid == main_id else "小号" if uid in small_ids else "当前账号"
            accounts.append((candidate, label))
        print("可用账号：")
        for index, (account, label) in enumerate(accounts, 1):
            marker = "（当前）" if str(account["userId"]) == current_id else ""
            print(f"{index}. [{label}] {account.get('nickname') or '未命名'} "
                  f"ID={account['userId']}{marker}")
        selected = user
        if len(accounts) > 1:
            while True:
                answer = input_fn("选择账号序号（回车保留当前账号，q 取消）：").strip()
                if answer.lower() == "q":
                    print("已取消，原有 token 缓存未改动。")
                    return
                if not answer:
                    break
                if answer.isdigit() and 1 <= int(answer) <= len(accounts):
                    selected = accounts[int(answer) - 1][0]
                    break
                print("序号无效，请重新输入。")
        target_id = str(selected["userId"])
        if target_id != current_id:
            data = self.request("/user/api/v1/bigsmall/switch/user",
                                {"toUserId": selected["userId"]}, token=token)
            self.check_result(data, "切换账号失败，原有缓存已保留")
            content = data.get("content")
            new_token = content.get("token") if isinstance(content, dict) else None
            if not isinstance(new_token, str) or not new_token.strip():
                raise AuthenticationUnavailableError("切换接口未返回 token，原有缓存已保留")
            verified_user, _ = self.account_info(new_token)
            if str(verified_user["userId"]) != target_id:
                raise AuthenticationUnavailableError("切换后账号不匹配，原有缓存已保留")
            token, selected = new_token, verified_user
        self.manager.set_token(token, metadata={
            "auth_method": "sms", "ignored_config_token": configured_token_digest(self.config),
            "user_id": target_id, "nickname": str(selected.get("nickname") or ""),
        })
        print(f"已保存账号：{selected.get('nickname') or '未命名'}（ID={target_id}）。"
              "后续抓取使用此账号；已运行的服务请重启以立即应用。")

    @staticmethod
    def check_result(data, fallback):
        if data.get("status") != 200 or data.get("success") is False:
            raise AuthenticationUnavailableError(str(data.get("message") or fallback))
