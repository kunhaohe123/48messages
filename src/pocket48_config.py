import json
from pathlib import Path
from typing import Any, Dict, Optional


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
