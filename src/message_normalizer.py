import logging
from typing import Any, Dict, List, Optional

from message_parser import parse_json_like, parse_member_role_from_json

logger = logging.getLogger(__name__)


def extract_user_from_ext(ext_info: Any) -> Dict[str, Any]:
    if not ext_info:
        return {}
    parsed = parse_json_like(ext_info)
    return parsed.get("user", {}) if isinstance(parsed, dict) else {}


def is_member_message(ext_info: Any) -> bool:
    if not ext_info:
        return False
    parsed = parse_json_like(ext_info)
    return parse_member_role_from_json(parsed)


def normalize_room_messages(
    raw_messages: List[Dict[str, Any]],
    server_id: Any,
    channel_id: Any,
    member_name: str,
) -> Dict[str, Any]:
    normalized_messages: List[Dict[str, Any]] = []
    oldest_raw_timestamp: Optional[int] = None
    newest_raw_timestamp: Optional[int] = None
    room_id = str(channel_id)

    for msg in raw_messages:
        message_timestamp = msg.get("msgTime")
        if message_timestamp is not None:
            if newest_raw_timestamp is None or message_timestamp > newest_raw_timestamp:
                newest_raw_timestamp = message_timestamp
            if oldest_raw_timestamp is None or message_timestamp < oldest_raw_timestamp:
                oldest_raw_timestamp = message_timestamp

        ext_info = msg.get("extInfo", "")
        parsed_ext_info = parse_json_like(ext_info)
        if not is_member_message(parsed_ext_info):
            continue
        if str(msg.get("msgType") or "") != "TEXT":
            continue

        message_id = msg.get("msgIdServer") or msg.get("msgIdClient")
        if not message_id:
            logger.debug(
                "Skip member TEXT message without message id: server_id=%s channel_id=%s timestamp=%s",
                server_id,
                channel_id,
                message_timestamp,
            )
            continue

        user_info = extract_user_from_ext(parsed_ext_info)
        normalized_messages.append(
            {
                "room_id": room_id,
                "server_id": server_id,
                "channel_id": channel_id,
                "owner_member_id": server_id,
                "member_name": member_name,
                "message_id": message_id,
                "user_id": user_info.get("userId"),
                "username": user_info.get("nickName"),
                "content": msg.get("bodys"),
                "msg_type": msg.get("msgType"),
                "ext_info": ext_info,
                "timestamp": message_timestamp,
            }
        )

    return {
        "messages": normalized_messages,
        "raw_count": len(raw_messages),
        "newest_raw_timestamp": newest_raw_timestamp,
        "oldest_raw_timestamp": oldest_raw_timestamp,
    }
