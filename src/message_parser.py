import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Set


MEMBER_ROLE_ID = 3
MEMBER_CHANNEL_ROLES: Set[Any] = {2, "2"}
MEMBER_ROLE_ID_KEYS: Set[str] = {"roleId", "channelRole"}
MEMBER_SENDER_ROLE = "member"
FAN_SENDER_ROLE = "fan"


def _is_member_role_value(value: Any) -> bool:
    if value == MEMBER_ROLE_ID:
        return True
    if str(value) == "2":
        return True
    return False


def is_member_role_value(value: Any) -> bool:
    return _is_member_role_value(value)


def _parse_member_role_from_json(data: Any) -> bool:
    if isinstance(data, dict):
        for key, value in data.items():
            if key in MEMBER_ROLE_ID_KEYS and _is_member_role_value(value):
                return True
            if _parse_member_role_from_json(value):
                return True
        return False
    if isinstance(data, list):
        return any(_parse_member_role_from_json(item) for item in data)
    return False


def parse_member_role_from_json(data: Any) -> bool:
    return _parse_member_role_from_json(data)


def _extract_member_sender_user_id(message: Dict[str, Any]) -> Optional[int]:
    ext_info = message.get("ext_info")
    if ext_info is None:
        return None
    parsed = _parse_json_like(ext_info)
    if not _parse_member_role_from_json(parsed):
        return None
    return message.get("user_id")


def extract_member_sender_user_id(message: Dict[str, Any]) -> Optional[int]:
    return _extract_member_sender_user_id(message)


def _determine_sender_role(value: Any) -> str:
    parsed = _parse_json_like(value)
    if _parse_member_role_from_json(parsed):
        return MEMBER_SENDER_ROLE
    if isinstance(value, str) and (
        '"roleId": 3' in value
        or '"channelRole": "2"' in value
        or '"channelRole": 2' in value
    ):
        return MEMBER_SENDER_ROLE
    return FAN_SENDER_ROLE


def determine_sender_role(value: Any) -> str:
    return _determine_sender_role(value)


def _determine_sender_role_from_message(message: Dict[str, Any]) -> str:
    for candidate in (
        message.get("ext_info"),
        message.get("content"),
        {
            "body": _parse_json_like(message.get("content")),
            "extInfo": _parse_json_like(message.get("ext_info")),
        },
    ):
        if _determine_sender_role(candidate) == MEMBER_SENDER_ROLE:
            return MEMBER_SENDER_ROLE
    return FAN_SENDER_ROLE


def determine_sender_role_from_message(message: Dict[str, Any]) -> str:
    return _determine_sender_role_from_message(message)


def _message_server_id(message: Dict[str, Any]) -> Any:
    return message.get("server_id") or message.get("owner_member_id")


def message_server_id(message: Dict[str, Any]) -> Any:
    return _message_server_id(message)


def _sqlite_sender_role_case_expression(ext_info_column: str = "ext_info") -> str:
    return (
        "CASE "
        f"WHEN {ext_info_column} LIKE '%\"roleId\": 3%' "
        f'OR {ext_info_column} LIKE \'%"channelRole": "2"%\' '
        f"OR {ext_info_column} LIKE '%\"channelRole\": 2%' "
        f"THEN '{MEMBER_SENDER_ROLE}' ELSE '{FAN_SENDER_ROLE}' END"
    )


def _mysql_sender_role_case_expression(
    raw_message_column: str = "raw_message_json",
    ext_info_column: str = "ext_info_json",
) -> str:
    return (
        "CASE "
        f"WHEN {raw_message_column} LIKE '%\\\"roleId\\\": 3%' "
        f'OR {raw_message_column} LIKE \'%\\"channelRole\\": \\"2\\"%\' '
        f"OR {raw_message_column} LIKE '%\\\"channelRole\\\": 2%' "
        f"OR {ext_info_column} LIKE '%\\\"roleId\\\": 3%' "
        f'OR {ext_info_column} LIKE \'%\\"channelRole\\": \\"2\\"%\' '
        f"OR {ext_info_column} LIKE '%\\\"channelRole\\\": 2%' "
        f"THEN '{MEMBER_SENDER_ROLE}' ELSE '{FAN_SENDER_ROLE}' END"
    )


def _json_dumps(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=False)


def json_dumps(value: Any) -> Optional[str]:
    return _json_dumps(value)


def _parse_json_like(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def parse_json_like(value: Any) -> Any:
    return _parse_json_like(value)


def _find_first_value(data: Any, keys: set[str]) -> Optional[Any]:
    if isinstance(data, dict):
        for key, value in data.items():
            if key in keys and value not in (None, ""):
                return value
            nested = _find_first_value(value, keys)
            if nested not in (None, ""):
                return nested
    elif isinstance(data, list):
        for item in data:
            nested = _find_first_value(item, keys)
            if nested not in (None, ""):
                return nested
    return None


def find_first_value(data: Any, keys: set[str]) -> Optional[Any]:
    return _find_first_value(data, keys)


def _extract_text_content(body: Any, ext_info: Any) -> Optional[str]:
    # 不同消息类型把文本放在不同字段里，这里尽量抽取出一个可检索的文本摘要。
    candidates: List[str] = []
    for source in (_parse_json_like(body), _parse_json_like(ext_info)):
        if isinstance(source, str):
            candidates.append(source)
            continue

        text = _find_first_value(
            source,
            {
                "text",
                "messageText",
                "replyText",
                "faipaiContent",
                "content",
                "title",
                "desc",
            },
        )
        if text not in (None, ""):
            candidates.append(str(text))

    if not candidates:
        return None

    deduped: List[str] = []
    for item in candidates:
        if item not in deduped:
            deduped.append(item)
    return " | ".join(deduped)


def extract_text_content(body: Any, ext_info: Any) -> Optional[str]:
    return _extract_text_content(body, ext_info)


def _extract_media_fields(body: Any, ext_info: Any) -> Dict[str, Any]:
    # 消息体和 extInfo 的字段命名并不稳定，这里做一层宽松归一化。
    merged = {
        "body": _parse_json_like(body),
        "extInfo": _parse_json_like(ext_info),
    }

    return {
        "media_url": _find_first_value(
            merged, {"url", "playUrl", "streamPath", "coverPath"}
        ),
        "media_cover_url": _find_first_value(
            merged, {"coverUrl", "coverPath", "thumbnailUrl"}
        ),
        "media_duration": _find_first_value(merged, {"duration", "playTime", "time"}),
        "width": _find_first_value(merged, {"width"}),
        "height": _find_first_value(merged, {"height"}),
        "reply_to_text": _find_first_value(merged, {"replyText", "messageText"}),
        "flip_user_name": _find_first_value(merged, {"faipaiName", "replyName"}),
        "flip_question": _find_first_value(merged, {"faipaiContent", "question"}),
        "flip_answer": _find_first_value(
            merged, {"messageText", "answer", "replyText"}
        ),
        "ext_json": _json_dumps(merged),
    }


def extract_media_fields(body: Any, ext_info: Any) -> Dict[str, Any]:
    return _extract_media_fields(body, ext_info)


def _timestamp_ms_to_datetime(value: Any) -> datetime:
    try:
        timestamp_ms = int(value)
        if timestamp_ms > 0:
            return datetime.fromtimestamp(timestamp_ms / 1000)
    except (TypeError, ValueError):
        pass
    return datetime.now()


def timestamp_ms_to_datetime(value: Any) -> datetime:
    return _timestamp_ms_to_datetime(value)

