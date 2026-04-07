import csv
import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pymysql
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)

MEMBER_ROLE_ID = 3
MEMBER_CHANNEL_ROLES: Set[Any] = {2, "2"}
MEMBER_ROLE_ID_KEYS: Set[str] = {"roleId", "channelRole"}


def _is_member_role_value(value: Any) -> bool:
    if value == MEMBER_ROLE_ID:
        return True
    if str(value) == "2":
        return True
    return False


def _parse_member_role_from_json(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    user = data.get("user")
    if isinstance(user, dict) and _is_member_role_value(user.get("roleId")):
        return True
    if _is_member_role_value(data.get("channelRole")):
        return True
    return False


def _extract_member_sender_user_id(message: Dict[str, Any]) -> Optional[int]:
    ext_info = message.get("ext_info")
    if ext_info is None:
        return None
    parsed = _parse_json_like(ext_info)
    if not _parse_member_role_from_json(parsed):
        return None
    return message.get("user_id")


def _json_dumps(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=False)


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


def _determine_sender_role(ext_info_str: str) -> str:
    if not ext_info_str:
        return "fan"
    if (
        '"roleId": 3' in ext_info_str
        or '"channelRole": "2"' in ext_info_str
        or '"channelRole": 2' in ext_info_str
    ):
        return "member"
    return "fan"


def _timestamp_ms_to_datetime(value: Any) -> datetime:
    try:
        timestamp_ms = int(value)
        if timestamp_ms > 0:
            return datetime.fromtimestamp(timestamp_ms / 1000)
    except (TypeError, ValueError):
        pass
    return datetime.now()


class MessageStorage(ABC):
    """统一存储接口，抓取层只依赖这些能力，不关心底层是 SQLite 还是 MySQL。"""

    @abstractmethod
    def save_message(self, message: Dict[str, Any]) -> bool:
        pass

    def save_messages(self, messages: List[Dict[str, Any]]) -> int:
        saved_count = 0
        for message in messages:
            if self.save_message(message):
                saved_count += 1
        return saved_count

    @abstractmethod
    def get_messages(self, room_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_latest_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def export_messages(
        self,
        output_path: str,
        room_id: Optional[str] = None,
        limit: Optional[int] = None,
        output_format: str = "json",
    ) -> int:
        pass

    @abstractmethod
    def record_fetch(
        self,
        room_id: str,
        messages_count: int,
        status: str,
        error_message: Optional[str] = None,
        last_message_id: Optional[str] = None,
        last_message_time_ms: Optional[int] = None,
    ) -> None:
        pass

    @abstractmethod
    def get_statistics(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def list_rooms(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def list_senders(self, room_id: Optional[str] = None) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def search_messages(
        self,
        room_id: Optional[str] = None,
        sender_keyword: Optional[str] = None,
        keyword: Optional[str] = None,
        msg_type: Optional[str] = None,
        sender_role: Optional[str] = None,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_message_detail(self, message_id: str) -> Optional[Dict[str, Any]]:
        pass


class SQLiteStorage(MessageStorage):
    def __init__(self, db_path: str = "data/messages.db"):
        self.db_path = db_path
        self._init_database()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_database(self):
        # SQLite 版本只保留最小表结构，适合本地试跑和调试。
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id TEXT NOT NULL,
                message_id TEXT UNIQUE,
                user_id TEXT,
                username TEXT,
                content TEXT,
                msg_type TEXT,
                ext_info TEXT,
                timestamp INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fetch_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id TEXT,
                fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                messages_count INTEGER,
                status TEXT,
                error_message TEXT
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_room_id
            ON messages(room_id, timestamp DESC)
        """)
        conn.commit()
        conn.close()

    def save_message(self, message: Dict[str, Any]) -> bool:
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO messages
                (room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    message.get("room_id"),
                    message.get("message_id"),
                    message.get("user_id"),
                    message.get("username"),
                    _json_dumps(message.get("content")),
                    str(message.get("msg_type") or ""),
                    _json_dumps(message.get("ext_info")),
                    message.get("timestamp"),
                ),
            )
            conn.commit()
            affected = cursor.rowcount
            conn.close()
            return affected > 0
        except Exception as exc:
            logger.error("保存消息失败: %s", exc)
            return False

    def save_messages(self, messages: List[Dict[str, Any]]) -> int:
        if not messages:
            return 0

        try:
            conn = self._connect()
            cursor = conn.cursor()
            message_ids = [msg.get("message_id") for msg in messages]
            existing_before: set = set()
            if message_ids:
                placeholders = ", ".join(["?"] * len(message_ids))
                cursor.execute(
                    f"SELECT message_id FROM messages WHERE message_id IN ({placeholders})",
                    message_ids,
                )
                existing_before = {row[0] for row in cursor.fetchall()}

            cursor.executemany(
                """
                INSERT OR IGNORE INTO messages
                (room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    (
                        message.get("room_id"),
                        message.get("message_id"),
                        message.get("user_id"),
                        message.get("username"),
                        _json_dumps(message.get("content")),
                        str(message.get("msg_type") or ""),
                        _json_dumps(message.get("ext_info")),
                        message.get("timestamp"),
                    )
                    for message in messages
                ],
            )
            conn.commit()

            cursor.execute(
                f"SELECT message_id FROM messages WHERE message_id IN ({placeholders})",
                message_ids,
            )
            existing_after = {row[0] for row in cursor.fetchall()}
            conn.close()
            return len(existing_after - existing_before)
        except Exception as exc:
            logger.error("批量保存消息失败: %s", exc)
            return 0

    def get_messages(self, room_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp, created_at
            FROM messages WHERE room_id = ? ORDER BY timestamp DESC LIMIT ?
        """,
            (room_id, limit),
        )
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                "room_id": row[0],
                "message_id": row[1],
                "user_id": row[2],
                "username": row[3],
                "content": row[4],
                "msg_type": row[5],
                "ext_info": row[6],
                "timestamp": row[7],
                "created_at": row[8],
            }
            for row in rows
        ]

    def get_latest_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        messages = self.get_messages(room_id, 1)
        return messages[0] if messages else None

    def export_messages(
        self,
        output_path: str,
        room_id: Optional[str] = None,
        limit: Optional[int] = None,
        output_format: str = "json",
    ) -> int:
        conn = self._connect()
        cursor = conn.cursor()
        query = "SELECT room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp, created_at FROM messages"
        params: List[Any] = []
        if room_id:
            query += " WHERE room_id = ?"
            params.append(room_id)
        query += " ORDER BY timestamp DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = cursor.execute(query, params).fetchall()
        conn.close()
        messages = [
            {
                "room_id": row[0],
                "message_id": row[1],
                "user_id": row[2],
                "username": row[3],
                "content": row[4],
                "msg_type": row[5],
                "ext_info": row[6],
                "timestamp": row[7],
                "created_at": row[8],
            }
            for row in rows
        ]
        if output_format == "json":
            with open(output_path, "w", encoding="utf-8") as file:
                json.dump(messages, file, ensure_ascii=False, indent=2)
            return len(messages)
        if output_format == "csv":
            with open(output_path, "w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=list(messages[0].keys())
                    if messages
                    else [
                        "room_id",
                        "message_id",
                        "user_id",
                        "username",
                        "content",
                        "msg_type",
                        "ext_info",
                        "timestamp",
                        "created_at",
                    ],
                )
                writer.writeheader()
                writer.writerows(messages)
            return len(messages)
        raise ValueError(f"不支持的导出格式: {output_format}")

    def record_fetch(
        self,
        room_id: str,
        messages_count: int,
        status: str,
        error_message: Optional[str] = None,
        last_message_id: Optional[str] = None,
        last_message_time_ms: Optional[int] = None,
    ) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO fetch_logs (room_id, messages_count, status, error_message) VALUES (?, ?, ?, ?)",
            (room_id, messages_count, status, error_message),
        )
        conn.commit()
        conn.close()

    def get_statistics(self) -> Dict[str, Any]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM messages")
        total_messages = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT room_id) FROM messages")
        total_rooms = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM fetch_logs WHERE status = 'success'")
        successful_fetches = cursor.fetchone()[0]
        cursor.execute("""
            SELECT room_id, COUNT(*) as cnt FROM messages
            GROUP BY room_id ORDER BY cnt DESC LIMIT 10
        """)
        top_rooms = cursor.fetchall()
        conn.close()
        return {
            "total_messages": total_messages,
            "total_rooms": total_rooms,
            "successful_fetches": successful_fetches,
            "top_rooms": top_rooms,
        }

    def list_rooms(self) -> List[Dict[str, Any]]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT room_id AS id, room_id AS name, COUNT(*) AS message_count, MAX(timestamp) AS latest_timestamp
            FROM messages
            GROUP BY room_id
            ORDER BY latest_timestamp DESC
        """)
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                "id": row[0],
                "name": row[1],
                "message_count": row[2],
                "latest_timestamp": row[3],
            }
            for row in rows
        ]

    def list_senders(self, room_id: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = self._connect()
        cursor = conn.cursor()
        query = """
            SELECT user_id, username, COUNT(*) AS message_count, MAX(timestamp) AS latest_timestamp
            FROM messages
        """
        params: List[Any] = []
        if room_id:
            query += " WHERE room_id = ?"
            params.append(room_id)
        query += """
            GROUP BY user_id, username
            ORDER BY latest_timestamp DESC
        """
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                "user_id": row[0],
                "username": row[1],
                "message_count": row[2],
                "latest_timestamp": row[3],
            }
            for row in rows
        ]

    def search_messages(
        self,
        room_id: Optional[str] = None,
        sender_keyword: Optional[str] = None,
        keyword: Optional[str] = None,
        msg_type: Optional[str] = None,
        sender_role: Optional[str] = None,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        conn = self._connect()
        cursor = conn.cursor()
        where_clauses: List[str] = []
        params: List[Any] = []

        if room_id:
            where_clauses.append("room_id = ?")
            params.append(room_id)
        if sender_keyword:
            where_clauses.append("(username LIKE ? OR user_id LIKE ?)")
            like_value = f"%{sender_keyword}%"
            params.extend([like_value, like_value])
        if keyword:
            where_clauses.append("(content LIKE ? OR ext_info LIKE ?)")
            like_value = f"%{keyword}%"
            params.extend([like_value, like_value])
        if msg_type:
            where_clauses.append("msg_type = ?")
            params.append(msg_type)
        if start_time_ms is not None:
            where_clauses.append("timestamp >= ?")
            params.append(start_time_ms)
        if end_time_ms is not None:
            where_clauses.append("timestamp <= ?")
            params.append(end_time_ms)
        if sender_role == "member":
            where_clauses.append(
                '(ext_info LIKE \'%"roleId": 3%\' OR ext_info LIKE \'%"channelRole": "2"%\' OR ext_info LIKE \'%"channelRole": 2%\')'
            )
        elif sender_role == "fan":
            where_clauses.append(
                'NOT (ext_info LIKE \'%"roleId": 3%\' OR ext_info LIKE \'%"channelRole": "2"%\' OR ext_info LIKE \'%"channelRole": 2%\')'
            )

        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        cursor.execute(f"SELECT COUNT(*) FROM messages{where_sql}", params)
        total = cursor.fetchone()[0]

        data_params = params + [limit, offset]
        cursor.execute(
            f"""
            SELECT room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp, created_at
            FROM messages
            {where_sql}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """,
            data_params,
        )
        rows = cursor.fetchall()
        conn.close()
        items = [
            {
                "room_id": row[0],
                "message_id": row[1],
                "user_id": row[2],
                "username": row[3],
                "sender_role": _determine_sender_role(str(row[6])),
                "content": row[4],
                "msg_type": row[5],
                "ext_info": row[6],
                "timestamp": row[7],
                "created_at": row[8],
            }
            for row in rows
        ]
        return {"total": total, "items": items}

    def get_message_detail(self, message_id: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp, created_at
            FROM messages
            WHERE message_id = ?
            LIMIT 1
        """,
            (message_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return {
            "room_id": row[0],
            "message_id": row[1],
            "user_id": row[2],
            "username": row[3],
            "sender_role": _determine_sender_role(str(row[6])),
            "content": row[4],
            "msg_type": row[5],
            "ext_info": row[6],
            "timestamp": row[7],
            "created_at": row[8],
        }


class MySQLStorage(MessageStorage):
    _pool_size: int = 10
    _pool: List[pymysql.connections.Connection] = []
    _pool_lock: Any = None

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        charset: str = "utf8mb4",
        pool_size: int = 10,
    ):
        self.connection_args = {
            "host": host,
            "port": int(port),
            "database": database,
            "user": user,
            "password": password,
            "charset": charset,
            "cursorclass": DictCursor,
            "autocommit": False,
        }
        self._server_connection_args = {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "charset": charset,
            "cursorclass": DictCursor,
            "autocommit": False,
        }
        self._pool_size = pool_size
        self._ensure_database()
        self._init_database()
        self._init_pool()

    @classmethod
    def _get_pool_lock(cls):
        if cls._pool_lock is None:
            import threading

            cls._pool_lock = threading.Lock()
        return cls._pool_lock

    def _init_pool(self):
        lock = self._get_pool_lock()
        with lock:
            if not MySQLStorage._pool:
                MySQLStorage._pool = []

    def _get_connection(self) -> pymysql.connections.Connection:
        lock = self._get_pool_lock()
        with lock:
            if MySQLStorage._pool:
                return MySQLStorage._pool.pop()
        conn = pymysql.connect(**self.connection_args)
        return conn

    def _return_connection(self, conn: pymysql.connections.Connection):
        try:
            lock = self._get_pool_lock()
            with lock:
                if len(MySQLStorage._pool) < self._pool_size:
                    MySQLStorage._pool.append(conn)
                    return
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    @contextmanager
    def _get_conn(self):
        conn = self._get_connection()
        try:
            yield conn
        finally:
            self._return_connection(conn)

    def _connect(self):
        return pymysql.connect(**self.connection_args)

    def _connect_server(self):
        return pymysql.connect(**self._server_connection_args)

    def _ensure_database(self):
        database_name = self.connection_args["database"]
        conn = self._connect_server()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            conn.commit()
        finally:
            conn.close()

    def _init_database(self):
        conn = self._connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS members (
                        id BIGINT PRIMARY KEY,
                        member_name VARCHAR(255) NOT NULL,
                        room_id BIGINT NOT NULL,
                        sender_user_id BIGINT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY idx_members_room_id (room_id),
                        KEY idx_members_sender_user_id (sender_user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS rooms (
                        id BIGINT PRIMARY KEY,
                        owner_member_id BIGINT NOT NULL,
                        room_name VARCHAR(255) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY idx_rooms_owner_member_id (owner_member_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS messages (
                        message_id VARCHAR(128) PRIMARY KEY,
                        room_id BIGINT NOT NULL,
                        sender_user_id BIGINT NULL,
                        sender_name VARCHAR(255) NULL,
                        owner_member_id BIGINT NOT NULL,
                        message_type VARCHAR(64) NOT NULL,
                        sub_type VARCHAR(64) NULL,
                        text_content LONGTEXT NULL,
                        message_time DATETIME NOT NULL,
                        message_time_ms BIGINT NOT NULL,
                        is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                        raw_brief LONGTEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY idx_messages_room_time (room_id, message_time),
                        KEY idx_messages_owner_time (owner_member_id, message_time),
                        KEY idx_messages_sender_user_id (sender_user_id),
                        KEY idx_messages_message_time_ms (message_time_ms)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS message_payloads (
                        message_id VARCHAR(128) PRIMARY KEY,
                        media_url TEXT NULL,
                        media_path TEXT NULL,
                        media_cover_url TEXT NULL,
                        media_duration BIGINT NULL,
                        width INT NULL,
                        height INT NULL,
                        reply_to_message_id VARCHAR(128) NULL,
                        reply_to_text LONGTEXT NULL,
                        flip_user_name VARCHAR(255) NULL,
                        flip_question LONGTEXT NULL,
                        flip_answer LONGTEXT NULL,
                        ext_json LONGTEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS crawl_tasks (
                        id BIGINT PRIMARY KEY AUTO_INCREMENT,
                        room_id BIGINT NOT NULL,
                        task_type VARCHAR(32) NOT NULL,
                        status VARCHAR(32) NOT NULL,
                        start_time_ms BIGINT NOT NULL,
                        end_time_ms BIGINT NOT NULL,
                        last_message_time_ms BIGINT NULL,
                        error_message TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY idx_crawl_tasks_room_id (room_id),
                        KEY idx_crawl_tasks_status (status),
                        KEY idx_crawl_tasks_last_message_time_ms (last_message_time_ms)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS crawl_checkpoints (
                        room_id BIGINT PRIMARY KEY,
                        last_message_id VARCHAR(128) NULL,
                        last_message_time_ms BIGINT NULL,
                        last_success_at DATETIME NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY idx_crawl_checkpoints_last_message_time_ms (last_message_time_ms)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _find_member_sender_user_id(
        self, messages: List[Dict[str, Any]]
    ) -> Optional[int]:
        for message in messages:
            sender_user_id = _extract_member_sender_user_id(message)
            if sender_user_id not in (None, ""):
                return int(sender_user_id) if sender_user_id else None
        return None

    def backfill_member_sender_user_ids(self) -> int:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT owner_member_id, sender_user_id
                        FROM messages
                        WHERE raw_brief LIKE %s OR raw_brief LIKE %s
                        ORDER BY owner_member_id, message_time_ms DESC
                        """,
                        ('%"roleId": 3%', '%"channelRole": "2"%'),
                    )
                    latest_by_member: Dict[Any, Any] = {}
                    for row in cursor.fetchall():
                        owner_member_id = row["owner_member_id"]
                        sender_user_id = row["sender_user_id"]
                        if (
                            owner_member_id not in latest_by_member
                            and sender_user_id not in (None, "")
                        ):
                            latest_by_member[owner_member_id] = sender_user_id

                    updated = 0
                    for owner_member_id, sender_user_id in latest_by_member.items():
                        updated += cursor.execute(
                            "UPDATE members SET sender_user_id=%s WHERE id=%s",
                            (sender_user_id, owner_member_id),
                        )
                conn.commit()
                return updated
            except Exception:
                conn.rollback()
                raise

    def save_message(self, message: Dict[str, Any]) -> bool:
        owner_member_id = message.get("owner_member_id")
        room_id = message.get("room_id")
        if owner_member_id is None or room_id is None:
            raise ValueError(f"消息缺少 owner_member_id 或 room_id，无法写入 MySQL")

        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO members (id, member_name, room_id, sender_user_id)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            member_name = VALUES(member_name),
                            room_id = VALUES(room_id),
                            sender_user_id = COALESCE(VALUES(sender_user_id), sender_user_id),
                            updated_at = CURRENT_TIMESTAMP
                """,
                        (
                            owner_member_id,
                            message.get("member_name")
                            or message.get("username")
                            or str(owner_member_id),
                            room_id,
                            _extract_member_sender_user_id(message),
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO rooms (id, owner_member_id, room_name)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            owner_member_id = VALUES(owner_member_id),
                            room_name = VALUES(room_name),
                            updated_at = CURRENT_TIMESTAMP
                """,
                        (
                            room_id,
                            owner_member_id,
                            message.get("member_name") or str(room_id),
                        ),
                    )

                    message_id = str(message.get("message_id") or "")
                    inserted = cursor.execute(
                        """
                        INSERT IGNORE INTO messages (
                            message_id, room_id, sender_user_id, sender_name, owner_member_id,
                            message_type, sub_type, text_content, message_time, message_time_ms,
                            is_deleted, raw_brief
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            message_id,
                            room_id,
                            message.get("user_id"),
                            message.get("username"),
                            owner_member_id,
                            str(message.get("msg_type") or "UNKNOWN"),
                            message.get("sub_type"),
                            _extract_text_content(
                                message.get("content"), message.get("ext_info")
                            ),
                            _timestamp_ms_to_datetime(message.get("timestamp")),
                            message.get("timestamp"),
                            0,
                            _json_dumps(
                                {
                                    "body": _parse_json_like(message.get("content")),
                                    "extInfo": _parse_json_like(
                                        message.get("ext_info")
                                    ),
                                }
                            ),
                        ),
                    )

                    if inserted:
                        payload = _extract_media_fields(
                            message.get("content"), message.get("ext_info")
                        )
                        cursor.execute(
                            """
                            INSERT INTO message_payloads (
                                message_id, media_url, media_path, media_cover_url, media_duration,
                                width, height, reply_to_message_id, reply_to_text,
                                flip_user_name, flip_question, flip_answer, ext_json
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                media_url = VALUES(media_url),
                                media_cover_url = VALUES(media_cover_url),
                                media_duration = VALUES(media_duration),
                                width = VALUES(width),
                                height = VALUES(height),
                                reply_to_text = VALUES(reply_to_text),
                                flip_user_name = VALUES(flip_user_name),
                                flip_question = VALUES(flip_question),
                                flip_answer = VALUES(flip_answer),
                                ext_json = VALUES(ext_json)
                        """,
                            (
                                message_id,
                                payload["media_url"],
                                None,
                                payload["media_cover_url"],
                                payload["media_duration"],
                                payload["width"],
                                payload["height"],
                                None,
                                payload["reply_to_text"],
                                payload["flip_user_name"],
                                payload["flip_question"],
                                payload["flip_answer"],
                                payload["ext_json"],
                            ),
                        )

                conn.commit()
                return inserted > 0
            except Exception:
                conn.rollback()
                raise

    def save_messages(self, messages: List[Dict[str, Any]]) -> int:
        if not messages:
            return 0

        first_message = messages[0]
        owner_member_id = first_message.get("owner_member_id")
        room_id = first_message.get("room_id")
        if owner_member_id is None or room_id is None:
            raise ValueError(f"消息缺少 owner_member_id 或 room_id，无法写入 MySQL")

        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    member_sender_user_id = self._find_member_sender_user_id(messages)
                    message_ids = [
                        str(message.get("message_id") or "") for message in messages
                    ]
                    existing_message_ids: set[str] = set()
                    if message_ids:
                        placeholders = ", ".join(["%s"] * len(message_ids))
                        cursor.execute(
                            f"SELECT message_id FROM messages WHERE message_id IN ({placeholders})",
                            message_ids,
                        )
                        existing_message_ids = {
                            str(row["message_id"] or "") for row in cursor.fetchall()
                        }
                    cursor.execute(
                        """
                        INSERT INTO members (id, member_name, room_id, sender_user_id)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            member_name = VALUES(member_name),
                            room_id = VALUES(room_id),
                            sender_user_id = COALESCE(VALUES(sender_user_id), sender_user_id),
                            updated_at = CURRENT_TIMESTAMP
                """,
                        (
                            owner_member_id,
                            first_message.get("member_name")
                            or first_message.get("username")
                            or str(owner_member_id),
                            room_id,
                            member_sender_user_id,
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO rooms (id, owner_member_id, room_name)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            owner_member_id = VALUES(owner_member_id),
                            room_name = VALUES(room_name),
                            updated_at = CURRENT_TIMESTAMP
                """,
                        (
                            room_id,
                            owner_member_id,
                            first_message.get("member_name") or str(room_id),
                        ),
                    )

                    message_rows = []
                    payload_rows = []
                    pending_payload_message_ids: set[str] = set()
                    for message in messages:
                        message_id = str(message.get("message_id") or "")
                        message_rows.append(
                            (
                                message_id,
                                message.get("room_id"),
                                message.get("user_id"),
                                message.get("username"),
                                message.get("owner_member_id"),
                                str(message.get("msg_type") or "UNKNOWN"),
                                message.get("sub_type"),
                                _extract_text_content(
                                    message.get("content"), message.get("ext_info")
                                ),
                                _timestamp_ms_to_datetime(message.get("timestamp")),
                                message.get("timestamp"),
                                0,
                                _json_dumps(
                                    {
                                        "body": _parse_json_like(
                                            message.get("content")
                                        ),
                                        "extInfo": _parse_json_like(
                                            message.get("ext_info")
                                        ),
                                    }
                                ),
                            )
                        )
                        if (
                            message_id not in existing_message_ids
                            and message_id not in pending_payload_message_ids
                        ):
                            payload = _extract_media_fields(
                                message.get("content"), message.get("ext_info")
                            )
                            pending_payload_message_ids.add(message_id)
                            payload_rows.append(
                                (
                                    message_id,
                                    payload["media_url"],
                                    None,
                                    payload["media_cover_url"],
                                    payload["media_duration"],
                                    payload["width"],
                                    payload["height"],
                                    None,
                                    payload["reply_to_text"],
                                    payload["flip_user_name"],
                                    payload["flip_question"],
                                    payload["flip_answer"],
                                    payload["ext_json"],
                                )
                            )

                    cursor.executemany(
                        """
                        INSERT IGNORE INTO messages (
                            message_id, room_id, sender_user_id, sender_name, owner_member_id,
                            message_type, sub_type, text_content, message_time, message_time_ms,
                            is_deleted, raw_brief
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        message_rows,
                    )
                    saved_count = max(cursor.rowcount, 0)

                    if payload_rows:
                        cursor.executemany(
                            """
                            INSERT INTO message_payloads (
                                message_id, media_url, media_path, media_cover_url, media_duration,
                                width, height, reply_to_message_id, reply_to_text,
                                flip_user_name, flip_question, flip_answer, ext_json
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                media_url = VALUES(media_url),
                                media_cover_url = VALUES(media_cover_url),
                                media_duration = VALUES(media_duration),
                                width = VALUES(width),
                                height = VALUES(height),
                                reply_to_text = VALUES(reply_to_text),
                                flip_user_name = VALUES(flip_user_name),
                                flip_question = VALUES(flip_question),
                                flip_answer = VALUES(flip_answer),
                                ext_json = VALUES(ext_json)
                        """,
                            payload_rows,
                        )

                conn.commit()
                return saved_count
            except Exception:
                conn.rollback()
                raise

    def get_messages(self, room_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            m.room_id,
                            m.message_id,
                            m.sender_user_id AS user_id,
                            m.sender_name AS username,
                            m.text_content AS content,
                            m.message_type AS msg_type,
                            mp.ext_json AS ext_info,
                            m.message_time_ms AS timestamp,
                            m.created_at
                        FROM messages m
                        LEFT JOIN message_payloads mp ON mp.message_id = m.message_id
                        WHERE m.room_id = %s
                        ORDER BY m.message_time DESC
                        LIMIT %s
                    """,
                        (room_id, limit),
                    )
                    return list(cursor.fetchall())
            except Exception:
                logger.error("获取房间消息失败 room_id=%s: %s", room_id, exc_info=True)
                raise

    def get_latest_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT message_id, message_time_ms AS timestamp
                        FROM messages
                        WHERE room_id = %s
                        ORDER BY message_time DESC
                        LIMIT 1
                    """,
                        (room_id,),
                    )
                    return cursor.fetchone()
            except Exception:
                logger.error("获取最新消息失败 room_id=%s: %s", room_id, exc_info=True)
                raise

    def export_messages(
        self,
        output_path: str,
        room_id: Optional[str] = None,
        limit: Optional[int] = None,
        output_format: str = "json",
    ) -> int:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    query = """
                        SELECT
                            m.room_id,
                            m.message_id,
                            m.sender_user_id AS user_id,
                            m.sender_name AS username,
                            m.text_content AS content,
                            m.message_type AS msg_type,
                            mp.ext_json AS ext_info,
                            m.message_time_ms AS timestamp,
                            m.created_at
                        FROM messages m
                        LEFT JOIN message_payloads mp ON mp.message_id = m.message_id
                    """
                    params: List[Any] = []
                    if room_id:
                        query += " WHERE m.room_id = %s"
                        params.append(room_id)
                    query += " ORDER BY m.message_time DESC"
                    if limit is not None:
                        query += " LIMIT %s"
                        params.append(limit)
                    cursor.execute(query, params)
                    messages = list(cursor.fetchall())
            except Exception:
                logger.error("导出消息失败 room_id=%s: %s", room_id, exc_info=True)
                raise

        if output_format == "json":
            with open(output_path, "w", encoding="utf-8") as file:
                json.dump(messages, file, ensure_ascii=False, indent=2, default=str)
            return len(messages)
        if output_format == "csv":
            fieldnames = (
                list(messages[0].keys())
                if messages
                else [
                    "room_id",
                    "message_id",
                    "user_id",
                    "username",
                    "content",
                    "msg_type",
                    "ext_info",
                    "timestamp",
                    "created_at",
                ]
            )
            with open(output_path, "w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(messages)
            return len(messages)
        raise ValueError(f"不支持的导出格式: {output_format}")

    def record_fetch(
        self,
        room_id: str,
        messages_count: int,
        status: str,
        error_message: Optional[str] = None,
        last_message_id: Optional[str] = None,
        last_message_time_ms: Optional[int] = None,
    ) -> None:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO crawl_tasks (
                            room_id, task_type, status, start_time_ms, end_time_ms,
                            last_message_time_ms, error_message
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            room_id,
                            "incremental",
                            status,
                            int(datetime.now().timestamp() * 1000),
                            int(datetime.now().timestamp() * 1000),
                            last_message_time_ms,
                            error_message,
                        ),
                    )
                    if status == "success":
                        cursor.execute(
                            """
                            INSERT INTO crawl_checkpoints (
                                room_id, last_message_id, last_message_time_ms, last_success_at
                            ) VALUES (%s, %s, %s, NOW())
                            ON DUPLICATE KEY UPDATE
                                last_message_id = COALESCE(VALUES(last_message_id), last_message_id),
                                last_message_time_ms = COALESCE(VALUES(last_message_time_ms), last_message_time_ms),
                                last_success_at = VALUES(last_success_at),
                                updated_at = CURRENT_TIMESTAMP
                        """,
                            (room_id, last_message_id, last_message_time_ms),
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                logger.error(
                    "记录抓取状态失败 room_id=%s, status=%s: %s",
                    room_id,
                    status,
                    exc_info=True,
                )
                raise

    def get_statistics(self) -> Dict[str, Any]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) AS cnt FROM messages")
                    total_messages = cursor.fetchone()["cnt"]
                    cursor.execute(
                        "SELECT COUNT(DISTINCT room_id) AS cnt FROM messages"
                    )
                    total_rooms = cursor.fetchone()["cnt"]
                    cursor.execute(
                        "SELECT COUNT(*) AS cnt FROM crawl_tasks WHERE status = 'success'"
                    )
                    successful_fetches = cursor.fetchone()["cnt"]
                    cursor.execute("""
                        SELECT room_id, COUNT(*) AS cnt
                        FROM messages
                        GROUP BY room_id
                        ORDER BY cnt DESC
                        LIMIT 10
                    """)
                    top_rooms_rows = cursor.fetchall()
            except Exception:
                logger.error("获取统计信息失败: %s", exc_info=True)
                raise

        return {
            "total_messages": total_messages,
            "total_rooms": total_rooms,
            "successful_fetches": successful_fetches,
            "top_rooms": [(row["room_id"], row["cnt"]) for row in top_rooms_rows],
        }

    def list_rooms(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT
                            r.id,
                            COALESCE(r.room_name, CAST(r.id AS CHAR)) AS name,
                            COUNT(m.message_id) AS message_count,
                            MAX(m.message_time_ms) AS latest_timestamp
                        FROM rooms r
                        LEFT JOIN messages m ON m.room_id = r.id
                        GROUP BY r.id, r.room_name
                        ORDER BY latest_timestamp DESC, r.id DESC
                    """)
                    return list(cursor.fetchall())
            except Exception:
                logger.error("获取房间列表失败: %s", exc_info=True)
                raise

    def list_senders(self, room_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    query = """
                        SELECT
                            sender_user_id AS user_id,
                            sender_name AS username,
                            COUNT(*) AS message_count,
                            MAX(message_time_ms) AS latest_timestamp
                        FROM messages
                    """
                    params: List[Any] = []
                    if room_id:
                        query += " WHERE room_id = %s"
                        params.append(room_id)
                    query += """
                        GROUP BY sender_user_id, sender_name
                        ORDER BY latest_timestamp DESC, username ASC
                    """
                    cursor.execute(query, params)
                    return list(cursor.fetchall())
            except Exception:
                logger.error(
                    "获取发送者列表失败 room_id=%s: %s", room_id, exc_info=True
                )
                raise

    def search_messages(
        self,
        room_id: Optional[str] = None,
        sender_keyword: Optional[str] = None,
        keyword: Optional[str] = None,
        msg_type: Optional[str] = None,
        sender_role: Optional[str] = None,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    where_clauses: List[str] = []
                    params: List[Any] = []

                    if room_id:
                        where_clauses.append("m.room_id = %s")
                        params.append(room_id)
                    if sender_keyword:
                        where_clauses.append(
                            "(m.sender_name LIKE %s OR CAST(m.sender_user_id AS CHAR) LIKE %s)"
                        )
                        like_value = f"%{sender_keyword}%"
                        params.extend([like_value, like_value])
                    if keyword:
                        where_clauses.append(
                            "("
                            "m.text_content LIKE %s OR "
                            "m.raw_brief LIKE %s OR "
                            "mp.ext_json LIKE %s OR "
                            "mp.flip_question LIKE %s OR "
                            "mp.flip_answer LIKE %s OR "
                            "mp.reply_to_text LIKE %s"
                            ")"
                        )
                        like_value = f"%{keyword}%"
                        params.extend(
                            [
                                like_value,
                                like_value,
                                like_value,
                                like_value,
                                like_value,
                                like_value,
                            ]
                        )
                    if msg_type:
                        where_clauses.append("m.message_type = %s")
                        params.append(msg_type)
                    if start_time_ms is not None:
                        where_clauses.append("m.message_time_ms >= %s")
                        params.append(start_time_ms)
                    if end_time_ms is not None:
                        where_clauses.append("m.message_time_ms <= %s")
                        params.append(end_time_ms)
                    if sender_role == "member":
                        where_clauses.append(
                            "(m.raw_brief LIKE %s OR m.raw_brief LIKE %s OR mp.ext_json LIKE %s OR mp.ext_json LIKE %s)"
                        )
                        params.extend(
                            [
                                '%"roleId": 3%',
                                '%"channelRole": "2"%',
                                '%"roleId": 3%',
                                '%"channelRole": "2"%',
                            ]
                        )
                    elif sender_role == "fan":
                        where_clauses.append(
                            "NOT (m.raw_brief LIKE %s OR m.raw_brief LIKE %s OR mp.ext_json LIKE %s OR mp.ext_json LIKE %s)"
                        )
                        params.extend(
                            [
                                '%"roleId": 3%',
                                '%"channelRole": "2"%',
                                '%"roleId": 3%',
                                '%"channelRole": "2"%',
                            ]
                        )

                    where_sql = (
                        f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
                    )
                    count_query = (
                        "SELECT COUNT(*) AS cnt FROM messages m "
                        "LEFT JOIN message_payloads mp ON mp.message_id = m.message_id"
                        f"{where_sql}"
                    )
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()["cnt"]

                    data_query = (
                        """
                        SELECT
                            m.room_id,
                            COALESCE(r.room_name, CAST(m.room_id AS CHAR)) AS room_name,
                            m.message_id,
                            m.sender_user_id AS user_id,
                            m.sender_name AS username,
                            CASE
                                WHEN m.raw_brief LIKE '%%\"roleId\": 3%%'
                                     OR m.raw_brief LIKE '%%\"channelRole\": \"2\"%%'
                                     OR mp.ext_json LIKE '%%\"roleId\": 3%%'
                                     OR mp.ext_json LIKE '%%\"channelRole\": \"2\"%%'
                                THEN 'member'
                                ELSE 'fan'
                            END AS sender_role,
                            m.text_content AS content,
                            m.message_type AS msg_type,
                            mp.ext_json AS ext_info,
                            m.message_time_ms AS timestamp,
                            m.created_at,
                            mp.media_url,
                            mp.media_cover_url,
                            mp.reply_to_text,
                            mp.flip_question,
                            mp.flip_answer
                        FROM messages m
                        LEFT JOIN rooms r ON r.id = m.room_id
                        LEFT JOIN message_payloads mp ON mp.message_id = m.message_id
                        """
                        f"{where_sql}"
                        " ORDER BY m.message_time DESC, m.message_id DESC LIMIT %s OFFSET %s"
                    )
                    cursor.execute(data_query, params + [limit, offset])
                    items = list(cursor.fetchall())
                    return {"total": total, "items": items}
            except Exception:
                logger.error("搜索消息失败: %s", exc_info=True)
                raise

    def get_message_detail(self, message_id: str) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            m.room_id,
                            COALESCE(r.room_name, CAST(m.room_id AS CHAR)) AS room_name,
                            m.message_id,
                            m.sender_user_id AS user_id,
                            m.sender_name AS username,
                            m.owner_member_id,
                            CASE
                                WHEN m.raw_brief LIKE '%%\"roleId\": 3%%'
                                     OR m.raw_brief LIKE '%%\"channelRole\": \"2\"%%'
                                     OR mp.ext_json LIKE '%%\"roleId\": 3%%'
                                     OR mp.ext_json LIKE '%%\"channelRole\": \"2\"%%'
                                THEN 'member'
                                ELSE 'fan'
                            END AS sender_role,
                            m.message_type AS msg_type,
                            m.sub_type,
                            m.text_content AS content,
                            m.raw_brief,
                            m.message_time,
                            m.message_time_ms AS timestamp,
                            m.created_at,
                            mp.media_url,
                            mp.media_cover_url,
                            mp.media_duration,
                            mp.width,
                            mp.height,
                            mp.reply_to_text,
                            mp.flip_user_name,
                            mp.flip_question,
                            mp.flip_answer,
                            mp.ext_json AS ext_info
                        FROM messages m
                        LEFT JOIN rooms r ON r.id = m.room_id
                        LEFT JOIN message_payloads mp ON mp.message_id = m.message_id
                        WHERE m.message_id = %s
                        LIMIT 1
                    """,
                        (message_id,),
                    )
                    return cursor.fetchone()
            except Exception:
                logger.error(
                    "获取消息详情失败 message_id=%s: %s", message_id, exc_info=True
                )
                raise


class StorageConfigError(ValueError):
    pass


def _validate_storage_config(storage_config: Dict[str, Any], storage_type: str) -> None:
    errors: List[str] = []
    if storage_type == "mysql":
        if not storage_config.get("database"):
            errors.append("database (MySQL database name) is required")
        if not storage_config.get("user"):
            errors.append("user (MySQL user) is required")
        if not storage_config.get("password"):
            errors.append("password (MySQL password) is required")
        if not storage_config.get("host"):
            errors.append("host (MySQL host) is required")
    if errors:
        raise StorageConfigError(f"Invalid storage config: {', '.join(errors)}")


def create_storage(config: Dict[str, Any]) -> MessageStorage:
    """按配置选择具体存储实现。"""
    storage_config = config.get("storage", {})
    storage_type = storage_config.get("type", "mysql")
    _validate_storage_config(storage_config, storage_type)
    if storage_type == "sqlite":
        return SQLiteStorage(storage_config.get("database", "data/messages.db"))
    if storage_type == "mysql":
        return MySQLStorage(
            host=storage_config.get("host", "localhost"),
            port=storage_config.get("port", 3306),
            database=storage_config["database"],
            user=storage_config["user"],
            password=storage_config["password"],
            charset=storage_config.get("charset", "utf8mb4"),
            pool_size=storage_config.get("pool_size", 10),
        )
    raise ValueError(f"不支持的存储类型: {storage_type}")
