import csv
import json
import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from message_parser import (
    FAN_SENDER_ROLE,
    MEMBER_SENDER_ROLE,
    determine_sender_role_from_message,
    json_dumps,
    _sqlite_sender_role_case_expression,
)
from message_storage import MessageStorage

logger = logging.getLogger(__name__)


class SQLiteStorage(MessageStorage):
    def __init__(self, db_path: str = "data/messages.db"):
        self.db_path = db_path
        self._init_database()

    def _connect(self):
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(self.db_path)

    @contextmanager
    def _get_conn(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def sync_members(self, members: List[Dict[str, Any]]) -> int:
        rows = []
        for member in members:
            if not isinstance(member, dict):
                continue
            member_id = (
                member.get("memberId")
                if member.get("memberId") is not None
                else member.get("id")
            )
            owner_name = (
                member.get("ownerName")
                or member.get("memberName")
                or member.get("nickname")
                or ""
            )
            if member_id is None:
                continue
            if member.get("serverId") is None or member.get("channelId") is None:
                continue
            if not owner_name:
                continue
            rows.append(
                (
                    member_id,
                    owner_name,
                    member.get("nickname"),
                    member.get("serverId"),
                    member.get("channelId"),
                    member.get("roomId"),
                    member.get("team"),
                    member.get("avatar"),
                )
            )

        if not rows:
            return 0

        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO members (
                    id, owner_name, nickname, server_id, channel_id, room_id, team, avatar
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    owner_name = excluded.owner_name,
                    nickname = excluded.nickname,
                    server_id = excluded.server_id,
                    channel_id = excluded.channel_id,
                    room_id = excluded.room_id,
                    team = excluded.team,
                    avatar = excluded.avatar,
                    updated_at = CURRENT_TIMESTAMP
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def _init_database(self):
        # SQLite 版本只保留最小表结构，适合本地试跑和调试。
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_id TEXT NOT NULL,
                    message_id TEXT UNIQUE,
                    user_id TEXT,
                    username TEXT,
                    member_name TEXT,
                    sender_role TEXT NOT NULL DEFAULT 'fan',
                    content TEXT,
                    msg_type TEXT,
                    ext_info TEXT,
                    timestamp INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS members (
                    id INTEGER PRIMARY KEY,
                    owner_name TEXT NOT NULL,
                    nickname TEXT,
                    server_id INTEGER NOT NULL UNIQUE,
                    channel_id INTEGER NOT NULL UNIQUE,
                    room_id TEXT,
                    team TEXT,
                    avatar TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS crawl_history_checkpoints (
                    server_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    oldest_covered_message_id TEXT,
                    oldest_covered_time_ms INTEGER,
                    resume_next_time INTEGER,
                    target_time_ms INTEGER,
                    status TEXT NOT NULL DEFAULT 'idle',
                    cursor_verified INTEGER NOT NULL DEFAULT 0,
                    last_page_count INTEGER NOT NULL DEFAULT 0,
                    last_run_started_at TIMESTAMP,
                    last_run_finished_at TIMESTAMP,
                    last_error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (server_id, channel_id)
                )
            """
            )
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_room_id
                ON messages(room_id, timestamp DESC)
            """)
            cursor.execute("PRAGMA table_info(messages)")
            columns = {row[1] for row in cursor.fetchall()}
            if "member_name" not in columns:
                cursor.execute("ALTER TABLE messages ADD COLUMN member_name TEXT")
            if "sender_role" not in columns:
                cursor.execute(
                    "ALTER TABLE messages ADD COLUMN sender_role TEXT NOT NULL DEFAULT 'fan'"
                )
                cursor.execute(
                    f"UPDATE messages SET sender_role = {_sqlite_sender_role_case_expression()}"
                )
            else:
                cursor.execute(
                    f"UPDATE messages SET sender_role = {_sqlite_sender_role_case_expression()} WHERE sender_role IS NULL OR sender_role = ''"
                )
                cursor.execute(
                    f"UPDATE messages SET sender_role = {_sqlite_sender_role_case_expression()} WHERE sender_role = '{FAN_SENDER_ROLE}' AND ({_sqlite_sender_role_case_expression()} = '{MEMBER_SENDER_ROLE}')"
                )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_role_time
                ON messages(sender_role, timestamp DESC)
            """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_room_role_time
                ON messages(room_id, sender_role, timestamp DESC)
            """
            )
            conn.commit()

    def save_message(self, message: Dict[str, Any]) -> bool:
        if not message.get("message_id"):
            logger.debug("跳过缺少 message_id 的消息: %s", message)
            return False
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO messages
                    (room_id, message_id, user_id, username, member_name, sender_role, content, msg_type, ext_info, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        message.get("room_id"),
                        message.get("message_id"),
                        message.get("user_id"),
                        message.get("username"),
                        message.get("member_name"),
                        determine_sender_role_from_message(message),
                        json_dumps(message.get("content")),
                        str(message.get("msg_type") or ""),
                        json_dumps(message.get("ext_info")),
                        message.get("timestamp"),
                    ),
                )
                conn.commit()
                affected = cursor.rowcount
            return affected > 0
        except Exception as exc:
            logger.error("保存消息失败: %s", exc)
            return False

    def save_messages(self, messages: List[Dict[str, Any]]) -> int:
        messages = [message for message in messages if message.get("message_id")]
        if not messages:
            return 0

        try:
            with self._get_conn() as conn:
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
                    (room_id, message_id, user_id, username, member_name, sender_role, content, msg_type, ext_info, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    [
                        (
                            message.get("room_id"),
                            message.get("message_id"),
                            message.get("user_id"),
                            message.get("username"),
                            message.get("member_name"),
                            determine_sender_role_from_message(message),
                            json_dumps(message.get("content")),
                            str(message.get("msg_type") or ""),
                            json_dumps(message.get("ext_info")),
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
            return len(existing_after - existing_before)
        except Exception as exc:
            logger.error("批量保存消息失败: %s", exc)
            return 0

    def get_messages(self, room_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT room_id, message_id, user_id, username, member_name, sender_role, content, msg_type, ext_info, timestamp, created_at
                FROM messages WHERE room_id = ? ORDER BY timestamp DESC LIMIT ?
            """,
                (room_id, limit),
            )
            rows = cursor.fetchall()
        return [
            {
                "room_id": row[0],
                "message_id": row[1],
                "user_id": row[2],
                "username": row[3],
                "member_name": row[4],
                "sender_role": row[5],
                "content": row[6],
                "msg_type": row[7],
                "ext_info": row[8],
                "timestamp": row[9],
                "created_at": row[10],
            }
            for row in rows
        ]

    def get_latest_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        messages = self.get_messages(room_id, 1)
        return messages[0] if messages else None

    def get_history_checkpoint(
        self, server_id: int, channel_id: int
    ) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT server_id, channel_id, oldest_covered_message_id, oldest_covered_time_ms,
                       resume_next_time, target_time_ms, status, cursor_verified,
                       last_page_count, last_run_started_at, last_run_finished_at,
                       last_error_message, created_at, updated_at
                FROM crawl_history_checkpoints
                WHERE server_id = ? AND channel_id = ?
                LIMIT 1
            """,
                (server_id, channel_id),
            )
            row = cursor.fetchone()
        if not row:
            return None
        return {
            "server_id": row[0],
            "channel_id": row[1],
            "oldest_covered_message_id": row[2],
            "oldest_covered_time_ms": row[3],
            "resume_next_time": row[4],
            "target_time_ms": row[5],
            "status": row[6],
            "cursor_verified": bool(row[7]),
            "last_page_count": row[8],
            "last_run_started_at": row[9],
            "last_run_finished_at": row[10],
            "last_error_message": row[11],
            "created_at": row[12],
            "updated_at": row[13],
        }

    def start_history_fetch(
        self, server_id: int, channel_id: int, target_time_ms: int
    ) -> None:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO crawl_history_checkpoints (
                    server_id, channel_id, target_time_ms, status,
                    last_page_count, last_run_started_at, last_run_finished_at,
                    last_error_message, updated_at
                ) VALUES (?, ?, ?, 'running', 0, CURRENT_TIMESTAMP, NULL, NULL, CURRENT_TIMESTAMP)
                ON CONFLICT(server_id, channel_id) DO UPDATE SET
                    target_time_ms = excluded.target_time_ms,
                    status = 'running',
                    last_page_count = 0,
                    last_run_started_at = CURRENT_TIMESTAMP,
                    last_run_finished_at = NULL,
                    last_error_message = NULL,
                    updated_at = CURRENT_TIMESTAMP
            """,
                (server_id, channel_id, target_time_ms),
            )
            conn.commit()

    def update_history_checkpoint_progress(
        self,
        server_id: int,
        channel_id: int,
        oldest_covered_message_id: Optional[str],
        oldest_covered_time_ms: Optional[int],
        resume_next_time: Optional[int],
        last_page_count: int,
        cursor_verified: Optional[bool] = None,
    ) -> None:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE crawl_history_checkpoints
                SET oldest_covered_message_id = COALESCE(?, oldest_covered_message_id),
                    oldest_covered_time_ms = COALESCE(?, oldest_covered_time_ms),
                    resume_next_time = ?,
                    status = 'running',
                    cursor_verified = COALESCE(?, cursor_verified),
                    last_page_count = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE server_id = ? AND channel_id = ?
            """,
                (
                    oldest_covered_message_id,
                    oldest_covered_time_ms,
                    resume_next_time,
                    None if cursor_verified is None else int(cursor_verified),
                    last_page_count,
                    server_id,
                    channel_id,
                ),
            )
            conn.commit()

    def finish_history_fetch_success(
        self,
        server_id: int,
        channel_id: int,
        target_time_ms: int,
        oldest_covered_message_id: Optional[str],
        oldest_covered_time_ms: Optional[int],
        resume_next_time: Optional[int],
        last_page_count: int,
        cursor_verified: Optional[bool] = None,
    ) -> None:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE crawl_history_checkpoints
                SET target_time_ms = ?,
                    oldest_covered_message_id = COALESCE(?, oldest_covered_message_id),
                    oldest_covered_time_ms = COALESCE(?, oldest_covered_time_ms),
                    resume_next_time = ?,
                    status = 'success',
                    cursor_verified = COALESCE(?, cursor_verified),
                    last_page_count = ?,
                    last_run_finished_at = CURRENT_TIMESTAMP,
                    last_error_message = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE server_id = ? AND channel_id = ?
            """,
                (
                    target_time_ms,
                    oldest_covered_message_id,
                    oldest_covered_time_ms,
                    resume_next_time,
                    None if cursor_verified is None else int(cursor_verified),
                    last_page_count,
                    server_id,
                    channel_id,
                ),
            )
            conn.commit()

    def finish_history_fetch_failed(
        self,
        server_id: int,
        channel_id: int,
        status: str,
        error_message: Optional[str],
        resume_next_time: Optional[int],
        last_page_count: int,
    ) -> None:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE crawl_history_checkpoints
                SET status = ?,
                    resume_next_time = ?,
                    last_page_count = ?,
                    last_run_finished_at = CURRENT_TIMESTAMP,
                    last_error_message = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE server_id = ? AND channel_id = ?
            """,
                (
                    status,
                    resume_next_time,
                    last_page_count,
                    error_message,
                    server_id,
                    channel_id,
                ),
            )
            conn.commit()

    def export_messages(
        self,
        output_path: str,
        room_id: Optional[str] = None,
        limit: Optional[int] = None,
        output_format: str = "json",
    ) -> int:
        fieldnames = [
            "room_id",
            "message_id",
            "user_id",
            "username",
            "member_name",
            "sender_role",
            "content",
            "msg_type",
            "ext_info",
            "timestamp",
            "created_at",
        ]
        if output_format not in {"json", "csv"}:
            raise ValueError(f"不支持的导出格式: {output_format}")

        with self._get_conn() as conn:
            cursor = conn.cursor()
            query = "SELECT room_id, message_id, user_id, username, member_name, sender_role, content, msg_type, ext_info, timestamp, created_at FROM messages"
            params: List[Any] = []
            if room_id:
                query += " WHERE room_id = ?"
                params.append(room_id)
            query += " ORDER BY timestamp DESC"
            if limit is not None:
                query += " LIMIT ?"
                params.append(limit)
            cursor.execute(query, params)

            def row_to_dict(row):
                return {
                    "room_id": row[0],
                    "message_id": row[1],
                    "user_id": row[2],
                    "username": row[3],
                    "member_name": row[4],
                    "sender_role": row[5],
                    "content": row[6],
                    "msg_type": row[7],
                    "ext_info": row[8],
                    "timestamp": row[9],
                    "created_at": row[10],
                }

            count = 0
            if output_format == "json":
                with open(output_path, "w", encoding="utf-8") as file:
                    file.write("[")
                    first = True
                    while True:
                        rows = cursor.fetchmany(1000)
                        if not rows:
                            break
                        for row in rows:
                            if not first:
                                file.write(",")
                            file.write("\n")
                            json.dump(row_to_dict(row), file, ensure_ascii=False)
                            first = False
                            count += 1
                    if count:
                        file.write("\n")
                    file.write("]")
                return count

            with open(output_path, "w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                while True:
                    rows = cursor.fetchmany(1000)
                    if not rows:
                        break
                    writer.writerows(row_to_dict(row) for row in rows)
                    count += len(rows)
                return count

    def record_fetch(
        self,
        room_id: str,
        messages_count: int,
        status: str,
        error_message: Optional[str] = None,
        last_message_id: Optional[str] = None,
        last_message_time_ms: Optional[int] = None,
        server_id: Optional[int] = None,
        channel_id: Optional[int] = None,
    ) -> None:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO fetch_logs (room_id, messages_count, status, error_message) VALUES (?, ?, ?, ?)",
                (room_id, messages_count, status, error_message),
            )
            conn.commit()

    def get_statistics(self) -> Dict[str, Any]:
        with self._get_conn() as conn:
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
        return {
            "total_messages": total_messages,
            "total_rooms": total_rooms,
            "successful_fetches": successful_fetches,
            "top_rooms": top_rooms,
        }

    def list_rooms(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    room_stats.room_id AS id,
                    COALESCE(mem.owner_name, room_stats.room_id) AS name,
                    room_stats.message_count,
                    room_stats.latest_timestamp
                FROM (
                    SELECT room_id, COUNT(*) AS message_count, MAX(timestamp) AS latest_timestamp
                    FROM messages
                    GROUP BY room_id
                ) room_stats
                LEFT JOIN members mem ON CAST(mem.channel_id AS TEXT) = room_stats.room_id
                ORDER BY room_stats.latest_timestamp DESC
            """)
            rows = cursor.fetchall()
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
        with self._get_conn() as conn:
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
        member_server_id: Optional[int] = None,
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
            cursor = conn.cursor()
            where_clauses: List[str] = []
            params: List[Any] = []
    
            if room_id:
                where_clauses.append("room_id = ?")
                params.append(room_id)
            if member_server_id is not None:
                where_clauses.append("user_id = ?")
                params.append(str(member_server_id))
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
            if sender_role in {MEMBER_SENDER_ROLE, FAN_SENDER_ROLE}:
                where_clauses.append("sender_role = ?")
                params.append(sender_role)
    
            where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            cursor.execute(f"SELECT COUNT(*) FROM messages{where_sql}", params)
            total = cursor.fetchone()[0]
            if limit <= 0:
                return {"total": total, "items": []}
    
            data_params = params + [limit, offset]
            cursor.execute(
                f"""
                SELECT room_id, message_id, user_id, username, member_name, sender_role, content, msg_type, ext_info, timestamp, created_at
                FROM messages
                {where_sql}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
            """,
                data_params,
            )
            rows = cursor.fetchall()
        items = [
            {
                "room_id": row[0],
                "message_id": row[1],
                "user_id": row[2],
                "username": row[3],
                "member_name": row[4],
                "sender_role": row[5],
                "content": row[6],
                "msg_type": row[7],
                "ext_info": row[8],
                "timestamp": row[9],
                "created_at": row[10],
            }
            for row in rows
        ]
        return {"total": total, "items": items}

    def get_message_detail(self, message_id: str) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT room_id, message_id, user_id, username, member_name, sender_role, content, msg_type, ext_info, timestamp, created_at
                FROM messages
                WHERE message_id = ?
                LIMIT 1
            """,
                (message_id,),
            )
            row = cursor.fetchone()
        if not row:
            return None
        return {
            "room_id": row[0],
            "message_id": row[1],
            "user_id": row[2],
            "username": row[3],
            "member_name": row[4],
            "sender_role": row[5],
            "content": row[6],
            "msg_type": row[7],
            "ext_info": row[8],
            "timestamp": row[9],
            "created_at": row[10],
        }

    def list_members(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    mem.server_id,
                    mem.owner_name,
                    COUNT(msg.message_id) AS message_count,
                    MAX(msg.timestamp) AS latest_timestamp
                FROM members mem
                LEFT JOIN messages msg
                  ON CAST(mem.channel_id AS TEXT) = msg.room_id
                 AND msg.sender_role = ?
                GROUP BY mem.server_id, mem.owner_name
                ORDER BY mem.owner_name ASC, mem.server_id ASC
                """,
                (MEMBER_SENDER_ROLE,),
            )
            rows = cursor.fetchall()
        return [
            {
                "server_id": row[0],
                "owner_name": row[1],
                "message_count": row[2],
                "latest_timestamp": row[3],
            }
            for row in rows
        ]

    def get_top_member_for_day(self, start_time_ms: int) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    COALESCE(NULLIF(username, ''), CAST(user_id AS TEXT), '-') AS member_name,
                    COUNT(*) AS message_count
                FROM messages
                WHERE msg_type = ?
                  AND timestamp >= ?
                  AND sender_role = ?
                GROUP BY COALESCE(NULLIF(username, ''), CAST(user_id AS TEXT), '-')
                ORDER BY message_count DESC, MAX(timestamp) DESC
                LIMIT 1
                """,
                ("TEXT", start_time_ms, MEMBER_SENDER_ROLE),
            )
            row = cursor.fetchone()
        if not row:
            return None
        return {
            "member_name": row[0],
            "message_count": row[1],
        }

    def get_viewer_summary(self, today_start_ms: int) -> Dict[str, Any]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM messages WHERE sender_role = ? AND msg_type = ?) AS total_messages,
                    (SELECT COUNT(DISTINCT room_id) FROM messages) AS total_rooms,
                    (SELECT COUNT(*) FROM messages WHERE sender_role = ? AND msg_type = ? AND timestamp >= ?) AS today_messages
                """,
                (
                    MEMBER_SENDER_ROLE,
                    "TEXT",
                    MEMBER_SENDER_ROLE,
                    "TEXT",
                    today_start_ms,
                ),
            )
            row = cursor.fetchone()
            top_member_today = self.get_top_member_for_day(today_start_ms)
        return {
            "total_messages": row[0] if row else 0,
            "total_rooms": row[1] if row else 0,
            "today_messages": row[2] if row else 0,
            "top_member_name": top_member_today.get("member_name")
            if top_member_today
            else "-",
            "top_member_count": top_member_today.get("message_count")
            if top_member_today
            else 0,
        }
