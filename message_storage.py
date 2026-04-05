import csv
import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

import pymysql
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)


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
            if key in keys and value not in (None, ''):
                return value
            nested = _find_first_value(value, keys)
            if nested not in (None, ''):
                return nested
    elif isinstance(data, list):
        for item in data:
            nested = _find_first_value(item, keys)
            if nested not in (None, ''):
                return nested
    return None


def _extract_text_content(body: Any, ext_info: Any) -> Optional[str]:
    candidates: List[str] = []
    for source in (_parse_json_like(body), _parse_json_like(ext_info)):
        if isinstance(source, str):
            candidates.append(source)
            continue

        text = _find_first_value(source, {
            'text', 'messageText', 'replyText', 'faipaiContent', 'content', 'title', 'desc'
        })
        if text not in (None, ''):
            candidates.append(str(text))

    if not candidates:
        return None

    deduped: List[str] = []
    for item in candidates:
        if item not in deduped:
            deduped.append(item)
    return ' | '.join(deduped)


def _extract_media_fields(body: Any, ext_info: Any) -> Dict[str, Any]:
    merged = {
        'body': _parse_json_like(body),
        'extInfo': _parse_json_like(ext_info),
    }
    return {
        'media_url': _find_first_value(merged, {'url', 'playUrl', 'streamPath', 'coverPath'}),
        'media_cover_url': _find_first_value(merged, {'coverUrl', 'coverPath', 'thumbnailUrl'}),
        'media_duration': _find_first_value(merged, {'duration', 'playTime', 'time'}),
        'width': _find_first_value(merged, {'width'}),
        'height': _find_first_value(merged, {'height'}),
        'reply_to_text': _find_first_value(merged, {'replyText', 'messageText'}),
        'flip_user_name': _find_first_value(merged, {'faipaiName', 'replyName'}),
        'flip_question': _find_first_value(merged, {'faipaiContent', 'question'}),
        'flip_answer': _find_first_value(merged, {'messageText', 'answer', 'replyText'}),
        'ext_json': _json_dumps(merged),
    }


def _timestamp_ms_to_datetime(value: Any) -> datetime:
    try:
        timestamp_ms = int(value)
        if timestamp_ms > 0:
            return datetime.fromtimestamp(timestamp_ms / 1000)
    except (TypeError, ValueError):
        pass
    return datetime.now()


class MessageStorage(ABC):
    @abstractmethod
    def save_message(self, message: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def get_messages(self, room_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_latest_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def export_messages(self, output_path: str, room_id: Optional[str] = None,
                        limit: Optional[int] = None, output_format: str = 'json') -> int:
        pass

    @abstractmethod
    def record_fetch(self, room_id: str, messages_count: int, status: str,
                     error_message: Optional[str] = None,
                     last_message_id: Optional[str] = None,
                     last_message_time_ms: Optional[int] = None) -> None:
        pass

    @abstractmethod
    def get_statistics(self) -> Dict[str, Any]:
        pass


class SQLiteStorage(MessageStorage):
    def __init__(self, db_path: str = 'messages.db'):
        self.db_path = db_path
        self._init_database()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_database(self):
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
            cursor.execute("""
                INSERT OR IGNORE INTO messages
                (room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                message.get('room_id'),
                message.get('message_id'),
                message.get('user_id'),
                message.get('username'),
                _json_dumps(message.get('content')),
                str(message.get('msg_type') or ''),
                _json_dumps(message.get('ext_info')),
                message.get('timestamp'),
            ))
            conn.commit()
            affected = cursor.rowcount
            conn.close()
            return affected > 0
        except Exception as exc:
            logger.error('保存消息失败: %s', exc)
            return False

    def get_messages(self, room_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp, created_at
            FROM messages WHERE room_id = ? ORDER BY timestamp DESC LIMIT ?
        """, (room_id, limit))
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                'room_id': row[0],
                'message_id': row[1],
                'user_id': row[2],
                'username': row[3],
                'content': row[4],
                'msg_type': row[5],
                'ext_info': row[6],
                'timestamp': row[7],
                'created_at': row[8],
            }
            for row in rows
        ]

    def get_latest_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        messages = self.get_messages(room_id, 1)
        return messages[0] if messages else None

    def export_messages(self, output_path: str, room_id: Optional[str] = None,
                        limit: Optional[int] = None, output_format: str = 'json') -> int:
        conn = self._connect()
        cursor = conn.cursor()
        query = "SELECT room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp, created_at FROM messages"
        params: List[Any] = []
        if room_id:
            query += ' WHERE room_id = ?'
            params.append(room_id)
        query += ' ORDER BY timestamp DESC'
        if limit is not None:
            query += ' LIMIT ?'
            params.append(limit)
        rows = cursor.execute(query, params).fetchall()
        conn.close()
        messages = [
            {
                'room_id': row[0],
                'message_id': row[1],
                'user_id': row[2],
                'username': row[3],
                'content': row[4],
                'msg_type': row[5],
                'ext_info': row[6],
                'timestamp': row[7],
                'created_at': row[8],
            }
            for row in rows
        ]
        if output_format == 'json':
            with open(output_path, 'w', encoding='utf-8') as file:
                json.dump(messages, file, ensure_ascii=False, indent=2)
            return len(messages)
        if output_format == 'csv':
            with open(output_path, 'w', encoding='utf-8', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=list(messages[0].keys()) if messages else [
                    'room_id', 'message_id', 'user_id', 'username', 'content', 'msg_type', 'ext_info', 'timestamp', 'created_at'
                ])
                writer.writeheader()
                writer.writerows(messages)
            return len(messages)
        raise ValueError(f'不支持的导出格式: {output_format}')

    def record_fetch(self, room_id: str, messages_count: int, status: str,
                     error_message: Optional[str] = None,
                     last_message_id: Optional[str] = None,
                     last_message_time_ms: Optional[int] = None) -> None:
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
        cursor.execute('SELECT COUNT(*) FROM messages')
        total_messages = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(DISTINCT room_id) FROM messages')
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
            'total_messages': total_messages,
            'total_rooms': total_rooms,
            'successful_fetches': successful_fetches,
            'top_rooms': top_rooms,
        }


class MySQLStorage(MessageStorage):
    def __init__(self, host: str, port: int, database: str, user: str, password: str,
                 charset: str = 'utf8mb4'):
        self.connection_args = {
            'host': host,
            'port': int(port),
            'database': database,
            'user': user,
            'password': password,
            'charset': charset,
            'cursorclass': DictCursor,
            'autocommit': False,
        }

    def _connect(self):
        return pymysql.connect(**self.connection_args)

    def save_message(self, message: Dict[str, Any]) -> bool:
        owner_member_id = message.get('owner_member_id')
        room_id = message.get('room_id')
        if owner_member_id is None or room_id is None:
            raise ValueError('消息缺少 owner_member_id 或 room_id，无法写入 MySQL')

        conn = self._connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO members (id, member_name, room_id)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        member_name = VALUES(member_name),
                        room_id = VALUES(room_id),
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    owner_member_id,
                    message.get('member_name') or message.get('username') or str(owner_member_id),
                    room_id,
                ))
                cursor.execute("""
                    INSERT INTO rooms (id, owner_member_id, room_name)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        owner_member_id = VALUES(owner_member_id),
                        room_name = VALUES(room_name),
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    room_id,
                    owner_member_id,
                    message.get('member_name') or str(room_id),
                ))

                message_id = str(message.get('message_id') or '')
                inserted = cursor.execute("""
                    INSERT IGNORE INTO messages (
                        message_id, room_id, sender_user_id, sender_name, owner_member_id,
                        message_type, sub_type, text_content, message_time, message_time_ms,
                        is_deleted, raw_brief
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    message_id,
                    room_id,
                    message.get('user_id'),
                    message.get('username'),
                    owner_member_id,
                    str(message.get('msg_type') or 'UNKNOWN'),
                    message.get('sub_type'),
                    _extract_text_content(message.get('content'), message.get('ext_info')),
                    _timestamp_ms_to_datetime(message.get('timestamp')),
                    message.get('timestamp'),
                    0,
                    _json_dumps({
                        'body': _parse_json_like(message.get('content')),
                        'extInfo': _parse_json_like(message.get('ext_info')),
                    }),
                ))

                if inserted:
                    payload = _extract_media_fields(message.get('content'), message.get('ext_info'))
                    cursor.execute("""
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
                    """, (
                        message_id,
                        payload['media_url'],
                        None,
                        payload['media_cover_url'],
                        payload['media_duration'],
                        payload['width'],
                        payload['height'],
                        None,
                        payload['reply_to_text'],
                        payload['flip_user_name'],
                        payload['flip_question'],
                        payload['flip_answer'],
                        payload['ext_json'],
                    ))

            conn.commit()
            return inserted > 0
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_messages(self, room_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
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
                """, (room_id, limit))
                return list(cursor.fetchall())
        finally:
            conn.close()

    def get_latest_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT message_id, message_time_ms AS timestamp
                    FROM messages
                    WHERE room_id = %s
                    ORDER BY message_time DESC
                    LIMIT 1
                """, (room_id,))
                return cursor.fetchone()
        finally:
            conn.close()

    def export_messages(self, output_path: str, room_id: Optional[str] = None,
                        limit: Optional[int] = None, output_format: str = 'json') -> int:
        conn = self._connect()
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
                    query += ' WHERE m.room_id = %s'
                    params.append(room_id)
                query += ' ORDER BY m.message_time DESC'
                if limit is not None:
                    query += ' LIMIT %s'
                    params.append(limit)
                cursor.execute(query, params)
                messages = list(cursor.fetchall())
        finally:
            conn.close()

        if output_format == 'json':
            with open(output_path, 'w', encoding='utf-8') as file:
                json.dump(messages, file, ensure_ascii=False, indent=2, default=str)
            return len(messages)
        if output_format == 'csv':
            fieldnames = list(messages[0].keys()) if messages else [
                'room_id', 'message_id', 'user_id', 'username', 'content', 'msg_type', 'ext_info', 'timestamp', 'created_at'
            ]
            with open(output_path, 'w', encoding='utf-8', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(messages)
            return len(messages)
        raise ValueError(f'不支持的导出格式: {output_format}')

    def record_fetch(self, room_id: str, messages_count: int, status: str,
                     error_message: Optional[str] = None,
                     last_message_id: Optional[str] = None,
                     last_message_time_ms: Optional[int] = None) -> None:
        conn = self._connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO crawl_tasks (
                        room_id, task_type, status, start_time_ms, end_time_ms,
                        last_message_time_ms, error_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    room_id,
                    'incremental',
                    status,
                    int(datetime.now().timestamp() * 1000),
                    int(datetime.now().timestamp() * 1000),
                    last_message_time_ms,
                    error_message,
                ))
                if status == 'success':
                    cursor.execute("""
                        INSERT INTO crawl_checkpoints (
                            room_id, last_message_id, last_message_time_ms, last_success_at
                        ) VALUES (%s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE
                            last_message_id = VALUES(last_message_id),
                            last_message_time_ms = VALUES(last_message_time_ms),
                            last_success_at = VALUES(last_success_at),
                            updated_at = CURRENT_TIMESTAMP
                    """, (room_id, last_message_id, last_message_time_ms))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_statistics(self) -> Dict[str, Any]:
        conn = self._connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) AS cnt FROM messages')
                total_messages = cursor.fetchone()['cnt']
                cursor.execute('SELECT COUNT(DISTINCT room_id) AS cnt FROM messages')
                total_rooms = cursor.fetchone()['cnt']
                cursor.execute("SELECT COUNT(*) AS cnt FROM crawl_tasks WHERE status = 'success'")
                successful_fetches = cursor.fetchone()['cnt']
                cursor.execute("""
                    SELECT room_id, COUNT(*) AS cnt
                    FROM messages
                    GROUP BY room_id
                    ORDER BY cnt DESC
                    LIMIT 10
                """)
                top_rooms_rows = cursor.fetchall()
        finally:
            conn.close()

        return {
            'total_messages': total_messages,
            'total_rooms': total_rooms,
            'successful_fetches': successful_fetches,
            'top_rooms': [(row['room_id'], row['cnt']) for row in top_rooms_rows],
        }


def create_storage(config: Dict[str, Any]) -> MessageStorage:
    storage_config = config.get('storage', {})
    storage_type = storage_config.get('type', 'mysql')
    if storage_type == 'sqlite':
        return SQLiteStorage(storage_config.get('database', 'messages.db'))
    if storage_type == 'mysql':
        return MySQLStorage(
            host=storage_config.get('host', 'localhost'),
            port=storage_config.get('port', 3306),
            database=storage_config['database'],
            user=storage_config['user'],
            password=storage_config['password'],
            charset=storage_config.get('charset', 'utf8mb4'),
        )
    raise ValueError(f'不支持的存储类型: {storage_type}')
