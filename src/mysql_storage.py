import csv
import json
import logging
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import pymysql
from pymysql.cursors import DictCursor

from message_parser import (
    FAN_SENDER_ROLE,
    MEMBER_SENDER_ROLE,
    determine_sender_role_from_message,
    extract_media_fields,
    extract_text_content,
    json_dumps,
    message_server_id,
    _mysql_sender_role_case_expression,
    parse_json_like,
    timestamp_ms_to_datetime,
)
from message_storage import MessageStorage

logger = logging.getLogger(__name__)


class MySQLStorage(MessageStorage):
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
            "autocommit": True,
        }
        self._server_connection_args = {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "charset": charset,
            "cursorclass": DictCursor,
            "autocommit": True,
        }
        self._pool_size = pool_size
        self._pool: List[pymysql.connections.Connection] = []
        self._pool_lock = threading.Lock()
        self._ensure_database()
        self._init_database()

    def _get_pool_lock(self):
        return self._pool_lock

    def _get_connection(self) -> pymysql.connections.Connection:
        lock = self._get_pool_lock()
        with lock:
            if self._pool:
                conn = self._pool.pop()
                try:
                    conn.ping(reconnect=True)
                    return conn
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
        conn = pymysql.connect(**self.connection_args)
        return conn

    def _return_connection(self, conn: pymysql.connections.Connection):
        try:
            conn.ping(reconnect=False)
            lock = self._get_pool_lock()
            with lock:
                if len(self._pool) < self._pool_size:
                    self._pool.append(conn)
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
                        id BIGINT PRIMARY KEY COMMENT '成员ID',
                        owner_name VARCHAR(255) NOT NULL COMMENT '成员姓名',
                        pinyin VARCHAR(255) NULL COMMENT '成员姓名拼音',
                        nickname VARCHAR(255) NULL COMMENT '昵称',
                        birthday VARCHAR(32) NULL COMMENT '生日',
                        birthplace VARCHAR(255) NULL COMMENT '出生地',
                        constellation VARCHAR(64) NULL COMMENT '星座',
                        height INT NULL COMMENT '身高',
                        blood_type VARCHAR(32) NULL COMMENT '血型',
                        hobbies TEXT NULL COMMENT '爱好',
                        specialty TEXT NULL COMMENT '特长',
                        group_id BIGINT NULL COMMENT '团体ID',
                        group_name VARCHAR(128) NULL COMMENT '团体名称',
                        team_id BIGINT NULL COMMENT '队伍ID',
                        team VARCHAR(128) NULL COMMENT '队伍名称',
                        period_id BIGINT NULL COMMENT '期数ID',
                        period_name VARCHAR(128) NULL COMMENT '期数名称',
                        `class` VARCHAR(64) NULL COMMENT '班级',
                        jtime VARCHAR(32) NULL COMMENT '加入时间',
                        ptime VARCHAR(32) NULL COMMENT '升格时间',
                        gtime VARCHAR(32) NULL COMMENT '毕业时间',
                        qtime VARCHAR(32) NULL COMMENT '退团时间',
                        election_rank VARCHAR(64) NULL COMMENT '总选排名',
                        note TEXT NULL COMMENT '备注',
                        account VARCHAR(255) NULL COMMENT '账号标识',
                        room_id BIGINT NULL COMMENT '房间ID',
                        live_room_id BIGINT NULL COMMENT '直播房间ID',
                        server_id BIGINT NOT NULL COMMENT '房间 server_id',
                        channel_id BIGINT NOT NULL COMMENT '房间 channel_id',
                        wb_uid VARCHAR(64) NULL COMMENT '微博UID',
                        wb_name VARCHAR(255) NULL COMMENT '微博名称',
                        avatar TEXT NULL COMMENT '头像',
                        full_photo1 TEXT NULL COMMENT '大图1',
                        full_photo2 TEXT NULL COMMENT '大图2',
                        full_photo3 TEXT NULL COMMENT '大图3',
                        full_photo4 TEXT NULL COMMENT '大图4',
                        status INT NULL COMMENT '成员状态',
                        ctime BIGINT NULL COMMENT '源数据创建时间戳',
                        utime BIGINT NULL COMMENT '源数据更新时间戳',
                        is_in_group TINYINT(1) NULL COMMENT '是否在团',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                        UNIQUE KEY uk_members_server_id (server_id),
                        UNIQUE KEY uk_members_channel_id (channel_id),
                        KEY idx_members_owner_name (owner_name),
                        KEY idx_members_group_name (group_name),
                        KEY idx_members_team (team),
                        KEY idx_members_room_id (room_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='成员表'
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS messages (
                        message_id VARCHAR(128) PRIMARY KEY COMMENT '消息ID',
                        room_id BIGINT NULL COMMENT '房间ID',
                        server_id BIGINT NOT NULL COMMENT '房间 server_id',
                        channel_id BIGINT NOT NULL COMMENT '房间 channel_id',
                        sender_user_id BIGINT NULL COMMENT '发送者用户ID',
                        sender_name VARCHAR(255) NULL COMMENT '发送者昵称',
                        member_name VARCHAR(255) NULL COMMENT '成员名称快照',
                        sender_role VARCHAR(16) NOT NULL DEFAULT 'fan' COMMENT '发送者角色(member/fan)',
                        message_type VARCHAR(64) NOT NULL COMMENT '消息类型',
                        sub_type VARCHAR(64) NULL COMMENT '消息子类型',
                        text_content LONGTEXT NULL COMMENT '文本内容',
                        ext_info_json LONGTEXT NULL COMMENT 'ext_info 原始内容',
                        raw_message_json LONGTEXT NULL COMMENT '原始消息JSON',
                        message_time DATETIME NOT NULL COMMENT '消息时间',
                        message_time_ms BIGINT NOT NULL COMMENT '消息毫秒时间戳',
                        is_deleted TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否删除',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                        KEY idx_messages_room_time (room_id, message_time),
                        KEY idx_messages_room_role_time (room_id, sender_role, message_time_ms),
                        KEY idx_messages_server_time (server_id, message_time_ms),
                        KEY idx_messages_server_role_time (server_id, sender_role, message_time_ms),
                        KEY idx_messages_channel_time (channel_id, message_time_ms),
                        KEY idx_messages_sender_role_time (sender_role, message_time_ms),
                        KEY idx_messages_sender_user_id (sender_user_id),
                        KEY idx_messages_message_time_ms (message_time_ms),
                        CONSTRAINT fk_messages_server_id
                            FOREIGN KEY (server_id) REFERENCES members (server_id)
                            ON UPDATE CASCADE
                            ON DELETE RESTRICT
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消息表'
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS message_payloads (
                        message_id VARCHAR(128) PRIMARY KEY COMMENT '消息ID',
                        media_url TEXT NULL COMMENT '媒体URL',
                        media_path TEXT NULL COMMENT '本地媒体路径',
                        media_cover_url TEXT NULL COMMENT '媒体封面URL',
                        media_duration BIGINT NULL COMMENT '媒体时长',
                        width INT NULL COMMENT '宽度',
                        height INT NULL COMMENT '高度',
                        reply_to_message_id VARCHAR(128) NULL COMMENT '回复目标消息ID',
                        reply_to_text LONGTEXT NULL COMMENT '回复目标文本',
                        flip_user_name VARCHAR(255) NULL COMMENT '翻牌用户名称',
                        flip_question LONGTEXT NULL COMMENT '翻牌问题',
                        flip_answer LONGTEXT NULL COMMENT '翻牌回答',
                        ext_json LONGTEXT NULL COMMENT '扩展JSON',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消息扩展表'
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS crawl_tasks (
                        id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '任务ID',
                        channel_id BIGINT NOT NULL COMMENT '房间 channel_id',
                        server_id BIGINT NOT NULL COMMENT '房间 server_id',
                        task_type VARCHAR(32) NOT NULL COMMENT '任务类型',
                        status VARCHAR(32) NOT NULL COMMENT '任务状态',
                        start_time_ms BIGINT NOT NULL COMMENT '开始时间戳',
                        end_time_ms BIGINT NOT NULL COMMENT '结束时间戳',
                        last_message_time_ms BIGINT NULL COMMENT '最后消息时间戳',
                        error_message TEXT NULL COMMENT '错误信息',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                        KEY idx_crawl_tasks_channel_id (channel_id),
                        KEY idx_crawl_tasks_server_id (server_id),
                        KEY idx_crawl_tasks_status (status),
                        KEY idx_crawl_tasks_last_message_time_ms (last_message_time_ms)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='抓取任务表'
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS crawl_checkpoints (
                        server_id BIGINT NOT NULL COMMENT '房间 server_id',
                        channel_id BIGINT NOT NULL COMMENT '房间 channel_id',
                        last_message_id VARCHAR(128) NULL COMMENT '最后消息ID',
                        last_message_time_ms BIGINT NULL COMMENT '最后消息时间戳',
                        last_success_at DATETIME NULL COMMENT '最后成功时间',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                        PRIMARY KEY (server_id, channel_id),
                        KEY idx_crawl_checkpoints_last_message_time_ms (last_message_time_ms)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='抓取断点表'
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS crawl_history_checkpoints (
                        server_id BIGINT NOT NULL COMMENT '房间 server_id',
                        channel_id BIGINT NOT NULL COMMENT '房间 channel_id',
                        oldest_covered_message_id VARCHAR(128) NULL COMMENT '已连续覆盖的最老成员消息ID',
                        oldest_covered_time_ms BIGINT NULL COMMENT '已连续覆盖的最老时间戳',
                        resume_next_time BIGINT NULL COMMENT '下次历史续翻优先尝试的 nextTime',
                        target_time_ms BIGINT NULL COMMENT '本次历史补抓目标时间',
                        status VARCHAR(32) NOT NULL DEFAULT 'idle' COMMENT '历史补抓状态',
                        cursor_verified TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否验证过 resume_next_time 可复用',
                        last_page_count INT NOT NULL DEFAULT 0 COMMENT '最近一次补抓已翻页数',
                        last_run_started_at DATETIME NULL COMMENT '最近一次历史补抓开始时间',
                        last_run_finished_at DATETIME NULL COMMENT '最近一次历史补抓结束时间',
                        last_error_message TEXT NULL COMMENT '最近一次错误',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                        PRIMARY KEY (server_id, channel_id),
                        KEY idx_history_oldest_time (oldest_covered_time_ms),
                        KEY idx_history_status (status)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='历史抓取断点表'
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        migration_key VARCHAR(128) PRIMARY KEY COMMENT '迁移标识',
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '执行时间'
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='数据库迁移记录'
                    """
                )
                self._ensure_messages_sender_role_schema(cursor)
                self._run_mysql_migrations(cursor)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _mysql_index_exists(self, cursor, table_name: str, index_name: str) -> bool:
        cursor.execute(
            f"SHOW INDEX FROM `{table_name}` WHERE Key_name = %s", (index_name,)
        )
        return cursor.fetchone() is not None

    def _mysql_migration_applied(self, cursor, migration_key: str) -> bool:
        cursor.execute(
            "SELECT migration_key FROM schema_migrations WHERE migration_key = %s LIMIT 1",
            (migration_key,),
        )
        return cursor.fetchone() is not None

    def _mark_mysql_migration_applied(self, cursor, migration_key: str) -> None:
        cursor.execute(
            "INSERT IGNORE INTO schema_migrations (migration_key) VALUES (%s)",
            (migration_key,),
        )

    def _run_mysql_migration_once(self, cursor, migration_key: str, migration) -> None:
        if self._mysql_migration_applied(cursor, migration_key):
            return
        migration(cursor)
        self._mark_mysql_migration_applied(cursor, migration_key)

    def _run_mysql_migrations(self, cursor) -> None:
        migrations = (
            (
                "2026_06_04_messages_sender_role_backfill",
                self._backfill_messages_sender_role_and_member_name,
            ),
        )
        for migration_key, migration in migrations:
            self._run_mysql_migration_once(cursor, migration_key, migration)

    def _ensure_messages_sender_role_schema(self, cursor) -> None:
        cursor.execute("SHOW COLUMNS FROM messages LIKE 'member_name'")
        has_member_name = cursor.fetchone() is not None
        if not has_member_name:
            cursor.execute(
                "ALTER TABLE messages ADD COLUMN member_name VARCHAR(255) NULL COMMENT '成员名称快照' AFTER sender_name"
            )

        cursor.execute("SHOW COLUMNS FROM messages LIKE 'sender_role'")
        has_sender_role = cursor.fetchone() is not None
        if not has_sender_role:
            cursor.execute(
                "ALTER TABLE messages ADD COLUMN sender_role VARCHAR(16) NOT NULL DEFAULT 'fan' COMMENT '发送者角色(member/fan)' AFTER member_name"
            )

        index_definitions = {
            "idx_messages_room_role_time": "CREATE INDEX idx_messages_room_role_time ON messages (room_id, sender_role, message_time_ms)",
            "idx_messages_server_role_time": "CREATE INDEX idx_messages_server_role_time ON messages (server_id, sender_role, message_time_ms)",
            "idx_messages_sender_role_time": "CREATE INDEX idx_messages_sender_role_time ON messages (sender_role, message_time_ms)",
        }
        for index_name, statement in index_definitions.items():
            if not self._mysql_index_exists(cursor, "messages", index_name):
                cursor.execute(statement)

    def _backfill_messages_sender_role_and_member_name(self, cursor) -> None:
        cursor.execute(
            f"UPDATE messages SET sender_role = {_mysql_sender_role_case_expression()} WHERE sender_role IS NULL OR sender_role = ''"
        )
        cursor.execute(
            f"UPDATE messages SET sender_role = {_mysql_sender_role_case_expression()} WHERE sender_role = '{FAN_SENDER_ROLE}' AND ({_mysql_sender_role_case_expression()} = '{MEMBER_SENDER_ROLE}')"
        )
        cursor.execute(
            """
            UPDATE messages m
            LEFT JOIN members mem ON mem.server_id = m.server_id
            SET m.member_name = mem.owner_name
            WHERE (m.member_name IS NULL OR m.member_name = '')
              AND mem.owner_name IS NOT NULL
              AND mem.owner_name <> ''
            """
        )

    def _normalize_member_record(self, member: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": member.get("memberId")
            if member.get("memberId") is not None
            else member.get("id"),
            "owner_name": member.get("ownerName")
            or member.get("memberName")
            or member.get("nickname")
            or member.get("pinyin")
            or "",
            "pinyin": member.get("pinyin"),
            "nickname": member.get("nickname"),
            "birthday": member.get("birthday"),
            "birthplace": member.get("birthplace"),
            "constellation": member.get("constellation"),
            "height": member.get("height"),
            "blood_type": member.get("bloodType"),
            "hobbies": member.get("hobbies"),
            "specialty": member.get("specialty"),
            "group_id": member.get("groupId"),
            "group_name": member.get("groupName"),
            "team_id": member.get("teamId"),
            "team": member.get("team"),
            "period_id": member.get("periodId"),
            "period_name": member.get("periodName"),
            "class": member.get("class"),
            "jtime": member.get("jtime"),
            "ptime": member.get("ptime"),
            "gtime": member.get("gtime"),
            "qtime": member.get("qtime"),
            "election_rank": member.get("rank"),
            "note": member.get("note"),
            "account": member.get("account"),
            "room_id": member.get("roomId"),
            "live_room_id": member.get("liveRoomId"),
            "server_id": member.get("serverId"),
            "channel_id": member.get("channelId"),
            "wb_uid": member.get("wbUid"),
            "wb_name": member.get("wbName"),
            "avatar": member.get("avatar"),
            "full_photo1": member.get("fullPhoto1"),
            "full_photo2": member.get("fullPhoto2"),
            "full_photo3": member.get("fullPhoto3"),
            "full_photo4": member.get("fullPhoto4"),
            "status": member.get("status"),
            "ctime": member.get("ctime"),
            "utime": member.get("utime"),
            "is_in_group": member.get("isInGroup"),
        }

    def _get_member_name_map(self, cursor, server_ids: List[Any]) -> Dict[Any, str]:
        normalized_ids = []
        for server_id in server_ids:
            if server_id in (None, ""):
                continue
            if server_id not in normalized_ids:
                normalized_ids.append(server_id)
        if not normalized_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(normalized_ids))
        cursor.execute(
            f"SELECT server_id, owner_name FROM members WHERE server_id IN ({placeholders})",
            normalized_ids,
        )
        return {
            row["server_id"]: row["owner_name"]
            for row in cursor.fetchall()
            if row.get("owner_name")
        }

    def sync_members(self, members: List[Dict[str, Any]]) -> int:
        rows = []
        for member in members:
            row = self._normalize_member_record(member)
            if (
                row["id"] is None
                or row["server_id"] is None
                or row["channel_id"] is None
                or not row["owner_name"]
            ):
                logger.warning(
                    "跳过不完整的成员配置: %s",
                    member.get("ownerName")
                    or member.get("memberName")
                    or member.get("nickname")
                    or member,
                )
                continue
            rows.append(row)

        if not rows:
            return 0

        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """
                        INSERT INTO members (
                            id, owner_name, pinyin, nickname, birthday, birthplace, constellation,
                            height, blood_type, hobbies, specialty, group_id, group_name, team_id,
                            team, period_id, period_name, `class`, jtime, ptime, gtime, qtime, election_rank,
                            note, account, room_id, live_room_id, server_id, channel_id, wb_uid,
                            wb_name, avatar, full_photo1, full_photo2, full_photo3, full_photo4,
                            status, ctime, utime, is_in_group
                        ) VALUES (
                            %(id)s, %(owner_name)s, %(pinyin)s, %(nickname)s, %(birthday)s, %(birthplace)s, %(constellation)s,
                            %(height)s, %(blood_type)s, %(hobbies)s, %(specialty)s, %(group_id)s, %(group_name)s, %(team_id)s,
                            %(team)s, %(period_id)s, %(period_name)s, %(class)s, %(jtime)s, %(ptime)s, %(gtime)s, %(qtime)s, %(election_rank)s,
                            %(note)s, %(account)s, %(room_id)s, %(live_room_id)s, %(server_id)s, %(channel_id)s, %(wb_uid)s,
                            %(wb_name)s, %(avatar)s, %(full_photo1)s, %(full_photo2)s, %(full_photo3)s, %(full_photo4)s,
                            %(status)s, %(ctime)s, %(utime)s, %(is_in_group)s
                        ) ON DUPLICATE KEY UPDATE
                            owner_name = VALUES(owner_name),
                            pinyin = VALUES(pinyin),
                            nickname = VALUES(nickname),
                            birthday = VALUES(birthday),
                            birthplace = VALUES(birthplace),
                            constellation = VALUES(constellation),
                            height = VALUES(height),
                            blood_type = VALUES(blood_type),
                            hobbies = VALUES(hobbies),
                            specialty = VALUES(specialty),
                            group_id = VALUES(group_id),
                            group_name = VALUES(group_name),
                            team_id = VALUES(team_id),
                            team = VALUES(team),
                            period_id = VALUES(period_id),
                            period_name = VALUES(period_name),
                            `class` = VALUES(`class`),
                            jtime = VALUES(jtime),
                            ptime = VALUES(ptime),
                            gtime = VALUES(gtime),
                            qtime = VALUES(qtime),
                            election_rank = VALUES(election_rank),
                            note = VALUES(note),
                            account = VALUES(account),
                            room_id = VALUES(room_id),
                            live_room_id = VALUES(live_room_id),
                            wb_uid = VALUES(wb_uid),
                            wb_name = VALUES(wb_name),
                            avatar = VALUES(avatar),
                            full_photo1 = VALUES(full_photo1),
                            full_photo2 = VALUES(full_photo2),
                            full_photo3 = VALUES(full_photo3),
                            full_photo4 = VALUES(full_photo4),
                            status = VALUES(status),
                            ctime = VALUES(ctime),
                            utime = VALUES(utime),
                            is_in_group = VALUES(is_in_group),
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        rows,
                    )
                    server_ids = [row["server_id"] for row in rows]
                    if server_ids:
                        placeholders = ", ".join(["%s"] * len(server_ids))
                        cursor.execute(
                            f"""
                            UPDATE messages m
                            JOIN members mem ON mem.server_id = m.server_id
                            SET m.member_name = mem.owner_name
                            WHERE m.server_id IN ({placeholders})
                            """,
                            server_ids,
                        )
                conn.commit()
                return len(rows)
            except Exception:
                conn.rollback()
                raise

    def _serialize_raw_message(self, message: Dict[str, Any]) -> Optional[str]:
        return json_dumps(
            {
                "body": parse_json_like(message.get("content")),
                "extInfo": parse_json_like(message.get("ext_info")),
            }
        )

    def save_message(self, message: Dict[str, Any]) -> bool:
        if not message.get("message_id"):
            logger.debug("跳过缺少 message_id 的消息: %s", message)
            return False
        room_id = message.get("room_id")
        server_id = message_server_id(message)
        channel_id = message.get("channel_id")
        if server_id is None or channel_id is None:
            raise ValueError("消息缺少 server_id 或 channel_id，无法写入 MySQL")

        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    message_id = str(message.get("message_id") or "")
                    raw_message_json = self._serialize_raw_message(message)
                    member_name = self._get_member_name_map(cursor, [server_id]).get(
                        server_id
                    )
                    inserted = cursor.execute(
                        """
                        INSERT IGNORE INTO messages (
                            message_id, room_id, server_id, channel_id, sender_user_id, sender_name,
                            member_name, sender_role, message_type, sub_type, text_content, ext_info_json, raw_message_json,
                            message_time, message_time_ms, is_deleted
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            message_id,
                            room_id,
                            server_id,
                            channel_id,
                            message.get("user_id"),
                            message.get("username"),
                            member_name,
                            determine_sender_role_from_message(message),
                            str(message.get("msg_type") or "UNKNOWN"),
                            message.get("sub_type"),
                            extract_text_content(
                                message.get("content"), message.get("ext_info")
                            ),
                            json_dumps(parse_json_like(message.get("ext_info"))),
                            raw_message_json,
                            timestamp_ms_to_datetime(message.get("timestamp")),
                            message.get("timestamp"),
                            0,
                        ),
                    )

                    if inserted:
                        payload = extract_media_fields(
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
        messages = [message for message in messages if message.get("message_id")]
        if not messages:
            return 0

        first_message = messages[0]
        server_id = message_server_id(first_message)
        channel_id = first_message.get("channel_id")
        if server_id is None or channel_id is None:
            raise ValueError("消息缺少 server_id 或 channel_id，无法写入 MySQL")

        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    member_name_map = self._get_member_name_map(
                        cursor,
                        [message_server_id(message) for message in messages],
                    )
                    message_rows = []
                    payload_rows = []
                    pending_payload_message_ids: set[str] = set()
                    for message in messages:
                        message_id = str(message.get("message_id") or "")
                        raw_message_json = self._serialize_raw_message(message)
                        message_rows.append(
                            (
                                message_id,
                                message.get("room_id"),
                                message.get("server_id")
                                or message.get("owner_member_id"),
                                message.get("channel_id"),
                                message.get("user_id"),
                                message.get("username"),
                                member_name_map.get(message_server_id(message)),
                                determine_sender_role_from_message(message),
                                str(message.get("msg_type") or "UNKNOWN"),
                                message.get("sub_type"),
                                extract_text_content(
                                    message.get("content"), message.get("ext_info")
                                ),
                                json_dumps(parse_json_like(message.get("ext_info"))),
                                raw_message_json,
                                timestamp_ms_to_datetime(message.get("timestamp")),
                                message.get("timestamp"),
                                0,
                            )
                        )
                        if message_id not in pending_payload_message_ids:
                            payload = extract_media_fields(
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
                            message_id, room_id, server_id, channel_id, sender_user_id, sender_name,
                            member_name, sender_role, message_type, sub_type, text_content, ext_info_json, raw_message_json,
                            message_time, message_time_ms, is_deleted
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                            m.member_name,
                            m.sender_role,
                            m.text_content AS content,
                            m.message_type AS msg_type,
                            COALESCE(m.ext_info_json, mp.ext_json) AS ext_info,
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
                logger.error("获取房间消息失败 room_id=%s", room_id, exc_info=True)
                raise

    def get_latest_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT last_message_id AS message_id, last_message_time_ms AS timestamp
                        FROM crawl_checkpoints
                        WHERE channel_id = %s
                        LIMIT 1
                    """,
                        (room_id,),
                    )
                    checkpoint = cursor.fetchone()
                    if checkpoint and checkpoint.get("timestamp"):
                        return checkpoint

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
                logger.error("获取最新消息失败 room_id=%s", room_id, exc_info=True)
                raise

    def get_history_checkpoint(
        self, server_id: int, channel_id: int
    ) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            server_id,
                            channel_id,
                            oldest_covered_message_id,
                            oldest_covered_time_ms,
                            resume_next_time,
                            target_time_ms,
                            status,
                            cursor_verified,
                            last_page_count,
                            last_run_started_at,
                            last_run_finished_at,
                            last_error_message,
                            created_at,
                            updated_at
                        FROM crawl_history_checkpoints
                        WHERE server_id = %s AND channel_id = %s
                        LIMIT 1
                    """,
                        (server_id, channel_id),
                    )
                    row = cursor.fetchone()
                    if not row:
                        return None
                    row["cursor_verified"] = bool(row.get("cursor_verified"))
                    return row
            except Exception:
                logger.error(
                    "获取历史抓取断点失败 server_id=%s channel_id=%s: %s",
                    server_id,
                    channel_id,
                    exc_info=True,
                )
                raise

    def start_history_fetch(
        self, server_id: int, channel_id: int, target_time_ms: int
    ) -> None:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO crawl_history_checkpoints (
                            server_id, channel_id, target_time_ms, status,
                            last_page_count, last_run_started_at, last_run_finished_at,
                            last_error_message
                        ) VALUES (%s, %s, %s, 'running', 0, NOW(), NULL, NULL)
                        ON DUPLICATE KEY UPDATE
                            target_time_ms = VALUES(target_time_ms),
                            resume_next_time = NULL,
                            status = 'running',
                            cursor_verified = 0,
                            last_page_count = 0,
                            last_run_started_at = NOW(),
                            last_run_finished_at = NULL,
                            last_error_message = NULL,
                            updated_at = CURRENT_TIMESTAMP
                    """,
                        (server_id, channel_id, target_time_ms),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

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
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE crawl_history_checkpoints
                        SET oldest_covered_message_id = COALESCE(%s, oldest_covered_message_id),
                            oldest_covered_time_ms = COALESCE(%s, oldest_covered_time_ms),
                            resume_next_time = %s,
                            status = 'running',
                            cursor_verified = COALESCE(%s, cursor_verified),
                            last_page_count = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE server_id = %s AND channel_id = %s
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
            except Exception:
                conn.rollback()
                raise

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
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE crawl_history_checkpoints
                        SET target_time_ms = %s,
                            oldest_covered_message_id = COALESCE(%s, oldest_covered_message_id),
                            oldest_covered_time_ms = COALESCE(%s, oldest_covered_time_ms),
                            resume_next_time = %s,
                            status = 'success',
                            cursor_verified = COALESCE(%s, cursor_verified),
                            last_page_count = %s,
                            last_run_finished_at = NOW(),
                            last_error_message = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE server_id = %s AND channel_id = %s
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
            except Exception:
                conn.rollback()
                raise

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
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE crawl_history_checkpoints
                        SET status = %s,
                            resume_next_time = %s,
                            last_page_count = %s,
                            last_run_finished_at = NOW(),
                            last_error_message = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE server_id = %s AND channel_id = %s
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
            except Exception:
                conn.rollback()
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
                            m.member_name,
                            m.sender_role,
                            m.text_content AS content,
                            m.message_type AS msg_type,
                            COALESCE(m.ext_info_json, mp.ext_json) AS ext_info,
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
                logger.error("导出消息失败 room_id=%s", room_id, exc_info=True)
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
                    "member_name",
                    "sender_role",
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
        server_id: Optional[int] = None,
        channel_id: Optional[int] = None,
    ) -> None:
        if server_id is None or channel_id is None:
            raise ValueError("记录抓取状态需要 server_id 和 channel_id")
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO crawl_tasks (
                            channel_id, server_id, task_type, status, start_time_ms, end_time_ms,
                            last_message_time_ms, error_message
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            channel_id,
                            server_id,
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
                                server_id, channel_id, last_message_id, last_message_time_ms, last_success_at
                            ) VALUES (%s, %s, %s, %s, NOW())
                            ON DUPLICATE KEY UPDATE
                                last_message_id = COALESCE(VALUES(last_message_id), last_message_id),
                                last_message_time_ms = COALESCE(VALUES(last_message_time_ms), last_message_time_ms),
                                last_success_at = VALUES(last_success_at),
                                updated_at = CURRENT_TIMESTAMP
                        """,
                            (
                                server_id,
                                channel_id,
                                last_message_id,
                                last_message_time_ms,
                            ),
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
                        "SELECT COUNT(DISTINCT channel_id) AS cnt FROM messages"
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
                logger.error("获取统计信息失败", exc_info=True)
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
                            m.room_id AS id,
                            COALESCE(mem.owner_name, CAST(m.room_id AS CHAR), CAST(m.channel_id AS CHAR)) AS name,
                            COUNT(m.message_id) AS message_count,
                            MAX(m.message_time_ms) AS latest_timestamp
                        FROM messages m
                        LEFT JOIN members mem ON mem.server_id = m.server_id
                        GROUP BY m.room_id, mem.owner_name, m.channel_id
                        ORDER BY latest_timestamp DESC, m.room_id DESC
                    """)
                    return list(cursor.fetchall())
            except Exception:
                logger.error("获取房间列表失败", exc_info=True)
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
            try:
                with conn.cursor() as cursor:
                    where_clauses: List[str] = []
                    params: List[Any] = []

                    if room_id:
                        where_clauses.append("m.room_id = %s")
                        params.append(room_id)
                    if member_server_id is not None:
                        where_clauses.append("m.server_id = %s")
                        params.append(member_server_id)
                    if sender_keyword:
                        where_clauses.append(
                            "(m.sender_name LIKE %s OR m.member_name LIKE %s OR mem.owner_name LIKE %s)"
                        )
                        like_value = f"%{sender_keyword}%"
                        params.extend([like_value, like_value, like_value])
                    if keyword:
                        where_clauses.append(
                            "("
                            "m.text_content LIKE %s OR "
                            "m.raw_message_json LIKE %s OR "
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
                        where_clauses.append("m.sender_role = %s")
                        params.append(MEMBER_SENDER_ROLE)
                    elif sender_role == "fan":
                        where_clauses.append("m.sender_role = %s")
                        params.append(FAN_SENDER_ROLE)

                    where_sql = (
                        f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
                    )
                    count_query = (
                        "SELECT COUNT(*) AS cnt FROM messages m "
                        "LEFT JOIN message_payloads mp ON mp.message_id = m.message_id "
                        "LEFT JOIN members mem ON mem.server_id = m.server_id"
                        f"{where_sql}"
                    )
                    cursor.execute(count_query, params)
                    total = cursor.fetchone()["cnt"]
                    if limit <= 0:
                        return {"total": total, "items": []}

                    data_query = (
                        """
                        SELECT
                            m.room_id,
                            COALESCE(mem.owner_name, CAST(m.room_id AS CHAR), CAST(m.channel_id AS CHAR)) AS room_name,
                            m.message_id,
                            m.sender_user_id AS user_id,
                            m.sender_name AS username,
                            COALESCE(m.member_name, mem.owner_name) AS member_name,
                            m.sender_role,
                            m.text_content AS content,
                            m.message_type AS msg_type,
                            COALESCE(m.ext_info_json, mp.ext_json) AS ext_info,
                            m.message_time_ms AS timestamp,
                            m.created_at,
                            mp.media_url,
                            mp.media_cover_url,
                            mp.reply_to_text,
                            mp.flip_question,
                            mp.flip_answer,
                            COALESCE(m.member_name, mem.owner_name) AS room_member_name
                        FROM messages m
                        LEFT JOIN message_payloads mp ON mp.message_id = m.message_id
                        LEFT JOIN members mem ON mem.server_id = m.server_id
                        """
                        f"{where_sql}"
                        " ORDER BY m.message_time DESC, m.message_id DESC LIMIT %s OFFSET %s"
                    )
                    cursor.execute(data_query, params + [limit, offset])
                    items = list(cursor.fetchall())
                    return {"total": total, "items": items}
            except Exception:
                logger.error("搜索消息失败", exc_info=True)
                raise

    def get_message_detail(self, message_id: str) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            m.room_id,
                            COALESCE(mem.owner_name, CAST(m.room_id AS CHAR), CAST(m.channel_id AS CHAR)) AS room_name,
                            m.message_id,
                            m.sender_user_id AS user_id,
                            m.sender_name AS username,
                            m.server_id,
                            COALESCE(m.member_name, mem.owner_name) AS member_name,
                            m.sender_role,
                            m.message_type AS msg_type,
                            m.sub_type,
                            m.text_content AS content,
                            m.raw_message_json AS raw_brief,
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
                            COALESCE(m.ext_info_json, mp.ext_json) AS ext_info
                        FROM messages m
                        LEFT JOIN members mem ON mem.server_id = m.server_id
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

    def list_members(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            server_id,
                            owner_name,
                            room_id,
                            channel_id
                        FROM members
                        ORDER BY owner_name ASC, server_id ASC
                        """
                    )
                    return list(cursor.fetchall())
            except Exception:
                logger.error("获取成员列表失败", exc_info=True)
                raise

    def get_top_member_for_day(self, start_time_ms: int) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            COALESCE(NULLIF(m.member_name, ''), NULLIF(mem.owner_name, ''), NULLIF(m.sender_name, ''), CAST(m.sender_user_id AS CHAR), '-') AS member_name,
                            COUNT(*) AS message_count
                        FROM messages m
                        LEFT JOIN message_payloads mp ON mp.message_id = m.message_id
                        LEFT JOIN members mem ON mem.server_id = m.server_id
                        WHERE m.message_type = %s
                          AND m.message_time_ms >= %s
                          AND m.sender_role = %s
                        GROUP BY COALESCE(NULLIF(m.member_name, ''), NULLIF(mem.owner_name, ''), NULLIF(m.sender_name, ''), CAST(m.sender_user_id AS CHAR), '-')
                        ORDER BY message_count DESC, MAX(m.message_time_ms) DESC
                        LIMIT 1
                        """,
                        (
                            "TEXT",
                            start_time_ms,
                            MEMBER_SENDER_ROLE,
                        ),
                    )
                    row = cursor.fetchone()
                    if not row:
                        return None
                    return {
                        "member_name": row["member_name"],
                        "message_count": row["message_count"],
                    }
            except Exception:
                logger.error("查询当日活跃成员失败", exc_info=True)
                raise

    def get_viewer_summary(self, today_start_ms: int) -> Dict[str, Any]:
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            (SELECT COUNT(*) FROM messages WHERE sender_role = %s AND message_type = %s) AS total_messages,
                            (SELECT COUNT(DISTINCT channel_id) FROM messages) AS total_rooms,
                            (SELECT COUNT(*) FROM messages WHERE sender_role = %s AND message_type = %s AND message_time_ms >= %s) AS today_messages
                        """,
                        (
                            MEMBER_SENDER_ROLE,
                            "TEXT",
                            MEMBER_SENDER_ROLE,
                            "TEXT",
                            today_start_ms,
                        ),
                    )
                    summary_row = cursor.fetchone() or {}
                    top_member_today = self.get_top_member_for_day(today_start_ms)
                    return {
                        "total_messages": summary_row.get("total_messages", 0),
                        "total_rooms": summary_row.get("total_rooms", 0),
                        "today_messages": summary_row.get("today_messages", 0),
                        "top_member_name": top_member_today.get("member_name")
                        if top_member_today
                        else "-",
                        "top_member_count": top_member_today.get("message_count")
                        if top_member_today
                        else 0,
                    }
            except Exception:
                logger.error("获取查看器汇总失败", exc_info=True)
                raise
