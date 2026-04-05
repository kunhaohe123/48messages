"""
口袋48房间消息抓取工具
用于抓取成员房间消息并保存到 SQLite。

注意：此工具仅供学习研究使用，请遵守口袋48用户协议。
"""

import json
import csv
import time
import sqlite3
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path
from abc import ABC, abstractmethod

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MessageStorage(ABC):
    """消息存储抽象类"""

    @abstractmethod
    def save_message(self, message: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def get_messages(self, room_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        pass


class SQLiteStorage(MessageStorage):
    """SQLite 存储实现"""

    def __init__(self, db_path: str = "messages.db"):
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        conn = sqlite3.connect(self.db_path)
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
            CREATE INDEX IF NOT EXISTS idx_room_id
            ON messages(room_id, timestamp DESC)
        """)

        conn.commit()
        conn.close()
        logger.info("数据库初始化完成: %s", self.db_path)

    def save_message(self, message: Dict[str, Any]) -> bool:
        try:
            conn = sqlite3.connect(self.db_path)
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
                message.get('content'),
                message.get('msg_type'),
                message.get('ext_info'),
                message.get('timestamp')
            ))

            conn.commit()
            affected = cursor.rowcount
            conn.close()
            return affected > 0

        except Exception as exc:
            logger.error("保存消息失败: %s", exc)
            return False

    def get_messages(self, room_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp, created_at
            FROM messages
            WHERE room_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
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
        messages = self.get_messages(room_id, limit=1)
        return messages[0] if messages else None

    def export_messages(self, output_path: str, room_id: Optional[str] = None, limit: Optional[int] = None,
                        output_format: str = 'json'):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = """
            SELECT room_id, message_id, user_id, username, content, msg_type, ext_info, timestamp, created_at
            FROM messages
        """
        params: List[Any] = []
        conditions = []
        if room_id:
            conditions.append('room_id = ?')
            params.append(room_id)
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
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
            fieldnames = [
                'room_id', 'message_id', 'user_id', 'username', 'content',
                'msg_type', 'ext_info', 'timestamp', 'created_at'
            ]
            with open(output_path, 'w', encoding='utf-8', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(messages)
            return len(messages)

        raise ValueError(f'不支持的导出格式: {output_format}')


class Pocket48Client:
    """口袋48 API 客户端"""

    def __init__(self, config_path: str = "config.json"):
        self.config = self._load_config(config_path)
        self.session = requests.Session()
        self.storage = self._init_storage()
        self.auth_token = self.config.get('pocket48', {}).get('token')

        self._setup_session()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        with open(path, 'r', encoding='utf-8') as file:
            return json.load(file)

    def _init_storage(self) -> MessageStorage:
        storage_config = self.config.get('storage', {})
        storage_type = storage_config.get('type', 'sqlite')
        db_path = storage_config.get('database', 'messages.db')

        if storage_type != 'sqlite':
            raise ValueError(f"不支持的存储类型: {storage_type}")
        return SQLiteStorage(db_path)

    def _api_config(self) -> Dict[str, Any]:
        return self.config.get('api', {})

    def _pocket48_config(self) -> Dict[str, Any]:
        return self.config.get('pocket48', {})

    def _build_app_info(self) -> str:
        app_info = self._pocket48_config().get('appInfo', {})
        return json.dumps(app_info, ensure_ascii=False, separators=(',', ':'))

    def _setup_session(self):
        pocket48_config = self._pocket48_config()
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'zh-Hans-CN;q=1, zh-Hant-CN;q=0.9, en-CN;q=0.7',
            'Content-Type': 'application/json;charset=utf-8',
            'P-Sign-Type': pocket48_config.get('pSignType', 'V0'),
            'User-Agent': pocket48_config.get('userAgent', ''),
            'appInfo': self._build_app_info(),
            'pa': pocket48_config.get('pa', ''),
        }
        self.session.headers.update({key: value for key, value in headers.items() if value})

    def _get_url(self, path_key: str, default_path: str) -> str:
        api_config = self._api_config()
        base_url = api_config.get('base_url', 'https://pocketapi.48.cn').rstrip('/')
        path = api_config.get(path_key, default_path)
        return f"{base_url}{path}"

    def _get_authenticated_headers(self) -> Dict[str, str]:
        if not self.auth_token:
            raise RuntimeError('未登录或缺少 token')
        return {'token': self.auth_token}

    def _extract_user_from_ext(self, ext_info: str) -> Dict[str, Any]:
        if not ext_info:
            return {}
        try:
            data = json.loads(ext_info)
            return data.get('user', {}) if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}

    def login(self) -> bool:
        if self.auth_token:
            logger.info("使用配置中的 token，跳过登录")
            return True

        pocket48_config = self._pocket48_config()
        mobile = pocket48_config.get('mobile')
        encrypted_password = pocket48_config.get('encryptedPassword')

        if not mobile or not encrypted_password:
            logger.error("缺少 mobile 或 encryptedPassword，无法登录")
            return False

        payload = {
            'deviceToken': pocket48_config.get('deviceToken', ''),
            'loginType': 'MOBILE_PWD',
            'loginMobile': {
                'mobile': mobile,
                'pwd': encrypted_password,
            }
        }

        try:
            logger.info("开始登录...")
            response = self.session.post(
                self._get_url('login_path', '/user/api/v2/login/app/app_login'),
                json=payload,
                timeout=self._api_config().get('timeout', 30),
            )
            response.raise_for_status()

            data = response.json()
            if data.get('status') != 200 or not data.get('success'):
                logger.error("登录失败: %s", data.get('message'))
                return False

            content = data.get('content', {})
            self.auth_token = content.get('token') or content.get('userInfo', {}).get('token')
            if not self.auth_token:
                logger.error("登录成功但未返回 token")
                return False

            logger.info("登录成功: userId=%s", content.get('userInfo', {}).get('userId'))
            return True

        except Exception as exc:
            logger.error("登录异常: %s", exc)
            return False

    def get_room_messages(self, member: Dict[str, Any], limit: int = 100, next_time: int = 0) -> Dict[str, Any]:
        if not self.auth_token:
            logger.error("未登录或登录已过期")
            return {'messages': [], 'next_time': next_time}

        server_id = member.get('serverId')
        channel_id = member.get('channelId')
        if server_id is None or channel_id is None:
            logger.error("成员配置缺少 serverId 或 channelId: %s", member.get('name'))
            return {'messages': [], 'next_time': next_time}

        payload = {
            'limit': limit,
            'serverId': server_id,
            'channelId': channel_id,
            'nextTime': next_time,
        }

        try:
            logger.info("获取房间消息: %s(%s)", member.get('name', channel_id), channel_id)
            response = self.session.post(
                self._get_url('message_list_path', '/im/api/v1/team/message/list/all'),
                json=payload,
                headers=self._get_authenticated_headers(),
                timeout=self._api_config().get('timeout', 30),
            )
            response.raise_for_status()

            data = response.json()
            if data.get('status') != 200 or not data.get('success'):
                logger.error("获取消息失败: %s", data.get('message'))
                return {'messages': [], 'next_time': next_time}

            content = data.get('content', {})
            messages = content.get('message', [])
            normalized_messages = []
            room_id = str(channel_id)
            for msg in messages:
                ext_info = msg.get('extInfo', '')
                user_info = self._extract_user_from_ext(ext_info)
                normalized_messages.append({
                    'room_id': room_id,
                    'message_id': msg.get('msgIdServer') or msg.get('msgIdClient'),
                    'user_id': user_info.get('userId'),
                    'username': user_info.get('nickName'),
                    'content': msg.get('bodys'),
                    'msg_type': msg.get('msgType'),
                    'ext_info': ext_info,
                    'timestamp': msg.get('msgTime'),
                })

            logger.info("获取到 %s 条消息", len(normalized_messages))
            return {
                'messages': normalized_messages,
                'next_time': content.get('nextTime', next_time),
            }

        except Exception as exc:
            logger.error("获取消息异常: %s", exc)
            return {'messages': [], 'next_time': next_time}

    def save_messages(self, messages: List[Dict[str, Any]]) -> int:
        saved_count = 0
        for msg in messages:
            if self.storage.save_message(msg):
                saved_count += 1
        return saved_count

    def _get_latest_local_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        if isinstance(self.storage, SQLiteStorage):
            return self.storage.get_latest_message(room_id)
        return None

    def _filter_new_messages(self, room_id: str, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        latest_local = self._get_latest_local_message(room_id)
        if not latest_local:
            return messages

        latest_timestamp = latest_local.get('timestamp') or 0
        latest_message_id = latest_local.get('message_id')
        filtered: List[Dict[str, Any]] = []
        for msg in messages:
            msg_timestamp = msg.get('timestamp') or 0
            msg_id = msg.get('message_id')
            if msg_timestamp > latest_timestamp:
                filtered.append(msg)
                continue
            if msg_timestamp == latest_timestamp and msg_id != latest_message_id:
                filtered.append(msg)

        return filtered

    def monitor_room(self, member: Dict[str, Any], interval: int = 60, limit: int = 100):
        room_id = str(member.get('channelId'))
        logger.info("开始监控房间 %s，间隔 %s 秒", room_id, interval)

        first_fetch = True
        while True:
            try:
                # 每轮都取最新一页，再用本地库过滤旧消息，避免持续向历史翻页。
                result = self.get_room_messages(member, limit=limit, next_time=0)
                messages = result['messages']
                if not first_fetch:
                    messages = self._filter_new_messages(room_id, messages)

                if messages:
                    saved = self.save_messages(messages)
                    logger.info("房间 %s 保存了 %s 条新消息", room_id, saved)

                first_fetch = False

                time.sleep(interval)

            except KeyboardInterrupt:
                logger.info("停止监控")
                break
            except Exception as exc:
                logger.error("监控异常: %s", exc)
                time.sleep(interval * 2)


class MessageScraper:
    """消息抓取器主类"""

    def __init__(self, config_path: str = "config.json"):
        self.client = Pocket48Client(config_path)
        self.config = self.client.config

    def run(self):
        if not self.client.login():
            logger.error("登录失败，程序退出")
            return

        members = self.config.get('members', [])
        monitor_config = self.config.get('monitor', {})
        if not members:
            logger.warning("没有配置监控成员")
            return

        logger.info("开始监控 %s 个成员", len(members))
        interval = monitor_config.get('interval', 60)
        limit = monitor_config.get('limit', 100)
        for member in members:
            if member.get('channelId') is not None and member.get('serverId') is not None:
                self.client.monitor_room(member, interval=interval, limit=limit)

    def export(self, output_path: str, output_format: str, room_id: Optional[str], limit: Optional[int]):
        if not isinstance(self.client.storage, SQLiteStorage):
            raise ValueError('当前仅支持从 SQLite 导出消息')

        count = self.client.storage.export_messages(
            output_path=output_path,
            room_id=room_id,
            limit=limit,
            output_format=output_format,
        )
        logger.info('已导出 %s 条消息到 %s', count, output_path)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='口袋48房间消息抓取工具')
    parser.add_argument('-c', '--config', default='config.json', help='配置文件路径')
    parser.add_argument('--export-format', choices=['json', 'csv'], help='导出数据库中的消息')
    parser.add_argument('--output', help='导出文件路径')
    parser.add_argument('--room-id', help='仅导出指定房间的消息')
    parser.add_argument('--limit', type=int, help='导出消息数量上限')
    args = parser.parse_args()

    try:
        scraper = MessageScraper(args.config)
        if args.export_format:
            if not args.output:
                raise ValueError('使用导出功能时必须提供 --output')
            scraper.export(
                output_path=args.output,
                output_format=args.export_format,
                room_id=args.room_id,
                limit=args.limit,
            )
            return
        scraper.run()
    except Exception as exc:
        logger.error("程序异常: %s", exc)
        raise


if __name__ == '__main__':
    main()
