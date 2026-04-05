"""
口袋48持久化抓取方案。

当前版本基于已抓到的真实接口：
- 登录: /user/api/v2/login/app/app_login
- 消息列表: /im/api/v1/team/message/list/all
"""

import json
import time
import logging
import threading
from typing import Any, Dict, List, Optional
from pathlib import Path

import requests

from message_storage import MessageStorage, create_storage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TokenManager:
    """Token 管理器"""

    def __init__(self, token_file: str = 'token.json'):
        self.token_file = token_file
        self.token_data = self._load_token()

    def _load_token(self) -> Dict[str, Any]:
        path = Path(self.token_file)
        if not path.exists():
            return {}
        with open(path, 'r', encoding='utf-8') as file:
            return json.load(file)

    def _save_token(self):
        with open(self.token_file, 'w', encoding='utf-8') as file:
            json.dump(self.token_data, file, ensure_ascii=False, indent=2)

    def set_token(self, access_token: str, expires_in: int = 86400):
        self.token_data = {
            'access_token': access_token,
            'expires_at': time.time() + expires_in,
            'acquired_at': time.time(),
        }
        self._save_token()
        logger.info('Token 已保存')

    def get_token(self) -> Optional[str]:
        if not self.token_data:
            return None
        if time.time() >= self.token_data.get('expires_at', 0):
            logger.warning('Token 已过期')
            return None
        return self.token_data.get('access_token')

    def clear(self):
        self.token_data = {}
        path = Path(self.token_file)
        if path.exists():
            path.unlink()
        logger.info('Token 已清除')


class PersistentPocket48Scraper:
    """持久化口袋48抓取器"""

    def __init__(self, config_path: str = 'config.json'):
        self.config = self._load_config(config_path)
        self.session = requests.Session()
        token_file = self.config.get('storage', {}).get('token_file', 'token.json')
        self.token_manager = TokenManager(token_file)
        self.storage = self._init_storage()
        self.running = False
        self.threads: List[threading.Thread] = []

        configured_token = self.config.get('pocket48', {}).get('token')
        if configured_token and not self.token_manager.get_token():
            self.token_manager.set_token(configured_token)

        self._setup_session()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f'配置文件不存在: {config_path}')

        with open(path, 'r', encoding='utf-8') as file:
            return json.load(file)

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
        return f'{base_url}{path}'

    def _authenticated_headers(self) -> Dict[str, str]:
        token = self.token_manager.get_token()
        if not token:
            raise RuntimeError('缺少有效 token')
        return {'token': token}

    def _init_storage(self) -> MessageStorage:
        return create_storage(self.config)

    def _extract_user(self, ext_info: str) -> Dict[str, Any]:
        if not ext_info:
            return {}
        try:
            data = json.loads(ext_info)
        except json.JSONDecodeError:
            return {}
        return data.get('user', {}) if isinstance(data, dict) else {}

    def login(self) -> bool:
        if self.token_manager.get_token():
            logger.info('使用已保存的 Token')
            return True

        pocket48_config = self._pocket48_config()
        mobile = pocket48_config.get('mobile')
        encrypted_password = pocket48_config.get('encryptedPassword')
        if not mobile or not encrypted_password:
            logger.error('缺少 mobile 或 encryptedPassword，无法登录')
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
            response = self.session.post(
                self._get_url('login_path', '/user/api/v2/login/app/app_login'),
                json=payload,
                timeout=self._api_config().get('timeout', 30),
            )
            response.raise_for_status()
            data = response.json()
            if data.get('status') != 200 or not data.get('success'):
                logger.error('登录失败: %s', data.get('message'))
                return False

            content = data.get('content', {})
            token = content.get('token') or content.get('userInfo', {}).get('token')
            if not token:
                logger.error('登录成功但未返回 token')
                return False

            valid_time_minutes = content.get('userInfo', {}).get('validTime', 40)
            expires_in = max(int(valid_time_minutes) * 60, 600)
            self.token_manager.set_token(token, expires_in)
            logger.info('登录成功: userId=%s', content.get('userInfo', {}).get('userId'))
            return True

        except Exception as exc:
            logger.error('登录异常: %s', exc)
            return False

    def get_room_messages(self, member: Dict[str, Any], limit: int = 100, next_time: int = 0) -> Dict[str, Any]:
        if not self.login():
            return {'messages': [], 'next_time': next_time}

        payload = {
            'limit': limit,
            'serverId': member.get('serverId'),
            'channelId': member.get('channelId'),
            'nextTime': next_time,
        }

        try:
            response = self.session.post(
                self._get_url('message_list_path', '/im/api/v1/team/message/list/all'),
                json=payload,
                headers=self._authenticated_headers(),
                timeout=self._api_config().get('timeout', 30),
            )
            response.raise_for_status()

            data = response.json()
            if data.get('status') != 200 or not data.get('success'):
                logger.error('获取消息失败: %s', data.get('message'))
                return {'messages': [], 'next_time': next_time}

            content = data.get('content', {})
            normalized: List[Dict[str, Any]] = []
            room_id = str(member.get('channelId'))
            for msg in content.get('message', []):
                ext_info = msg.get('extInfo', '')
                user = self._extract_user(ext_info)
                normalized.append({
                    'room_id': room_id,
                    'owner_member_id': member.get('serverId'),
                    'member_name': member.get('name', room_id),
                    'message_id': msg.get('msgIdServer') or msg.get('msgIdClient'),
                    'user_id': user.get('userId'),
                    'username': user.get('nickName'),
                    'content': msg.get('bodys'),
                    'msg_type': msg.get('msgType'),
                    'ext_info': ext_info,
                    'timestamp': msg.get('msgTime'),
                })

            return {
                'messages': normalized,
                'next_time': content.get('nextTime', next_time),
            }

        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in {401, 403}:
                logger.warning('Token 失效，清除后重试登录')
                self.token_manager.clear()
            logger.error('获取消息异常: %s', exc)
            return {'messages': [], 'next_time': next_time}
        except Exception as exc:
            logger.error('获取消息异常: %s', exc)
            return {'messages': [], 'next_time': next_time}

    def save_messages(self, messages: List[Dict[str, Any]], room_id: str) -> int:
        try:
            return self.storage.save_messages(messages)
        except Exception as exc:
            logger.error('保存消息失败: %s', exc)
            return 0

    def _get_latest_local_message(self, room_id: str) -> Optional[Dict[str, Any]]:
        return self.storage.get_latest_message(room_id)

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
        room_name = member.get('name', room_id)
        logger.info('开始监控房间 %s(%s)，间隔 %s 秒', room_name, room_id, interval)

        first_fetch = True
        consecutive_errors = 0
        max_errors = 5

        while self.running:
            try:
                # 每轮只拉最新一页，依赖本地库判断哪些消息是新增的。
                result = self.get_room_messages(member, limit=limit, next_time=0)
                messages = result['messages']
                if not first_fetch:
                    messages = self._filter_new_messages(room_id, messages)

                if messages:
                    saved = self.save_messages(messages, room_id)
                    latest_message = max(messages, key=lambda item: item.get('timestamp') or 0)
                    self.storage.record_fetch(
                        room_id=room_id,
                        messages_count=saved,
                        status='success',
                        last_message_id=latest_message.get('message_id'),
                        last_message_time_ms=latest_message.get('timestamp'),
                    )
                    logger.info('房间 %s: 获取 %s 条，保存 %s 条新消息', room_id, len(messages), saved)
                    consecutive_errors = 0
                else:
                    self.storage.record_fetch(room_id=room_id, messages_count=0, status='success')
                    consecutive_errors = 0

                first_fetch = False

                if consecutive_errors >= max_errors:
                    logger.warning('房间 %s 连续失败 %s 次，尝试重新登录', room_id, max_errors)
                    self.token_manager.clear()
                    consecutive_errors = 0

                time.sleep(interval)

            except KeyboardInterrupt:
                break
            except Exception as exc:
                logger.error('监控异常: %s', exc)
                self.storage.record_fetch(room_id=room_id, messages_count=0, status='failed', error_message=str(exc))
                consecutive_errors += 1
                time.sleep(interval)

        logger.info('房间 %s 监控已停止', room_id)

    def start_monitoring(self):
        if self.running:
            logger.warning('监控已在运行')
            return

        members = self.config.get('members', [])
        if not members:
            logger.warning('没有配置任何成员')
            return

        if not self.login():
            logger.error('登录失败，无法启动监控')
            return

        self.running = True
        monitor_config = self.config.get('monitor', {})
        interval = monitor_config.get('interval', 60)
        limit = monitor_config.get('limit', 100)

        for member in members:
            if member.get('channelId') is None or member.get('serverId') is None:
                logger.warning('跳过缺少 serverId/channelId 的成员配置: %s', member)
                continue

            thread = threading.Thread(
                target=self.monitor_room,
                args=(member, interval, limit),
                daemon=True,
            )
            thread.start()
            self.threads.append(thread)
            time.sleep(1)

        logger.info('已启动 %s 个房间的监控', len(self.threads))

    def stop_monitoring(self):
        self.running = False
        logger.info('停止监控')

    def get_statistics(self) -> Dict[str, Any]:
        return self.storage.get_statistics()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='口袋48持久化抓取工具')
    parser.add_argument('-c', '--config', default='config.json', help='配置文件')
    parser.add_argument('--stats', action='store_true', help='显示统计信息')
    args = parser.parse_args()

    try:
        scraper = PersistentPocket48Scraper(args.config)

        if args.stats:
            stats = scraper.get_statistics()
            print('\n=== 抓取统计 ===')
            print(f"总消息数: {stats['total_messages']}")
            print(f"监控房间数: {stats['total_rooms']}")
            print(f"成功抓取次数: {stats['successful_fetches']}")
            print('\n消息数前10的房间:')
            for room, count in stats['top_rooms']:
                print(f'  {room}: {count} 条')
            return

        scraper.start_monitoring()
        try:
            while scraper.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print('\n正在停止...')
            scraper.stop_monitoring()

    except Exception as exc:
        logger.error('程序异常: %s', exc)
        raise


if __name__ == '__main__':
    main()
