"""
口袋48房间消息抓取工具
用于抓取成员房间消息并保存到数据库。

注意：此工具仅供学习研究使用，请遵守口袋48用户协议。
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

class Pocket48Client:
    """口袋48 API 客户端"""

    def __init__(self, config_path: str = "config.json"):
        self.config = self._load_config(config_path)
        self.session = requests.Session()
        self.storage = self._init_storage()
        token_file = self.config.get('storage', {}).get('token_file', 'token.json')
        self.token_manager = TokenManager(token_file)
        configured_token = self.config.get('pocket48', {}).get('token')
        if configured_token and not self.token_manager.get_token():
            self.token_manager.set_token(configured_token)

        self._setup_session()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        with open(path, 'r', encoding='utf-8') as file:
            return json.load(file)

    def _init_storage(self) -> MessageStorage:
        return create_storage(self.config)

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
        token = self.token_manager.get_token()
        if not token:
            raise RuntimeError('未登录或缺少有效 token')
        return {'token': token}

    def _extract_user_from_ext(self, ext_info: str) -> Dict[str, Any]:
        if not ext_info:
            return {}
        try:
            data = json.loads(ext_info)
            return data.get('user', {}) if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}

    def login(self) -> bool:
        if self.token_manager.get_token():
            logger.info('使用已保存的 Token')
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
            token = content.get('token') or content.get('userInfo', {}).get('token')
            if not token:
                logger.error("登录成功但未返回 token")
                return False

            valid_time_minutes = content.get('userInfo', {}).get('validTime', 40)
            expires_in = max(int(valid_time_minutes) * 60, 600)
            self.token_manager.set_token(token, expires_in)

            logger.info("登录成功: userId=%s", content.get('userInfo', {}).get('userId'))
            return True

        except Exception as exc:
            logger.error("登录异常: %s", exc)
            return False

    def get_room_messages(self, member: Dict[str, Any], limit: int = 100, next_time: int = 0) -> Dict[str, Any]:
        if not self.login():
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
                    'owner_member_id': server_id,
                    'member_name': member.get('name', room_id),
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

        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in {401, 403}:
                logger.warning('Token 失效，清除后等待下次重新登录')
                self.token_manager.clear()
            logger.error("获取消息异常: %s", exc)
            return {'messages': [], 'next_time': next_time}
        except Exception as exc:
            logger.error("获取消息异常: %s", exc)
            return {'messages': [], 'next_time': next_time}

    def save_messages(self, messages: List[Dict[str, Any]]) -> int:
        return self.storage.save_messages(messages)

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
        logger.info("开始监控房间 %s(%s)，间隔 %s 秒", room_name, room_id, interval)

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
                    latest_message = max(messages, key=lambda item: item.get('timestamp') or 0)
                    self.storage.record_fetch(
                        room_id=room_id,
                        messages_count=saved,
                        status='success',
                        last_message_id=latest_message.get('message_id'),
                        last_message_time_ms=latest_message.get('timestamp'),
                    )
                    logger.info("房间 %s(%s) 保存了 %s 条新消息", room_name, room_id, saved)
                else:
                    self.storage.record_fetch(room_id=room_id, messages_count=0, status='success')

                first_fetch = False

                time.sleep(interval)

            except KeyboardInterrupt:
                logger.info("停止监控房间 %s(%s)", room_name, room_id)
                break
            except Exception as exc:
                logger.error("监控异常 %s(%s): %s", room_name, room_id, exc)
                self.storage.record_fetch(room_id=room_id, messages_count=0, status='failed', error_message=str(exc))
                time.sleep(interval * 2)


class MessageScraper:
    """消息抓取器主类"""

    def __init__(self, config_path: str = "config.json"):
        self.client = Pocket48Client(config_path)
        self.config = self.client.config
        self.threads: List[threading.Thread] = []

    def _select_members(self, member_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        members = self.config.get('members', [])
        if not member_names:
            return members

        selected = [member for member in members if member.get('name') in set(member_names)]
        missing_names = [name for name in member_names if name not in {member.get('name') for member in selected}]
        for name in missing_names:
            logger.warning("未找到成员配置: %s", name)
        return selected

    def run(self, member_names: Optional[List[str]] = None):
        if not self.client.login():
            logger.error("登录失败，程序退出")
            return

        members = self._select_members(member_names)
        monitor_config = self.config.get('monitor', {})
        if not members:
            logger.warning("没有配置监控成员")
            return

        logger.info("开始监控 %s 个成员", len(members))
        interval = monitor_config.get('interval', 60)
        limit = monitor_config.get('limit', 100)

        for member in members:
            if member.get('channelId') is not None and member.get('serverId') is not None:
                thread = threading.Thread(
                    target=self.client.monitor_room,
                    args=(member, interval, limit),
                    daemon=True,
                )
                thread.start()
                self.threads.append(thread)

        logger.info("已启动 %s 个房间的监控", len(self.threads))

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("停止监控")

    def _run_member_once(self, member: Dict[str, Any], fetch_limit: int, semaphore: Optional[threading.Semaphore] = None):
        room_id = member.get('channelId')
        server_id = member.get('serverId')
        room_name = member.get('name', room_id)
        if room_id is None or server_id is None:
            logger.warning("跳过缺少 serverId/channelId 的成员配置: %s", member)
            return

        try:
            if semaphore is not None:
                semaphore.acquire()
            result = self.client.get_room_messages(member, limit=fetch_limit, next_time=0)
            messages = result['messages']
            saved = self.client.save_messages(messages) if messages else 0

            if messages:
                latest_message = max(messages, key=lambda item: item.get('timestamp') or 0)
                self.client.storage.record_fetch(
                    room_id=str(room_id),
                    messages_count=saved,
                    status='success',
                    last_message_id=latest_message.get('message_id'),
                    last_message_time_ms=latest_message.get('timestamp'),
                )
            else:
                self.client.storage.record_fetch(
                    room_id=str(room_id),
                    messages_count=0,
                    status='success',
                )

            logger.info("单次抓取 %s(%s): 获取 %s 条，保存 %s 条", room_name, room_id, len(messages), saved)
        except Exception as exc:
            self.client.storage.record_fetch(
                room_id=str(room_id),
                messages_count=0,
                status='failed',
                error_message=str(exc),
            )
            logger.error("单次抓取异常 %s(%s): %s", room_name, room_id, exc)
        finally:
            if semaphore is not None:
                semaphore.release()

    def run_once(self, limit: Optional[int] = None, member_names: Optional[List[str]] = None,
                 max_workers: Optional[int] = None):
        if not self.client.login():
            logger.error("登录失败，程序退出")
            return

        members = self._select_members(member_names)
        monitor_config = self.config.get('monitor', {})
        if not members:
            logger.warning("没有配置监控成员")
            return

        fetch_limit = limit or monitor_config.get('limit', 100)
        worker_count = max_workers or len(members)
        worker_count = max(1, min(worker_count, len(members)))
        logger.info("开始单次抓取 %s 个成员，并发 %s", len(members), worker_count)

        threads: List[threading.Thread] = []
        semaphore = threading.Semaphore(worker_count)
        for member in members:
            thread = threading.Thread(target=self._run_member_once, args=(member, fetch_limit, semaphore))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def export(self, output_path: str, output_format: str, room_id: Optional[str], limit: Optional[int]):
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
    parser.add_argument('--once', action='store_true', help='每个成员只抓取一次后退出')
    parser.add_argument('--member', action='append', help='仅抓取指定成员，可重复传入')
    parser.add_argument('--workers', type=int, help='单次抓取时的最大并发成员数')
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
        if args.once:
            scraper.run_once(limit=args.limit, member_names=args.member, max_workers=args.workers)
            return
        scraper.run(member_names=args.member)
    except Exception as exc:
        logger.error("程序异常: %s", exc)
        raise


if __name__ == '__main__':
    main()
