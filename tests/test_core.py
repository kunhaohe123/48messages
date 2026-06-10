import json
import sys
import tempfile
import threading
import unittest
from unittest import mock
from pathlib import Path
from typing import Any, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from message_parser import (  # noqa: E402
    FAN_SENDER_ROLE,
    MEMBER_SENDER_ROLE,
    determine_sender_role_from_message,
    extract_media_fields,
    extract_text_content,
    parse_member_role_from_json,
)
from message_normalizer import normalize_room_messages  # noqa: E402
from message_viewer import create_app  # noqa: E402
from message_storage import StorageConfigError, create_storage  # noqa: E402
from mysql_storage import MySQLStorage  # noqa: E402
from pocket48_scraper import (  # noqa: E402
    AuthenticationUnavailableError,
    MessageScraper,
    Pocket48Client,
    TokenManager,
    _normalize_member_config,
    load_config,
)
from sqlite_storage import SQLiteStorage  # noqa: E402


def make_message(
    message_id: str,
    *,
    room_id: str = "room-1",
    username: str = "成员A",
    content: Optional[Any] = None,
    ext_info: Optional[Any] = None,
    timestamp: int = 1_700_000_000_000,
) -> dict:
    return {
        "room_id": room_id,
        "message_id": message_id,
        "user_id": "user-1",
        "username": username,
        "member_name": "成员A",
        "content": content if content is not None else {"text": "今天也要加油"},
        "msg_type": "TEXT",
        "ext_info": ext_info if ext_info is not None else {"user": {"roleId": 3}},
        "timestamp": timestamp,
    }


class ConfigTests(unittest.TestCase):
    def test_normalize_member_config_copies_id_to_member_id(self):
        member = _normalize_member_config(
            {"id": 417331, "ownerName": "成员官方名"},
            index=1,
        )

        self.assertEqual(member["id"], 417331)
        self.assertEqual(member["memberId"], 417331)

    def test_load_config_reads_members_next_to_config_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_path = config_dir / "config.json"
            members_path = config_dir / "members.json"
            config_path.write_text(
                json.dumps({"storage": {"type": "sqlite", "database": ":memory:"}}),
                encoding="utf-8",
            )
            members_path.write_text(
                json.dumps(
                    [
                        {
                            "id": 1,
                            "ownerName": "成员A",
                            "serverId": 10,
                            "channelId": 20,
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            config = load_config(str(config_path))

        self.assertEqual(config["members"][0]["memberId"], 1)
        self.assertEqual(config["members"][0]["ownerName"], "成员A")

    def test_pocket48_client_initializes_with_sqlite_storage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_path = config_dir / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "pocket48": {},
                        "storage": {
                            "type": "sqlite",
                            "database": str(config_dir / "messages.db"),
                        },
                    }
                ),
                encoding="utf-8",
            )
            (config_dir / "members.json").write_text(
                json.dumps(
                    [
                        {
                            "id": 1,
                            "ownerName": "成员A",
                            "serverId": 10,
                            "channelId": 20,
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            client = Pocket48Client(str(config_path))

        self.assertIsInstance(client.storage, SQLiteStorage)

    def test_configured_token_replaces_stale_token_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_path = config_dir / "config.json"
            token_path = config_dir / "token.json"
            token_path.write_text(
                json.dumps(
                    {
                        "access_token": "old-token",
                        "expires_at": 4_000_000_000,
                        "acquired_at": 1_700_000_000,
                    }
                ),
                encoding="utf-8",
            )
            config_path.write_text(
                json.dumps(
                    {
                        "pocket48": {"token": "new-token"},
                        "storage": {
                            "type": "sqlite",
                            "database": str(config_dir / "messages.db"),
                            "token_file": str(token_path),
                        },
                    }
                ),
                encoding="utf-8",
            )
            (config_dir / "members.json").write_text(
                json.dumps(
                    [
                        {
                            "id": 1,
                            "ownerName": "成员A",
                            "serverId": 10,
                            "channelId": 20,
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            Pocket48Client(str(config_path))

            token_data = json.loads(token_path.read_text(encoding="utf-8"))

        self.assertEqual(token_data["access_token"], "new-token")

    def test_create_mysql_storage_allows_missing_password(self):
        with mock.patch("mysql_storage.MySQLStorage") as storage_cls:
            create_storage(
                {
                    "storage": {
                        "type": "mysql",
                        "host": "localhost",
                        "database": "48pocket",
                        "user": "root",
                    }
                }
            )

        self.assertEqual(storage_cls.call_args.kwargs["password"], "")

    def test_create_mysql_storage_can_skip_schema_initialization(self):
        with mock.patch("mysql_storage.MySQLStorage") as storage_cls:
            create_storage(
                {
                    "storage": {
                        "type": "mysql",
                        "host": "localhost",
                        "database": "48pocket",
                        "user": "root",
                    }
                },
                initialize_schema=False,
            )

        self.assertFalse(storage_cls.call_args.kwargs["initialize_schema"])

    def test_create_mysql_storage_rejects_unsafe_database_name(self):
        with self.assertRaises(StorageConfigError):
            create_storage(
                {
                    "storage": {
                        "type": "mysql",
                        "host": "localhost",
                        "database": "48pocket`; DROP DATABASE mysql; --",
                        "user": "root",
                    }
                }
            )


class TokenManagerTests(unittest.TestCase):
    def test_invalid_token_cache_is_ignored(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "token.json"
            token_path.write_text("{invalid json", encoding="utf-8")

            with self.assertLogs("pocket48_scraper", level="WARNING") as logs:
                manager = TokenManager(str(token_path))

        self.assertEqual(manager.token_data, {})
        self.assertIn("Token cache ignored", "\n".join(logs.output))

    def test_token_cache_save_uses_private_file_permissions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "token.json"
            manager = TokenManager(str(token_path))
            manager.set_token("abc", expires_in=600)

            mode = token_path.stat().st_mode & 0o777

        self.assertEqual(mode, 0o600)

    def test_token_refresh_is_safe_under_concurrent_writes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "token.json"
            manager = TokenManager(str(token_path))
            manager.set_token("abc", expires_in=1)

            threads = [
                threading.Thread(target=manager.refresh_expiry, args=(600, False))
                for _ in range(8)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            token_data = json.loads(token_path.read_text(encoding="utf-8"))

        self.assertEqual(token_data["access_token"], "abc")
        self.assertGreater(token_data["expires_at"], token_data["acquired_at"])


class Pocket48ClientTests(unittest.TestCase):
    def test_token_rejection_raises_authentication_error_without_wrapping(self):
        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "status": 401,
                    "success": False,
                    "message": "token expired",
                }

        class FakeSession:
            def post(self, *args, **kwargs):
                return FakeResponse()

        class FakeTokenManager:
            def clear(self):
                self.cleared = True

        client = Pocket48Client.__new__(Pocket48Client)
        client.notifier = mock.Mock()
        client.token_manager = FakeTokenManager()
        client.password_login_blocked_reason = None
        client._get_session = lambda: FakeSession()
        client._get_url = lambda key, default: "https://example.invalid/messages"
        client._api_config = lambda: {"timeout": 1}
        client._get_authenticated_headers = lambda: {"token": "expired"}
        client._block_password_login = lambda reason: setattr(
            client, "password_login_blocked_reason", reason
        )

        with self.assertRaises(AuthenticationUnavailableError):
            client.get_room_messages(
                {"serverId": 10, "channelId": 20, "ownerName": "成员A"}
            )

    def test_latest_fetch_stops_when_local_boundary_is_seen(self):
        class FakeStorage:
            def get_latest_message(self, room_id):
                return {"message_id": "local", "timestamp": 1000}

        class FakeClient(Pocket48Client):
            def __init__(self):
                self.storage = FakeStorage()
                self.pages = [
                    (
                        {
                            "messages": [
                                make_message("new", room_id="20", timestamp=2000),
                                make_message("local", room_id="20", timestamp=1000),
                            ],
                            "next_time": 123,
                            "raw_count": 2,
                            "newest_raw_timestamp": 2000,
                            "oldest_raw_timestamp": 1000,
                        },
                        0,
                    )
                ]

            def ensure_authenticated(self):
                return None

            def _begin_fetch_round(self):
                return None

            def _get_room_messages_with_retry(self, member, limit, next_time):
                return self.pages.pop(0)

        client = FakeClient()
        messages = client.fetch_latest_incremental_messages(
            {"serverId": 10, "channelId": 20, "ownerName": "成员A"}
        )

        self.assertEqual([message["message_id"] for message in messages], ["new"])
        self.assertEqual(client.pages, [])

    def test_latest_fetch_respects_max_pages(self):
        class FakeStorage:
            def get_latest_message(self, room_id):
                return {"message_id": "old", "timestamp": 1}

        class FakeClient(Pocket48Client):
            def __init__(self):
                self.storage = FakeStorage()
                self.calls = 0

            def ensure_authenticated(self):
                return None

            def _begin_fetch_round(self):
                return None

            def _get_room_messages_with_retry(self, member, limit, next_time):
                self.calls += 1
                return (
                    {
                        "messages": [
                            make_message(
                                f"m-{self.calls}",
                                room_id="20",
                                timestamp=1_700_000_000_000 - self.calls,
                            )
                        ],
                        "next_time": self.calls,
                        "raw_count": 1,
                        "newest_raw_timestamp": 1_700_000_000_000 - self.calls,
                        "oldest_raw_timestamp": 1_700_000_000_000 - self.calls,
                    },
                    0,
                )

        client = FakeClient()
        messages = client.fetch_latest_incremental_messages(
            {"serverId": 10, "channelId": 20, "ownerName": "成员A"},
            max_pages=2,
        )

        self.assertEqual(client.calls, 2)
        self.assertEqual(len(messages), 2)


class MessageScraperTests(unittest.TestCase):
    def test_run_once_raises_when_any_member_fails(self):
        scraper = MessageScraper.__new__(MessageScraper)
        scraper.config = {
            "members": [
                {
                    "id": 1,
                    "ownerName": "成员A",
                    "serverId": 10,
                    "channelId": 20,
                },
                {
                    "id": 2,
                    "ownerName": "成员B",
                    "serverId": 11,
                    "channelId": 21,
                },
            ],
            "monitor": {"limit": 100},
        }
        scraper.client = mock.Mock()
        scraper.client.login.return_value = True
        scraper._run_member_once = mock.Mock(side_effect=[True, False])

        with self.assertRaisesRegex(RuntimeError, "failed for 1 member"):
            scraper.run_once(max_workers=1)


class MessageParsingTests(unittest.TestCase):
    def test_member_role_detection_handles_nested_role_fields(self):
        self.assertTrue(parse_member_role_from_json({"user": {"roleId": 3}}))
        self.assertTrue(parse_member_role_from_json({"user": {"channelRole": "2"}}))
        self.assertFalse(parse_member_role_from_json({"user": {"roleId": 1}}))
        self.assertFalse(parse_member_role_from_json({"user": {"roleId": 2}}))

    def test_sender_role_detection_distinguishes_member_and_fan_messages(self):
        member_message = make_message(
            "member-1",
            ext_info=json.dumps({"user": {"channelRole": 2}}),
        )
        fan_message = make_message(
            "fan-1",
            ext_info=json.dumps({"user": {"roleId": 1}}),
        )

        self.assertEqual(
            determine_sender_role_from_message(member_message),
            MEMBER_SENDER_ROLE,
        )
        self.assertEqual(
            determine_sender_role_from_message(fan_message),
            FAN_SENDER_ROLE,
        )

    def test_extract_text_and_media_fields_from_flexible_payloads(self):
        body = {"text": "正文", "media": {"url": "https://example.com/a.jpg"}}
        ext_info = {
            "replyText": "回复内容",
            "coverUrl": "https://example.com/cover.jpg",
            "duration": 12,
        }

        self.assertEqual(extract_text_content(body, ext_info), "正文 | 回复内容")

        media = extract_media_fields(body, ext_info)
        self.assertEqual(media["media_url"], "https://example.com/a.jpg")
        self.assertEqual(media["media_cover_url"], "https://example.com/cover.jpg")
        self.assertEqual(media["media_duration"], 12)
        self.assertEqual(media["reply_to_text"], "回复内容")

    def test_normalizer_skips_member_text_without_message_id(self):
        result = normalize_room_messages(
            raw_messages=[
                {
                    "msgTime": 1000,
                    "msgType": "TEXT",
                    "extInfo": {"user": {"roleId": 3, "userId": 10}},
                    "bodys": {"text": "没有 ID"},
                },
                {
                    "msgIdServer": "m-1",
                    "msgTime": 900,
                    "msgType": "TEXT",
                    "extInfo": {"user": {"roleId": 3, "userId": 10}},
                    "bodys": {"text": "有 ID"},
                },
            ],
            server_id=10,
            channel_id=20,
            member_name="成员A",
        )

        self.assertEqual(result["raw_count"], 2)
        self.assertEqual([item["message_id"] for item in result["messages"]], ["m-1"])


class MySQLStorageTests(unittest.TestCase):
    def test_connection_pools_are_instance_scoped(self):
        with mock.patch.object(MySQLStorage, "_ensure_database"), mock.patch.object(
            MySQLStorage, "_init_database"
        ):
            first = MySQLStorage("host-a", 3306, "db_a", "user", "")
            second = MySQLStorage("host-b", 3306, "db_b", "user", "")

        self.assertIsNot(first._pool, second._pool)
        self.assertIsNot(first._pool_lock, second._pool_lock)

    def test_sender_role_backfill_migration_runs_once(self):
        class FakeCursor:
            def __init__(self):
                self.applied = False
                self.statements = []
                self._last_statement = ""

            def execute(self, statement, params=None):
                self._last_statement = " ".join(statement.split())
                self.statements.append(self._last_statement)
                if self._last_statement.startswith(
                    "INSERT IGNORE INTO schema_migrations"
                ):
                    self.applied = True

            def fetchone(self):
                if "SHOW COLUMNS FROM messages LIKE" in self._last_statement:
                    return {"Field": "existing"}
                if self._last_statement.startswith(
                    "SELECT migration_key FROM schema_migrations"
                ):
                    return {"migration_key": "done"} if self.applied else None
                if self._last_statement.startswith("SHOW INDEX FROM"):
                    return {"Key_name": "existing"}
                return None

        storage = MySQLStorage.__new__(MySQLStorage)
        cursor = FakeCursor()

        storage._ensure_messages_sender_role_schema(cursor)
        storage._run_mysql_migrations(cursor)
        first_update_count = sum(
            statement.startswith("UPDATE messages") for statement in cursor.statements
        )
        storage._ensure_messages_sender_role_schema(cursor)
        storage._run_mysql_migrations(cursor)
        second_total_update_count = sum(
            statement.startswith("UPDATE messages") for statement in cursor.statements
        )

        self.assertEqual(first_update_count, 3)
        self.assertEqual(second_total_update_count, first_update_count)


class SQLiteStorageTests(unittest.TestCase):
    def test_sync_members_counts_valid_member_configs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SQLiteStorage(str(Path(tmpdir) / "messages.db"))
            synced_count = storage.sync_members(
                [
                    {
                        "id": 1,
                        "ownerName": "成员A",
                        "serverId": 10,
                        "channelId": 20,
                    },
                    {"id": 2, "ownerName": "缺频道", "serverId": 11},
                    "not-a-member",
                ]
            )

        self.assertEqual(synced_count, 1)

    def test_sync_members_persists_member_room_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SQLiteStorage(str(Path(tmpdir) / "messages.db"))
            storage.sync_members(
                [
                    {
                        "id": 1,
                        "ownerName": "成员A",
                        "serverId": 10,
                        "channelId": 20,
                    }
                ]
            )
            storage.save_message(make_message("m-1", room_id="20", timestamp=1000))

            rooms = storage.list_rooms()
            members = storage.list_members()

        self.assertEqual(rooms[0]["name"], "成员A")
        self.assertEqual(members[0]["owner_name"], "成员A")
        self.assertEqual(members[0]["message_count"], 1)

    def test_save_messages_skips_missing_message_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SQLiteStorage(str(Path(tmpdir) / "messages.db"))
            saved_count = storage.save_messages(
                [
                    make_message("m-1"),
                    {**make_message(""), "message_id": ""},
                    {**make_message("none"), "message_id": None},
                ]
            )

            result = storage.search_messages(sender_role=MEMBER_SENDER_ROLE)

        self.assertEqual(saved_count, 1)
        self.assertEqual(result["total"], 1)

    def test_save_messages_deduplicates_and_searches_member_text(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SQLiteStorage(str(Path(tmpdir) / "messages.db"))
            saved_count = storage.save_messages(
                [
                    make_message("m-1", content={"text": "第一条消息"}, timestamp=1000),
                    make_message("m-1", content={"text": "重复消息"}, timestamp=1000),
                    make_message("m-2", content={"text": "第二条消息"}, timestamp=2000),
                ]
            )

            result = storage.search_messages(
                keyword="第一条",
                sender_role=MEMBER_SENDER_ROLE,
                msg_type="TEXT",
            )

        self.assertEqual(saved_count, 2)
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["message_id"], "m-1")

    def test_search_messages_limit_zero_returns_count_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SQLiteStorage(str(Path(tmpdir) / "messages.db"))
            storage.save_messages(
                [
                    make_message("m-1", content={"text": "第一条消息"}, timestamp=1000),
                    make_message("m-2", content={"text": "第二条消息"}, timestamp=2000),
                ]
            )

            result = storage.search_messages(
                sender_role=MEMBER_SENDER_ROLE,
                msg_type="TEXT",
                limit=0,
            )

        self.assertEqual(result["total"], 2)
        self.assertEqual(result["items"], [])

    def test_viewer_summary_counts_member_text_messages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SQLiteStorage(str(Path(tmpdir) / "messages.db"))
            storage.save_messages(
                [
                    make_message(
                        "m-1",
                        room_id="room-1",
                        username="成员A",
                        content={"text": "今天第一条"},
                        timestamp=2_000,
                    ),
                    make_message(
                        "m-2",
                        room_id="room-2",
                        username="成员B",
                        content={"text": "今天第二条"},
                        timestamp=3_000,
                    ),
                    make_message(
                        "m-old",
                        room_id="room-1",
                        username="成员A",
                        content={"text": "旧消息"},
                        timestamp=500,
                    ),
                ]
            )

            summary = storage.get_viewer_summary(today_start_ms=1_000)

        self.assertEqual(summary["total_messages"], 3)
        self.assertEqual(summary["total_rooms"], 2)
        self.assertEqual(summary["today_messages"], 2)
        self.assertIn(summary["top_member_name"], {"成员A", "成员B"})
        self.assertEqual(summary["top_member_count"], 1)

    def test_history_checkpoint_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SQLiteStorage(str(Path(tmpdir) / "messages.db"))
            storage.start_history_fetch(10, 20, 1000)
            storage.update_history_checkpoint_progress(
                10,
                20,
                oldest_covered_message_id="m-1",
                oldest_covered_time_ms=900,
                resume_next_time=800,
                last_page_count=5,
                cursor_verified=True,
            )
            storage.finish_history_fetch_success(
                10,
                20,
                target_time_ms=1000,
                oldest_covered_message_id="m-1",
                oldest_covered_time_ms=900,
                resume_next_time=800,
                last_page_count=5,
                cursor_verified=True,
            )

            checkpoint = storage.get_history_checkpoint(10, 20)

        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint["status"], "success")
        self.assertEqual(checkpoint["oldest_covered_message_id"], "m-1")
        self.assertEqual(checkpoint["oldest_covered_time_ms"], 900)
        self.assertTrue(checkpoint["cursor_verified"])


class ViewerTests(unittest.TestCase):
    def test_viewer_index_and_detail_render_with_sqlite_storage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            db_path = tmp_path / "messages.db"
            config_path = tmp_path / "config.json"
            members_path = tmp_path / "members.json"

            storage = SQLiteStorage(str(db_path))
            storage.save_message(make_message("m-view", content={"text": "页面消息"}))

            config_path.write_text(
                json.dumps(
                    {
                        "storage": {
                            "type": "sqlite",
                            "database": str(db_path),
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            members_path.write_text("[]", encoding="utf-8")

            app = create_app(str(config_path))
            client = app.test_client()
            index_response = client.get("/")
            detail_response = client.get("/messages/m-view")

        self.assertEqual(index_response.status_code, 200)
        self.assertIn("页面消息", index_response.get_data(as_text=True))
        self.assertEqual(detail_response.status_code, 200)
        self.assertIn("m-view", detail_response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
