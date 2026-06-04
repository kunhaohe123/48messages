import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from message_storage import (  # noqa: E402
    FAN_SENDER_ROLE,
    MEMBER_SENDER_ROLE,
    SQLiteStorage,
    _determine_sender_role_from_message,
    _extract_media_fields,
    _extract_text_content,
    _parse_member_role_from_json,
)
from message_viewer import create_app  # noqa: E402
from pocket48_scraper import (  # noqa: E402
    Pocket48Client,
    TokenManager,
    _normalize_member_config,
    load_config,
)


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


class MessageParsingTests(unittest.TestCase):
    def test_member_role_detection_handles_nested_role_fields(self):
        self.assertTrue(_parse_member_role_from_json({"user": {"roleId": 3}}))
        self.assertTrue(_parse_member_role_from_json({"user": {"channelRole": "2"}}))
        self.assertFalse(_parse_member_role_from_json({"user": {"roleId": 1}}))

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
            _determine_sender_role_from_message(member_message),
            MEMBER_SENDER_ROLE,
        )
        self.assertEqual(
            _determine_sender_role_from_message(fan_message),
            FAN_SENDER_ROLE,
        )

    def test_extract_text_and_media_fields_from_flexible_payloads(self):
        body = {"text": "正文", "media": {"url": "https://example.com/a.jpg"}}
        ext_info = {
            "replyText": "回复内容",
            "coverUrl": "https://example.com/cover.jpg",
            "duration": 12,
        }

        self.assertEqual(_extract_text_content(body, ext_info), "正文 | 回复内容")

        media = _extract_media_fields(body, ext_info)
        self.assertEqual(media["media_url"], "https://example.com/a.jpg")
        self.assertEqual(media["media_cover_url"], "https://example.com/cover.jpg")
        self.assertEqual(media["media_duration"], 12)
        self.assertEqual(media["reply_to_text"], "回复内容")


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
