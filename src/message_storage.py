from abc import ABC, abstractmethod
import re
from typing import Any, Dict, List, Optional


class MessageStorage(ABC):
    """统一存储接口，抓取层只依赖这些能力，不关心底层是 SQLite 还是 MySQL。"""

    @abstractmethod
    def sync_members(self, members: List[Dict[str, Any]]) -> int:
        pass

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
    def get_history_checkpoint(
        self, server_id: int, channel_id: int
    ) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def start_history_fetch(
        self, server_id: int, channel_id: int, target_time_ms: int
    ) -> None:
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def finish_history_fetch_failed(
        self,
        server_id: int,
        channel_id: int,
        status: str,
        error_message: Optional[str],
        resume_next_time: Optional[int],
        last_page_count: int,
    ) -> None:
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
    def list_members(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def get_message_detail(self, message_id: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_top_member_for_day(self, start_time_ms: int) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_viewer_summary(self, today_start_ms: int) -> Dict[str, Any]:
        pass


class StorageError(RuntimeError):
    """存储层错误基类。"""


class StorageConfigError(StorageError, ValueError):
    pass


def _validate_storage_config(storage_config: Dict[str, Any], storage_type: str) -> None:
    errors: List[str] = []
    if storage_type == "mysql":
        database = storage_config.get("database")
        if not database:
            errors.append("database (MySQL database name) is required")
        elif not re.fullmatch(r"[A-Za-z0-9_]+", str(database)):
            errors.append("database must contain only letters, numbers, and underscores")
        if not storage_config.get("user"):
            errors.append("user (MySQL user) is required")
        if not storage_config.get("host"):
            errors.append("host (MySQL host) is required")
    if errors:
        raise StorageConfigError(f"Invalid storage config: {', '.join(errors)}")


def create_storage(
    config: Dict[str, Any], initialize_schema: Optional[bool] = None
) -> MessageStorage:
    """按配置选择具体存储实现。"""
    storage_config = config.get("storage", {})
    storage_type = storage_config.get("type", "mysql")
    _validate_storage_config(storage_config, storage_type)
    if initialize_schema is None:
        initialize_schema = bool(storage_config.get("auto_migrate_on_startup", True))
    if storage_type == "sqlite":
        from sqlite_storage import SQLiteStorage

        return SQLiteStorage(storage_config.get("database", "data/messages.db"))
    if storage_type == "mysql":
        from mysql_storage import MySQLStorage

        return MySQLStorage(
            host=storage_config.get("host", "localhost"),
            port=storage_config.get("port", 3306),
            database=storage_config["database"],
            user=storage_config["user"],
            password=storage_config.get("password", ""),
            charset=storage_config.get("charset", "utf8mb4"),
            pool_size=storage_config.get("pool_size", 10),
            initialize_schema=initialize_schema,
        )
    raise ValueError(f"不支持的存储类型: {storage_type}")
