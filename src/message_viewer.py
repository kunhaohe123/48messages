import argparse
import json
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Any, Dict, Optional

from flask import Flask, abort, jsonify, render_template, request, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

from message_storage import create_storage
from pocket48_scraper import DEFAULT_CONFIG_PATH, load_config


BASE_DIR = Path(__file__).resolve().parent.parent


def format_timestamp(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        ts = int(str(value))
        if ts > 1e12:
            ts = ts / 1000
        dt = datetime.fromtimestamp(ts)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError):
        return str(value)


def pretty_json(value: Any) -> str:
    if value in (None, ""):
        return "{}"
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return value
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


def parse_datetime_local(value: str, is_end: bool = False) -> Optional[int]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if len(value) == 10 and is_end:
        dt = dt + timedelta(days=1) - timedelta(milliseconds=1)
    return int(dt.timestamp() * 1000)


def create_app(config_path: str) -> Flask:
    config = load_config(config_path)
    storage = create_storage(config)
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "templates"),
        static_folder=str(BASE_DIR / "static"),
    )
    app.wsgi_app = ProxyFix(app.wsgi_app, x_prefix=1)
    app.jinja_env.filters["timestamp"] = format_timestamp
    app.jinja_env.filters["pretty_json"] = pretty_json

    def build_summary_stats() -> Dict[str, Any]:
        today_start = int(
            datetime.now()
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp()
            * 1000
        )
        return storage.get_viewer_summary(today_start)

    @app.route("/stats-summary")
    def stats_summary() -> Any:
        return jsonify(build_summary_stats())

    @app.route("/")
    def index() -> str:
        page = max(request.args.get("page", default=1, type=int), 1)
        page_size = min(
            max(request.args.get("page_size", default=20, type=int), 10), 200
        )
        room_id = (request.args.get("room_id") or "").strip() or None
        sender_keyword = (request.args.get("sender") or "").strip() or None
        keyword = (request.args.get("keyword") or "").strip() or None
        start_time = (request.args.get("start_time") or "").strip()
        end_time = (request.args.get("end_time") or "").strip()
        start_time_ms = parse_datetime_local(start_time) if start_time else None
        end_time_ms = parse_datetime_local(end_time, is_end=True) if end_time else None
        offset = (page - 1) * page_size

        rooms = storage.list_rooms()
        summary_stats = build_summary_stats()

        search_kwargs = {
            "room_id": room_id,
            "sender_keyword": sender_keyword,
            "keyword": keyword,
            "msg_type": "TEXT",
            "start_time_ms": start_time_ms,
            "end_time_ms": end_time_ms,
        }
        result = storage.search_messages(
            **search_kwargs,
            sender_role="member",
            limit=page_size,
            offset=offset,
        )
        total = result["total"]
        items = result["items"]
        total_pages = max(ceil(total / page_size), 1)

        filters = {
            "room_id": room_id or "",
            "sender": sender_keyword or "",
            "keyword": keyword or "",
            "start_time": start_time,
            "end_time": end_time,
            "page_size": page_size,
        }

        def build_index_url(target_page: int) -> str:
            query = {
                key: value
                for key, value in filters.items()
                if value not in ("", None)
            }
            query["page"] = target_page
            return url_for("index", **query)

        return render_template(
            "message_viewer/index.html",
            title="口袋房间消息查看",
            rooms=rooms,
            summary_stats=summary_stats,
            filters=filters,
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
            items=items,
            build_index_url=build_index_url,
        )

    @app.route("/messages/<message_id>")
    def message_detail(message_id: str) -> str:
        message = storage.get_message_detail(message_id)
        if not message:
            abort(404)

        return render_template(
            "message_viewer/detail.html",
            title="消息详情",
            message_id=message_id,
            message=message,
        )

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="口袋房间消息查看")
    parser.add_argument(
        "-c", "--config", default=DEFAULT_CONFIG_PATH, help="配置文件路径"
    )
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--port", type=int, default=8000, help="监听端口")
    parser.add_argument("--debug", action="store_true", help="开启调试模式")
    args = parser.parse_args()

    config_path = str(Path(args.config))
    app = create_app(config_path)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
