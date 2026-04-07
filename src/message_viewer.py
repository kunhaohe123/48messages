import argparse
import html
import json
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Any, Dict, Optional

from flask import Flask, abort, request

from message_storage import create_storage
from pocket48_scraper import DEFAULT_CONFIG_PATH


def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)


def format_timestamp(value: Any) -> str:
    if value in (None, ""):
        return "-"
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
    dt = datetime.fromisoformat(value)
    if len(value) == 10 and is_end:
        dt = dt + timedelta(days=1) - timedelta(milliseconds=1)
    return int(dt.timestamp() * 1000)


def render_layout(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{html.escape(title)}</title>
  <style>
    :root {{ color-scheme: light dark; }}
    body {{ font-family: Arial, sans-serif; margin: 0; background: #10131a; color: #edf2f7; }}
    a {{ color: #7dd3fc; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .wrap {{ max-width: 1320px; margin: 0 auto; padding: 24px; }}
    .header {{ display: flex; justify-content: space-between; gap: 16px; align-items: center; margin-bottom: 20px; flex-wrap: wrap; }}
    .panel {{ background: #171b24; border: 1px solid #2a3140; border-radius: 14px; padding: 18px; margin-bottom: 18px; box-shadow: 0 10px 30px rgba(0,0,0,0.2); }}
    .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; }}
    .stat {{ background: #0f172a; border-radius: 12px; padding: 14px; border: 1px solid #243047; }}
    .stat .label {{ color: #94a3b8; font-size: 12px; margin-bottom: 6px; }}
    .stat .value {{ font-size: 24px; font-weight: 700; }}
    form {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; align-items: end; }}
    label {{ display: block; font-size: 12px; color: #94a3b8; margin-bottom: 6px; }}
    input, select, button {{ width: 100%; box-sizing: border-box; background: #0f172a; color: #edf2f7; border: 1px solid #334155; border-radius: 10px; padding: 10px 12px; }}
    button {{ background: #2563eb; border-color: #2563eb; cursor: pointer; font-weight: 600; }}
    button:hover {{ background: #1d4ed8; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 12px 10px; border-bottom: 1px solid #263041; vertical-align: top; text-align: left; }}
    th {{ color: #94a3b8; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }}
    .mono {{ font-family: Consolas, monospace; word-break: break-all; }}
    .content {{ max-width: 520px; white-space: pre-wrap; word-break: break-word; }}
    .badge {{ display: inline-block; padding: 4px 8px; background: #1e293b; border: 1px solid #334155; border-radius: 999px; font-size: 12px; }}
    .badge-member {{ background: #3b0764; border-color: #7c3aed; color: #f5d0fe; }}
    .badge-fan {{ background: #0f172a; border-color: #334155; }}
    .pager {{ display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .kv {{ background: #0f172a; border: 1px solid #243047; border-radius: 12px; padding: 14px; }}
    .kv .key {{ color: #94a3b8; font-size: 12px; margin-bottom: 6px; }}
    pre {{ background: #0b1120; border: 1px solid #243047; border-radius: 12px; padding: 14px; overflow: auto; white-space: pre-wrap; word-break: break-word; }}
    .toolbar {{ display: flex; gap: 12px; flex-wrap: wrap; }}
    .toolbar a {{ padding: 8px 12px; background: #0f172a; border: 1px solid #334155; border-radius: 999px; }}
    .row-member td {{ background: rgba(124, 58, 237, 0.1); }}
    .row-member:hover td {{ background: rgba(124, 58, 237, 0.16); }}
    @media (max-width: 900px) {{ table, thead, tbody, th, td, tr {{ display: block; }} th {{ display: none; }} td {{ padding: 10px 0; }} tr {{ border-bottom: 1px solid #263041; padding: 12px 0; }} }}
  </style>
</head>
<body>
  <div class=\"wrap\">{body}</div>
</body>
</html>"""


def create_app(config_path: str) -> Flask:
    config = load_config(config_path)
    storage = create_storage(config)
    app = Flask(__name__)

    @app.route("/")
    def index() -> str:
        page = max(request.args.get("page", default=1, type=int), 1)
        page_size = min(
            max(request.args.get("page_size", default=50, type=int), 10), 200
        )
        room_id = (request.args.get("room_id") or "").strip() or None
        sender_keyword = (request.args.get("sender") or "").strip() or None
        keyword = (request.args.get("keyword") or "").strip() or None
        msg_type = (request.args.get("msg_type") or "").strip() or None
        sender_role = (request.args.get("sender_role") or "").strip() or None
        start_time = (request.args.get("start_time") or "").strip()
        end_time = (request.args.get("end_time") or "").strip()
        start_time_ms = parse_datetime_local(start_time) if start_time else None
        end_time_ms = parse_datetime_local(end_time, is_end=True) if end_time else None
        offset = (page - 1) * page_size

        rooms = storage.list_rooms()
        senders = storage.list_senders(room_id)
        stats = storage.get_statistics()

        search_kwargs = {
            "room_id": room_id,
            "sender_keyword": sender_keyword,
            "keyword": keyword,
            "msg_type": msg_type,
            "start_time_ms": start_time_ms,
            "end_time_ms": end_time_ms,
        }
        result = storage.search_messages(
            **search_kwargs,
            sender_role=sender_role,
            limit=page_size,
            offset=offset,
        )
        member_total = storage.search_messages(
            **search_kwargs,
            sender_role="member",
            limit=1,
            offset=0,
        )["total"]
        fan_total = storage.search_messages(
            **search_kwargs,
            sender_role="fan",
            limit=1,
            offset=0,
        )["total"]
        total = result["total"]
        items = result["items"]
        total_pages = max(ceil(total / page_size), 1)

        filters = {
            "room_id": room_id or "",
            "sender": sender_keyword or "",
            "keyword": keyword or "",
            "msg_type": msg_type or "",
            "sender_role": sender_role or "",
            "start_time": start_time,
            "end_time": end_time,
            "page_size": page_size,
        }

        query_without_page = "&".join(
            f"{key}={html.escape(str(value), quote=True)}"
            for key, value in filters.items()
            if value not in ("", None)
        )
        if query_without_page:
            query_without_page = "&" + query_without_page
        member_only_href = f"/?page=1{query_without_page}&sender_role=member"
        fan_only_href = f"/?page=1{query_without_page}&sender_role=fan"

        options = ['<option value="">全部房间</option>']
        for room in rooms:
            selected = " selected" if str(room["id"]) == (room_id or "") else ""
            label = f"{room.get('name') or room['id']} ({room.get('message_count', 0)})"
            options.append(
                f'<option value="{html.escape(str(room["id"]))}"{selected}>{html.escape(label)}</option>'
            )

        message_rows = []
        for item in items:
            content = (
                item.get("content")
                or item.get("flip_answer")
                or item.get("reply_to_text")
                or "-"
            )
            room_name = item.get("room_name") or item.get("room_id")
            is_member = item.get("sender_role") == "member"
            role_label = "成员" if is_member else "粉丝"
            role_class = "badge-member" if is_member else "badge-fan"
            row_class = "row-member" if is_member else ""
            message_rows.append(
                f"""
                <tr class=\"{row_class}\">
                  <td><a class=\"mono\" href=\"/messages/{html.escape(str(item["message_id"]))}\">{html.escape(str(item["message_id"]))}</a></td>
                  <td>{html.escape(str(room_name))}<div class=\"mono\">{html.escape(str(item["room_id"]))}</div></td>
                  <td>{html.escape(str(item.get("username") or "-"))}<div class=\"mono\">{html.escape(str(item.get("user_id") or "-"))}</div><div><span class=\"badge {role_class}\">{role_label}</span></div></td>
                  <td><span class=\"badge\">{html.escape(str(item.get("msg_type") or "-"))}</span></td>
                  <td class=\"content\">{html.escape(str(content))}</td>
                  <td>{html.escape(format_timestamp(item.get("timestamp")))}</td>
                </tr>
                """
            )

        sender_hints = (
            "".join(
                f'<span class="badge">{html.escape(str(sender.get("username") or sender.get("user_id") or "-"))}</span>'
                for sender in senders[:12]
            )
            or '<span class="badge">暂无成员</span>'
        )

        body = f"""
        <div class=\"header\">
          <div>
            <h1>成员消息查看后台</h1>
            <div>直接读取当前项目数据库中的消息记录</div>
          </div>
          <div class=\"toolbar\"><a href=\"/\">刷新</a><a href=\"{member_only_href}\">只看成员本人</a><a href=\"{fan_only_href}\">只看粉丝</a></div>
        </div>

        <div class=\"panel stats\">
          <div class=\"stat\"><div class=\"label\">总消息数</div><div class=\"value\">{stats["total_messages"]}</div></div>
          <div class=\"stat\"><div class=\"label\">房间数</div><div class=\"value\">{stats["total_rooms"]}</div></div>
          <div class=\"stat\"><div class=\"label\">成功抓取次数</div><div class=\"value\">{stats["successful_fetches"]}</div></div>
          <div class=\"stat\"><div class=\"label\">当前结果</div><div class=\"value\">{total}</div></div>
          <div class=\"stat\"><div class=\"label\">成员消息</div><div class=\"value\">{member_total}</div></div>
          <div class=\"stat\"><div class=\"label\">粉丝消息</div><div class=\"value\">{fan_total}</div></div>
        </div>

        <div class=\"panel\">
          <form method=\"get\">
            <div>
              <label for=\"room_id\">房间</label>
              <select id=\"room_id\" name=\"room_id\">{"".join(options)}</select>
            </div>
            <div>
              <label for=\"sender\">成员</label>
              <input id=\"sender\" name=\"sender\" value=\"{html.escape(filters["sender"])}\" placeholder=\"昵称或用户ID\">
            </div>
            <div>
              <label for=\"keyword\">关键词</label>
              <input id=\"keyword\" name=\"keyword\" value=\"{html.escape(filters["keyword"])}\" placeholder=\"消息内容、回复、附加字段\">
            </div>
            <div>
              <label for=\"sender_role\">身份</label>
              <select id=\"sender_role\" name=\"sender_role\">
                <option value=\"\">全部身份</option>
                <option value=\"member\"{" selected" if filters["sender_role"] == "member" else ""}>成员本人</option>
                <option value=\"fan\"{" selected" if filters["sender_role"] == "fan" else ""}>粉丝</option>
              </select>
            </div>
            <div>
              <label for=\"msg_type\">消息类型</label>
              <input id=\"msg_type\" name=\"msg_type\" value=\"{html.escape(filters["msg_type"])}\" placeholder=\"如 TEXT、IMAGE\">
            </div>
            <div>
              <label for=\"start_time\">开始时间</label>
              <input id=\"start_time\" name=\"start_time\" type=\"date\" value=\"{html.escape(filters["start_time"])}\">
            </div>
            <div>
              <label for=\"end_time\">结束时间</label>
              <input id=\"end_time\" name=\"end_time\" type=\"date\" value=\"{html.escape(filters["end_time"])}\">
            </div>
            <div>
              <label for=\"page_size\">每页条数</label>
              <input id=\"page_size\" name=\"page_size\" type=\"number\" min=\"10\" max=\"200\" value=\"{page_size}\">
            </div>
            <div><button type=\"submit\">查询</button></div>
          </form>
          <div style=\"margin-top:12px\">常见成员: {sender_hints}</div>
        </div>

        <div class=\"panel\">
          <table>
            <thead>
              <tr>
                <th>消息ID</th>
                <th>房间</th>
                <th>发送人</th>
                <th>类型</th>
                <th>内容摘要</th>
                <th>时间</th>
              </tr>
            </thead>
            <tbody>
              {"".join(message_rows) or '<tr><td colspan="6">没有匹配到消息</td></tr>'}
            </tbody>
          </table>
        </div>

        <div class=\"panel pager\">
          <span>第 {page} / {total_pages} 页，共 {total} 条</span>
          <a href=\"/?page=1{query_without_page}\">首页</a>
          <a href=\"/?page={max(page - 1, 1)}{query_without_page}\">上一页</a>
          <a href=\"/?page={min(page + 1, total_pages)}{query_without_page}\">下一页</a>
        </div>
        """
        return render_layout("成员消息查看后台", body)

    @app.route("/messages/<message_id>")
    def message_detail(message_id: str) -> str:
        message = storage.get_message_detail(message_id)
        if not message:
            abort(404)

        body = f"""
        <div class=\"header\">
          <div>
            <h1>消息详情</h1>
            <div class=\"mono\">{html.escape(str(message_id))}</div>
          </div>
          <div class=\"toolbar\"><a href=\"/\">返回列表</a></div>
        </div>

        <div class=\"grid\">
          <div class=\"kv\"><div class=\"key\">房间</div><div>{html.escape(str(message.get("room_name") or message.get("room_id") or "-"))}</div></div>
          <div class=\"kv\"><div class=\"key\">发送人</div><div>{html.escape(str(message.get("username") or "-"))}</div><div class=\"mono\">{html.escape(str(message.get("user_id") or "-"))}</div></div>
          <div class=\"kv\"><div class=\"key\">身份</div><div>{"成员本人" if message.get("sender_role") == "member" else "粉丝"}</div></div>
          <div class=\"kv\"><div class=\"key\">消息类型</div><div>{html.escape(str(message.get("msg_type") or "-"))}</div></div>
          <div class=\"kv\"><div class=\"key\">消息时间</div><div>{html.escape(format_timestamp(message.get("timestamp")))}</div></div>
        </div>

        <div class=\"panel\">
          <h2>文本内容</h2>
          <pre>{html.escape(str(message.get("content") or "-"))}</pre>
        </div>

        <div class=\"panel\">
          <h2>扩展字段</h2>
          <pre>{html.escape(pretty_json(message.get("ext_info")))}</pre>
        </div>

        <div class=\"panel\">
          <h2>原始摘要</h2>
          <pre>{html.escape(pretty_json(message.get("raw_brief")))}</pre>
        </div>
        """
        return render_layout("消息详情", body)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="口袋48消息查看后台")
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
