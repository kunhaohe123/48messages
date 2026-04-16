import argparse
import html
import json
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from flask import Flask, abort, jsonify, request

from message_storage import create_storage
from pocket48_scraper import DEFAULT_CONFIG_PATH, load_config


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


def build_query_string(params: Dict[str, Any]) -> str:
    pairs = {
        key: str(value) for key, value in params.items() if value not in ("", None)
    }
    return f"&{urlencode(pairs)}" if pairs else ""


def render_layout(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/themes/dark.css">
  <style>
    :root {{ color-scheme: light dark; }}
    * {{ box-sizing: border-box; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; margin: 0; background: #10131a; color: #edf2f7; line-height: 1.6; }}
    a {{ color: #7dd3fc; text-decoration: none; transition: all 0.2s ease; }}
    a:hover {{ text-decoration: underline; color: #38bdf8; }}
    .wrap {{ max-width: 1320px; margin: 0 auto; padding: 24px; }}
    .header {{ display: flex; justify-content: space-between; gap: 16px; align-items: center; margin-bottom: 20px; flex-wrap: wrap; }}
    .header h1 {{ margin: 0 0 4px 0; font-size: 24px; font-weight: 800; letter-spacing: -0.02em; }}
    .header div {{ color: #94a3b8; font-size: 14px; }}
    .panel {{ background: #171b24; border: 1px solid #334155; border-radius: 16px; padding: 20px; margin-bottom: 18px; box-shadow: 0 10px 30px rgba(0,0,0,0.2); transition: border-color 0.2s ease, box-shadow 0.2s ease, transform 0.2s ease; }}
    .panel:hover {{ border-color: #475569; box-shadow: 0 12px 32px rgba(15,23,42,0.32); }}
    .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; }}
    .stat {{ background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); border-radius: 16px; padding: 20px; border: 1px solid #334155; transition: border-color 0.2s ease, box-shadow 0.2s ease, transform 0.2s ease; display: flex; align-items: center; gap: 16px; position: relative; overflow: hidden; }}
    .stat::before {{ content: ''; position: absolute; top: 0; right: 0; width: 100px; height: 100px; background: radial-gradient(circle, rgba(59, 130, 246, 0.1) 0%, transparent 70%); }}
    .stat:hover {{ transform: translateY(-2px); border-color: #475569; box-shadow: 0 12px 32px rgba(15,23,42,0.32); }}
    .stat-icon {{ font-size: 32px; opacity: 0.9; }}
    .stat-content {{ flex: 1; }}
    .stat-value {{ font-size: 28px; font-weight: 800; color: #f1f5f9; margin-bottom: 4px; letter-spacing: -0.02em; }}
    .stat-value.time {{ font-size: 16px; font-weight: 700; font-family: Consolas, Monaco, 'Courier New', monospace; }}
    .stat-label {{ color: #94a3b8; font-size: 13px; font-weight: 500; }}
    form {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; align-items: end; }}
    label {{ display: block; font-size: 12px; color: #94a3b8; margin-bottom: 8px; }}
    input, select, button {{ width: 100%; box-sizing: border-box; background: #0f172a; color: #edf2f7; border: 1px solid #334155; border-radius: 14px; padding: 10px 12px; min-height: 44px; font-size: 14px; line-height: 1.4; transition: all 0.2s ease; }}
    select {{
      appearance: none;
      -webkit-appearance: none;
      -moz-appearance: none;
      min-height: 44px;
      padding-right: 42px;
      padding-left: 12px;
      font-size: 13px;
      line-height: 1.2;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 14 14' fill='none'%3E%3Cpath d='M2.5 5.25L7 9.75L11.5 5.25' stroke='%23e2e8f0' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 14px center;
      background-size: 14px 14px;
    }}
    select::-ms-expand {{ display: none; }}
    input::placeholder {{ color: #64748b; }}
    input[type="date"]::-webkit-calendar-picker-indicator {{ display: none; }}
    input:focus, select:focus {{ outline: none; -webkit-appearance: none; appearance: none; border-color: #2563eb; box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.16); }}
    button {{ min-height: 44px; background: #2563eb; border-color: #2563eb; cursor: pointer; font-weight: 600; }}
    button:hover {{ background: #1d4ed8; transform: translateY(-1px); }}
    button:active {{ transform: translateY(0); }}
    table {{ width: 100%; border-collapse: separate; border-spacing: 0; table-layout: fixed; font-size: 14px; overflow: hidden; border: 1px solid #334155; border-radius: 14px; background: #0f172a; }}
    th, td {{ padding: 12px 10px; border-bottom: 1px solid #263041; vertical-align: middle; text-align: left; }}
    th {{ color: #94a3b8; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; font-weight: 700; background: rgba(15, 23, 42, 0.9); }}
    tbody tr:last-child td {{ border-bottom: none; }}
    tr {{ transition: background-color 0.2s ease, box-shadow 0.2s ease; }}
    tr:hover {{ background: rgba(30, 41, 59, 0.42); box-shadow: inset 0 0 0 1px rgba(71, 85, 105, 0.35); }}
    .mono {{ font-family: Consolas, Monaco, 'Courier New', monospace; word-break: break-all; }}
    .cell-room {{ width: 180px; }}
    .cell-sender {{ width: 180px; }}
    .cell-type {{ width: 110px; }}
    .cell-time {{ width: 220px; white-space: nowrap; }}
    .content {{ width: 100%; white-space: pre-wrap; word-break: break-word; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; position: relative; line-height: 1.5; }}
    .content-link {{ display: block; color: inherit; text-decoration: none; }}
    .content-link:hover {{ text-decoration: underline; color: #38bdf8; }}
    .badge {{ display: inline-block; padding: 4px 8px; background: #1e293b; border: 1px solid #334155; border-radius: 999px; font-size: 12px; font-weight: 500; }}
    .badge-member {{ background: #3b0764; border-color: #7c3aed; color: #f5d0fe; }}
    .badge-TEXT {{ background: #0f172a; border-color: #334155; }}
    .badge-IMAGE {{ background: #0c4a6e; border-color: #0ea5e9; color: #bae6fd; }}
    .badge-VOICE {{ background: #4338ca; border-color: #818cf8; color: #ddd6fe; }}
    .badge-VIDEO {{ background: #7c1d1d; border-color: #f87171; color: #fee2e2; }}
    .badge-REPLY {{ background: #14532d; border-color: #4ade80; color: #bbf7d0; }}
    .badge-FLIP {{ background: #78350f; border-color: #fbbf24; color: #fef3c7; }}
    .pager {{ display: flex; gap: 12px; flex-wrap: wrap; align-items: center; padding: 12px 0; }}
    .pager a {{ padding: 6px 12px; background: #0f172a; border: 1px solid #334155; border-radius: 8px; font-size: 14px; transition: all 0.2s ease; }}
    .pager a:hover {{ background: #1e293b; border-color: #475569; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .kv {{ background: #0f172a; border: 1px solid #334155; border-radius: 14px; padding: 14px; transition: border-color 0.2s ease, box-shadow 0.2s ease, transform 0.2s ease; }}
    .kv:hover {{ border-color: #475569; box-shadow: 0 10px 24px rgba(15,23,42,0.24); transform: translateY(-1px); }}
    .kv .key {{ color: #94a3b8; font-size: 12px; margin-bottom: 6px; }}
    pre {{ background: #0b1120; border: 1px solid #334155; border-radius: 14px; padding: 14px; overflow: auto; white-space: pre-wrap; word-break: break-word; font-family: Consolas, Monaco, 'Courier New', monospace; font-size: 13px; line-height: 1.5; }}
    h2 {{ margin: 0 0 12px 0; font-size: 16px; font-weight: 700; color: #e2e8f0; letter-spacing: -0.01em; }}
    .toolbar {{ display: flex; gap: 12px; flex-wrap: wrap; }}
    .toolbar a {{ padding: 8px 12px; background: #0f172a; border: 1px solid #334155; border-radius: 999px; font-size: 14px; transition: all 0.2s ease; }}
    .toolbar a:hover {{ background: #1e293b; border-color: #475569; transform: translateY(-1px); }}
    @media (max-width: 900px) {{
      .wrap {{ padding: 16px; }}
      .panel {{ padding: 16px; border-radius: 14px; }}
      .header {{ flex-direction: column; align-items: flex-start; gap: 12px; }}
      .stats {{ grid-template-columns: repeat(2, 1fr); }}
      form {{ grid-template-columns: 1fr; }}
      input, select, button {{ padding-left: 14px; padding-right: 14px; }}
      select {{ padding-right: 42px; background-position: right 14px center; }}
      .flatpickr-input, .flatpickr-input:active {{ padding-left: 14px; padding-right: 40px; }}
      .date-input-wrap .flatpickr-clear {{ right: 14px; }}
      table, thead, tbody, th, td, tr {{ display: block; }}
      th {{ display: none; }}
       td {{ padding: 10px 0; border-bottom: 1px dashed #263041; }}
       td:first-child {{ padding-top: 16px; }}
       td:last-child {{ padding-bottom: 16px; }}
       tr {{ border-bottom: none; padding: 0; margin-bottom: 16px; border-radius: 8px; background: #0f172a; }}
       tr:hover {{ background: #151b28; }}
       td:before {{ content: attr(data-label); display: block; color: #64748b; font-size: 11px; text-transform: uppercase; margin-bottom: 4px; }}
       .cell-room, .cell-sender, .cell-type, .cell-time {{ width: auto; white-space: normal; }}
       .pager {{ flex-direction: column; align-items: stretch; gap: 8px; }}
       .pager a {{ text-align: center; }}
    }}
    .flatpickr-calendar {{
      background: #1e293b;
      border: 1px solid #334155;
      width: 272px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.5);
    }}
    .flatpickr-months {{
      padding-top: 2px;
    }}
    .flatpickr-prev-month,
    .flatpickr-next-month {{
      padding: 8px 10px;
    }}
    .flatpickr-prev-month svg,
    .flatpickr-next-month svg {{
      width: 12px;
      height: 12px;
    }}
    .flatpickr-day {{
      color: #edf2f7;
      height: 34px;
      line-height: 34px;
      max-width: 34px;
      border-radius: 8px;
    }}
    .flatpickr-day:hover {{
      background: #334155;
      border-color: #334155;
    }}
    .flatpickr-day.selected {{
      background: #2563eb;
      border-color: #2563eb;
    }}
    .flatpickr-months .flatpickr-month,
    .flatpickr-current-month .flatpickr-monthDropdown-months,
    .flatpickr-current-month input.cur-year {{
      color: #edf2f7;
    }}
    .flatpickr-current-month {{
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 4px;
      padding-top: 2px;
      height: 32px;
    }}
    .flatpickr-current-month .flatpickr-monthDropdown-months,
    .flatpickr-current-month input.cur-year {{
      min-height: auto;
      height: auto;
      padding: 0;
      border: none;
      border-radius: 0;
      background: transparent;
      box-shadow: none;
      line-height: 1.2;
    }}
    .flatpickr-current-month .flatpickr-monthDropdown-months {{
      width: auto;
      padding-right: 18px;
      font-size: 20px;
      font-weight: 400;
      background-position: right 2px center;
      background-size: 12px 12px;
    }}
    .flatpickr-current-month input.cur-year {{
      width: 4ch;
      font-size: 20px;
      font-weight: 300;
      vertical-align: baseline;
    }}
    .flatpickr-current-month .numInputWrapper {{
      width: auto;
    }}
    .flatpickr-weekday {{
      color: #94a3b8;
      font-size: 12px;
      font-weight: 600;
      line-height: 28px;
    }}
    .flatpickr-day.prevMonthDay,
    .flatpickr-day.nextMonthDay {{
      color: #64748b;
    }}
    .flatpickr-time input {{ color: #edf2f7; }}
    .flatpickr-input, .flatpickr-input:active {{
      background: #0f172a;
      color: #edf2f7;
      border: 1px solid #334155;
      border-radius: 14px;
      padding: 10px 12px;
      font-size: 14px;
      min-height: 44px;
      line-height: 1.2;
      width: 100%;
      box-sizing: border-box;
      appearance: none;
      -webkit-appearance: none;
    }}
    .flatpickr-input:focus {{
      outline: none;
      border-color: #2563eb;
      box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.16);
    }}
    .flatpickr-input::placeholder {{ color: #64748b; }}
    .date-input-wrap {{ position: relative; }}
    .date-input-wrap input,
    .date-input-wrap .flatpickr-input {{ padding-right: 40px; }}
    .date-input-wrap .flatpickr-clear {{
      position: absolute;
      right: 12px;
      top: 50%;
      transform: translateY(-50%);
      background: transparent;
      border: none;
      color: #64748b;
      cursor: pointer;
      font-size: 16px;
      line-height: 1;
      padding: 0;
      width: 20px;
      height: 20px;
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 3;
    }}
  </style>
</head>
<body>
  <div class="wrap">{body}</div>
</body>
  <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
  <script src="https://cdn.jsdelivr.net/npm/flatpickr/dist/l10n/zh.js"></script>
  <script>
    function loadSummaryStats() {{
      fetch('/stats-summary')
        .then(function(r) {{ return r.json(); }})
        .then(function(data) {{
          var totalEl = document.getElementById('stats-total-messages');
          if (totalEl) totalEl.textContent = data.total_messages;
          var roomsEl = document.getElementById('stats-total-rooms');
          if (roomsEl) roomsEl.textContent = data.total_rooms;
          var todayEl = document.getElementById('stats-today-messages');
          if (todayEl) todayEl.textContent = data.today_messages;
          var topCountEl = document.getElementById('stats-top-member-count');
          if (topCountEl) topCountEl.textContent = data.top_member_count;
          var topNameEl = document.getElementById('stats-top-member-name');
          if (topNameEl) topNameEl.textContent = '今日话痨：' + (data.top_member_name || '--');
        }})
        .catch(function() {{}});
    }}

    document.addEventListener('DOMContentLoaded', function() {{
      loadSummaryStats();

      // CDN 不可用时退回原生 date input，避免页面脚本直接报错。
      if (typeof flatpickr !== 'function') {{
        return;
      }}

      const startInput = document.querySelector('#start_time');
      const endInput = document.querySelector('#end_time');
      if (!startInput || !endInput) {{
        return;
      }}

      const startFp = flatpickr(startInput, {{
        locale: 'zh',
        dateFormat: 'Y-m-d',
        allowInput: true,
        altInput: true,
        altFormat: 'Y-m-d'
      }});

      const endFp = flatpickr(endInput, {{
        locale: 'zh',
        dateFormat: 'Y-m-d',
        allowInput: true,
        altInput: true,
        altFormat: 'Y-m-d'
      }});

      const startWrapper = startInput.closest('.date-input-wrap');
      const endWrapper = endInput.closest('.date-input-wrap');
      if (!startWrapper || !endWrapper) {{
        return;
      }}

      const addClearButton = (wrapper, fp, inputId) => {{
        // 找到实际的输入框元素（Flatpickr 创建的）
        const originalInput = document.getElementById(inputId);
        const flatpickrInput = wrapper.querySelector('input.flatpickr-input:not([type="hidden"])');
        const targetInput = flatpickrInput || originalInput;
        if (!targetInput) {{
          return;
        }}

        const clearBtn = document.createElement('button');
        clearBtn.type = 'button';
        clearBtn.className = 'flatpickr-clear';
        clearBtn.innerHTML = '✕';
        clearBtn.title = '清空';

        wrapper.appendChild(clearBtn);

        const syncClearButton = () => {{
          const hasValue = fp.selectedDates.length > 0 || Boolean(targetInput.value);
          clearBtn.style.display = hasValue ? 'flex' : 'none';
        }};

        fp.config.onChange.push(syncClearButton);
        fp.config.onValueUpdate.push(syncClearButton);

        // 这里是在 flatpickr 初始化完成后追加按钮，直接同步一次可避免查询后刷新页面时按钮丢失。
        syncClearButton();

        clearBtn.addEventListener('click', (e) => {{
          e.preventDefault();
          e.stopPropagation();
          fp.clear();
          syncClearButton();
        }});

        clearBtn.addEventListener('mouseenter', () => {{
          clearBtn.style.color = '#edf2f7';
        }});
        clearBtn.addEventListener('mouseleave', () => {{
          clearBtn.style.color = '#64748b';
        }});
      }};

      addClearButton(startWrapper, startFp, 'start_time');
      addClearButton(endWrapper, endFp, 'end_time');
    }});
  </script>
</html>"""


def create_app(config_path: str) -> Flask:
    config = load_config(config_path)
    storage = create_storage(config)
    app = Flask(__name__)

    _summary_cache: Dict[str, Any] = {"value": None, "expires_at": 0}
    _rooms_cache: Dict[str, Any] = {"value": None, "expires_at": 0}
    _CACHE_TTL_SECONDS = 30

    def _get_cached_summary_stats() -> Dict[str, Any]:
        now = datetime.now().timestamp()
        if _summary_cache["value"] is not None and _summary_cache["expires_at"] > now:
            return _summary_cache["value"]
        value = build_summary_stats()
        _summary_cache["value"] = value
        _summary_cache["expires_at"] = now + _CACHE_TTL_SECONDS
        return value

    def _get_cached_rooms() -> list:
        now = datetime.now().timestamp()
        if _rooms_cache["value"] is not None and _rooms_cache["expires_at"] > now:
            return _rooms_cache["value"]
        value = storage.list_rooms()
        _rooms_cache["value"] = value
        _rooms_cache["expires_at"] = now + _CACHE_TTL_SECONDS
        return value

    def build_summary_stats() -> Dict[str, Any]:
        today_start = int(
            datetime.now()
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp()
            * 1000
        )
        text_stats = storage.search_messages(
            sender_role="member",
            msg_type="TEXT",
            limit=0,
            offset=0,
        )
        rooms = storage.list_rooms()
        today_stats = storage.search_messages(
            sender_role="member",
            msg_type="TEXT",
            start_time_ms=today_start,
            limit=0,
            offset=0,
        )
        top_member_today = storage.get_top_member_for_day(today_start)
        return {
            "total_messages": text_stats["total"],
            "total_rooms": len(rooms),
            "today_messages": today_stats["total"],
            "top_member_name": top_member_today.get("member_name")
            if top_member_today
            else "-",
            "top_member_count": top_member_today.get("message_count")
            if top_member_today
            else 0,
        }

    @app.route("/stats-summary")
    def stats_summary() -> Any:
        return jsonify(_get_cached_summary_stats())

    @app.route("/")
    def index() -> str:
        page = max(request.args.get("page", default=1, type=int), 1)
        page_size = min(
            max(request.args.get("page_size", default=50, type=int), 10), 200
        )
        room_id = (request.args.get("room_id") or "").strip() or None
        sender_keyword = (request.args.get("sender") or "").strip() or None
        keyword = (request.args.get("keyword") or "").strip() or None
        start_time = (request.args.get("start_time") or "").strip()
        end_time = (request.args.get("end_time") or "").strip()
        start_time_ms = parse_datetime_local(start_time) if start_time else None
        end_time_ms = parse_datetime_local(end_time, is_end=True) if end_time else None
        offset = (page - 1) * page_size

        rooms = _get_cached_rooms()

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

        query_without_page = build_query_string(filters)

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
            msg_id = html.escape(
                str(
                    item.get("message_id")
                    or item.get("msg_id")
                    or item.get("id")
                    or "-"
                )
            )
            message_rows.append(
                f"""
                <tr>
                  <td data-label="房间" class="cell-room">{html.escape(str(room_name))}</td>
                  <td data-label="内容"><a class="content content-link" href="/messages/{msg_id}" title="{html.escape(str(content))}">{html.escape(str(content))}</a></td>
                  <td data-label="时间" class="cell-time">{html.escape(format_timestamp(item.get("timestamp")))}</td>
                </tr>
                """
            )

        body = f"""
        <div class="header">
          <div>
            <h1>口袋房间消息查看</h1>
          </div>
          <div class="toolbar"><a href="/">刷新</a></div>
        </div>

        <div class="panel stats">
          <div class="stat">
            <div class="stat-icon">💬</div>
            <div class="stat-content">
              <div class="stat-value" id="stats-total-messages">--</div>
              <div class="stat-label">总消息数</div>
            </div>
          </div>
          <div class="stat">
            <div class="stat-icon">🏠</div>
            <div class="stat-content">
              <div class="stat-value" id="stats-total-rooms">--</div>
              <div class="stat-label">房间数</div>
            </div>
          </div>
          <div class="stat">
            <div class="stat-icon">📅</div>
            <div class="stat-content">
              <div class="stat-value" id="stats-today-messages">--</div>
              <div class="stat-label">今日消息数</div>
            </div>
          </div>
          <div class="stat">
            <div class="stat-icon">⏰</div>
            <div class="stat-content">
              <div class="stat-value" id="stats-top-member-count">--</div>
              <div class="stat-label" id="stats-top-member-name">今日话痨：--</div>
            </div>
          </div>
        </div>

        <div class="panel">
          <form method="get">
            <div>
              <label for="room_id">房间</label>
              <select id="room_id" name="room_id">{"".join(options)}</select>
            </div>
            <div>
              <label for="sender">名字模糊搜</label>
              <input id="sender" name="sender" value="{html.escape(filters["sender"])}" placeholder="成员名或发送昵称">
            </div>
            <div>
              <label for="keyword">关键词</label>
              <input id="keyword" name="keyword" value="{html.escape(filters["keyword"])}" placeholder="消息内容、回复、附加字段">
            </div>
            <div>
              <label for="start_time">开始时间</label>
              <div class="date-input-wrap">
                <input id="start_time" name="start_time" type="date" value="{html.escape(start_time)}" placeholder="YYYY-MM-DD">
              </div>
            </div>
            <div>
              <label for="end_time">结束时间</label>
              <div class="date-input-wrap">
                <input id="end_time" name="end_time" type="date" value="{html.escape(end_time)}" placeholder="YYYY-MM-DD">
              </div>
            </div>
            <div>
              <label for="page_size">每页条数</label>
              <input id="page_size" name="page_size" type="number" min="10" max="200" value="{page_size}">
            </div>
            <div><button type="submit">查询</button></div>
          </form>
        </div>

        <div class="panel">
          <table>
            <thead>
              <tr>
                <th>房间</th>
                <th>内容摘要</th>
                <th>时间</th>
              </tr>
            </thead>
            <tbody>
              {"".join(message_rows) or '<tr><td colspan="3">没有匹配到消息</td></tr>'}
            </tbody>
          </table>
        </div>

        <div class="panel pager">
          <span>第 {page} / {total_pages} 页，共 {total} 条</span>
          <a href="/?page=1{query_without_page}">首页</a>
          <a href="/?page={max(page - 1, 1)}{query_without_page}">上一页</a>
          <a href="/?page={min(page + 1, total_pages)}{query_without_page}">下一页</a>
          <form method="get" style="display:inline-flex;gap:8px;align-items:center;">
            {"".join(f'<input type="hidden" name="{k}" value="{html.escape(str(v))}">' for k, v in filters.items() if v)}
            <label style="margin:0;font-size:12px;display:flex;align-items:center;gap:4px;">跳至<input type="number" name="page" min="1" max="{total_pages}" value="{page}" style="width:60px;padding:4px 8px;"></label>
            <button type="submit" style="padding:4px 12px;">跳转</button>
          </form>
        </div>
        """
        return render_layout("口袋房间消息查看", body)

    @app.route("/messages/<message_id>")
    def message_detail(message_id: str) -> str:
        message = storage.get_message_detail(message_id)
        if not message:
            abort(404)

        body = f"""
        <div class="header">
          <div>
            <h1>消息详情</h1>
            <div class="mono">{html.escape(str(message_id))}</div>
          </div>
          <div class="toolbar"><a href="/">返回列表</a></div>
        </div>

        <div class="grid">
          <div class="kv"><div class="key">房间</div><div>{html.escape(str(message.get("room_name") or message.get("room_id") or "-"))}</div></div>
          <div class="kv"><div class="key">发送人</div><div>{html.escape(str(message.get("username") or "-"))}</div><div class="mono">{html.escape(str(message.get("user_id") or "-"))}</div></div>
          <div class="kv"><div class="key">身份</div><div>成员本人</div></div>
          <div class="kv"><div class="key">消息类型</div><div>{html.escape(str(message.get("msg_type") or "-"))}</div></div>
          <div class="kv"><div class="key">消息时间</div><div>{html.escape(format_timestamp(message.get("timestamp")))}</div></div>
        </div>

        <div class="panel">
          <h2>文本内容</h2>
          <pre>{html.escape(str(message.get("content") or "-"))}</pre>
        </div>

        <div class="panel">
          <h2>扩展字段</h2>
          <pre>{html.escape(pretty_json(message.get("ext_info")))}</pre>
        </div>

        <div class="panel">
          <h2>原始摘要</h2>
          <pre>{html.escape(pretty_json(message.get("raw_brief")))}</pre>
        </div>
        """
        return render_layout("消息详情", body)

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
