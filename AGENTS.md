# AGENTS.md - Project Context

## Quick Start

This is a **Pocket48 message scraper** - a Python tool for fetching member messages from the Pocket48 app and storing them in MySQL.

**⚠️ Important**: This tool requires packet capture (抓包) to obtain authentication tokens. See `docs/抓包分析指南.md` for detailed setup instructions.

## Project Overview

- **Type**: Python CLI tool + Flask web viewer
- **Purpose**: Scrape member messages from Pocket48 rooms and persist to database
- **Language**: Python 3.x
- **Database**: MySQL (primary) or SQLite (fallback)

## Commands

### Installation
```bash
pip install -r requirements.txt
```

### Configuration Setup
```bash
cp config/config.example.json config/config.json
cp config/members.example.json config/members.json
# Edit both files with your credentials and member list
```

### Run Scrapers

**Continuous monitoring mode** (default):
```bash
python src/pocket48_scraper.py -c config/config.json
```

**One-time fetch**:
```bash
python src/pocket48_scraper.py -c config/config.json --once
```

**Fetch with history limit**:
```bash
python src/pocket48_scraper.py -c config/config.json --once --since-days 2 --max-pages 20
```

**View statistics**:
```bash
python src/pocket48_scraper.py -c config/config.json --stats
```

### Export Data
```bash
# Export to JSON
python src/pocket48_scraper.py -c config/config.json --export-format json --output data/messages.json

# Export to CSV
python src/pocket48_scraper.py -c config/config.json --export-format csv --output data/messages.csv
```

### Start Web Viewer
```bash
python src/message_viewer.py -c config/config.json --host 127.0.0.1 --port 8000
# Access at http://127.0.0.1:8000
```

## Architecture

```
48messages/
├── src/
│   ├── pocket48_scraper.py   # Main entry point - scraping logic
│   ├── message_storage.py    # Database abstraction layer
│   └── message_viewer.py     # Flask web UI for viewing messages
├── config/
│   ├── config.example.json   # Configuration template
│   ├── config.json           # Local config (git-ignored)
│   ├── members.example.json  # Member list template
│   └── members.json          # Local member data (git-ignored)
├── data/
│   ├── runtime/              # Token cache (git-ignored)
│   └── logs/                 # Application logs (git-ignored)
├── docs/                     # Documentation (Chinese)
│   ├── 抓包分析指南.md
│   ├── Charles抓包配置指南.md
│   ├── 持久化抓取指南.md
│   └── 数据库建表语句.sql
└── .github/workflows/
    └── deploy.yml            # Auto-deployment to production
```

## Configuration

### Required Files

**`config/config.json`** (from template):
- `pocket48.mobile`: Your phone number
- `pocket48.encryptedPassword`: Captured encrypted password from packet capture
- `pocket48.pa`: Captured `pa` header from packet capture
- `pocket48.appInfo`: Device/app fingerprint from capture
- `storage.*`: Database connection settings

**`config/members.json`** (from template):
Array of member objects with at minimum:
- `id`: Member ID
- `ownerName`: Official member name
- `serverId`: Server ID from API
- `channelId`: Channel/room ID from API

### Database Schema

Run `docs/数据库建表语句.sql` to initialize MySQL tables:
- `members`: Member metadata
- `messages`: Message content
- `message_payloads`: Extended message data
- `crawl_tasks`: Crawl task tracking
- `crawl_checkpoints`: Pagination state
- `crawl_history_checkpoints`: Historical backfill progress

## Deployment

### Production Setup (systemd)

Services managed on server:
- `48messages-scraper`: Continuous scraping service
- `48messages-viewer`: Web viewer service (port 8000)
- `nginx`: Reverse proxy (port 80)

**Common commands**:
```bash
# Check status
systemctl status 48messages-scraper 48messages-viewer nginx

# View logs
journalctl -u 48messages-scraper -f

# Restart services
systemctl restart 48messages-scraper 48messages-viewer

# Verify deployment
curl -I http://127.0.0.1:8000/
curl -I http://127.0.0.1/
```

### GitHub Actions Auto-Deploy

- **Trigger**: Push to `main` branch or manual dispatch
- **Workflow**: `.github/workflows/deploy.yml`
- **Process**: SSH to server, pull code, install deps if requirements.txt changed, restart services, verify viewer count matches database
- **Requirements**: Repository secrets `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY`, `DEPLOY_PORT`

## Conventions & Best Practices

### Code Style
- No formal linter config present - follow PEP 8
- Use type hints where practical
- Handle exceptions gracefully with retry logic

### Git Workflow
- `main` branch is production
- Auto-deploy on push to main
- Test locally before pushing: `python src/pocket48_scraper.py -c config/config.json --once`
- **Commit Message 规范**: 所有提交注释必须使用中文，参照项目历史注释风格（简洁、描述性强，突出变更内容）

### Rate Limiting & Ethics
- Built-in adaptive delays: 0s (first 20 pages) → 0.1s (21-100) → 0.3s (100+)
- Respect server response - stop on auth failures
- **Do not** aggressively scrape - this is for personal research only

## Important Constraints

### Authentication Requirements
This tool **cannot work without packet capture**:
1. Use Charles Proxy or Fiddler to intercept Pocket48 app traffic
2. Extract: `token`, `pa` header, `appInfo`, encrypted password
3. Tokens expire - monitor logs for "saved token rejected" errors
4. Re-capture when token expires (usually after several hours)

### File Handling
- **Never commit**: `config/config.json`, `config/members.json`, `data/runtime/token.json`
- **Log rotation**: 14 days retention in `data/logs/`
- **Export files**: Written to `data/` directory

### Database Notes
- Supports MySQL (recommended for production) and SQLite (local testing)
- Messages are deduplicated by `message_id`
- Only **member messages** are stored (fan messages filtered out)
- History checkpoints track pagination state for resume capability

## Troubleshooting

**Token expired errors**:
```bash
# Clear token cache
rm data/runtime/token.json
# Re-capture fresh token via Charles/Fiddler, update config.json
```

**Database connection issues**:
- Verify MySQL running and credentials correct in `config.json`
- Check `storage.charset` is `utf8mb4` for emoji support

**Viewer not showing data**:
```bash
# Check stats
python src/pocket48_scraper.py -c config/config.json --stats

# Verify database directly
mysql -u root -p 48pocket -e "SELECT COUNT(*) FROM messages;"
```

## Documentation

All detailed guides are in Chinese under `docs/`:
- `抓包分析指南.md` - How to capture packets and extract tokens
- `Charles抓包配置指南.md` - Charles Proxy setup
- `持久化抓取指南.md` - Persistent scraping strategies
- `数据库建表语句.sql` - MySQL schema

---

*⚠️ Disclaimer: For educational/research use only. Respect Pocket48 Terms of Service.*
