# 口袋48成员本人消息抓取工具

用于抓取口袋48成员聚聚房间里的成员本人消息，并写入 MySQL。

## 项目结构

```
48messages/
├── config/
│   ├── config.example.json  # 配置文件模板
│   └── config.json          # 本地配置（忽略提交）
├── data/
│   ├── runtime/
│   │   └── token.json       # 本地 token 缓存（忽略提交）
│   └── messages_export.json # 导出数据（忽略提交）
├── docs/
│   ├── 数据库建表语句.sql
│   ├── 抓包分析指南.md
│   ├── 持久化抓取指南.md
│   └── Charles抓包配置指南.md
├── src/
│   ├── pocket48_scraper.py  # 统一主程序
│   ├── message_storage.py
│   └── message_viewer.py
└── requirements.txt         # Python 依赖
```

## 使用步骤

### 1. 抓包分析（最重要）

详细步骤请查看 [docs/抓包分析指南.md](docs/抓包分析指南.md)

主要步骤：
1. 使用Charles/Fiddler抓包工具
2. 在手机或模拟器上登录口袋48
3. 进入成员房间，捕获消息列表接口
4. 记录 `token`、`pa`、`appInfo` 和响应格式

### 2. 配置项目

```bash
cp config/config.example.json config/config.json
cp config/members.example.json config/members.json
```

编辑 `config/config.json`，填写账号、接口和存储配置：

```json
{
  "pocket48": {
    "mobile": "手机号",
    "encryptedPassword": "抓包得到的加密密码",
    "deviceToken": "",
    "token": "可选，已登录时可直接填写",
    "userAgent": "PocketFans201807/7.1.35 (iPad; iOS 26.3.1; Scale/2.00)",
    "appInfo": {
      "vendor": "apple",
      "deviceId": "你的设备ID",
      "appVersion": "7.1.35",
      "appBuild": "25101021",
      "osVersion": "26.3.1",
      "osType": "ios",
      "deviceName": "iPad16,2",
      "os": "ios"
    },
    "pa": "抓包得到的pa请求头",
    "pSignType": "V0"
  },
  "storage": {
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "database": "48pocket",
    "user": "root",
    "password": "",
    "charset": "utf8mb4",
    "token_file": "data/runtime/token.json"
  }
}
```

编辑 `config/members.json`，单独维护成员列表：

```json
[
  {
    "id": 417331,
    "ownerName": "成员官方名",
    "pinyin": "ChengYuanMingZi",
    "nickname": "成员昵称",
    "birthday": "1996/12/17",
    "birthplace": "四川 成都",
    "constellation": "射手座",
    "height": 170,
    "bloodType": "-",
    "hobbies": "可选爱好",
    "specialty": "可选特长",
    "groupId": 12,
    "groupName": "GNZ48",
    "teamId": 1203,
    "team": "TEAM Z",
    "periodId": 1201,
    "periodName": "GNZ48 一期生",
    "class": "8",
    "jtime": "2016/10/23",
    "ptime": "2016/10/23",
    "rank": "38",
    "account": "可选账号标识",
    "roomId": "67342057",
    "liveRoomId": 5082313,
    "serverId": 951577,
    "channelId": 1312655,
    "wbUid": "可选微博UID",
    "wbName": "可选微博名",
    "avatar": "https://example.com/avatar.jpg",
    "fullPhoto1": "https://example.com/photo1.jpg",
    "fullPhoto2": "https://example.com/photo2.jpg",
    "fullPhoto3": "https://example.com/photo3.jpg",
    "fullPhoto4": "https://example.com/photo4.jpg",
    "status": 1,
    "ctime": 1553661473202,
    "utime": 1757659612547,
    "isInGroup": true,
    "note": "可选备注"
  }
]
```

程序启动后会先同步 `config/members.json` 中的成员资料，再写入消息数据。
为保证消息能够正常关联成员，`config/members.json` 至少必须提供：

- `id`
- `ownerName`
- `serverId`
- `channelId`

补充 `roomId` / `liveRoomId` / `team` / `avatar` / `wbUid` / `fullPhoto1` 等扩展字段后，后续更容易做成员资料、房间相册、直播录播、分组筛选等功能。

补充说明：

- 配置里的 `rank` 会写入数据库字段 `members.election_rank`
- 其他扩展字段会原样保留，方便后续直接复用

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 填写关键配置

根据抓包结果，填充 `config/config.json` 中的以下关键字段：

1. `encryptedPassword` - 登录接口中的 `loginMobile.pwd`
2. `pa` - 请求头中的 `pa`
3. `appInfo` - 请求头中的 `appInfo`
成员的 `id` / `ownerName` / `channelId` / `serverId` 请填写到 `config/members.json`。

### 5. 运行程序

`src/pocket48_scraper.py` 现在是统一入口，抓取、导出、统计都从这里执行。

持续抓取模式下，`config/config.json` 里的 `monitor.max_pages` 用来限制每个房间单轮最多翻多少页，默认示例值为 `5`。
单次抓取模式下，如果你传了 `--since-days` 但没有显式传 `--max-pages`，程序会自动使用 `monitor.since_days_max_pages` 作为保护值；默认示例值为 `20`，避免高活跃房间翻页过深。

```bash
python src/pocket48_scraper.py -c config/config.json
```

单次抓取并退出：

```bash
python src/pocket48_scraper.py -c config/config.json --once
```

只抓最近 2 天，并限制最多翻 20 页：

```bash
python src/pocket48_scraper.py -c config/config.json --once --since-days 2 --max-pages 20
```

查看统计：

```bash
python src/pocket48_scraper.py -c config/config.json --stats
```

### 抓取策略说明

- MySQL 模式下，程序启动时会先把 `config/members.json` 同步到 `members` 表
- 持续监控和 `--once` 都会先请求最新一页消息
- 抓取时只保留成员本人发送的消息，粉丝消息会在入库前过滤掉
- 如果本地最新消息还没有追上接口返回的数据边界，程序会继续使用返回的 `nextTime` 向历史翻页
- 分页会持续到命中本地已保存的最新消息，或接口没有更多历史页为止
- 如果某个房间本地还没有历史数据，且你没有显式传入 `--since-days`，脚本默认只回溯最近 30 天
- 如果你希望手动限制范围，可以传 `--since-days`，例如 `--since-days 2` 表示只抓最近 2 天
- 如果你希望限制单次执行的翻页深度，可以传 `--max-pages`，例如 `--max-pages 20` 表示最多翻 20 页
- 如果你要补更早历史，可以显式提高 `--max-pages`
- 持续抓取模式不会读取命令行里的 `--max-pages`，而是读取配置文件中的 `monitor.max_pages`；这个值越小越省资源，但在高活跃房间里越可能需要多轮才能追平
- 这比只抓单页更适合持久化增量抓取，但是否绝对不漏仍取决于服务端分页与接口稳定性

### 6. 导出已抓取消息

导出为 JSON：

```bash
python src/pocket48_scraper.py -c config/config.json --export-format json --output data/messages.json
```

导出为 CSV：

```bash
python src/pocket48_scraper.py -c config/config.json --export-format csv --output data/messages.csv
```

只导出单个房间最近 20 条：

```bash
python src/pocket48_scraper.py -c config/config.json --export-format json --output data/latest.json --room-id 1312655 --limit 20
```

### 7. 启动消息查看后台

项目已经提供一个轻量 Web 页面，用来查看数据库里已抓取的成员本人消息。

```bash
python src/message_viewer.py -c config/config.json --host 127.0.0.1 --port 8000
```

打开 `http://127.0.0.1:8000` 后可以：

- 按房间筛选成员本人消息
- 按成员昵称 / 成员姓名搜索成员本人消息
- 按关键词搜索成员本人消息内容和扩展字段
- 查看单条消息详情

房间基础信息直接来自 `members` 和 `messages`。

注意：这个页面直接读取当前配置对应的数据库，请自行做好访问控制，不要直接暴露到公网。

### 8. 服务器部署与维护

当前推荐的线上部署结构：

- `48messages-scraper`：持续抓取服务，systemd 开机自启
- `48messages-viewer`：消息查看页面，systemd 开机自启
- `nginx`：对外提供 `80` 端口并反代到 `127.0.0.1:8000`
- Python 虚拟环境：`/opt/48messages-venv`

常用维护命令：

```bash
# 查看服务状态
systemctl status 48messages-scraper
systemctl status 48messages-viewer
systemctl status nginx

# 查看实时日志
journalctl -u 48messages-scraper -f
journalctl -u 48messages-viewer -f

# 重启服务
systemctl restart 48messages-scraper
systemctl restart 48messages-viewer
systemctl restart nginx

# 停止服务
systemctl stop 48messages-scraper
systemctl stop 48messages-viewer

# 启动服务
systemctl start 48messages-scraper
systemctl start 48messages-viewer

# 检查 nginx 配置并重载
nginx -t
systemctl reload nginx
```

常用排查命令：

```bash
# 查看 80 和 8000 端口监听
ss -lntp | grep -E ':80|:8000'

# 测试本机 viewer 和 nginx 是否正常
curl -I http://127.0.0.1:8000/
curl -I http://127.0.0.1/

# 查看抓取统计
cd /opt/48messages
/opt/48messages-venv/bin/python src/pocket48_scraper.py -c config/config.json --stats

# 更新服务器依赖
/opt/48messages-venv/bin/pip install -r /opt/48messages/requirements.txt
```

HTTPS 说明：

- 如果要配置浏览器信任的 HTTPS，建议先准备一个域名，并把域名解析到服务器公网 IP
- 直接对公网 IP 配正式 HTTPS 证书通常不可行，最多只能使用自签名证书
- 域名准备好后，可再接入 Let's Encrypt + Nginx

自动部署说明：

- 仓库已配置 GitHub Actions 工作流 `Deploy`
- 当 `main` 分支收到新的 push 时，会自动通过 SSH 登录服务器并执行部署
- 部署脚本会自动拉取最新代码、按需安装依赖、重启抓取和查看服务，并检查本机访问是否正常
- 如果只是普通代码改动、`requirements.txt` 没变，部署时会跳过 `pip install`
- 如果需要手动部署，也可以在 GitHub 仓库的 `Actions -> Deploy` 页面点击 `Run workflow`

## 重要提示

⚠️ **仅供学习研究使用，请遵守口袋48用户协议**

- 不要大规模爬取数据
- 合理控制请求频率（建议间隔≥60秒）
- 使用测试账号而非主账号

## TODO

- [x] 对接登录接口
- [x] 对接房间消息接口
- [ ] 添加WebSocket实时消息支持
- [ ] 还原密码加密算法
- [x] 写入 MySQL（members / messages / message_payloads / crawl_tasks / crawl_checkpoints）
