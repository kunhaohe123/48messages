# 口袋48房间消息抓取工具

用于抓取口袋48成员的聚聚房间消息，并写入 MySQL。

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
│   ├── 抓包分析指南.md
│   ├── 持久化抓取指南.md
│   └── Charles抓包配置指南.md
├── src/
│   ├── pocket48_scraper.py  # 统一主程序
│   └── message_storage.py
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
```

编辑 `config/config.json`：

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
  "members": [
    {
      "name": "成员名字",
      "serverId": 951577,
      "channelId": 1312655
    }
  ],
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

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 修改代码

根据抓包结果，填充以下关键字段：

1. `encryptedPassword` - 登录接口中的 `loginMobile.pwd`
2. `pa` - 请求头中的 `pa`
3. `appInfo` - 请求头中的 `appInfo`
4. `channelId` / `serverId` - 消息列表接口的请求参数

### 5. 运行程序

`src/pocket48_scraper.py` 现在是统一入口，抓取、导出、统计都从这里执行。

```bash
python src/pocket48_scraper.py -c config/config.json
```

单次抓取并退出：

```bash
python src/pocket48_scraper.py -c config/config.json --once
```

查看统计：

```bash
python src/pocket48_scraper.py -c config/config.json --stats
```

### 抓取策略说明

- 持续监控和 `--once` 都会先请求最新一页消息
- 如果本地最新消息还没有追上接口返回的数据边界，程序会继续使用返回的 `nextTime` 向历史翻页
- 分页会持续到命中本地已保存的最新消息，或接口没有更多历史页为止
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
- [x] 写入 MySQL（members / rooms / messages / message_payloads / crawl_tasks / crawl_checkpoints）
