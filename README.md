# 口袋48房间消息抓取工具

用于抓取口袋48成员的聚聚房间消息。

## 项目结构

```
48messages/
├── 抓包分析指南.md      # 详细的抓包分析步骤
├── config.example.json  # 配置文件模板
├── pocket48_scraper.py  # 主程序
└── requirements.txt    # Python依赖
```

## 使用步骤

### 1. 抓包分析（最重要）

详细步骤请查看 [抓包分析指南.md](抓包分析指南.md)

主要步骤：
1. 使用Charles/Fiddler抓包工具
2. 在手机或模拟器上登录口袋48
3. 进入成员房间，捕获消息列表接口
4. 记录 `token`、`pa`、`appInfo` 和响应格式

### 2. 配置项目

```bash
cp config.example.json config.json
```

编辑 `config.json`：

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
  ]
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

```bash
python pocket48_scraper.py -c config.json
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
- [ ] 添加更多存储方式（MySQL、MongoDB）
