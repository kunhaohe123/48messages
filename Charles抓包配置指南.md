# Charles抓取口袋48详细配置指南

## 第一阶段：Charles基础配置

### 1.1 查看电脑IP地址

```
Charles菜单栏 → Help → Local IP Address
```

记住显示的IP地址，例如：`192.168.1.100`

### 1.2 配置代理端口

```
Charles菜单栏 → Proxy → Proxy Settings
```

设置：
- 端口：`8888`（默认）
- 勾选 ✅ Enable transparent HTTP proxying
- 点击 OK

### 1.3 配置HTTPS抓包

```
Charles菜单栏 → Proxy → SSL Proxying Settings
```

1. 勾选 ✅ Enable SSL Proxying
2. 点击 Add
3. 添加位置：
   - Host: `*`（或者具体域名）
   - Port: `443`
4. 点击 OK

## 第二阶段：手机端配置

### 2.1 确保手机和电脑在同一WiFi

**重要**：手机和电脑必须连**同一个WiFi网络**

### 2.2 设置手机代理

**iPhone设置**：
1. 设置 → WiFi → 点击当前WiFi右侧的 ⓘ
2. 滑到最底部 → 代理配置
3. 选择"手动"
4. 服务器：填入电脑IP地址（如 `192.168.1.100`）
5. 端口：`8888`
6. 点击存储

**Android设置**：
1. 设置 → WiFi → 长按当前WiFi → 修改网络
2. 高级选项 → 代理 → 手动
3. 代理服务器主机名：填入电脑IP地址
4. 代理服务器端口：`8888`
5. 点击保存

### 2.3 安装Charles证书到手机

#### 电脑上导出证书

```
Charles菜单栏 → Help → SSL Proxying → Save Charles Root Certificate
```

保存为 `.der` 文件（例如 `charles.der`）

#### 手机上安装证书

**iPhone**：
1. 把证书文件发送到手机（邮箱/AirDrop/微信）
2. 点击安装 → 提示"已下载描述文件"
3. 设置 → 通用 → VPN与设备管理 → 安装
4. 设置 → 通用 → 关于本机 → 证书信任设置 → 开启信任

**Android**：
1. 把证书文件传到手机
2. 设置 → 安全 → 加密与凭据 → 从存储设备安装
3. 选择证书文件 → 输入证书名称 → 安装

### 2.4 验证配置

1. Charles上查看是否显示手机连接
2. 手机浏览器访问 `http://www.charlesproxy.com/charles.crt`
3. 如果能下载证书并安装，说明配置成功

## 第三阶段：开始抓包

### 3.1 清空历史记录

```
Charles → Edit → Clear Sessions
```

### 3.2 打开口袋48App

1. 确保Charles正在运行（显示 Recording）
2. 打开手机上的口袋48App
3. **先不要登录**，观察Charles是否有请求

### 3.3 抓取登录请求

1. 在App中点击登录
2. 输入手机号 → 获取验证码
3. 输入验证码 → 登录

### 3.4 重点关注的请求

在Charles中，过滤以下关键词：

```
搜索关键词：
- login
- auth
- token
- session
- room
- message
- chat
```

重点记录：
- [ ] 登录接口的URL（完整地址）
- [ ] 登录请求的参数（JSON或Form）
- [ ] 登录响应返回的内容（特别是token）
- [ ] 房间消息相关的URL
- [ ] 请求头中的关键字段（Authorization、Cookie等）

## 第四阶段：分析API请求

### 4.1 查看请求详情

点击任意请求，查看：

**Overview（概览）**
- 请求URL
- 请求方法（GET/POST）
- 响应状态码

**Request（请求）**
- Headers（请求头）
- Query String（GET参数）
- Body（POST数据）

**Response（响应）**
- Headers（响应头）
- Body（响应数据，通常是JSON）

### 4.2 复制重要信息

对于登录请求，复制：
- 完整的URL
- 请求参数
- 响应数据（特别是token）

### 4.3 导出抓包数据

```
Charles → File → Export Session...
```

保存为 `.chlsj` 格式，便于后续分析

## 常见问题

### 问题1：手机无法连接Charles

检查：
- [ ] 手机和电脑在同一WiFi？
- [ ] 代理IP和端口配置正确？
- [ ] Charles是否正在运行？
- [ ] 防火墙是否阻止了8888端口？

### 问题2：抓不到HTTPS请求

检查：
- [ ] 是否安装了证书？
- [ ] 是否在手机上信任了证书？
- [ ] SSL Proxying是否启用？
- [ ] 目标域名是否添加到SSL Proxying？

### 问题3：看不到口袋48的请求

尝试：
- [ ] 确保App已登录/正在登录
- [ ] 检查Filter过滤条件
- [ ] 点击Sequence查看所有请求
- [ ] 查看Structure视图按域名分组

## 抓包检查清单

完成抓包后，请记录以下信息：

### 登录接口
- URL:
- 请求方法:
- 请求参数:
- 响应格式:

### Token信息
- Token字段名:
- Token位置:
- 有效期:

### 房间消息接口
- URL:
- 请求方法:
- 参数格式:

### 其他关键信息
- IMEI/设备ID:
- User-Agent:
- 其他请求头:

准备好后，把这些信息告诉我，我可以帮你完善代码！
