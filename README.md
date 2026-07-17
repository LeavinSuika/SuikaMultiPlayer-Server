<p align="center">
  <h1 align="center"> SuikaMultiPlayer Server</h1>
  <p align="center">
    多人在线同步听歌 · 后端服务
  </p>
  <p align="center">
    <a>
    <img src="./assets/app_icon.png" alt="logo" title="logo" width="200"/>
</a>
  </p>

---

## 📖 简介

SuikaMultiPlayer Server 是 SuikaMultiPlayer 的后端服务，提供用户认证、房间管理、实时播放同步等核心能力。

基于 **FastAPI + WebSocket** 构建，采用 **服务器权威时钟** 模型——播放进度完全由服务器维护，客户端仅接收并跟随，确保所有房间成员听到的内容完美同步。

### 技术栈

| 类别 | 技术 |
|------|------|
| 语言 | Python 3.14 |
| 框架 | FastAPI + Uvicorn |
| 数据库 | SQLite (aiosqlite) |
| 实时通信 | WebSocket |
| 包管理 | uv |
| 容器化 | Docker / Docker Compose |

### TODO List

- [ ] 投票切歌
- [ ] 随机播放
- [ ] 播放自定义链接
- [ ] 网易云歌单界面
- [ ] 历史记录

---

## 🚀 快速开始

### 环境要求

- Python 3.14+
- [uv](https://docs.astral.sh/uv/)

### 安装与运行

```bash
# 1. 克隆项目
git clone <repo-url>
cd SuikaMultiPlayer-Server

# 2. 安装依赖
uv sync

# 3. 修改配置（可选）
# 编辑 config/config.yaml，设置管理员账号、端口等

# 4. 启动服务
uv run main.py
```

服务默认监听 `0.0.0.0:8001`，启动后即可通过 `http://localhost:8001/health` 验证。

### Docker 部署

```bash
docker compose up -d
```

---

## 📁 项目结构

```text
SuikaMultiPlayer-Server/
├── main.py                       # 入口：读取配置，启动 uvicorn
├── requirements.txt              # 依赖列表（pip 兼容）
├── pyproject.toml                # uv 项目配置
├── uv.lock                       # 依赖锁定文件
├── Dockerfile
├── docker-compose.yml
├── config/
│   └── config.yaml               # 服务器配置
├── utils/
│   ├── api.py                    # FastAPI 应用主体（REST + WebSocket + 后台任务）
│   ├── database.py               # SQLite 数据库层（表定义、CRUD）
│   ├── music_link_fetcher.py     # 第三方音乐 API 集成
│   └── tools.py                  # 工具函数（密码哈希、权限校验等）
├── data/
│   └── database.db               # SQLite 数据库（运行时生成）
└── logs/
    └── server.log                # 运行日志（自动轮转，10MB/文件 × 5 个备份）
```

---

## 🔌 API 概览

### REST API

所有接口返回 JSON：`{"success": bool, "message": str, ...}`

| 分类 | 端点 | 说明 |
|------|------|------|
| 认证 | `POST /api/register` | 注册 |
| | `POST /api/login` | 登录 |
| | `POST /api/logout` | 登出 |
| | `POST /api/fetch_user` | 获取用户信息 |
| 用户管理 | `POST /api/reset_pwd` | 修改密码 |
| | `POST /api/update_avatar` | 更新头像 |
| | `POST /api/update_nickname` | 更新昵称 |
| | `POST /api/ban_user` | 封禁用户 (admin) |
| | `POST /api/unban_user` | 解封用户 (admin) |
| | `POST /api/ban_ip` | 封禁 IP (admin) |
| | `POST /api/unban_ip` | 解封 IP (admin) |
| | `POST /api/set_role` | 设置用户角色 (admin) |
| 房间管理 | `POST /api/create_room` | 创建房间 |
| | `POST /api/delete_room` | 删除房间 (owner) |
| | `POST /api/join_room` | 加入房间 |
| | `POST /api/leave_room` | 退出房间 |
| | `POST /api/kick_room_member` | 踢出成员 (owner/admin) |
| | `POST /api/transfer_room` | 转让房主 (owner) |
| | `GET /api/rooms` | 公开房间列表 |
| | `GET /api/user/{uuid}/rooms` | 用户已加入的房间 |
| 音乐 | `GET /api/music_link_get?track_id=...` | 获取音频直链 |
| | `GET /api/lrc_link_get?track_id=...` | 获取歌词 |
| | `GET /api/img_proxy?url=...` | 图片代理 |
| 健康检查 | `GET /health` | 服务状态 |

### WebSocket

| 连接 | 端点 | 用途 |
|------|------|------|
| 全局 WS | `/ws?user_uuid=...` | 心跳保活、在线状态 |
| 房间 WS | `/ws/room/{room_id}?user_uuid=...` | 播放同步、歌单管理、房间事件 |

**房间 WS 消息类型：**

| 方向 | type | 说明 |
|------|------|------|
| C→S | `ping` | 心跳 |
| C→S | `playback_update` | 播放控制 (owner/admin) |
| C→S | `playlist_add` | 添加歌曲 |
| C→S | `playlist_remove` | 移除歌曲 (owner/admin) |
| C→S | `pause_event` | 暂停/恢复 (owner/admin) |
| S→C | `playback_state` | 播放状态广播 |
| S→C | `playlist_update` | 歌单变更广播 |
| S→C | `user_joined` / `user_left` | 成员进出通知 |
| S→C | `room_info` | 房间信息（成员加入时） |
| S→C | `error` | 错误消息 |

---

## 🏗️ 架构要点

### 服务器权威时钟

播放进度由服务器的 `room_playback_task` 协程维护，以 `start_time` 为基准计算 `pos = now() - start_time`。服务器定时（默认每秒）向房间所有成员广播 `playback_state`，客户端据此同步本地播放器。客户端发送的是**控制指令**（播放/暂停/切歌/跳转），而非播放状态。

### 内存 + SQLite 混合存储

- **内存**：播放状态、播放列表、在线用户、心跳 —— 高频读写，追求低延迟
- **SQLite**：用户账户、房间信息、成员关系、封禁记录 —— 需要持久化

> 代价：服务器重启后播放状态和播放列表会丢失，但用户数据和房间结构不受影响。

### 权限模型

| 级别 | 角色 | 权限 |
|------|------|------|
| 系统 | `admin` | 封禁/解封用户和 IP，修改角色 |
| 系统 | `user` | 普通使用 |
| 房间 | `owner` | 删除房间、转让房主、踢人、播放控制 |
| 房间 | `admin` | 重命名、设置公开/私有、踢人、播放控制 |
| 房间 | `member` | 查看、添加歌曲 |

### 后台任务

| 任务 | 周期 | 说明 |
|------|------|------|
| `clean_heartbeats` | 60s | 清理超时心跳和离线用户 |
| `track_pos_align` | 1s | 推进播放位置 |
| `room_leader_user_set` | 10s | 随机选取同步基准用户 |
| `room_users_sync` | 10s | 同步 DB 与内存的房间列表 |

---

## 🔗 相关链接

- [SuikaMultiPlayer Client](https://github.com/LeavinSuika/SuikaMultiPlayer-Client) — 客户端

---

## 📄 许可证

本项目基于 GNU GPL v3 许可证开源，详见 [LICENSE](LICENSE)。
