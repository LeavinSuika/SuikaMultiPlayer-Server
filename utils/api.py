"""
-*-coding: utf-8-*-
本模块用于管理api连接
"""

import logging
import yaml
from pathlib import Path
from datetime import datetime, timezone
import asyncio
import time
import uuid
from utils import database
from utils import tools
from utils import music_link_fetcher
from pydantic import BaseModel, Field

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.responses import Response, FileResponse
from fastapi.exceptions import HTTPException
import httpx

logger = logging.getLogger(__name__)

# 读取配置
config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# 日志配置
logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s %(levelname)s] [%(module)s] %(message)s',
        datefmt='%H:%M:%S'
    )

# 图片存储目录
IMAGES_DIR = Path(__file__).parent.parent / 'data' / 'images'
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

# 音频链接缓存 {track_id: (url, timestamp)}
_music_cache = {}
_MUSIC_CACHE_TTL = 300  # 5 分钟

# 临时数据表
heartbeats = {}     # 用户心跳时间表 {user_uuid: time, ...}
user_rooms = {}     # 用户加入房间表 {user_uuid: room_id, ...}
room_users = {}     # 房间内用户表 {room_id: [user_uuid, ...], ...}
room_playlist = {}  # 各个房间的音乐列表 {room_id: [{track_id: str, duration: int}, ...], ...}
room_playback_tasks = {}  # {room_id: asyncio.Task}
command_queues = {}  # {room_id: asyncio.Queue}
room_playback_state = {}  # 播放状态快照 {room_id: {track_id, duration, is_playing, pos}}

# 初始化房间内用户表
async def init_room_users():
    room_list = await database.fetch_all_room_id()
    for rid in room_list:
        room_users[rid] = []

# 同步房间列表循环
async def room_users_sync():
    while True:
        await asyncio.sleep(10)
        room_list = await database.fetch_all_room_id()
        for room_id in room_users:
            if room_id not in room_list:
                room_users[room_id] = []

HEARTBEAT_TIMEOUT = config.get("connection").get("heartbeat_timeout")
SEND_DELAY = config.get("connection").get("send_delay", 1000) / 1000  # 转换为秒

state_lock = asyncio.Lock()

class TransferRoom(BaseModel):
    operator_uuid: str = Field(...)
    room_id: int
    to_uuid: str

# 心跳循环
async def clean_heartbeats():
    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        removed = []  # (uuid, room_id)

        async with state_lock:
            for uuid, t in list(heartbeats.items()):
                if (now - t).total_seconds() > HEARTBEAT_TIMEOUT:
                    heartbeats.pop(uuid, None)
                    rid = user_rooms.pop(uuid, None)
                    if rid and rid in room_users:
                        try:
                            room_users[rid].remove(uuid)
                        except ValueError:
                            pass
                    if rid:
                        removed.append((uuid, rid))

        # 广播 user_left
        for uuid, rid in removed:
            try:
                await ws_manager.broadcast_to_room(rid, {
                    "type": "user_left",
                    "user_uuid": uuid
                })
            except Exception:
                pass

async def _broadcast_playlist(room_id):
    """广播当前播放列表（含添加者信息）"""
    async with state_lock:
        pl = room_playlist.get(room_id, [])
        playlist_entries = [
            {"track_id": t["track_id"], "duration": t.get("duration", 0), "added_by": t.get("added_by", "")}
            for t in pl
        ]
    await ws_manager.broadcast_to_room(room_id, {
        "type": "playlist_update",
        "playlist": playlist_entries
    })

def _ensure_playback_task(room_id):
    """若房间不在播放且 playlist 非空，启动 playback task"""
    if room_id not in room_playback_tasks or room_playback_tasks[room_id].done():
        if room_playlist.get(room_id):
            command_queues[room_id] = asyncio.Queue()
            room_playback_tasks[room_id] = asyncio.create_task(
                room_playback_task(room_id)
            )

async def room_playback_task(room_id):
    """服务器权威播放引擎 — 每个房间一个协程"""
    while True:
        async with state_lock:
            playlist = room_playlist.get(room_id, [])
        if not playlist:
            break

        current = playlist[0]
        track_id = current["track_id"]
        duration = current["duration"]
        start_time = time.time()
        is_playing = True

        async with state_lock:
            room_playback_state[room_id] = {
                "track_id": track_id,
                "duration": duration,
                "is_playing": True,
                "pos": 0
            }

        await ws_manager.broadcast_to_room(room_id, {
            "type": "play_track",
            "track_id": track_id,
            "duration": duration,
            "pos": 0,
            "is_playing": True
        })
        logger.info(f"[room {room_id}] 开始播放 track={track_id} duration={duration}ms")

        while True:
            try:
                cmd = await asyncio.wait_for(
                    command_queues[room_id].get(),
                    timeout=SEND_DELAY
                )
            except asyncio.TimeoutError:
                cmd = None

            if cmd:
                cmd_type = cmd["type"]

                if cmd_type == "pause" and is_playing:
                    is_playing = False
                    pause_pos = int((time.time() - start_time) * 1000)
                    async with state_lock:
                        if room_id in room_playback_state:
                            room_playback_state[room_id]["is_playing"] = False
                    await ws_manager.broadcast_to_room(room_id, {
                        "type": "pause",
                        "user_uuid": cmd["user_uuid"],
                        "is_paused": True
                    })
                    logger.info(f"[room {room_id}] 暂停 pos={pause_pos}ms")

                elif cmd_type == "resume" and not is_playing:
                    start_time = time.time() - pause_pos / 1000
                    is_playing = True
                    async with state_lock:
                        if room_id in room_playback_state:
                            room_playback_state[room_id]["is_playing"] = True
                    await ws_manager.broadcast_to_room(room_id, {
                        "type": "resume",
                        "user_uuid": cmd["user_uuid"],
                        "is_paused": False
                    })
                    logger.info(f"[room {room_id}] 恢复播放")

                elif cmd_type == "seek":
                    seek_pos = cmd["pos"]
                    start_time = time.time() - seek_pos / 1000
                    await ws_manager.broadcast_to_room(room_id, {
                        "type": "seek",
                        "track_id": track_id,
                        "pos": seek_pos
                    })
                    logger.info(f"[room {room_id}] seek → {seek_pos}ms")

                elif cmd_type == "skip":
                    logger.info(f"[room {room_id}] skip")
                    break

                elif cmd_type == "playlist_add":
                    async with state_lock:
                        if room_id not in room_playlist:
                            room_playlist[room_id] = []
                        existing = {t["track_id"] for t in room_playlist[room_id]}
                        for t in cmd["tracks"]:
                            if t["track_id"] not in existing:
                                room_playlist[room_id].append(t)
                                existing.add(t["track_id"])
                    await _broadcast_playlist(room_id)

                elif cmd_type == "playlist_remove":
                    async with state_lock:
                        pl = room_playlist.get(room_id, [])
                        remove_ids = set(cmd["track_ids"])
                        room_playlist[room_id] = [
                            t for t in pl if t["track_id"] not in remove_ids
                        ]
                    await _broadcast_playlist(room_id)

            if is_playing:
                pos = int((time.time() - start_time) * 1000)
                if pos >= duration:
                    logger.info(f"[room {room_id}] 曲目结束 pos={pos}ms >= duration={duration}ms")
                    break
                async with state_lock:
                    if room_id in room_playback_state:
                        room_playback_state[room_id]["pos"] = pos
                await ws_manager.broadcast_to_room(room_id, {
                    "type": "playback_state",
                    "track_id": track_id,
                    "is_playing": True,
                    "pos": pos
                })

        # 切歌：移除队首
        async with state_lock:
            if room_playlist.get(room_id):
                room_playlist[room_id].pop(0)
        await _broadcast_playlist(room_id)

        async with state_lock:
            pl = room_playlist.get(room_id, [])
        if not pl:
            await ws_manager.broadcast_to_room(room_id, {
                "type": "pause_event",
                "is_paused": True
            })
            logger.info(f"[room {room_id}] 播放列表为空，退出播放循环")
            break

    async with state_lock:
        room_playback_tasks.pop(room_id, None)
        command_queues.pop(room_id, None)
        room_playback_state.pop(room_id, None)
    logger.info(f"[room {room_id}] playback task 已清理")
                    
@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_database()
    await init_room_users()
    logger.info("房间内用户表初始化成功")
    heartbeat_task = asyncio.create_task(clean_heartbeats())
    logger.info("心跳清理任务已启动")
    room_users_sync_task = asyncio.create_task(room_users_sync())
    logger.info("同步房间列表任务已启动")

    yield
    heartbeat_task.cancel()
    logger.info("心跳清理任务已停止")
    room_users_sync_task.cancel()
    logger.info("同步房间列表任务已停止")
    for room_id, task in list(room_playback_tasks.items()):
        task.cancel()
    logger.info("所有播放任务已停止")

app = FastAPI(lifespan=lifespan)

# 数据模板
class Register(BaseModel):
    user_name: str = Field(..., min_length=3, max_length=20, pattern="^[a-zA-Z0-9_]+$")
    pwd: str = Field(..., min_length=6, max_length=50)
    nickname: str = Field(..., max_length=20)
    
class Login(BaseModel):
    user_name: str = Field(..., min_length=3, max_length=20, pattern="^[a-zA-Z0-9_]+$")
    pwd: str = Field(..., min_length=6, max_length=50)
    
class FetchUser(BaseModel):
    user_uuid: str

class ResetPwd(BaseModel):
    user_uuid: str
    old_pwd: str
    new_pwd: str = Field(..., min_length=6, max_length=50)

class UpdateAvatar(BaseModel):
    user_uuid: str
    avatar_url: str
    avatar_key: str = ""

class UpdateNickname(BaseModel):
    user_uuid: str
    nickname: str = Field(..., max_length=20)
    
class BanUser(BaseModel):
    operator_uuid: str = Field(...)
    user_uuid: str
    ban_reason: str | None =None
    pardon_time: str | None = None
    
class UnbanUser(BaseModel):
    operator_uuid: str
    user_uuid: str
    
class BanIP(BaseModel):
    operator_uuid: str = Field(...)
    ip: str
    ban_reason: str | None = None
    pardon_time: str | None = None
    
class UnbanIP(BaseModel):
    operator_uuid: str = Field(...)
    ip: str

class SetRole(BaseModel):
    operator_uuid: str = Field(...)
    user_uuid: str
    role: str = Field(..., pattern="^(user|admin)$") 
    
class CreateRoom(BaseModel):
    name: str
    creator_uuid: str
    is_public: bool = True

class DeleteRoom(BaseModel):
    operator_uuid: str = Field(...)
    room_id: int
    
class RenameRoom(BaseModel):
    operator_uuid: str = Field(...)
    room_id: int
    name: str

class SetRoomIsPublic(BaseModel):
    operator_uuid: str = Field(...)
    room_id: int
    is_public: bool

class JoinRoom(BaseModel):
    room_id: int
    user_uuid: str

class LeaveRoom(BaseModel):
    room_id: int
    user_uuid: str

class KickRoomMember(BaseModel):
    operator_uuid: str = Field(...)
    room_id: int
    user_uuid: str

class SetRoomMemberRole(BaseModel):
    operator_uuid: str = Field(...)
    room_id: int
    user_uuid: str
    role: str = Field(..., pattern="^(owner|admin|member)$")

class FetchRoom(BaseModel):
    room_id: int
    user_uuid: str | None = None
    
class MusicLinkGet(BaseModel):
    track_id: str

class LrcLinkGet(BaseModel):
    track_id: str
    


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[int, dict[str, WebSocket]] = {}

    async def connect(self, room_id: int, user_uuid: str, websocket: WebSocket):
        await websocket.accept()
        if room_id not in self.active_connections:
            self.active_connections[room_id] = {}
        self.active_connections[room_id][user_uuid] = websocket

    def disconnect(self, room_id: int, user_uuid: str):
        if room_id in self.active_connections:
            self.active_connections[room_id].pop(user_uuid, None)
            if not self.active_connections[room_id]:
                del self.active_connections[room_id]

    async def broadcast_to_room(self, room_id: int, message: dict, exclude: str = None):
        if room_id not in self.active_connections:
            return
        disconnected = []
        for user_uuid, ws in self.active_connections[room_id].items():
            if user_uuid == exclude:
                continue
            try:
                await ws.send_json(message)
            except Exception:
                disconnected.append(user_uuid)
        for uuid in disconnected:
            self.disconnect(room_id, uuid)

    async def send_to_user(self, room_id: int, user_uuid: str, message: dict):
        if room_id in self.active_connections and user_uuid in self.active_connections[room_id]:
            try:
                await self.active_connections[room_id][user_uuid].send_json(message)
            except Exception:
                self.disconnect(room_id, user_uuid)

ws_manager = ConnectionManager()

# ==================== 全局 WebSocket（心跳 / 在线状态）====================
@app.websocket("/ws")
async def websocket_global(websocket: WebSocket):
    user_uuid = websocket.query_params.get("user_uuid")
    if not user_uuid:
        await websocket.close(code=4000, reason="missing user_uuid")
        return

    user = await database.fetch_user(user_uuid)
    if not user:
        await websocket.close(code=4001, reason="user not found")
        return

    banned, _ = await tools.check_user_banned(user_uuid)
    if banned:
        await websocket.close(code=4002, reason="user banned")
        return

    await websocket.accept()

    async with state_lock:
        heartbeats[user_uuid] = datetime.now(timezone.utc)

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "ping":
                async with state_lock:
                    heartbeats[user_uuid] = datetime.now(timezone.utc)
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"全局 WebSocket 错误 user {user_uuid}: {e}")
    finally:
        async with state_lock:
            heartbeats.pop(user_uuid, None)


# ==================== 房间 WebSocket（播放同步）====================
@app.websocket("/ws/room/{room_id}")
async def websocket_room(websocket: WebSocket, room_id: int):
    user_uuid = websocket.query_params.get("user_uuid")
    if not user_uuid:
        await websocket.close(code=4000, reason="missing user_uuid")
        return

    user = await database.fetch_user(user_uuid)
    if not user:
        await websocket.close(code=4001, reason="user not found")
        return

    banned, _ = await tools.check_user_banned(user_uuid)
    if banned:
        await websocket.close(code=4002, reason="user banned")
        return

    user_room = await database.fetch_user_room(user_uuid, room_id)
    if not user_room:
        await websocket.close(code=4003, reason="not a member of this room")
        return

    await ws_manager.connect(room_id, user_uuid, websocket)

    async with state_lock:
        user_rooms[user_uuid] = room_id
        if room_id not in room_users:
            room_users[room_id] = []
        if user_uuid not in room_users[room_id]:
            room_users[room_id].append(user_uuid)
        heartbeats[user_uuid] = datetime.now(timezone.utc)

    room_members = await database.fetch_room_members(room_id)
    owner_uuid = None
    for member in room_members:
        if member["role"] == "owner":
            owner_uuid = member["user_uuid"]
            break

    playlist_entries = [
        {"track_id": t["track_id"], "duration": t.get("duration", 0), "added_by": t.get("added_by", "")}
        for t in room_playlist.get(room_id, [])
    ]

    await ws_manager.send_to_user(room_id, user_uuid, {
        "type": "room_info",
        "playlist": playlist_entries,
        "owner": owner_uuid
    })

    # 若房间正在播放，立即同步当前播放状态给新用户
    async with state_lock:
        pstate = room_playback_state.get(room_id)
    if pstate:
        await ws_manager.send_to_user(room_id, user_uuid, {
            "type": "play_track",
            "track_id": pstate["track_id"],
            "duration": pstate["duration"],
            "pos": pstate["pos"],
            "is_playing": pstate["is_playing"]
        })

    await ws_manager.broadcast_to_room(room_id, {
        "type": "user_joined",
        "user_uuid": user_uuid
    }, exclude=user_uuid)

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "ping":
                async with state_lock:
                    heartbeats[user_uuid] = datetime.now(timezone.utc)
                await ws_manager.send_to_user(room_id, user_uuid, {"type": "pong"})

            elif msg_type == "playlist_add":
                tracks = data.get("tracks", [])
                for t in tracks:
                    if t.get("duration", 0) <= 0:
                        tid = t.get("track_id", "")
                        if tid:
                            try:
                                detail = await music_link_fetcher.song_detail_get(tid)
                                if detail and detail.get("duration", 0) > 0:
                                    t["duration"] = detail["duration"]
                                    logger.info(f"[room {room_id}] 查询到 track_id={tid} 实际时长={t['duration']}ms")
                                    continue
                            except Exception as e:
                                logger.warning(f"[room {room_id}] 查询歌曲时长失败 track_id={tid}: {e}")
                        t["duration"] = 240000
                        logger.info(f"[room {room_id}] track_id={tid} 使用默认时长 240000ms")
                async with state_lock:
                    if room_id not in room_playlist:
                        room_playlist[room_id] = []
                    existing_ids = {t["track_id"] for t in room_playlist[room_id]}
                    for t in tracks:
                        tid = t.get("track_id", "")
                        if tid and tid not in existing_ids:
                            room_playlist[room_id].append({
                                "track_id": tid,
                                "duration": t.get("duration", 0),
                                "added_by": user_uuid,
                            })
                            existing_ids.add(tid)
                    was_empty = len(room_playlist[room_id]) == len(tracks)

                _ensure_playback_task(room_id)
                await _broadcast_playlist(room_id)

            elif msg_type == "playlist_remove":
                if user_room["role"] not in ("owner", "admin"):
                    await ws_manager.send_to_user(room_id, user_uuid, {
                        "type": "error",
                        "message": "只有房主和管理员可以删除歌单歌曲"
                    })
                    continue

                remove_ids = set(data.get("tracks", []))
                if room_id in command_queues:
                    await command_queues[room_id].put({
                        "type": "playlist_remove",
                        "track_ids": remove_ids
                    })
                else:
                    async with state_lock:
                        pl = room_playlist.get(room_id, [])
                        room_playlist[room_id] = [
                            t for t in pl if t["track_id"] not in remove_ids
                        ]
                    await _broadcast_playlist(room_id)

            elif msg_type == "pause":
                if user_room["role"] not in ("owner", "admin"):
                    continue
                if room_id in command_queues:
                    await command_queues[room_id].put({
                        "type": "pause",
                        "user_uuid": user_uuid
                    })

            elif msg_type == "resume":
                if user_room["role"] not in ("owner", "admin"):
                    continue
                if room_id in command_queues:
                    await command_queues[room_id].put({
                        "type": "resume",
                        "user_uuid": user_uuid
                    })

            elif msg_type == "seek":
                if user_room["role"] not in ("owner", "admin"):
                    continue
                if room_id in command_queues:
                    await command_queues[room_id].put({
                        "type": "seek",
                        "pos": data.get("pos", 0)
                    })

            elif msg_type == "skip":
                if user_room["role"] not in ("owner", "admin"):
                    continue
                if room_id in command_queues:
                    await command_queues[room_id].put({"type": "skip"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error for user {user_uuid}: {e}")
    finally:
        ws_manager.disconnect(room_id, user_uuid)
        async with state_lock:
            if user_rooms.get(user_uuid) == room_id:
                user_rooms.pop(user_uuid, None)
            if room_id in room_users and user_uuid in room_users[room_id]:
                room_users[room_id].remove(user_uuid)
        await ws_manager.broadcast_to_room(room_id, {
            "type": "user_left",
            "user_uuid": user_uuid
        })

# 获取房间在线用户
@app.get("/api/room/{room_id}/online")
async def get_online_users(room_id: int):
    now = datetime.now(timezone.utc)
    online = []
    
    async with state_lock:
        for uuid, rid in user_rooms.items():
            if rid == room_id:
                last_beat = heartbeats.get(uuid)
                if last_beat and (now - last_beat).total_seconds() < HEARTBEAT_TIMEOUT:
                    online.append(uuid)
    
    return {"room_id": room_id, "online": online, "count": len(online)}

# 注册api
@app.post("/api/register")
async def register(info: Register, request: Request):
    client_ip = tools.get_client_ip(request)
    
    banned, msg = await tools.check_ip_banned(client_ip)
    if banned:
        logger.warning(f"注册被IP封禁拦截, ip: {client_ip}")
        return {"success": False, "message": msg}
        
    user_uuid = str(uuid.uuid4())
    success, msg = await database.register(user_uuid, info.user_name, info.pwd, client_ip, info.nickname)
    
    if success:
        
        success1, _ = await database.set_status(user_uuid, "online")
        success2, _ = await database.set_lastlogin(user_uuid, datetime.now(timezone.utc).strftime(tools.DATETIME_FORMAT))
        if not success1 or not success2:
            logger.warning(f"注册后设置在线状态失败, user_uuid: {user_uuid}")
            return {"success": False, "message": "系统错误，请稍后再试"}
        
        logger.info(f"用户注册成功, user_uuid: {user_uuid}, user_name: {info.user_name}")
        return {"success": True, "user_uuid": user_uuid}
    else:
        logger.warning(f"用户注册失败, user_name: {info.user_name}")
        return {"success": False, "message": msg}

# 登录api
@app.post("/api/login")
async def login(info: Login, request: Request):
    print(f"🔴 login 函数被调用: user_name={info.user_name}")
    client_ip = tools.get_client_ip(request)
    
    banned, msg = await tools.check_ip_banned(client_ip)
    if banned:
        logger.warning(f"登录被IP封禁拦截, ip: {client_ip}")
        return {"success": False, "message": msg}

    success, result = await database.login(info.user_name, info.pwd, client_ip)
    
    if success:
        banned, msg = await tools.check_user_banned(result)
        if banned:
            logger.warning(f"被封禁用户尝试登录, user_uuid: {result}")
            return {"success": False, "message": msg}

        success1, _ = await database.set_status(result, "online")
        success2, _ = await database.set_lastlogin(result, datetime.now(timezone.utc).strftime(tools.DATETIME_FORMAT))
        success3, _ = await database.set_ip(result, client_ip)
        if not success1 or not success2 or not success3:
            logger.error(f"登录后状态更新失败, user_uuid: {result}")
            return {"success": False, "message": "系统错误，请稍后再试"}
        logger.info(f"用户登录成功, user_uuid: {result}")
        return {"success": True, "user_uuid": result}
    else:
        logger.warning(f"用户登录失败, user_name: {info.user_name}")
        return {"success": False, "message": result}
    
# uuid查询/登录api
@app.post("/api/fetch_user")
async def fetch_user(info: FetchUser, request: Request):
    client_ip = tools.get_client_ip(request)
    
    banned, msg = await tools.check_ip_banned(client_ip)
    if banned:
        return {"success": False, "message": msg}

    req = await database.fetch_user(info.user_uuid)
    
    if not req:
        logger.warning(f"fetch_user 用户不存在, user_uuid: {info.user_uuid}")
        return {"success": False, "message": "用户不存在"}
    user_info = req[0]
    
    banned, msg = await tools.check_user_banned(info.user_uuid)
    if banned:
        logger.warning(f"fetch_user 用户被封禁, user_uuid: {info.user_uuid}")
        return {"success": False, "message": msg}
    
    if user_info["status"] == "offline":
        success1, _ = await database.set_status(user_info["user_uuid"], "online")
        success2, _ = await database.set_ip(user_info["user_uuid"], client_ip)
        success3, _ = await database.set_lastlogin(user_info["user_uuid"], datetime.now(timezone.utc).strftime(tools.DATETIME_FORMAT))
        if not success1 or not success2 or not success3:
            logger.error(f"fetch_user 状态更新失败, user_uuid: {info.user_uuid}")
            return {"success": False, "message": "系统错误，请稍后再试"}
    
    logger.info(f"fetch_user 查询成功, user_uuid: {info.user_uuid}")
    
    return {"success": True, 
            "user_info": {
                "user_uuid": user_info["user_uuid"],
                "user_name": user_info["user_name"],
                "nickname": user_info["nickname"],
                "avatar_url": user_info.get("avatar_url"),
                "role": user_info["role"],
                "status": user_info["status"],
                "last_login": user_info["last_login"]
            }
    }
    

# 修改密码api
@app.post("/api/reset_pwd")
async def reset_pwd(info: ResetPwd):
    success, msg = await database.change_pwd(info.user_uuid, info.old_pwd, info.new_pwd)
    if success:
        logger.info(f"密码修改成功, user_uuid: {info.user_uuid}")
        return {"success": True}
    else:
        logger.warning(f"密码修改失败, user_uuid: {info.user_uuid}")
        return {"success": False, "message": msg}

# 修改头像api
@app.post("/api/update_avatar")
async def update_avatar(info: UpdateAvatar):
    # 校验用户存在
    user = await database.fetch_user(info.user_uuid)
    if not user:
        logger.warning(f"头像修改失败(用户不存在), user_uuid: {info.user_uuid}")
        return {"success": False, "message": "用户不存在"}

    # 记录旧头像 key，用于成功后清理（None 视为无旧头像）
    old_avatar_key = (user[0].get("avatar_key") or "") if user else ""

    # 如果提供了 avatar_key（图床上传的 image_id），校验图片存在
    if info.avatar_key:
        found = False
        for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"):
            if (IMAGES_DIR / f"{info.avatar_key}{ext}").exists():
                found = True
                break
        if not found:
            return {"success": False, "message": "头像图片不存在，请重新上传"}

    success, msg = await database.set_avatar(info.user_uuid, info.avatar_url, info.avatar_key)
    if success:
        # 删除旧头像图片释放空间
        if old_avatar_key and old_avatar_key != info.avatar_key:
            deleted = False
            for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"):
                old_file = IMAGES_DIR / f"{old_avatar_key}{ext}"
                if old_file.exists():
                    old_file.unlink()
                    deleted = True
                    logger.info(f"旧头像已删除: {old_file.name}")
                    break
            if not deleted:
                logger.info(f"旧头像文件不存在(可能已被清理): {old_avatar_key}")
        logger.info(f"头像修改成功, user_uuid: {info.user_uuid}, old_key={old_avatar_key}, new_key={info.avatar_key}")
        return {"success": True}
    else:
        logger.warning(f"头像修改失败, user_uuid: {info.user_uuid}")
        return {"success": False, "message": msg}
    
# 修改昵称api
@app.post("/api/update_nickname")
async def update_nickname(info: UpdateNickname):
    success, msg = await database.set_nickname(info.user_uuid, info.nickname)
    if success:
        logger.info(f"昵称修改成功, user_uuid: {info.user_uuid}")
        return {"success": True}
    else:
        logger.warning(f"昵称修改失败, user_uuid: {info.user_uuid}")
        return {"success": False, "message": msg}

# 封禁用户
@app.post("/api/ban_user")
async def ban_user(info: BanUser):
    # 防止管理员封禁自己
    if info.operator_uuid == info.user_uuid:
        logger.warning(f"封禁自操作被拒绝, operator: {info.operator_uuid}")
        return {"success": False, "message": "不能封禁自己"}
    
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:
        success, msg = await database.ban_user(info.user_uuid, info.ban_reason, info.pardon_time)
        if success:
            logger.info(f"用户被封禁, target: {info.user_uuid}, operator: {info.operator_uuid}")
            return {"success": True}
        else:
            logger.warning(f"封禁用户失败, target: {info.user_uuid}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"封禁用户权限不足, operator: {info.operator_uuid}")
        return {"success": False, "message": message}

# 解封用户
@app.post("/api/unban_user")
async def unban_user(info: UnbanUser):
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:
        success, msg = await database.unban_user(info.user_uuid)
        if success:
            logger.info(f"用户被解封, target: {info.user_uuid}, operator: {info.operator_uuid}")
            return {"success": True}
        else:
            logger.warning(f"解封用户失败, target: {info.user_uuid}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"解封用户权限不足, operator: {info.operator_uuid}")
        return {"success": False, "message": message}
    
# 封禁ip
@app.post("/api/ban_ip")
async def ban_ip(info: BanIP):
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:
        success, msg = await database.ban_ip(info.ip, info.ban_reason, info.pardon_time, info.operator_uuid)
        if success:
            logger.info(f"IP被封禁, ip: {info.ip}, operator: {info.operator_uuid}")
            return {"success": True}
        else:
            logger.warning(f"封禁IP失败, ip: {info.ip}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"封禁IP权限不足, operator: {info.operator_uuid}")
        return {"success": False, "message": message}

# 解封ip
@app.post("/api/unban_ip")
async def unban_ip(info: UnbanIP):
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:
        success, msg = await database.unban_ip(info.ip, info.operator_uuid)
        if success:
            logger.info(f"IP被解封, ip: {info.ip}, operator: {info.operator_uuid}")
            return {"success": True}
        else:
            logger.warning(f"解封IP失败, ip: {info.ip}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"解封IP权限不足, operator: {info.operator_uuid}")
        return {"success": False, "message": message}

class AdminListQuery(BaseModel):
    operator_uuid: str = Field(...)

# 管理员查询所有用户
@app.get("/api/admin/users")
async def admin_list_users(operator_uuid: str):
    passed, message = await tools.check_admin_permission(operator_uuid)
    if not passed:
        return {"success": False, "message": message}
    users = await database.fetch_all_users()
    if users is None:
        return {"success": False, "message": "查询失败"}
    return {"success": True, "users": users}

# 管理员查询被封禁用户
@app.get("/api/admin/banned_users")
async def admin_list_banned_users(operator_uuid: str):
    passed, message = await tools.check_admin_permission(operator_uuid)
    if not passed:
        return {"success": False, "message": message}
    users = await database.fetch_banned_users()
    if users is None:
        return {"success": False, "message": "查询失败"}
    return {"success": True, "users": users}

# 管理员查询被封禁IP
@app.get("/api/admin/banned_ips")
async def admin_list_banned_ips(operator_uuid: str):
    passed, message = await tools.check_admin_permission(operator_uuid)
    if not passed:
        return {"success": False, "message": message}
    ips = await database.fetch_banned_ips()
    if ips is None:
        return {"success": False, "message": "查询失败"}
    return {"success": True, "ips": ips}

# 设置用户权限
@app.post("/api/set_role")
async def set_role(info: SetRole):

    if info.operator_uuid == info.user_uuid:
        logger.warning(f"设置角色自操作被拒绝, operator: {info.operator_uuid}")
        return {"success": False, "message": "不能修改自己的权限"}
    
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:

        if info.role == "user":
            can_demote, msg = await tools.check_last_admin(info.user_uuid)
            if not can_demote:
                logger.warning(f"取消最后一位管理员被拒绝, target: {info.user_uuid}")
                return {"success": False, "message": msg}
        
        success, msg = await database.set_role(info.user_uuid, info.role)
        if success:
            logger.info(f"用户角色被修改, target: {info.user_uuid}, role: {info.role}, operator: {info.operator_uuid}")
            return {"success": True}
        else:
            logger.warning(f"设置角色失败, target: {info.user_uuid}, role: {info.role}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"设置角色权限不足, operator: {info.operator_uuid}")
        return {"success": False, "message": message}

# 创建房间
@app.post("/api/create_room")
async def create_room(info: CreateRoom):
    user = await database.fetch_user(info.creator_uuid)
    if not user:
        logger.warning(f"创建房间失败, 用户不存在, creator: {info.creator_uuid}")
        return {"success": False, "message": "用户不存在"}
    
    banned, msg = await tools.check_user_banned(info.creator_uuid)
    if banned:
        logger.warning(f"创建房间失败, 用户被封禁, creator: {info.creator_uuid}")
        return {"success": False, "message": msg}
    
    success, msg = await database.create_room(info.name, info.creator_uuid, info.is_public)
    if success:
        room_users[msg] = []
        logger.info(f"房间创建成功, room_id: {msg}, name: {info.name}, creator: {info.creator_uuid}")
        return {"success": True, "room_id": msg}
    else:
        logger.warning(f"房间创建失败, creator: {info.creator_uuid}")
        return {"success": False, "message": msg}
    
# 删除房间
@app.post("/api/delete_room")
async def delete_room(info: DeleteRoom):
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id)
    if passed:
        success, msg = await database.delete_room(info.room_id)
        if success:
            room_users.pop(info.room_id, None)
            room_playlist.pop(info.room_id, None)
            if info.room_id in room_playback_tasks:
                room_playback_tasks[info.room_id].cancel()
                room_playback_tasks.pop(info.room_id, None)
            command_queues.pop(info.room_id, None)
            room_playback_state.pop(info.room_id, None)
            logger.info(f"房间删除成功, room_id: {info.room_id}, operator: {info.operator_uuid}")
            return {"success": True}
        else:
            logger.warning(f"房间删除失败, room_id: {info.room_id}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"删除房间权限不足, room_id: {info.room_id}, operator: {info.operator_uuid}")
        return {"success": False, "message": message}

# 设置房间名称
@app.post("/api/rename_room")
async def rename_room(info: RenameRoom):
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id, ["owner", "admin"])
    if passed:
        success, msg = await database.set_room_name(info.room_id, info.name)
        if success:
            logger.info(f"房间重命名, room_id: {info.room_id}, name: {info.name}")
            return {"success": True}
        else:
            logger.warning(f"房间重命名失败, room_id: {info.room_id}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"重命名房间权限不足, room_id: {info.room_id}, operator: {info.operator_uuid}")
        return {"success": False, "message": message}

# 设置房间是否公开
@app.post("/api/set_room_is_public")
async def set_room_is_public(info: SetRoomIsPublic):
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id, ["owner", "admin"])
    if passed:
        success, msg = await database.set_room_is_public(info.room_id, info.is_public)
        if success:
            logger.info(f"房间公开状态变更, room_id: {info.room_id}, is_public: {info.is_public}")
            return {"success": True}
        else:
            logger.warning(f"房间公开状态变更失败, room_id: {info.room_id}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"修改房间公开状态权限不足, room_id: {info.room_id}, operator: {info.operator_uuid}")
        return {"success": False, "message": message}

# 加入房间（持久化：注册为房间成员）
@app.post("/api/join_room")
async def join_room(info: JoinRoom):
    room = await database.fetch_room(info.room_id)
    if not room:
        logger.warning(f"加入房间失败, 房间不存在, room_id: {info.room_id}")
        return {"success": False, "message": "房间不存在"}

    if not room[0]["is_public"]:
        is_member = await database.is_room_member(info.room_id, info.user_uuid)
        if not is_member:
            logger.warning(f"加入房间失败, 房间未公开, room_id: {info.room_id}")
            return {"success": False, "message": "房间未公开, 仅成员可加入"}

    banned, msg = await tools.check_user_banned(info.user_uuid)
    if banned:
        logger.warning(f"加入房间失败, 用户被封禁, user_uuid: {info.user_uuid}")
        return {"success": False, "message": msg}

    success, msg = await database.join_room(info.room_id, info.user_uuid)
    if not success:
        return {"success": False, "message": msg}

    playlist_ids = [t["track_id"] for t in room_playlist.get(info.room_id, [])]

    room_members_records = await database.fetch_room_members(info.room_id)
    room_members = [m["user_uuid"] for m in room_members_records]

    async with state_lock:
        pstate = room_playback_state.get(info.room_id)
    if pstate:
        pstate = dict(pstate)
    logger.info(f"用户加入房间, room_id: {info.room_id}, user_uuid: {info.user_uuid}, playstatus={pstate is not None}")
    return {"success": True,
            "details": {
                "room_id": info.room_id,
                "room_name": room[0]["name"],
                "creator_uuid": room[0]["creator_uuid"],
                "is_public": room[0]["is_public"],
                "room_members": room_members,
                "count": len(room_members),
                "playlist": playlist_ids,
                "playstatus": pstate,
            }}

# 退出房间（持久化：从房间注销成员身份）
@app.post("/api/leave_room")
async def leave_room(info: LeaveRoom):
    user_in_room = await database.fetch_user_room(info.user_uuid, info.room_id)
    if not user_in_room:
        logger.warning(f"离开房间失败, 用户不在房间中, room_id: {info.room_id}, user_uuid: {info.user_uuid}")
        return {"success": False, "message": "用户不在此房间中"}

    if user_in_room["role"] == "owner":
        logger.warning(f"离开房间失败, 房主不能离开, room_id: {info.room_id}, user_uuid: {info.user_uuid}")
        return {"success": False, "message": "房主不能离开房间，请先转让房主身份或删除房间"}

    success, msg = await database.leave_room(info.room_id, info.user_uuid)
    if not success:
        return {"success": False, "message": msg}

    async with state_lock:
        user_rooms.pop(info.user_uuid, None)
        if info.room_id in room_users:
            try:
                room_users[info.room_id].remove(info.user_uuid)
            except ValueError:
                pass

    await ws_manager.broadcast_to_room(info.room_id, {
        "type": "user_left",
        "user_uuid": info.user_uuid
    })

    logger.info(f"用户离开房间, room_id: {info.room_id}, user_uuid: {info.user_uuid}")
    return {"success": True}

# 退出房间（仅断开实时同步，保留成员身份）
@app.post("/api/exit_room")
async def exit_room(info: LeaveRoom):
    user_in_room = await database.fetch_user_room(info.user_uuid, info.room_id)
    if not user_in_room:
        logger.warning(f"退出房间失败, 用户不是房间成员, room_id: {info.room_id}, user_uuid: {info.user_uuid}")
        return {"success": False, "message": "用户不在此房间中"}

    async with state_lock:
        user_rooms.pop(info.user_uuid, None)
        if info.room_id in room_users:
            try:
                room_users[info.room_id].remove(info.user_uuid)
            except ValueError:
                pass

    await ws_manager.broadcast_to_room(info.room_id, {
        "type": "user_left",
        "user_uuid": info.user_uuid
    })

    logger.info(f"用户退出房间同步, room_id: {info.room_id}, user_uuid: {info.user_uuid}")
    return {"success": True}

# 踢出指定房间成员
@app.post("/api/kick_room_member")
async def kick_room_member(info: KickRoomMember):
    target = await database.fetch_user_room(info.user_uuid, info.room_id)
    if target and target["role"] == "owner":
        logger.warning(f"踢人失败, 目标是房主, room_id: {info.room_id}, target: {info.user_uuid}")
        return {"success": False, "message": "不能踢出房主"}
    
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id, ["owner", "admin"])
    if passed:
        success, msg = await database.kick_room_member(info.user_uuid, info.room_id)
        if success:
            async with state_lock:
                user_rooms.pop(info.user_uuid, None)
                if info.room_id in room_users:
                    try:
                        room_users[info.room_id].remove(info.user_uuid)
                    except ValueError:
                        pass
            await ws_manager.broadcast_to_room(info.room_id, {
                "type": "user_left",
                "user_uuid": info.user_uuid
            })
            logger.info(f"用户被踢出房间, room_id: {info.room_id}, target: {info.user_uuid}, operator: {info.operator_uuid}")
            return {"success": True}
        else:
            logger.warning(f"踢人失败, room_id: {info.room_id}, target: {info.user_uuid}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"踢人权限不足, room_id: {info.room_id}, operator: {info.operator_uuid}")
        return {"success": False, "message": message}
    
# 设置指定房间成员角色
@app.post("/api/set_room_members_role")
async def set_room_members_role(info: SetRoomMemberRole):

    allowed, msg = await tools.check_room_owner_self_action(
        info.operator_uuid, info.room_id, info.user_uuid, "修改角色"
    )
    if not allowed:
        logger.warning(f"设置房间成员角色被拒绝, room_id: {info.room_id}, target: {info.user_uuid}, operator: {info.operator_uuid}")
        return {"success": False, "message": msg}
    
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id)
    if passed:
        success, msg = await database.set_room_members_role(info.room_id, info.user_uuid, info.role)
        if success:
            logger.info(f"房间成员角色被修改, room_id: {info.room_id}, target: {info.user_uuid}, role: {info.role}, operator: {info.operator_uuid}")
            return {"success": True}
        else:
            logger.warning(f"设置房间成员角色失败, room_id: {info.room_id}, target: {info.user_uuid}")
            return {"success": False, "message": msg}
    else:
        logger.warning(f"设置房间成员角色权限不足, room_id: {info.room_id}, operator: {info.operator_uuid}")
        return {"success": False, "message": message}
    
# 查询房间信息
@app.post("/api/fetch_room")
async def fetch_room(info: FetchRoom):
    room_info = await database.fetch_room(info.room_id)
    if not room_info:
        return {"success": False, "message": "房间不存在"}
    
    room_data = room_info[0]
    
    if not room_data["is_public"]:
        if not info.user_uuid:
            return {"success": False, "message": "房间未公开"}
        is_member = await database.is_room_member(info.room_id, info.user_uuid)
        if not is_member:
            return {"success": False, "message": "房间未公开"}
    
    room = await database.fetch_room_members(info.room_id)
    room_members = []
    room_members_detail = []  # 含角色信息

    for dicts in room:
        room_members.append(dicts["user_uuid"])
        room_members_detail.append({
            "user_uuid": dicts["user_uuid"],
            "role": dicts["role"],
        })

    async with state_lock:
        pstate = room_playback_state.get(info.room_id)
    if pstate:
        pstate = dict(pstate)  # 浅拷贝，避免后续修改影响
    logger.info(f"[fetch_room] room_id={info.room_id}, playstatus={pstate}")
    return {
        "success": True,
        "room_id": info.room_id,
        "room_name": room_data["name"],
        "creator_uuid": room_data["creator_uuid"],
        "is_public": room_data["is_public"],
        "room_members": room_members,
        "room_members_detail": room_members_detail,
        "count": len(room_members),
        "playstatus": pstate,
    }

# 获取音乐链接
@app.get("/api/music_link_get")
async def music_link_get(info: MusicLinkGet):
    tid = info.track_id

    # 检查缓存
    cached = _music_cache.get(tid)
    if cached:
        cached_url, ts = cached
        if time.time() - ts < _MUSIC_CACHE_TTL:
            logger.info(f"音乐链接缓存命中, track_id: {tid}")
            return {"success": True, "url": cached_url, "cached": True}
        else:
            del _music_cache[tid]

    url = await music_link_fetcher.music_link_get(track_id=tid)
    if not url:
        logger.warning(f"获取音乐链接失败, track_id: {tid}")
        return {"success": False, "message": "url获取失败"}

    _music_cache[tid] = (url, time.time())
    logger.info(f"获取音乐链接成功(已缓存), track_id: {tid}")
    return {"success": True, "url": url}

# 获取歌词链接
@app.get("/api/lrc_link_get")
async def lrc_link_get(info: LrcLinkGet):
    lrc = await music_link_fetcher.lrc_link_get(track_id=info.track_id)
    if not lrc:
        logger.warning(f"获取歌词链接失败, track_id: {info.track_id}")
        return {"success": False, "message": "url获取失败"}
    
    logger.info(f"获取歌词链接成功, track_id: {info.track_id}")
    return {"success": True, "lrc": lrc}

# 获取公开房间列表
@app.get("/api/rooms")
async def get_rooms():
    rooms = await database.fetch_public_rooms()
    return {"success": True, "rooms": rooms}

# 登出（仅持久化状态，在线状态清理由 WS 断开处理）
@app.post("/api/logout")
async def logout(info: FetchUser):
    await database.set_status(info.user_uuid, "offline")
    logger.info(f"用户登出, user_uuid: {info.user_uuid}")
    return {"success": True}

# 转让房主身份
@app.post("/api/transfer_room")
async def transfer_room(info: TransferRoom):
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id)
    if not passed:
        logger.warning(f"转让房主权限不足, room_id: {info.room_id}, operator: {info.operator_uuid}")
        return {"success": False, "message": message}

    success, msg = await database.transfer_room(info.room_id, info.operator_uuid, info.to_uuid)
    if success:
        await ws_manager.broadcast_to_room(info.room_id, {
            "type": "room_info",
            "owner": info.to_uuid,
            "message": "房主已变更"
        })
        logger.info(f"房主转让成功, room_id: {info.room_id}, from: {info.operator_uuid}, to: {info.to_uuid}")
        return {"success": True}
    else:
        logger.warning(f"房主转让失败, room_id: {info.room_id}")
        return {"success": False, "message": msg}

# 获取用户加入的房间列表（含房间名等完整信息）
@app.get("/api/user/{user_uuid}/rooms")
async def get_user_rooms(user_uuid: str):
    rooms = await database.fetch_user_rooms_info(user_uuid)
    if rooms is None:
        return {"success": False, "message": "查询失败"}
    return {"success": True, "rooms": rooms}

# ===================== 图床 API =====================

# 上传图片 → 返回 url + image_id
@app.post("/api/upload_image")
async def upload_image(request: Request, file: UploadFile = File(...)):
    # 仅允许图片类型
    if file.content_type not in ("image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp"):
        raise HTTPException(status_code=400, detail="不支持的文件类型，仅允许 jpeg/png/gif/webp/bmp")

    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:  # 10MB 上限
        raise HTTPException(status_code=400, detail="图片大小不能超过 10MB")

    image_id = uuid.uuid4().hex
    ext = os.path.splitext(file.filename or ".png")[1] or ".png"
    filename = f"{image_id}{ext}"
    filepath = IMAGES_DIR / filename
    filepath.write_bytes(contents)

    # 用请求的 Host 头构造 URL，避免 0.0.0.0 的问题
    req_host = request.headers.get("host", "")
    if req_host and not req_host.startswith("0.0.0.0"):
        url = f"http://{req_host}/api/images/{image_id}"
    else:
        cfg_host = config.get("connection", {}).get("host", "127.0.0.1")
        if cfg_host == "0.0.0.0":
            cfg_host = "127.0.0.1"
        port = config.get("connection", {}).get("port", 8001)
        url = f"http://{cfg_host}:{port}/api/images/{image_id}"

    logger.info(f"图片上传成功: {filename} ({len(contents)} bytes)")
    return {"success": True, "image_id": image_id, "url": url}

# 获取图片 → 返回图片文件
@app.get("/api/images/{image_id}")
async def get_image(image_id: str):
    for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"):
        filepath = IMAGES_DIR / f"{image_id}{ext}"
        if filepath.exists():
            content_type_map = {
                ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp",
            }
            return FileResponse(filepath, media_type=content_type_map.get(ext, "image/png"))
    raise HTTPException(status_code=404, detail="图片不存在")

# 删除图片
@app.delete("/api/upload_image")
async def delete_image(image_id: str):
    deleted = False
    for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"):
        filepath = IMAGES_DIR / f"{image_id}{ext}"
        if filepath.exists():
            filepath.unlink()
            deleted = True
            logger.info(f"图片已删除: {filepath.name}")
    if deleted:
        return {"success": True}
    raise HTTPException(status_code=404, detail="图片不存在")

# 图片代理（解决网易云CDN在Flutter桌面端加载失败问题）
@app.get("/api/img_proxy")
async def img_proxy(url: str):
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "image/jpeg")
            return Response(content=resp.content, media_type=content_type)
    except Exception as e:
        logger.warning(f"图片代理获取失败: {url} - {e}")
        raise HTTPException(status_code=404, detail="image not found")

# 健康检查
@app.get("/health")
async def health_check():
    return {"status": "ok"}