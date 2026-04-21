"""
-*-coding: utf-8-*-
本模块用于管理api连接
"""

import logging
import yaml
import random
from pathlib import Path
from datetime import datetime, timezone
import asyncio
import uuid
from utils import database
from utils import tools
from utils import music_link_fetcher
from pydantic import BaseModel, Field
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request

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

# 临时数据表
heartbeats = {}     # 用户心跳时间表 {user_uuid: time, ...}
user_rooms = {}     # 用户加入房间表 {user_uuid: room_id, ...}
room_users = {}     # 房间内用户表 {room_id: [user_uuid, ...], ...}
room_leader_user = {}        # 各个房间同步标准用户表 {room_id: user_uuid, ...}
room_playstatus = {}        # 各个房间播放的状态表 {room_id: {track_id: int, is_played: bool, pos: int}, ...}
room_playlist = {}          # 各个房间的音乐列表 {room_id: [track_id, ...], ...}

# 初始化房间内用户表
async def init_room_users():
    room_list = await database.fetch_all_room_id()
    room_users = dict.fromkeys(room_list, [])
    return room_users

# 同步房间列表循环
async def room_users_sync():
    while True:
        await asyncio.sleep(10)
        room_list = await database.fetch_all_room_id()
        for room_id in room_users:
            if room_id not in room_list:
                room_users[room_id] = []

HEARTBEAT_TIMEOUT = config.get("connection").get("heartbeat_timeout")

state_lock = asyncio.Lock()

# 播放状态
class PlaybackState(BaseModel):
    track_id: str
    is_played: bool
    pos: int = Field(..., ge=0)
    timestamp: str

class HeartbeatRequest(BaseModel):
    user_uuid: str
    room_id: int | None = None
    playback_state: Optional[PlaybackState] = None

# 心跳循环
async def clean_heartbeats():
    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        
        async with state_lock:
            for uuid, t in list(heartbeats.items()):
                if (now - t).total_seconds() > HEARTBEAT_TIMEOUT:
                    heartbeats.pop(uuid, None)
                    room_users[user_rooms[uuid]].remove(uuid)
                    user_rooms.pop(uuid, None)

# 歌曲时间同步循环
async def track_pos_align():
    while True:
        await asyncio.sleep(1)
        async with state_lock:
            for room_id, detail in room_playstatus:
                if detail["is_played"]:
                    room_playstatus[room_id]["pos"] = detail["pos"] + 1000
                    
# 时间同步基准用户设置循环
async def room_leader_user_set():
    while True:
        await asyncio.sleep(10)
        async with state_lock:
            all_rooms = list(room_users.keys)
            be_set_rooms = list(room_leader_user.keys)
            rooms = [room for room in all_rooms if room not in be_set_rooms]
            for room in rooms:
                if room_users[room] is not []:
                    users = room_users[room]
                    room_leader_user[room] = random.choice(users)
                    
@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_database()
    room_users = await init_room_users()
    logger.info("房间内用户表初始化成功")
    heartbeat_task = asyncio.create_task(clean_heartbeats())
    logger.info("心跳清理任务已启动")
    pos_align_task = asyncio.create_task(track_pos_align())
    logger.info("歌曲时间同步任务已启动")
    room_leader_user_set_task = asyncio.create_task(room_leader_user_set())
    logger.info("时间同步基准用户设置任务已启动")
    room_users_sync_task = asyncio.create_task(room_users_sync())
    logger.info("同步房间列表任务已启动")
    
    yield
    heartbeat_task.cancel()
    logger.info("心跳清理任务已停止")
    pos_align_task.cancel()
    logger.info("歌曲时间同步任务已停止")
    room_leader_user_set_task.cancel()
    logger.info("时间同步基准用户设置任务已停止")
    room_users_sync_task.cancel()
    logger.info("同步房间列表任务已停止")

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
    avatar_key: str

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
    
class MusicLinkGet(BaseModel):
    track_id: str

class LrcLinkGet(BaseModel):
    track_id: str
    
class PlaybackSync(BaseModel):
    room_id: str
    track_id: str
    position: int
    is_played: bool
    
    

# api处理
# 心跳api
@app.post("/heartbeat")
async def heartbeat(info: HeartbeatRequest):
    async with state_lock:
        heartbeats[info.user_uuid] = datetime.now(timezone.utc)
        
        if info.room_id is None:
            try:
                room_users[user_rooms[info.user_uuid]].remove(info.user_uuid)
                user_rooms.pop(info.user_uuid, None)
            except ValueError or KeyError:
                logger.debug("用户未加入任何房间，跳过删除")
            except Exception as e:
                logger.error(f"用户房间表修改失败: {e}")
                
        else:
            try:
                user_rooms[info.user_uuid] = info.room_id
                if info.user_uuid not in room_users[info.room_id]:
                    room_users[info.room_id].append(info.user_uuid)
            except Exception as e:
                return {"status": False, "message": "房间ID不存在"}
        
        if info.user_uuid == room_leader_user[info.room_id]:
            status = dict(info.playback_state)
            now = datetime.now(timezone.utc)
            status["pos"] = status["pos"] + (now.timestamp() - status["timestamp"].timestamp()) * 1000
            status.pop["timestamp", None]
            room_playstatus[info.room_id] = status
    return {"status": True}

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
        return {"success": False, "message": msg}
        
    user_uuid = str(uuid.uuid4())
    success, msg = await database.register(user_uuid, info.user_name, info.pwd, client_ip, info.nickname)
    
    if success:
        
        success1, _ = await database.set_status(user_uuid, "online")
        success2, _ = await database.set_lastlogin(user_uuid, datetime.now(timezone.utc).strftime(tools.DATETIME_FORMAT))
        if not success1 or not success2:
            return {"success": False, "message": "系统错误，请稍后再试"}
        
        return {"success": True, "user_uuid": user_uuid}
    else:
        return {"success": False, "message": msg}

# 登录api
@app.post("/api/login")
async def login(info: Login, request: Request):
    client_ip = tools.get_client_ip(request)
    
    banned, msg = await tools.check_ip_banned(client_ip)
    if banned:
        return {"success": False, "message": msg}

    success, result = await database.login(info.user_name, info.pwd, client_ip)
    
    if success:
        banned, msg = await tools.check_user_banned(result)
        if banned:
            return {"success": False, "message": msg}

        success1, _ = await database.set_status(result, "online")
        success2, _ = await database.set_lastlogin(result, datetime.now(timezone.utc).strftime(tools.DATETIME_FORMAT))
        success3, _ = await database.set_ip(result, client_ip)
        if not success1 or not success2 or not success3:
            return {"success": False, "message": "系统错误，请稍后再试"}
        
        return {"success": True, "user_uuid": result}
    else:
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
        return {"success": False, "message": "用户不存在"}
    user_info = req[0]
    
    banned, msg = await tools.check_user_banned(info.user_uuid)
    if banned:
        return {"success": False, "message": msg}
    
    if user_info["status"] == "offline":
        success1, _ = await database.set_status(user_info["user_uuid"], "online")
        success2, _ = await database.set_ip(user_info["user_uuid"], client_ip)
        success3, _ = await database.set_lastlogin(user_info["user_uuid"], datetime.now(timezone.utc).strftime(tools.DATETIME_FORMAT))
        if not success1 or not success2 or not success3:
            return {"success": False, "message": "系统错误，请稍后再试"}
    
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
        return {"success": True}
    else:
        return {"success": False, "message": msg}

# 修改头像api
@app.post("/api/update_avatar")
async def update_avatar(info: UpdateAvatar):
    success, msg = await database.set_avatar(info.user_uuid, info.avatar_url, info.avatar_key)
    if success:
        return {"success": True}
    else:
        return {"success": False, "message": msg}
    
# 修改昵称api
@app.post("/api/update_nickname")
async def update_nickname(info: UpdateNickname):
    success, msg = await database.set_nickname(info.user_uuid, info.nickname)
    if success:
        return {"success": True}
    else:
        return {"success": False, "message": msg}

# 封禁用户
@app.post("/api/ban_user")
async def ban_user(info: BanUser):
    # 防止管理员封禁自己
    if info.operator_uuid == info.user_uuid:
        return {"success": False, "message": "不能封禁自己"}
    
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:
        success, msg = await database.ban_user(info.user_uuid, info.ban_reason, info.pardon_time)
        if success:
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}

# 解封用户
@app.post("/api/unban_user")
async def unban_user(info: UnbanUser):
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:
        success, msg = await database.unban_user(info.user_uuid)
        if success:
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}
    
# 封禁ip
@app.post("/api/ban_ip")
async def ban_ip(info: BanIP):
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:
        success, msg = await database.ban_ip(info.ip, info.ban_reason, info.pardon_time, info.operator_uuid)
        if success:
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}

# 解封ip
@app.post("/api/unban_ip")
async def unban_ip(info: UnbanIP):
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:
        success, msg = await database.unban_ip(info.ip, info.operator_uuid)
        if success:
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}

# 设置用户权限
@app.post("/api/set_role")
async def set_role(info: SetRole):

    if info.operator_uuid == info.user_uuid:
        return {"success": False, "message": "不能修改自己的权限"}
    
    passed, message = await tools.check_admin_permission(info.operator_uuid)
    if passed:

        if info.role == "user":
            can_demote, msg = await tools.check_last_admin(info.user_uuid)
            if not can_demote:
                return {"success": False, "message": msg}
        
        success, msg = await database.set_role(info.user_uuid, info.role)
        if success:
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}

# 创建房间
@app.post("/api/create_room")
async def create_room(info: CreateRoom):
    user = await database.fetch_user(info.creator_uuid)
    if not user:
        return {"success": False, "message": "用户不存在"}
    
    banned, msg = await tools.check_user_banned(info.creator_uuid)
    if banned:
        return {"success": False, "message": msg}
    
    success, msg = await database.create_room(info.name, info.creator_uuid, info.is_public)
    if success:
        room_users[msg] = []
        return {"success": True, "room_id": msg}
    else:
        return {"success": False, "message": msg}
    
# 删除房间
@app.post("/api/delete_room")
async def delete_room(info: DeleteRoom):
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id)
    if passed:
        success, msg = await database.delete_room(info.room_id)
        if success:
            tmp = room_users.pop(info.room_id, None)
            if tmp is None:
                logger.warning("临时房间表内room_id删除失败")
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}

# 设置房间名称
@app.post("/api/rename_room")
async def rename_room(info: RenameRoom):
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id, ["owner", "admin"])
    if passed:
        success, msg = await database.set_room_name(info.room_id, info.name)
        if success:
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}

# 设置房间是否公开
@app.post("/api/set_room_is_public")
async def set_room_is_public(info: SetRoomIsPublic):
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id, ["owner", "admin"])
    if passed:
        success, msg = await database.set_room_is_public(info.room_id, info.is_public)
        if success:
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}

# 加入房间
@app.post("/api/join_room")
async def join_room(info: JoinRoom):
    room = await database.fetch_room(info.room_id)
    if not room:
        return {"success": False, "message": "房间不存在"}
    
    if not room[0]["is_public"]:
        return {"success": False, "message": "房间未公开"}
    
    banned, msg = await tools.check_user_banned(info.user_uuid)
    if banned:
        return {"success": False, "message": msg}
    
    success, msg = await database.join_room(info.room_id, info.user_uuid)
    if success:
        async with state_lock:
            try:
                user_rooms[info.user_uuid] = info.room_id
                room_users[info.room_id].append(info.user_uuid)
            except Exception:
                pass
            
        playlist = room_playlist[info.room_id]
        playstatus = room_playstatus[info.room_id]
        playstatus["timestamp"] = datetime.now(timezone.utc).strftime(tools.DATETIME_FORMAT)
        
        return {"success": True, 
                "details": {
                    "playlist": playlist, 
                    "playstatus": playstatus
                    }
                }
    else:
        return {"success": False, "message": msg}

# 退出房间
@app.post("/api/leave_room")
async def leave_room(info: LeaveRoom):

    user = await database.fetch_user(info.user_uuid)
    if not user:
        return {"success": False, "message": "用户不存在"}
    
    # 验证用户是否在该房间中
    user_in_room = await database.fetch_user_room(info.user_uuid, info.room_id)
    if not user_in_room:
        return {"success": False, "message": "用户不在此房间中"}
    
    # 检查是否是房主，房主不能直接退出，需要先转让或删除房间
    if user_in_room["role"] == "owner":
        return {"success": False, "message": "房主不能退出房间，请先转让房主身份或删除房间"}
    
    success, msg = await database.leave_room(info.room_id, info.user_uuid)
    if success:
        return {"success": True}
    else:
        return {"success": False, "message": msg}

# 踢出指定房间成员
@app.post("/api/kick_room_member")
async def kick_room_member(info: KickRoomMember):
    target = await database.fetch_user_room(info.user_uuid, info.room_id)
    if target and target["role"] == "owner":
        return {"success": False, "message": "不能踢出房主"}
    
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id, ["owner", "admin"])
    if passed:
        success, msg = await database.kick_room_member(info.user_uuid, info.room_id)
        if success:
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}
    
# 设置指定房间成员角色
@app.post("/api/set_room_members_role")
async def set_room_members_role(info: SetRoomMemberRole):

    allowed, msg = await tools.check_room_owner_self_action(
        info.operator_uuid, info.room_id, info.user_uuid, "修改角色"
    )
    if not allowed:
        return {"success": False, "message": msg}
    
    passed, message = await tools.check_room_permission(info.operator_uuid, info.room_id)
    if passed:
        success, msg = await database.set_room_members_role(info.room_id, info.user_uuid, info.role)
        if success:
            return {"success": True}
        else:
            return {"success": False, "message": msg}
    else:
        return {"success": False, "message": message}
    
# 查询房间信息
@app.post("/api/fetch_room")
async def fetch_room(info: FetchRoom):
    room_info = await database.fetch_room(info.room_id)
    if not room_info:
        return {"success": False, "message": "房间不存在"}
    
    room_data = room_info[0]
    
    if not room_data["is_public"]:
        return {"success": False, "message": "房间未公开"}
    
    room = await database.fetch_room_members(info.room_id)
    room_members = []
    
    for dicts in room:
        room_members.append(dicts["user_uuid"])
    
    return {
        "success": True,
        "room_id": info.room_id,
        "room_name": room_data["name"],
        "creator_uuid": room_data["creator_uuid"],
        "is_public": room_data["is_public"],
        "room_members": room_members,
        "count": len(room_members)
    }

# 获取音乐链接
@app.get("/api/music_link_get")
async def music_link_get(info: MusicLinkGet):
    url = await music_link_fetcher.music_link_get(track_id=info.track_id)
    if not url:
        return {"success": False, "message": "url获取失败"}
    
    return {"success": True, "url": url}

# 获取歌词链接
@app.get("/api/lrc_link_get")
async def lrc_link_get(info: LrcLinkGet):
    lrc = await music_link_fetcher.lrc_link_get(track_id=info.track_id)
    if not lrc:
        return {"success": False, "message": "url获取失败"}
    
    return {"success": True, "lrc": lrc}