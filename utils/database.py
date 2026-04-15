"""
-*-coding: utf-8-*-
本模块用于管理数据库
"""

import logging
import yaml
import uuid
from utils import tools
from pathlib import Path
from datetime import datetime, timezone
import asyncio
import aiosqlite

logger = logging.getLogger(__name__)
# 数据库路径
db_path = Path(__file__).parent.parent / 'data' / 'database.db'
# 日期时间格式常量
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# 读取配置
config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

async def init_database():
    """
    初始化所有数据表
    如果表不存在则自动创建
    Returns:
        成功返回True
    """
    async with aiosqlite.connect(db_path) as db:
        # Init
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA foreign_keys=ON")
        
        # 用户表
        await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    user_uuid TEXT UNIQUE NOT NULL,
                    user_name TEXT UNIQUE NOT NULL,
                    pwd TEXT NOT NULL, 
                    ip TEXT NOT NULL,
                    nickname TEXT NOT NULL,
                    avatar_url TEXT,
                    avatar_key TEXT,
                    role TEXT DEFAULT 'user' CHECK(role IN ('user', 'admin')),
                    status TEXT DEFAULT 'offline' CHECK(status IN ('online', 'offline')),
                    is_banned BOOLEAN DEFAULT 0,
                    ban_reason TEXT,
                    banned_at TIMESTAMP,
                    pardon_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP
                )
            """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_usernickname ON users(nickname)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_uuid ON users(user_uuid)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(user_name)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)")
        
        # 房间表
        await db.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                room_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                creator_uuid TEXT NOT NULL,
                is_public BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (creator_uuid) REFERENCES users(user_uuid) ON DELETE CASCADE
            )
        """)
        
        # 设置房间表的自增ID从10000开始
        cursor = await db.execute("SELECT seq FROM sqlite_sequence WHERE name = 'rooms'")
        row = await cursor.fetchone()
        if row is None:
            await db.execute("""
                INSERT INTO sqlite_sequence(name, seq) 
                VALUES ('rooms', 9999)
            """)
        await db.commit()
        
        # 房间成员表
        await db.execute("""
            CREATE TABLE IF NOT EXISTS room_members (
                room_id INTEGER NOT NULL,
                user_uuid TEXT NOT NULL,
                role TEXT DEFAULT 'member' CHECK(role IN ('owner', 'admin', 'member')),
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (room_id, user_uuid),
                FOREIGN KEY (room_id) REFERENCES rooms(room_id) ON DELETE CASCADE,
                FOREIGN KEY (user_uuid) REFERENCES users(user_uuid) ON DELETE CASCADE
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_room_members_room_id ON room_members(room_id)")
        
        # 封禁IP表
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ban_ip (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip TEXT UNIQUE NOT NULL,
                ban_reason TEXT,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pardon_time TIMESTAMP,
                operator_uuid TEXT,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (operator_uuid) REFERENCES users(user_uuid) ON DELETE SET NULL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ban_ip_value ON ban_ip(ip)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ban_ip_is_active ON ban_ip(is_active)")
        
        hash_pwd = tools.hash_pwd(config.get("database").get("default_admin_pwd"))
        name = config.get("database").get("default_admin")
        await db.execute("""
            INSERT INTO users (user_uuid, user_name, pwd, ip, nickname, role)
            VALUES (?, ?, ?, ?, ?, ?)
        """,(str(uuid.uuid4()), name, hash_pwd, "127.0.0.1", name, "admin"))
        await db.commit()
        
    logger.info("数据库初始化完成!")
    return True

async def register(user_uuid, user_name, pwd, ip, nickname):
    """
    注册新用户
    
    Args:
        user_uuid: 用户UUID（唯一）
        user_name: 用户名（唯一）
        pwd: 密码
        ip: 用户IP
        nickname: 昵称
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    
    hash_pwd = tools.hash_pwd(pwd)
    
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            await db.execute("""
                INSERT INTO users (user_uuid, user_name, pwd, ip, nickname)
                VALUES (?, ?, ?, ?, ?)
            """,(user_uuid, user_name, hash_pwd, ip, nickname))
            await db.commit()
        except Exception as e:
            if "user_name" in str(e):
                logger.warning(f"用户名已存在: {user_name}")
                return False, f"用户名已存在: {user_name}"
            
            logger.error(f"用户创建失败:{e}")
            return False, "系统错误，请稍后再试"
        
    logger.info(f"用户:{nickname} - UUID:{user_uuid} 创建成功！")
    return True, None

async def login(user_name, pwd, ip):
    """
    登录验证
    
    Args:
        user_name: 用户名（唯一）
        pwd: 密码
        ip: 用户IP
        
    Returns:
        成功返回 (True, user_uuid)
        失败返回 (False, error_message)
    """
    logger.info(f"收到登录请求 用户名: {user_name}, IP: {ip}")
    
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute("""
                SELECT * FROM users WHERE user_name = ?
            """,(user_name,))
            
            row = await cursor.fetchone()
            if not row:
                return False, "用户名或密码错误"
            user = dict(row)
            
            # 密码验证
            if not tools.verify_password(pwd, user["pwd"]):
                return False, "用户名或密码错误"
            
        except Exception as e:
            logger.error(f"用户登录失败:{e}")
            return False, "系统错误，请稍后再试"
    logger.info(f"用户: {user_name} uuid: {user['user_uuid']} 登录成功")
    return True, user["user_uuid"]

async def change_pwd(user_uuid, old_pwd, new_pwd):
    """
    修改密码
    
    Args:
        user_uuid: 用户 UUID
        old_pwd: 旧密码
        new_pwd: 新密码
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    logger.info(f"收到修改密码请求 用户uuid: {user_uuid}")
    
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute("""
                SELECT * FROM users WHERE user_uuid = ?
            """,(user_uuid,))
            row = await cursor.fetchone()
            
            if not row:
                logger.info(f"修改密码失败: 用户不存在")
                return False, "用户不存在"
            
            if not tools.verify_password(old_pwd, row["pwd"]):
                logger.info(f"修改密码失败: 原密码错误")
                return False, "原密码错误"
            
            new_pwd_hash = tools.hash_pwd(new_pwd)
            await db.execute("""
                UPDATE users SET pwd = ? WHERE user_uuid = ?
            """, (new_pwd_hash, user_uuid))
            await db.commit()
            
            logger.info(f"修改密码成功")
            return True, None
            
        except Exception as e:
            logger.error(f"修改密码失败:{e}")
            return False, "系统错误，请稍后再试"

async def _set_item(table, where_field, where_value, set_field, set_value):
    """
    设置项目
    
    Args:
        table: 要设置的表
        where_field: WHERE 条件字段名
        where_value: WHERE 条件值
        set_field: 要更新的字段名
        set_value: 更新的值
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            cursor = await db.execute(f"""
                UPDATE {table} SET {set_field} = ? WHERE {where_field} = ?
            """,(set_value, where_value))
            
            if cursor.rowcount == 0:
                logger.warning(f"{where_field} 不存在，{set_field} 更新失败")
                return False, f"{where_field} 不存在，{set_field} 更新失败"
            await db.commit()
            
            logger.info(f"{where_field}: {where_value} - {set_field}: {set_value} 设置成功！")
            return True, None
        except Exception as e:
            logger.error(f"{set_field} 设置失败:{e}")
            return False, "设置失败, 请稍后再试"


# 用户操作函数
async def set_avatar(user_uuid, avatar_url, avatar_key):
    """
    设置用户头像
    
    Args:
        user_uuid: 用户UUID（唯一）
        avatar_url: 头像url
        avatar_key: 头像唯一密钥
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            cursor = await db.execute("""
                UPDATE users SET avatar_url = ?, avatar_key = ? WHERE user_uuid = ?
            """,(avatar_url, avatar_key, user_uuid))
            
            if cursor.rowcount == 0:
                logger.warning(f"{user_uuid} 不存在，头像更新失败")
                return False, f"{user_uuid} 不存在，头像更新失败"
            await db.commit()
            
            logger.info(f"用户UUID: {user_uuid} - 头像设置成功！")
            return True, None
        except Exception as e:
            logger.error(f"头像设置失败:{e}")
            return False, "设置失败, 请稍后再试"

async def set_ip(user_uuid, ip):
    """
    设置用户IP
    
    Args:
        user_uuid: 用户UUID（唯一）
        ip: 用户ip
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    req = await _set_item("users", "user_uuid", user_uuid, "ip", ip)
    return req

async def set_nickname(user_uuid, nickname):
    """
    设置用户昵称
    
    Args:
        user_uuid: 用户UUID（唯一）
        nickname: 用户昵称
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    req = await _set_item("users", "user_uuid", user_uuid, "nickname", nickname)
    return req

async def set_status(user_uuid, status):
    """
    设置用户状态
    
    Args:
        user_uuid: 用户UUID（唯一）
        status: 用户状态
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    req = await _set_item("users", "user_uuid", user_uuid, "status", status)
    return req

async def ban_user(user_uuid="", ban_reason=None, pardon_time=None):
    """
    封禁用户
    
    Args:
        user_uuid: 用户UUID（唯一）
        ban_reason: 封禁理由
        pardon_time: 解封时间(UTC时间)
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            cursor = await db.execute("""
                UPDATE users 
                SET is_banned = 1,
                    ban_reason = ?,
                    banned_at = ?,
                    pardon_time = ?
                WHERE user_uuid = ?
            """,(ban_reason, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), pardon_time, user_uuid))
            if cursor.rowcount == 0:
                logger.warning(f"用户 {user_uuid} 不存在，封禁失败")
                return False, f"用户 {user_uuid} 不存在，封禁失败"
            await db.commit()
            logger.info(f"用户 {user_uuid} 已被封禁")
            return True, None
        except Exception as e:
            logger.error(f"封禁失败: {e}")
            return False, "封禁失败, 请稍后再试"
        
async def unban_user(user_uuid=""):
    """
    解封用户
    
    Args:
        user_uuid: 用户UUID（唯一）
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            cursor = await db.execute("""
                UPDATE users 
                SET is_banned = 0,
                    ban_reason = NULL,
                    banned_at = NULL,
                    pardon_time = NULL
                WHERE user_uuid = ?
            """,(user_uuid,))
            if cursor.rowcount == 0:
                logger.warning(f"用户 {user_uuid} 不存在，解封失败")
                return False, f"用户 {user_uuid} 不存在，解封失败"
            await db.commit()
            logger.info(f"用户 {user_uuid} 已被解封")
            return True, None
        except Exception as e:
            logger.error(f"解封失败: {e}")
            return False, "解封失败, 请稍后再试"
        
async def ban_ip(ip, ban_reason=None, pardon_time=None, operator_uuid=None):
    """
    封禁IP
    
    Args:
        ip: IP地址
        ban_reason: 封禁理由
        pardon_time: 解封时间(UTC时间)
        operator_uuid: 操作者UUID
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            cursor = await db.execute("""
                SELECT id FROM ban_ip
                WHERE ip = ? AND is_active = 1
            """,(ip,))
            
            existing = await cursor.fetchone()
            if existing:
                logger.warning(f"IP: {ip} 已被封禁")
                cursor = await db.execute("""
                    UPDATE ban_ip 
                    SET ban_reason = ?, pardon_time = ?, operator_uuid = ?
                    WHERE ip = ? AND is_active = 1
                """,(ban_reason, pardon_time, operator_uuid, ip))
                await db.commit()
                logger.info(f"IP: {ip} 封禁信息已更新")
                return True, None

            await db.execute("""
                INSERT INTO ban_ip (ip, ban_reason, pardon_time, operator_uuid)
                VALUES (?, ?, ?, ?)
            """,(ip, ban_reason, pardon_time, operator_uuid))
            await db.commit()
            logger.info(f"IP {ip} 封禁成功")
            return True, None
            
        except Exception as e:
            logger.error(f"封禁IP失败: {e}")
            return False, "封禁IP失败, 请稍后再试"

async def unban_ip(ip, operator_uuid=None):
    """
    解封IP
    
    Args:
        ip: IP地址
        operator_uuid: 操作者UUID
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            cursor = await db.execute("""
                UPDATE ban_ip 
                SET is_active = 0,
                    pardon_time = ?,
                    operator_uuid = ?
                WHERE ip = ? AND is_active = 1
            """, (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), operator_uuid, ip))
            
            if cursor.rowcount == 0:
                logger.warning(f"IP {ip} 不存在，解封失败")
                return False, f"IP {ip} 不存在，解封失败"
            
            await db.commit()
            logger.info(f"IP {ip} 已被解封")
            return True, None
        
        except Exception as e:
            logger.error(f"解封失败: {e}")
            return False, "解封失败, 请稍后再试"

async def set_role(user_uuid, role):
    """
    设置用户权限
    
    Args:
        user_uuid: 用户UUID（唯一）
        role: 用户权限
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    req = await _set_item("users", "user_uuid", user_uuid, "role", role)
    return req

async def set_lastlogin(user_uuid, last_login):
    """
    设置用户最后登录时间
    
    Args:
        user_uuid: 用户UUID（唯一）
        last_login: 用户最后登录时间
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    req = await _set_item("users", "user_uuid", user_uuid, "last_login", last_login)
    return req


# 房间操作函数
async def create_room(name="", creator_uuid="", is_public=1):
    """
    创建新房间
    
    Args:
        name: 房间名称
        creator_uuid: 创建者UUID
        is_public: 是否公开
        
    Returns:
        成功返回 (True, room_id)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            # 创建房间
            cursor = await db.execute("""
                INSERT INTO rooms (name,creator_uuid,is_public)
                VALUES (?, ?, ?)
            """,(name, creator_uuid, is_public))
            room_id = cursor.lastrowid

            # 加入房间
            await db.execute("""
                INSERT INTO room_members (room_id,user_uuid,role)
                VALUES (?, ?, ?)
            """,(room_id, creator_uuid, "owner"))
            await db.commit()
        
            logger.info(f"房间:{name} - 创建者:{creator_uuid} 创建成功！")
            return True, room_id
            
        except Exception as e:
            await db.rollback()
            logger.error(f"房间创建失败:{e}")
            return False, "房间创建失败, 请稍后再试"


async def delete_room(room_id):
    """
    删除房间
    
    Args:
        room_id: 房间ID
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            # 删除房间
            cursor = await db.execute("""
                DELETE FROM rooms
                WHERE room_id = ?
            """,(room_id,))
            await db.commit()
            
            
            if cursor.rowcount == 0:
                logger.warning(f"房间 {room_id} 不存在，删除失败")
                return False, f"房间 {room_id} 不存在，删除失败"
            
            logger.info(f"房间 {room_id} 删除成功！")
            return True, None
            
        except Exception as e:
            await db.rollback()
            logger.error(f"房间删除失败:{e}")
            return False, "房间删除失败"
            
async def set_room_name(room_id, name):
    """
    设置房间名称
    
    Args:
        room_id: 房间ID
        name: 房间名称
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    req = await _set_item("rooms", "room_id", room_id, "name", name)
    return req

async def set_room_is_public(room_id, is_public):
    """
    设置房间是否公开
    
    Args:
        room_id: 房间ID
        is_public: 房间是否公开
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    req = await _set_item("rooms", "room_id", room_id, "is_public", is_public)
    return req

# 加入房间
async def join_room(room_id, user_uuid):
    """
    加入房间
    
    Args:
        room_id: 房间id
        user_uuid: 用户UUID
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        
        try:
            await db.execute("""
                INSERT INTO room_members (room_id,user_uuid)
                VALUES (?, ?)
            """,(room_id, user_uuid))
            await db.commit()
            
            logger.info(f"房间:{room_id} - 成员:{user_uuid} 加入成功")
            return True, None
            
        except aiosqlite.IntegrityError:
            return False, "你已在该房间中"
        
        except Exception as e:
            logger.error(f"房间加入失败:{e}")
            return False, "房间加入失败, 请稍后再试"
        
# 退出房间
async def leave_room(room_id, user_uuid):
    """
    退出房间
    
    Args:
        room_id: 房间id
        user_uuid: 用户UUID
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            cursor = await db.execute("""
                DELETE FROM room_members
                WHERE room_id = ? AND user_uuid = ?
            """,(room_id, user_uuid))
            await db.commit()
            
            if cursor.rowcount == 0:
                logger.warning(f"用户 {user_uuid} 不在房间 {room_id} 中")
                return False, f"用户 {user_uuid} 不在房间 {room_id} 中"
            
            logger.info(f"用户 {user_uuid} 退出房间 {room_id}")
            return True, None
            
        except Exception as e:
            logger.error(f"房间退出失败:{e}")
            return False, "房间退出失败, 请稍后再试"
        
# 踢出房间成员
async def kick_room_member(user_uuid, room_id):
    """
    踢出指定房间成员
    
    Args:
        user_uuid: 用户UUID
        room_id: 房间ID
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            cursor = await db.execute("""
                DELETE FROM room_members
                WHERE room_id = ? AND user_uuid = ?
            """,(room_id, user_uuid))
            await db.commit()
            
            if cursor.rowcount == 0:
                logger.warning(f"用户 {user_uuid} 不在房间 {room_id} 中")
                return False, f"用户 {user_uuid} 不在房间 {room_id} 中"
            
            logger.info(f"用户 {user_uuid} 被踢出房间 {room_id}")
            return True, None
            
        except Exception as e:
            logger.error(f"踢出成员失败:{e}")
            return False, "踢出成员失败, 请稍后再试"

# 设置指定房间成员角色
async def set_room_members_role(room_id, user_uuid, role):
    """
    设置指定房间成员角色
    
    Args:
        room_id: 房间ID
        user_uuid: 用户UUID
        role: 角色
        
    Returns:
        成功返回 (True, None)
        失败返回 (False, error_message)
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            cursor = await db.execute("""
                UPDATE room_members SET role = ? WHERE room_id = ? AND user_uuid = ?
            """,(role, room_id, user_uuid))
            
            if cursor.rowcount == 0:
                logger.warning("room_id或user_uuid 不存在，role 更新失败")
                return False, "room_id或user_uuid 不存在，role 更新失败"
            await db.commit()
            
            logger.info(f"room_id: {room_id} - {user_uuid}-role: {role} 设置成功！")
            return True, None
        except Exception as e:
            logger.error(f"role 设置失败:{e}")
            return False, "role 设置失败"

# 查询函数
async def _db_fetch(table, where_field, where_value):
    """
    查询多个个项目
    
    Args:
        table: 要查询的表
        where_field: WHERE 条件字段名
        where_value: WHERE 条件值
        
    Returns:
        成功返回 [dict, dict...] 列表
        无数据返回空列表 []
        发生异常返回 None
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute(f"""
                SELECT * FROM {table} WHERE {where_field} = ?
            """,(where_value,))
            
            rows = await cursor.fetchall()
            if not rows:
                logger.warning(f"{table} - {where_field}: {where_value} 未查询到数据")
                return []
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"{where_field} 查询失败:{e}")
            return None

async def fetch_user(user_uuid):
    """
    通过uuid查询用户信息
    
    Args:
        user_uuid: 用户uuid
        
    Returns:
        成功返回 [dict, dict...] 列表
        无数据返回空列表 []
        发生异常返回 None
    """
    result = await _db_fetch("users", "user_uuid", user_uuid)
    return result

async def fetch_ban_ip(ip):
    """
    通过ip查询IP封禁信息
    
    Args:
        ip: IP
        
    Returns:
        成功返回 [dict, dict...] 列表
        无数据返回空列表 []
        发生异常返回 None
    """
    result = await _db_fetch("ban_ip", "ip", ip)
    return result

async def fetch_room(room_id):
    """
    通过房间id查询房间信息
    
    Args:
        room_id: 房间id
        
    Returns:
        成功返回 [dict, dict...] 列表
        无数据返回空列表 []
        发生异常返回 None
    """
    result = await _db_fetch("rooms", "room_id", room_id)
    return result

async def fetch_room_members(room_id):
    """
    通过房间id查询加入房间用户
    
    Args:
        room_id: 房间id
        
    Returns:
        成功返回 [dict, dict...] 列表
        无数据返回空列表 []
        发生异常返回 None
    """
    result = await _db_fetch("room_members", "room_id", room_id)
    return result

async def fetch_user_joined(user_uuid):
    """
    通过用户uuid查询加入的房间信息
    
    Args:
        user_uuid: 用户uuid
        
    Returns:
        成功返回 [dict, dict...] 列表
        无数据返回空列表 []
        发生异常返回 None
    """
    result = await _db_fetch("room_members", "user_uuid", user_uuid)
    return result

async def fetch_user_room(user_uuid, room_id):
    """
    通过用户uuid和房间id查询用户在房间的信息
    
    Args:
        user_uuid: 用户uuid
        room_id: 房间id
        
    Returns:
        成功返回 dict
        无数据返回 None
        发生异常返回 None
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute("""
                SELECT * FROM room_members WHERE user_uuid = ? AND room_id = ?
            """,(user_uuid, room_id))
            
            row = await cursor.fetchone()
            if not row:
                logger.warning(f"room_members - 房间: {room_id} - 用户UUID: {user_uuid} 未查询到数据")
                return None
            
            return dict(row)
            
        except Exception as e:
            logger.error(f"查询失败:{e}")
            return None
    
async def count_admins():
    """
    统计管理员数量
    
    Returns:
        管理员数量，查询失败返回 0
    """
    async with aiosqlite.connect(db_path) as db:
        try:
            cursor = await db.execute("""
                SELECT COUNT(*) FROM users WHERE role = 'admin'
            """)
            result = await cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"统计管理员数量失败: {e}")
            return 0

# 测试
async def test():
    await init_database() 
    result = await fetch_user("123123")
    logger.info(result)

if __name__ == "__main__":
    
    # 临时配置 logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    asyncio.run(test())
