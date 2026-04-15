"""
-*-coding: utf-8-*-
工具集
"""

from fastapi import Request
from datetime import datetime, timezone, timedelta
import bcrypt

# 日期时间格式常量
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def get_client_ip(request: Request) -> str:
    """获取客户端真实 IP"""
    # 从 X-Forwarded-For 获取
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    
    # 从 X-Real-IP 获取
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip
    
    # 从 client.host 获取
    return request.client.host

def hash_pwd(pwd):
    """密码哈希加密"""
    pwd_bytes = pwd.encode('utf-8')
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pwd_bytes, salt).decode('utf-8')

def verify_password(pwd, hashed):
    """验证密码"""
    pwd_bytes = pwd.encode('utf-8')
    hashed_bytes = hashed.encode('utf-8')
    return bcrypt.checkpw(pwd_bytes, hashed_bytes)

def utc_to_beijing(utc_str: str) -> str | None:
    """
    UTC 时间转北京时间
    
    Args:
        utc_str: UTC 时间字符串，格式为 "%Y-%m-%d %H:%M:%S"
        
    Returns:
        北京时间字符串，格式错误返回 None
    """
    try:
        utc_time = datetime.strptime(utc_str, DATETIME_FORMAT)
        utc_time = utc_time.replace(tzinfo=timezone.utc)
        beijing_time = utc_time.astimezone(timezone(timedelta(hours=8)))
        return beijing_time.strftime(DATETIME_FORMAT)
    except (ValueError, TypeError) as e:
        return None

async def check_admin_permission(operator_uuid):
    """检查操作者是否为管理员"""
    from utils import database
    
    tmp = await database.fetch_user(operator_uuid)
    if not tmp:
        return False, "操作者不存在"
    
    operator = tmp[0]
    
    if operator["role"] != "admin":
        return False, "权限不足"
    
    return True, ""

async def check_room_permission(operator_uuid, room_id, required_roles=None):
    """
    检查操作者在房间的权限
    
    Args:
        operator_uuid: 操作者 UUID
        room_id: 房间 ID
        required_roles: 允许的角色列表，默认只允许 owner
    """
    from utils import database
    
    if required_roles is None:
        required_roles = ["owner"]
    
    operator = await database.fetch_user_room(operator_uuid, room_id)
    if not operator:
        return False, "操作者不存在"
    
    if operator["role"] not in required_roles:
        return False, "权限不足"
    
    return True, ""

async def check_ip_banned(ip):
    """
    检查 IP 是否被封禁
    
    Returns:
        (False, None) - 未被封禁
        (True, error_message) - 被封禁
    """
    from utils import database
    
    tmp = await database.fetch_ban_ip(ip)
    if not tmp:
        return False, None
    
    ban_info = tmp[0]
    if not ban_info["is_active"]:
        return False, None
    

    if ban_info["pardon_time"] is not None:
        try:
            pardon_dt = datetime.strptime(ban_info["pardon_time"], DATETIME_FORMAT)
            pardon_dt = pardon_dt.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > pardon_dt:
                # 封禁已过期，自动解封
                await database.unban_ip(ip)
                return False, None
        except (ValueError, TypeError):
            pass  # 时间格式错误，继续显示封禁信息
    
    if ban_info["pardon_time"] is None:
        return True, "IP已被永久封禁"
    
    pardon_time = utc_to_beijing(ban_info["pardon_time"])
    return True, f"IP已被封禁, 解封时间: {pardon_time}"

async def check_user_banned(user_uuid):
    """
    检查用户是否被封禁
    
    Returns:
        (False, None) - 未被封禁
        (True, error_message) - 被封禁
    """
    from utils import database
    
    tmp = await database.fetch_user(user_uuid)
    if not tmp:
        return False, None
    
    user_info = tmp[0]
    if not user_info.get("is_banned"):
        return False, None
    

    if user_info.get("pardon_time") is not None:
        try:
            pardon_dt = datetime.strptime(user_info["pardon_time"], DATETIME_FORMAT)
            pardon_dt = pardon_dt.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > pardon_dt:
                # 封禁已过期，自动解封
                await database.unban_user(user_uuid)
                return False, None
        except (ValueError, TypeError):
            pass  # 时间格式错误，继续显示封禁信息
    
    if user_info.get("pardon_time") is None:
        return True, "用户已被永久封禁"
    
    pardon_time = utc_to_beijing(user_info["pardon_time"])
    return True, f"用户已被封禁, 解封时间: {pardon_time}"

async def check_admin_self_action(operator_uuid, target_uuid, action="操作"):
    """
    检查管理员是否在对自己执行敏感操作
    
    Args:
        operator_uuid: 操作者 UUID
        target_uuid: 目标用户 UUID
        action: 操作名称，用于错误消息
        
    Returns:
        (True, None) - 允许操作
        (False, error_message) - 不允许操作
    """
    if operator_uuid == target_uuid:
        return False, f"不能对自己执行此操作: {action}"
    return True, None

async def check_last_admin(user_uuid):
    """
    检查用户是否是最后一个管理员
    
    Args:
        user_uuid: 用户 UUID
        
    Returns:
        (True, None) - 不是最后一个管理员
        (False, error_message) - 是最后一个管理员
    """
    from utils import database
    
    admin_count = await database.count_admins()
    if admin_count <= 1:
        user = await database.fetch_user(user_uuid)
        if user and user[0]["role"] == "admin":
            return False, "不能取消最后一个管理员的权限"
    return True, None

async def check_room_owner_self_action(operator_uuid, room_id, target_uuid, action="操作"):
    """
    检查房主是否在对自己执行敏感操作
    
    Args:
        operator_uuid: 操作者 UUID
        room_id: 房间 ID
        target_uuid: 目标用户 UUID
        action: 操作名称，用于错误消息
        
    Returns:
        (True, None) - 允许操作
        (False, error_message) - 不允许操作
    """
    from utils import database
    
    operator = await database.fetch_user_room(operator_uuid, room_id)
    if operator and operator["role"] == "owner" and operator_uuid == target_uuid:
        return False, f"房主不能对自己执行此操作: {action}"
    return True, None