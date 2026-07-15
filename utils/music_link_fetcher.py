"""
-*-coding: utf-8-*-
本模块使用配置的api获取音乐播放链接
"""


import logging
import yaml
from pathlib import Path
import httpx
import asyncio

logger = logging.getLogger(__name__)

# 默认超时设置（秒）
DEFAULT_TIMEOUT = 10.0

# 读取api配置
config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

async def music_link_get(source= "netease", track_id= ""):
    """
    根据音乐源和歌曲ID获取音频直链
    
    Args:
        source (str): 音乐源。可选项，参数值netease(默认)、tencent、tidal、spotify、ytmusic、qobuz、joox、deezer、migu、kugou、kuwo、ximalaya、apple。部分可能失效，建议使用稳定音乐源
        track_id (str): 歌曲的唯一标识ID。不能为空字符串

    Returns:
        str or None: 成功时返回音频直链URL，失败时返回 None
    """
    api = config.get("api").get("get_api")
    
    if api is None:
        logger.error("配置文件缺少必需的条目: 'get_api'")
        return None

    if track_id == "":
        logger.error("缺少必需的传入项目: 'track_id'")
        return None
    
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as c:
        try:
            req = await c.get(api.format(source=source, track_id=track_id))
            req.raise_for_status()
            data = req.json()
            return data.get("url")
        except httpx.TimeoutException:
            logger.error(f"获取音乐链接超时: 请求超过 {DEFAULT_TIMEOUT} 秒")
            return None
        except Exception as e:
            logger.error(f"获取音乐链接失败: {e}")
            return None


async def lrc_link_get(track_id):
    """
    获取歌曲的歌词数据。
    
    此函数调用网易云音乐 API，获取指定歌曲的所有歌词版本，
    包括原始歌词、翻译歌词、罗马音等 7 种格式。
    
    Args:
        track_id (str): 歌曲的唯一标识 ID。
    
    Returns:
        dict: 包含 7 个元素的字典，每个元素是对应版本的歌词字符串；
               主要使用yrc, ytlrc, yromalrc；
               如果请求失败，各元素可能为 None 或空字符串
               
               各字段含义:
               - lrc: 原始歌词（带时间戳）
               - tlyric: 翻译歌词
               - romalrc: 罗马音歌词
               - yrc: 逐字歌词
               - ytlrc: 逐字翻译歌词
               - yromalrc: 逐字罗马音歌词

    """
    if track_id == "":
        logger.error("缺少必需的传入项目: 'track_id'")
        return None
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as c:
        try:
            req = await c.get(f"https://music.163.com/api/song/lyric?os=pc&id={track_id}&lv=-1&rv=-1&tv=-1&yv=-1")
            req.raise_for_status()
            data = req.json()
            return data
        except asyncio.TimeoutError:
            logger.error(f"获取歌词超时: 请求超过 {DEFAULT_TIMEOUT} 秒")
            return None
        except Exception as e:
            logger.error(f"获取歌词失败: {e}")
            return None

async def song_detail_get(track_id):
    """
    获取歌曲详细信息（包括时长）。

    Args:
        track_id (str): 歌曲的唯一标识 ID。

    Returns:
        dict or None: 成功时返回歌曲信息字典（含 duration 毫秒），失败返回 None
    """
    if not track_id:
        logger.error("缺少必需的传入项目: 'track_id'")
        return None
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as c:
        try:
            req = await c.get(
                f"https://music.163.com/api/song/detail?ids=[{track_id}]"
            )
            req.raise_for_status()
            data = req.json()
            songs = data.get("songs", [])
            if songs:
                return songs[0]
            logger.warning(f"歌曲详情为空, track_id: {track_id}")
            return None
        except asyncio.TimeoutError:
            logger.error(f"获取歌曲详情超时: {track_id}")
            return None
        except Exception as e:
            logger.error(f"获取歌曲详情失败: {e}")
            return None


# 测试
async def test():
    result = await lrc_link_get(track_id="22803896")
    logger.info(result)

if __name__ == "__main__":
    
    # 临时配置 logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    asyncio.run(test())