"""爬虫基础模块 / Crawler Base Module
提供共享的 HTTP Session 管理
"""

import aiohttp

# 共享 aiohttp Session（应在应用生命周期内复用）
_session: aiohttp.ClientSession | None = None


async def get_http_session() -> aiohttp.ClientSession:
    """获取或创建全局 aiohttp Session"""
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        _session = aiohttp.ClientSession(connector=connector)
    return _session


async def close_http_session():
    """关闭全局 aiohttp Session"""
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None
