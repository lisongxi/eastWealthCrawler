"""
创建项目异常体系，便于捕获异常类型，并关联日志体系

自定义异常给程序带来更高的可读性和可维护性，可以更好的应用面向对象的概念,通过类和对象来管理异常,而不仅仅是简单的字符串。
"""


class Error(Exception):
    """根异常
    异常出自 自编程序
    """


class ProxyAddrError(Error):
    """获取代理地址异常
    """


class RequestBlockError(Error):
    """获取板块信息异常
    """
