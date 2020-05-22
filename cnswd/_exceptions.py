class RetryException(Exception):
    """可以重新尝试的异常"""
    pass


class MaybeChanged(Exception):
    pass


class FrequentAccess(Exception):
    """部分网站会识别爬虫，在一段时间内禁止访问"""
    pass


class ConnectFailed(Exception):
    """网络连接失败，需要中断程序"""
    pass


class NoWebData(Exception):
    """无数据异常"""
    pass


class FutureDate(Exception):
    """未来日期"""
    pass


class ForbidPparallel(Exception):
    """禁止并行"""
    pass
