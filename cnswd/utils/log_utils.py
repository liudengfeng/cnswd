import logbook
from logbook.more import ColorizedStderrHandler, StderrHandler

from ..setting.config import LOG_TO_FILE
from .path_utils import data_root

# 设置显示日志
logbook.set_datetime_format('local')


def make_logger(name, to_file=None):
    """生成logger对象"""
    logger = logbook.Logger(name)
    if to_file is None:
        to_file = LOG_TO_FILE
    if to_file:
        fp = data_root('log')
        fn = fp / f"{name}.txt"
        logger.handlers.append(logbook.FileHandler(fn))
    # 使用coloram经常导致递归错误
    # handler = ColorizedStderrHandler()
    handler = StderrHandler()
    handler.push_application()
    return logger
