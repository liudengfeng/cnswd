"""

基础配置文件

h5查询字段名称中不得含`（`
"""
from pathlib import Path

from .constants import MARKET_START

DEFAULT_CONFIG = {
    # 数据目录
    'data_root': Path("D://") / '.cnswd',  # 或者 Path.home() / '.cnswd'
    # 驱动程序位置 文件目录使用`/`
    'geckodriver_path': r'C:/tools/geckodriver.exe',
}

LOG_TO_FILE = False        # 是否将日志写入到文件
TIMEOUT = 90               # 最长等待时间，单位：秒。>20秒
# 轮询时间缩短
POLL_FREQUENCY = 0.2

default_start_date = MARKET_START.strftime(r'%Y-%m-%d')
