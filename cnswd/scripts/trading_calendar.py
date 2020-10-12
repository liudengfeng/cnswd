"""
trading_date

存储对象
1. 所有交易日历
2. 最新交易的股票代码列表

bug：网易指数数据并不能确保及时更新
解决方案：
    存储最近的交易日合并

工作日每天 9:16 执行
"""
import re

import pandas as pd
import requests
from numpy.random import shuffle

from ..mongodb import get_db
from ..setting.constants import MARKET_START
from ..utils import data_root, ensure_dt_localize, make_logger
from ..websource.tencent import get_recent_trading_stocks
from ..websource.wy import fetch_history, fetch_quote
from .trading_codes import read_all_stock_codes

DATE_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2})')
logger = make_logger("交易日历")


def _add_prefix(stock_code):
    pre = stock_code[0]
    if pre == '6':
        return 'sh{}'.format(stock_code)
    else:
        return 'sz{}'.format(stock_code)


def _is_today_trading(codes):
    """只有实际成交后才会体现当天为交易日"""
    today = pd.Timestamp.today()
    quotes = fetch_quote(codes)
    dts = [doc['time'][:10] for doc in quotes]
    return today.strftime(r"%Y/%m/%d") in dts


def update(tdates, last_month):
    db = get_db('stockdb')
    collection = db['交易日历']
    doc = {
        'tdates': tdates,
        "last_month": last_month,
        "update_time": pd.Timestamp.now()
    }
    if collection.estimated_document_count() == 0:
        collection.insert_one(doc)
    else:
        collection.find_one_and_replace({}, doc)


def local_hist():
    db = get_db('stockdb')
    collection = db['交易日历']
    try:
        return collection.find_one()['last_month']
    except TypeError:
        return []


def hist_tdates():
    """最近一个月的数据"""
    today = pd.Timestamp.today()
    codes = read_all_stock_codes()
    shuffle(codes)
    codes = codes[:10]
    is_trading = _is_today_trading(codes)
    # 日期按降序排列
    tdates = [d for d in fetch_history('000001', None, None, True).index[:25]]
    if is_trading:
        tdates.append(today.normalize())
    # 添加本地数据
    tdates.extend([pd.Timestamp(d) for d in local_hist()])
    return sorted(set(tdates))[-25:]


def refresh():
    """刷新交易日历"""
    # 取决于提取时间，如网络尚未提供最新的数据，可能不包含当日甚至最近日期的数据
    # 最初日期为 1990-12-19
    history = fetch_history('000001', None, None, True)
    tdates = [d for d in history.index]
    last_month = hist_tdates()
    tdates.extend(last_month)
    tdates = list(set(tdates))
    tdates = sorted(tdates)
    update(tdates, last_month)
    logger.info('完成')


def get_tdates():
    """交易日列表

    Returns:
        list: 交易日`datetime`列表
    """
    db = get_db('stockdb')
    collection = db['交易日历']
    if collection.estimated_document_count() == 0:
        refresh()
    return collection.find_one()['tdates']


def is_trading_day(dt):
    """是否为交易日

    Args:
        dt (Timestamp): 输入时间

    Returns:
        bool: 如为交易日返回True，否则返回False
    """
    assert isinstance(dt, pd.Timestamp)
    dt = ensure_dt_localize(dt).tz_localize(None).normalize()
    tdates = get_tdates()
    return dt.to_pydatetime() in tdates
