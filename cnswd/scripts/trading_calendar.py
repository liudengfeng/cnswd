"""
trading_date

存储对象
1. 所有交易日历
2. 最新交易的股票代码列表

工作日每天 9：31分执行
"""
import re

import pandas as pd
import requests
from numpy.random import shuffle

from ..setting.constants import MARKET_START
from ..store import TradingDateStore, DataBrowseStore
from ..utils import data_root, ensure_dt_localize
from ..websource.tencent import get_recent_trading_stocks
from ..websource.wy import fetch_history

DATE_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2})')


def get_all_stock_codes():
    try:
        # 包含全部代码
        df = DataBrowseStore.query('1')
        codes = df['股票代码'].values.tolist()
    except Exception:
        # 近期交易的股票代码
        codes = get_recent_trading_stocks()
    return codes


def _add_prefix(stock_code):
    pre = stock_code[0]
    if pre == '6':
        return 'sh{}'.format(stock_code)
    else:
        return 'sz{}'.format(stock_code)


def _is_today_trading(codes):
    today = pd.Timestamp.today()
    url_fmt = 'http://hq.sinajs.cn/list={}'
    url = url_fmt.format(','.join(map(_add_prefix, codes)))
    r = requests.get(url)
    dts = re.findall(DATE_PATTERN, r.text)
    return today.strftime(r"%Y-%m-%d") in dts


def add_info(dates):
    # 使用写模式覆盖原数据
    df = pd.DataFrame({'trading_date': dates})
    # 必须将`添加`参数设置为False，使用覆盖式!!!
    TradingDateStore.append(df, kw={'append': False})
    TradingDateStore.set_attr('codes', get_all_stock_codes())
    # 非必要操作
    TradingDateStore.create_table_index(None)


def handle_today():
    today = pd.Timestamp.today()
    codes = get_all_stock_codes()
    shuffle(codes)
    codes = codes[:10]
    if _is_today_trading(codes):
        return today.normalize()
    else:
        return None


def refresh_trading_calendar():
    """刷新交易日历"""
    today = pd.Timestamp.today()
    yesterday = today - pd.Timedelta(days=1)
    dts = pd.date_range(MARKET_START.tz_localize(None), yesterday, freq='B')
    # 数据可能并不完整
    history = fetch_history('000001', None, None, True)
    dates = []
    for d in dts:
        if d in history.index:
            dates.append(d)
    today = handle_today()
    if today:
        dates.append(today)
    add_info(dates)
    print('done')


def is_trading_day(dt):
    """是否为交易日历"""
    assert isinstance(dt, pd.Timestamp)
    dt = ensure_dt_localize(dt).tz_localize(None).normalize()
    df = TradingDateStore.query()
    return dt.to_datetime64() in df['trading_date'].values
