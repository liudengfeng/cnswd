"""
trading_date

存储对象
1. 所有交易日历
2. 最新交易的股票代码列表

bug：网易指数数据并不能确保及时更新
解决方案：
    存储最近的交易日合并

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
    """只有实际成交后才会体现当天为交易日"""
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


def hist_tdates():
    """最近一个月的数据"""
    today = pd.Timestamp.today()
    codes = get_all_stock_codes()
    shuffle(codes)
    codes = codes[:10]
    is_trading = _is_today_trading(codes)
    fp = data_root('last_month_tdates.pkl')
    if not fp.exists():
        # 日期按降序排列
        tdates = [
            d for d in fetch_history('000001', None, None, True).index[:25]
        ]
        if is_trading:
            tdates.append(today.normalize())
        tdates = list(set(tdates))
        tdates = sorted(tdates)
        s = pd.Series(tdates)
        s.to_pickle(str(fp))
    else:
        # 始终保存最近最近一个月的数据
        tdates = [d for d in pd.read_pickle(str(fp))][-24:]
        if is_trading:
            tdates.append(today.normalize())
        tdates = list(set(tdates))
        tdates = sorted(tdates)
        s = pd.Series(tdates)
        s.to_pickle(str(fp))
    return [d for d in pd.read_pickle(str(fp))]


def refresh_trading_calendar():
    """刷新交易日历"""
    # 取决于提取时间，如网络尚未提供最新的数据，可能不包含当日甚至最近日期的数据
    # 最初日期为 1990-12-19
    history = fetch_history('000001', None, None, True)
    tdates = []
    for d in history.index:
        tdates.append(d)
    last_month_tdates = hist_tdates()
    tdates.extend(last_month_tdates)
    tdates = list(set(tdates))
    tdates = sorted(tdates)
    add_info(tdates)
    print('done')


def is_trading_day(dt):
    """是否为交易日历"""
    assert isinstance(dt, pd.Timestamp)
    dt = ensure_dt_localize(dt).tz_localize(None).normalize()
    df = TradingDateStore.query()
    return dt.to_datetime64() in df['trading_date'].values
