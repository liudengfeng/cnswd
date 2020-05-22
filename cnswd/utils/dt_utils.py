"""

日期及时间辅助工具

返回Timestamp对象
"""
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.api.types import is_number
from pandas.tseries.offsets import (BDay, Day, Hour, Minute, MonthBegin,
                                    MonthEnd, QuarterBegin, QuarterEnd, Week,
                                    YearBegin, YearEnd)

from .._exceptions import FutureDate
from ..setting.constants import MARKET_START, TZ


def parse_date(x, format=r'%Y-%m-%d'):
    """以指定日期格式强制转换为日期

    Arguments:
        x {date like object} -- 日期对象

    Keyword Arguments:
        format {str} -- 日期格式 (default: {r'%Y-%m-%d'})

    Returns:
        date -- 日期对象
    """
    if x in ('-', '--', None):
        return pd.NaT
    return pd.to_datetime(x,
                          errors='coerce',
                          infer_datetime_format=True,
                          format=format).date()


def time_for_next_update(last_time, freq='D', num=9, is_end=False):
    """前次更新后下一次更新时间

    Arguments:
        last_time {obj} -- 上次时间

    Keyword Arguments:
        freq {str} -- 更新周期 (default: {'D'})
        num {int} -- 日级别以下为单位数，以上为小时数 (default: {9})
        is_end {bool} -- 是否为周期尾部 (default: {False})

    Raises:
        TypeError: 不能识别的周期类型

    Returns:
        Timestamp -- 下一次更新时间

    Notes:
        一、 freq < D
            `num`代表周期数
            上一时点`normalize`后移动`num`周期，不考虑开始及结束问题
        二、 freq in D、B
            `num`代表小时
            对于历史时间，上一时点`normalize`后一律移动到下一个周期，且将小时调整到指定的num
            如上一时点其日期为当前日期，且在其`normalize`及调整小时后的值晚于上一时点，则取调整后的值
        三、 freq > D 开始及结束才有效
            `num`无效
            如周初、周末、月初、月末、季初、季末、年初、年末
            此时num数字不起作用
    """
    valid_freq = ('B', 'D', 'W', 'M', 'Q', 'H', 'MIN')
    if pd.isnull(last_time):
        return pd.Timestamp(MARKET_START)
    assert isinstance(last_time,
                      pd.Timestamp), f'类型错误，希望Timestamp，实际为{type(last_time)}'
    now = pd.Timestamp.now(tz=last_time.tz)
    assert last_time <= now, '过去时间必须小于当前时间'
    freq = freq.upper()
    if freq == 'MIN':
        offset = Minute(n=num)
        return offset.apply(last_time.floor(freq))
    if freq == 'H':
        offset = Hour(n=num)
        return offset.apply(last_time.floor(freq))
    if freq == 'D':
        # √ 此处要考虑小时数
        limit = last_time.floor(freq).replace(hour=num)
        if last_time < limit:
            return limit
        else:
            offset = Day()
            return offset.apply(last_time.floor(freq)).replace(hour=num)
    if freq == 'B':
        offset = BDay()
        # 工作日
        if last_time.weekday() in range(0, 5):
            # √ 此处要考虑小时数
            limit = last_time.normalize().replace(hour=num)
            if last_time < limit:
                return limit
            else:
                return offset.apply(last_time.normalize()).replace(hour=num)
        else:
            return offset.apply(last_time.normalize()).replace(hour=num)
    if freq == 'W':
        nw = last_time.normalize() + pd.Timedelta(weeks=1)
        if is_end:
            return nw + pd.Timedelta(days=7 - nw.weekday()) - pd.Timedelta(
                nanoseconds=1)
        else:
            return nw - pd.Timedelta(days=nw.weekday())
    if freq == 'M':
        if is_end:
            offset = MonthEnd(n=2)
            res = offset.apply(last_time.normalize())
            if last_time.is_month_end:
                res = offset.rollback(res)
            return res
        else:
            offset = MonthBegin()
            return offset.apply(last_time.normalize())
    if freq == 'Q':
        if is_end:
            offset = QuarterEnd(n=2, startingMonth=3, normalize=True)
            res = offset.apply(last_time)
            if last_time.is_quarter_end:
                offset = QuarterEnd(n=-1, startingMonth=3, normalize=True)
                res = offset.apply(res)
            return res
        else:
            offset = QuarterBegin(n=1, normalize=True, startingMonth=1)
            return offset.apply(last_time)
    if freq == 'Y':
        if last_time.year == now.year:
            if is_end:
                return last_time.normalize().replace(year=now.year,
                                                     month=12,
                                                     day=31)
            else:
                return last_time.normalize().replace(year=now.year,
                                                     month=1,
                                                     day=1)
        if is_end:
            offset = YearEnd(normalize=True, month=12, n=2)
            res = offset.apply(last_time)
            if last_time.is_year_end:
                offset = YearEnd(n=-1, month=12, normalize=True)
                res = offset.apply(res)
            return res
        else:
            offset = YearBegin(normalize=True, month=1, n=1)
            return offset.apply(last_time)
    raise ValueError('不能识别的周期类型，仅接受{}。实际输入为{}'.format(valid_freq, freq))


def _sanitize_date(obj, default):
    """转换为日期对象，如果为None则使用默认值。输出datetime.date对象"""
    if isinstance(obj, pd.Timestamp):
        return obj
    if isinstance(obj, np.datetime64):
        return pd.Timestamp(obj)
    # 务必排在dt.date之前
    if isinstance(obj, dt.datetime):
        return ensure_dt_localize(obj)
    if isinstance(obj, dt.date):
        return ensure_dt_localize(pd.Timestamp(obj))
    if is_number(obj):
        return ensure_dt_localize(pd.Timestamp(dt.date(obj, 1, 1)))
    if isinstance(obj, str):
        return ensure_dt_localize(pd.Timestamp(pd.to_datetime(obj)))
    if obj is None:
        return default
    raise ValueError(f'输入"{obj}"不能识别为日期')


def sanitize_dates(start, end, localize=False):
    """
    返回日期二元组(Timestamp, Timestamp)

    当开始日期为None，返回市场开始日期；
    当结束日期为None, 返回当前日期；
    """
    start = _sanitize_date(start, MARKET_START)
    end = _sanitize_date(end, pd.Timestamp.now(tz=TZ))
    start = ensure_dt_localize(start)
    end = ensure_dt_localize(end)
    if start > end:
        raise FutureDate('开始日期必须小于或等于结束日期')
    if localize:
        return start, end
    else:
        return start.tz_localize(None), end.tz_localize(None)


def get_last_refresh_time(fp):
    """文件或目录最新修改时间"""
    assert isinstance(fp, Path), f"应为Path对象，got {type(fp)}"
    try:
        # 必须指定时区
        return pd.Timestamp(fp.stat().st_mtime, unit='s', tz=TZ)
    except FileNotFoundError:
        return MARKET_START


def is_trading_time():
    """判断当前是否为交易时段"""
    now = pd.Timestamp('now')
    if now.weekday() in (5, 6):
        return False
    current_time = now.time()
    am_start = pd.Timestamp('9:30').time()
    am_end = pd.Timestamp('11:30').time()
    pm_start = pd.Timestamp('13:00').time()
    pm_end = pd.Timestamp('15:00').time()
    is_am = am_start <= current_time <= am_end
    is_pm = pm_start <= current_time <= pm_end
    return is_am or is_pm


def ensure_dt_localize(dt):
    """确保Timestamp时区本地化"""
    assert isinstance(dt, pd.Timestamp), '必须为Timestamp对象'
    if dt.tzname() != 'CST':
        if dt.tz is None:
            return dt.tz_localize(TZ)
        return dt.tz_convert(TZ)
    return dt
