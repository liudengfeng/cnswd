import pandas as pd

from .dt_utils import sanitize_dates
from .tools import ensure_list


def loop_period_by(start, end, freq='B', exclude_future=True):
    """按指定频率划循环期间"""
    res = []
    assert freq in ('D', 'B', 'W', 'M', 'Q', 'Y')
    start, end = sanitize_dates(start, end)
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)
    ps = pd.period_range(start, end, freq=freq)
    fmt = r'%Y-%m-%d'
    today = pd.Timestamp('today')
    for p in ps:
        s = pd.Timestamp(p.asfreq('D', 'start').strftime(fmt))
        e = pd.Timestamp(p.asfreq('D', 'end').strftime(fmt))
        # 如果排除未来日期
        if exclude_future:
            if e > today:
                continue
        if s < start:
            s = start
        if e > end:
            e = end
        # 开始日期须小于等于结束日期
        if s <= e:
            res.append((s, e))
    return res


def batch_loop(iterable, batch_num):
    """分批循环项目"""
    res = []
    items = ensure_list(iterable)
    total_num = len(items)
    for start in range(0, total_num, batch_num):
        end = start + batch_num if start + batch_num < total_num else total_num
        res.append(items[start:end])
    return res
