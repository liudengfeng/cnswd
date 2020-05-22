import asyncio
import random
import time

import pandas as pd


from ..store import DisclosureStore
from ..utils import ensure_dtypes
from ..websource.disclosures import fetch_one_day

col_dtypes = {
    'd_cols': ['公告时间'],
    's_cols': ['下载网址', '公告标题', '股票代码', '股票简称'],
    'i_cols': ['序号'],
}
KW = {
    'min_itemsize': {
        '公告标题': 360,
        '下载网址': 65,
        '股票简称': 14,
    }
}


def get_dates(store):
    """循环列表"""
    min_start = pd.Timestamp('2010-01-01')
    start = store.get_attr('max_dt', min_start)  # - pd.Timedelta(days=1)
    if start < min_start:
        start = min_start
    end = pd.Timestamp('now')
    if end.hour >= 16:
        end = end + pd.Timedelta(days=1)
    dates = pd.date_range(start, end)
    dates = [d.strftime(r'%Y%m%d') for d in dates]
    return dates


def _append(dfs, store):
    col = '公告时间'
    min_start = pd.Timestamp('2010-01-01')
    old_max_dt = store.get_attr('max_dt', min_start)
    df = pd.concat(dfs)
    new_max_dt = df[col].max()
    cond = df[col] > old_max_dt
    to_add = df[cond]
    try:
        store.append(to_add, KW)
        print(f'添加{len(to_add)}行')
        store.set_attr('max_dt', max(old_max_dt, new_max_dt))
    except Exception as e:
        print(f"{e!r}")


async def refresh():
    """刷新"""
    # 自最后日期起，至当日（或明日）至
    # 初始化时，如果单次时段超过半年，会导致远程主机强迫关闭了一个现有的连接
    # 休眠一段，再次运行即可。
    dfs = []
    with DisclosureStore() as store:
        for d in get_dates(store):
            df = await fetch_one_day(d)
            df = ensure_dtypes(df, **col_dtypes)
            dfs.append(df)
            delay = random.randint(100, 500) / 100
            asyncio.sleep(delay)
        _append(dfs, store)
        store.create_table_index(None)
