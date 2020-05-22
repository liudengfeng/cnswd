import math
import time
from functools import partial
from multiprocessing import Pool

import pandas as pd
from numpy.random import shuffle

from ..setting.constants import MAX_WORKER
from ..store import TradingDateStore, WyCjmxStore, WyStockDailyStore
from ..utils import data_root, ensure_dtypes, loop_codes, make_logger
from ..websource.wy import fetch_cjmx
from .trading_calendar import is_trading_day

logger = make_logger('网易股票成交明细')
DATE_FMT = r'%Y-%m-%d'
COMPLETED = {}


def _last_5():
    """最近的5个交易日"""
    with TradingDateStore() as store:
        df = store.query()
    dates = df['trading_date'].values[-5:]
    return dates


def _wy_fix_data(df):
    dts = df.日期.dt.strftime(DATE_FMT) + ' ' + df.时间
    df['成交时间'] = pd.to_datetime(dts)
    del df['时间']
    del df['日期']
    df = df.rename(columns={'价格': '成交价', '涨跌额': '价格变动', '方向': '性质'})
    df = ensure_dtypes(df,
                       d_cols=['成交时间'],
                       s_cols=['股票代码', '性质'],
                       i_cols=['成交量'],
                       f_cols=['成交价', '成交额'])
    # 保留2位小数
    df = df.round({'价格变动': 2, '成交额': 2, '成交价': 2})
    df.fillna(0.0, inplace=True)
    return df


def get_batch_data(codes, date):
    status = {}
    res = []
    date_str = date.strftime(DATE_FMT)
    for _ in range(3):
        for code in codes:
            if status.get(code, False):
                continue
            try:
                df = fetch_cjmx(code, date_str)
                res.append(df)
                logger.info(f'股票：{code} {date_str} 共{len(df):>3}行')
                status[code] = True
            except Exception as e:
                logger.info(f'股票：{code} {date_str} {e!r}')
                status[code] = False
                continue
        time.sleep(0.5)
    failed = [k for k, v in status.items() if not v]
    if len(failed):
        print(f'{date_str} 以下股票成交明细提取失败')
        print(failed)
    df = pd.concat(res)
    df = _wy_fix_data(df)
    return df


def get_traded_codes(date):
    """当天交易的股票代码列表"""
    with WyStockDailyStore() as store:
        df = store.query(None, None, date, date)
        i = df[df['成交量'] > 0].index
        return i.get_level_values(1).tolist()


def downloaded_codes(date, store):
    """已经下载的股票代码"""
    df = store.query(None, None, date, date)
    codes = df.index.get_level_values(1).unique().tolist()
    return codes


def _refresh(date, store):
    """刷新指定日期成交明细数据(只能为近5天)"""
    date = pd.Timestamp(date)
    ok = COMPLETED.get(date, False)
    if ok:
        return
    t_codes = get_traded_codes(date)
    d_codes = downloaded_codes(date, store)
    codes = list(set(t_codes).difference(set(d_codes)))
    shuffle(codes)
    print(f'{date.strftime(DATE_FMT)} 共{len(codes)}只股票需要刷新')
    if len(codes) == 0:
        COMPLETED[date] = True
        return
    batch_num = math.ceil(len(codes) / MAX_WORKER)
    batchs = loop_codes(codes, batch_num)
    func = partial(get_batch_data, date=date)
    with Pool(MAX_WORKER) as pool:
        dfs = pool.map_async(func, batchs).get()
        # 不支持并行写入
        for df in dfs:
            store.append(df)


def refresh(date, store):
    """刷新指定日期成交明细数据(只能为近5天)"""
    for i in range(1, 4):
        print(f"第{i}次尝试")
        _refresh(date, store)


def refresh_last_5():
    """刷新最近5天成交明细"""
    with WyCjmxStore() as store:
        for d in _last_5():
            refresh(d, store)
        store.create_table_index(None)
        store.update_record(None)
