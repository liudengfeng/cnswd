import math
import multiprocessing
import time
from functools import lru_cache, partial
from multiprocessing import Pool

import pandas as pd
from numpy.random import shuffle
from retry.api import retry_call

from ..mongodb import get_db
from ..scripts.trading_calendar import is_trading_day
from ..setting.constants import MAX_WORKER
from ..utils import batch_loop, data_root, ensure_dtypes, make_logger
from ..utils.db_utils import to_dict
from ..websource.wy import fetch_cjmx

logger = make_logger('成交明细')
DATE_FMT = r'%Y-%m-%d'


def _last_5():
    """最近的5个交易日"""
    db = get_db()
    try:
        return db['交易日历'].find_one()['last_month'][-5:]
    except Exception:
        today = pd.Timestamp('today').normalize()
        dates = pd.date_range(today - pd.Timedelta(days=5), today)
        return [d.to_pydatetime() for d in dates]


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


def bacth_refresh(codes, timestamp):
    db = get_db('cjmx')
    date_str = timestamp.strftime(DATE_FMT)
    collection = db[date_str]
    if collection.estimated_document_count() == 0:
        create_index(collection)
    status = {}
    for code in codes:
        try:
            df = retry_call(fetch_cjmx, [code, date_str],
                            delay=0.3,
                            tries=3,
                            logger=logger)
            if not df.empty:
                df = _wy_fix_data(df)
                collection.insert_many(to_dict(df))
            logger.info(f'股票 {code} {date_str} 共 {len(df):>5} 行')
            status[code] = True
        except Exception as e:
            logger.info(f'股票 {code} 日期 {date_str} {e!r}')
            status[code] = False

    failed = [k for k, v in status.items() if not v]
    if len(failed):
        logger.warning(f'{date_str} 以下股票成交明细提取失败')
        logger.warning(failed)
    return len(failed) == 0


def was_traded(db, code, timestamp):
    collection = db[code]
    filter = {'日期': timestamp, '成交量': {'$gt': 0}}
    if collection.find_one(filter, {'_id': 1}):
        return True
    else:
        return False


@lru_cache(None)
def get_traded_codes(timestamp):
    """当天交易的股票代码列表"""
    db = get_db('wy_stock_daily')
    codes = db.list_collection_names()
    return [code for code in codes if was_traded(db, code, timestamp)]


def completed_codes(timestamp):
    """已经下载的股票代码"""
    db = get_db('cjmx')
    collection = db[timestamp.strftime(DATE_FMT)]
    return collection.distinct('股票代码')


def _refresh(timestamp):
    """刷新指定日期成交明细数据(只能为近5天)"""
    t_codes = get_traded_codes(timestamp)
    d_codes = completed_codes(timestamp)
    codes = list(set(t_codes).difference(set(d_codes)))
    if len(codes) == 0:
        return True
    shuffle(codes)
    logger.info(f'{timestamp.strftime(DATE_FMT)} 共 {len(codes)} 股票')
    completed = bacth_refresh(codes, timestamp)
    return completed


def refresh(timestamp):
    """刷新指定日期成交明细数据(只能为近5天)"""
    for i in range(1, 4):
        logger.info(f"第{i}次尝试 {timestamp}")
        completed = _refresh(timestamp)
        if completed:
            break


def create_index(collection):
    collection.create_index([("成交时间", -1)], name='dt_index')
    collection.create_index([("股票代码", 1)], name='code_index')


def refresh_last_5():
    """刷新最近5天成交明细"""
    tdates = [pd.Timestamp(d) for d in _last_5()]
    with Pool(MAX_WORKER) as pool:
        r = pool.map_async(refresh, tdates)
        r.wait()
