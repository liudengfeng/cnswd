import re
import time
from multiprocessing import Pool

import pandas as pd

from cnswd.mongodb import get_db
from cnswd.websource.wy import fetch_fhpg

from ..setting.constants import MARKET_START, MAX_WORKER
from ..utils import make_logger
from .base import get_stock_status

logger = make_logger('网易分红配股')
NAMES = ['分红配股', '配股一览', '增发一览', '历年融资计划']
D_PAT = re.compile('日期$|日$')
START = MARKET_START.tz_localize(None)


def create_index_for(collection):
    # 不存在索性信息时，创建索引
    if not collection.index_information():
        collection.create_index([("公告日期", 1)])
        collection.create_index([("股票代码", 1)])
        collection.create_index([("公告日期", 1), ("股票代码", 1)])


def get_max_dt(collection, code):
    """最后日期"""
    pipe = [
        {
            '$match': {'股票代码': code}
        },
        {
            '$sort': {
                '公告日期': -1
            }
        },
        {
            '$project': {
                '_id': 0,
                '公告日期': 1
            }
        },
        {
            '$limit': 1
        },
    ]
    try:
        res = list(collection.aggregate(pipe))[0]
        dt = pd.Timestamp(res['公告日期'])
        return dt
    except (IndexError, ):
        return START


def _fix_data(df):
    if df.empty:
        return df
    for col in df.columns:
        if D_PAT.findall(col):
            df[col] = pd.to_datetime(df[col], errors='ignore')
    return df


def _droped_null(doc):
    res = {}
    for k, v in doc.items():
        if not pd.isnull(doc[k]):
            res[k] = v
    return res


def _refresh(code):
    db = get_db('wy')
    try:
        dfs = fetch_fhpg(code)
    except ValueError:
        return
    for name, df in zip(NAMES, dfs):
        if df.empty:
            continue
        df['股票代码'] = code
        df = _fix_data(df)
        collection = db[name]
        create_index_for(collection)
        last_dt = get_max_dt(collection, code)
        df = df[df['公告日期'] > last_dt]
        if not df.empty:
            for doc in df.to_dict('records'):
                collection.insert_one(_droped_null(doc))


def refresh():
    t = time.time()
    codes = get_stock_status().keys()
    with Pool(MAX_WORKER) as pool:
        list(pool.imap_unordered(_refresh, codes))
    logger.info(f"股票数量 {len(codes)}, 用时 {time.time() - t:.2f}秒")
