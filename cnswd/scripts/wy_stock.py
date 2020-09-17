import time
from multiprocessing import Pool
from random import shuffle

import pandas as pd
from retry.api import retry_call

from .._exceptions import ConnectFailed
from ..mongodb import get_db
from ..setting.constants import MARKET_START, MAX_WORKER
from ..utils import ensure_dtypes, make_logger
from ..utils.db_utils import to_dict
from ..websource.wy import fetch_history
from .base import get_stock_status

logger = make_logger('网易股票日线')
db_name = "wy_stock_daily"
col_dtypes = {
    'd_cols': ['日期'],
    's_cols': ['股票代码', '名称'],
    'i_cols': ['成交量', '成交笔数'],
    # 'f_cols': ['前收盘', '开盘价']
}


def find_last_date(collection):
    pipe = [
        {
            '$sort': {
                '日期': -1
            }
        },
        {
            '$project': {
                '_id': 0,
                '日期': 1
            }
        },
        {
            '$limit': 1
        },
    ]
    try:
        return list(collection.aggregate(pipe))[0]['日期']
    except (IndexError, ):
        return MARKET_START.tz_localize(None)


def create_index(collection):
    collection.create_index([("日期", -1)], unique=True, name='dt_index')


def _fix_data(df):
    code_col = '股票代码'
    # 去掉股票代码前缀 '
    df[code_col] = df[code_col].map(lambda x: x[1:])
    return df


def _one(code):
    db = get_db(db_name)
    collection = db[code]
    if collection.estimated_document_count() == 0:
        create_index(collection)
    start = find_last_date(collection) + pd.Timedelta(days=1)
    start = pd.Timestamp(start)
    start_str = start.strftime(r"%Y-%m-%d")
    df = retry_call(fetch_history,
                    fkwargs={
                        'code': code,
                        'start': start
                    },
                    exceptions=(ConnectionError, ValueError, ConnectFailed),
                    delay=0.3,
                    logger=logger,
                    tries=3)
    if df.empty:
        logger.info(f"股票代码 {code} 开始日期 {start_str} 数据为空")
        return
    df.reset_index(inplace=True)
    df = ensure_dtypes(df, **col_dtypes)
    fixed = _fix_data(df)
    fixed.drop(['股票代码'], axis=1, inplace=True)
    docs = to_dict(fixed)
    collection.insert_many(docs)
    logger.info(f"股票代码 {code} 开始日期 {start_str} 插入 {len(docs)} 行")


def refresh():
    t = time.time()
    # 在市交易股票
    codes = [code for code, dt in get_stock_status().items() if dt is None]
    shuffle(codes)
    for _ in range(3):
        with Pool(MAX_WORKER) as pool:
            list(pool.imap_unordered(_one, codes))
        time.sleep(1)
    logger.info(f"股票数量 {len(codes)}, 用时 {time.time() - t:.4f}秒")
