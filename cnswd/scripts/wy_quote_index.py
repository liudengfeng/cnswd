"""股票指数实时报价"""

import pandas as pd
from tqdm import tqdm

from ..mongodb import get_db
from ..scripts.trading_calendar import is_trading_day
from ..utils import make_logger
from ..websource.wy import fetch_quote

DB_NAME = 'wy_index_quotes'
DATE_KEY = 'update'
# DT_FMT = r"%Y/%m/%d H:M:S"
logger = make_logger('实时报价')


def create_index(collection):
    collection.create_index([(DATE_KEY, -1)], name='dt_index')


def _to_timestamp(d):
    dt_keys = [DATE_KEY, 'time']
    for k in dt_keys:
        d[k] = pd.to_datetime(d[k])
    return d


def refresh():
    """刷新股票指数实时报价"""
    codes_db = get_db()
    codes = codes_db['指数列表'].find_one()['codes']
    db = get_db(DB_NAME)
    today = pd.Timestamp('today').floor('D')
    name = today.strftime(r"%Y-%m-%d")
    collection = db[name]
    if collection.estimated_document_count() == 0:
        create_index(collection)
    # 后台计划任务控制运行时间点。此处仅仅判断当天是否为交易日
    if not is_trading_day(today):
        logger.warning(f"{today} 非交易日")
        return
    docs = [_to_timestamp(doc) for doc in fetch_quote(codes, True)]
    docs = filter(lambda d: d[DATE_KEY].floor('D') == today, docs)
    r = collection.insert_many(list(docs))
    logger.info(f'Inserted {len(r.inserted_ids)} rows')
