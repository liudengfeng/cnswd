"""股票实时报价"""

import pandas as pd
from tqdm import tqdm

from ..mongodb import get_db
from ..scripts.trading_calendar import is_trading_day
from ..utils import make_logger
from ..websource.wy import fetch_quote

DB_NAME = 'wy_quotes'
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
    """刷新实时报价"""
    codes_db = get_db()
    codes = codes_db['股票列表'].find_one()['codes']
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
    docs = [_to_timestamp(doc) for doc in fetch_quote(codes)]
    docs = filter(lambda d: d[DATE_KEY].floor('D') == today, docs)
    r = collection.insert_many(list(docs))
    logger.info(f'Inserted {len(r.inserted_ids)} rows')


QUOTE_COL_MAPS = {
    '股票代码': 'code',
    '股票简称': 'name',
    '开盘': 'open',
    '前收盘': 'yestclose',
    '现价': 'price',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'turnover',
    '买1量': 'bidvol1',
    '买1价': 'bid1',
    '买2量': 'bidvol2',
    '买3价': 'bid3',
    '买3量': 'bidvol3',
    '买4价': 'bid4',
    '买4量': 'bidvol4',
    '买5价': 'bid5',
    '买5量': 'bidvol5',
    '买1价': 'bid1',
    '卖1量': 'askvol1',
    '卖1价': 'ask1',
    '卖2量': 'askvol2',
    '卖2价': 'ask2',
    '卖3量': 'askvol3',
    '卖3价': 'ask3',
    '卖4量': 'askvol4',
    '卖4价': 'ask4',
    '卖5量': 'askvol5',
    '卖5价': 'ask5',
    '批次': 'update',
    '时间': 'time'}


def _to_wy_dict(doc):
    new_dict = {}
    for k, v in QUOTE_COL_MAPS.items():
        new_dict[v] = doc[k]
    new_dict['_id'] = doc['_id']
    return new_dict


def sina_to_wy():
    """转移数据"""
    src_db = get_db('quotes')
    tgt_db = get_db(DB_NAME)
    with tqdm(src_db.list_collection_names()) as it:
        for date_str in it:
            src_collection = src_db[date_str]
            docs = src_collection.find({})
            to_add = [_to_wy_dict(doc) for doc in docs]
            tgt_collection = tgt_db[date_str]
            create_index(tgt_collection)
            tgt_collection.insert_many(to_add)
