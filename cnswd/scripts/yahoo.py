"""
使用 https://github.com/dpguthrie/yahooquery
"""
import os
import time
from random import shuffle

import pandas as pd
from pymongo.errors import DuplicateKeyError
from retry import retry
from tqdm import tqdm
from yahooquery import Ticker

from ..mongodb import get_db
from ..scripts.trading_codes import read_all_stock_codes
from ..setting.constants import MAX_WORKER
from ..utils import batch_loop, make_logger
from ..utils.db_utils import to_dict
from .yahoo_utils import get_cname_maps

logger = make_logger('雅虎财经')


def create_index(name, code_field, dt_field, periodType, unique):
    db = get_db('yahoo')
    coll = db[name]
    if name not in db.list_collection_names():
        coll.create_index([(code_field, 1)], name='code_index')
        coll.create_index([(dt_field, -1)], name='dt_index')
        coll.create_index([(dt_field, -1), (code_field, 1), (periodType, 1)],
                          unique=unique,
                          name='id_index')


def to_yahoo_ticker(code):
    if code[0] in ('0', '2', '3'):
        return f"{code}.SZ"
    else:
        return f"{code}.SS"


@retry(ConnectionError, tries=3, delay=1, logger=logger)
def _handle_df(stock, pbar):
    """添加模式"""
    db = get_db('yahoo')
    for item in ['balance_sheet', 'income_statement', 'cash_flow', 'valuation_measures']:
        pbar.set_description(f"{item}")
        maps = get_cname_maps(item)
        if item != 'valuation_measures':
            df = getattr(stock, item)('q')
        else:
            df = getattr(stock, item)
        if 'error' in df.keys():
            raise ConnectionError(
                f"{item} {df['error']} 股票列表：\n {stock.symbols}")
        df.reset_index(inplace=True)
        df['symbol'] = df['symbol'].map(lambda x: x.split('.')[0])
        collection = db[item]
        create_index(item, maps['symbol'],
                     maps['asOfDate'], maps['periodType'], True)
        for doc in df.to_dict('records'):
            to_add = {maps.get(k, k): v for k, v in doc.items()}
            try:
                collection.insert_one(to_add)
            except DuplicateKeyError:
                pass


@retry(ConnectionError, tries=3, delay=1, logger=logger)
def _handle_doc(stock, pbar):
    db = get_db('yahoo')
    for item in ['financial_data', 'key_stats']:
        pbar.set_description(f"{item}")
        maps = get_cname_maps(item)
        collection = db[item]
        records = getattr(stock, item)
        if 'error' in records.keys():
            raise ConnectionError(
                f"{item} {records['error']} 股票列表：\n {stock.symbols}")
        for symbol, doc in records.items():
            code = symbol.split('.')[0]
            if isinstance(doc, dict):
                to_add = {maps.get(k, k): v for k, v in doc.items()}
                to_add[maps['symbol']] = code
                to_add['更新时间'] = pd.Timestamp.now()
                collection.insert_one(to_add)


def _max_ids():
    res = {}
    db = get_db('yahoo')
    for item in ['financial_data', 'key_stats']:
        collection = db[item]
        try:
            res[item] = collection.find_one(
                projection={'_id': 1}, sort=[('_id', -1)])['_id']
        except Exception:
            res[item] = None
    return res

# 废弃


def _delete_old(old_ids):
    db = get_db('yahoo')
    for item in ['financial_data', 'key_stats']:
        collection = db[item]
        id_ = old_ids[item]
        if id_:
            result = collection.delete_many({'_id': {'$lte': id_}})
            logger.info(f"删除 {item} 旧数据 {result.deleted_count} 行")


def _refresh(codes):
    with tqdm(batch_loop(codes, 4*8)) as pbar:
        for batch in pbar:
            symbols = ','.join(list(map(to_yahoo_ticker, batch)))
            stock = Ticker(symbols)
            try:
                _handle_df(stock, pbar)
                _handle_doc(stock, pbar)
            except ConnectionError:
                # 当三次尝试后，依旧存在异常，忽略
                pass
        return True


def refresh():
    codes = read_all_stock_codes()
    shuffle(codes)
    logger.info(f'股票总量 {len(codes)}')
    _refresh(codes)
