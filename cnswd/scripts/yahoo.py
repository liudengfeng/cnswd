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
def _handle_df(stock):
    """添加模式"""
    db = get_db('yahoo')
    for item in ['balance_sheet', 'income_statement', 'cash_flow', 'valuation_measures']:
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
        create_index(item, 'symbol', 'asOfDate', 'periodType', True)
        for doc in df.to_dict('records'):
            try:
                collection.insert_one(doc)
            except DuplicateKeyError:
                pass


@retry(ConnectionError, tries=3, delay=1, logger=logger)
def _handle_doc(stock):
    db = get_db('yahoo')
    for item in ['financial_data', 'key_stats']:
        records = getattr(stock, item)
        if 'error' in records.keys():
            raise ConnectionError(
                f"{item} {records['error']} 股票列表：\n {stock.symbols}")
        for symbol, doc in records.items():
            code = symbol.split('.')[0]
            if isinstance(doc, dict):
                collection = db[item]
                doc['symbol'] = code
                doc['update_time'] = pd.Timestamp.now()
                collection.insert_one(doc)


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


def _delete_old(old_ids):
    db = get_db('yahoo')
    for item in ['financial_data', 'key_stats']:
        collection = db[item]
        id_ = old_ids[item]
        if id_:
            result = collection.delete_many({'_id': {'$lte': id_}})
            logger.info(f"删除 {item} 旧数据 {result.deleted_count} 行")


def _refresh(codes):
    for batch in tqdm(batch_loop(codes, 4*8)):
        symbols = ','.join(list(map(to_yahoo_ticker, batch)))
        stock = Ticker(symbols)  # , asynchronous=True, max_workers=MAX_WORKER)
        try:
            _handle_df(stock)
            _handle_doc(stock)
        except ConnectionError:
            # 当三次尝试后，依旧存在异常，忽略
            pass
    return True


def refresh():
    codes = read_all_stock_codes()
    shuffle(codes)
    logger.info(f'股票总量 {len(codes)}')
    # old_ids = _max_ids()
    # if _refresh(codes):
    #     _delete_old(old_ids)
    _refresh(codes)
