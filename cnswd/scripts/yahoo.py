"""
使用 https://github.com/dpguthrie/yahooquery
"""
import os
import time

import pandas as pd
from pymongo.errors import DuplicateKeyError
from retry.api import retry_call
from yahooquery import Ticker

from ..mongodb import get_db
from ..scripts.trading_codes import read_all_stock_codes
from ..setting.constants import MAX_WORKER
from ..utils import make_logger
from ..utils.db_utils import to_dict

logger = make_logger('雅虎财经')

ITEMS = [
    # 'financialData',
    'balanceSheetHistory',
    'cashflowStatementHistory',
    'incomeStatementHistory',
    'incomeStatementHistoryQuarterly',
    'cashflowStatementHistoryQuarterly',
    'balanceSheetHistoryQuarterly',
]


def create_index(name):
    db = get_db('yahoo')
    coll = db[name]
    if name not in db.list_collection_names():
        dt_field = 'endDate'  #'asOfDate' if name.endswith('Quarterly') else 'endDate'
        coll.create_index([("stockCode", 1)], name='code_index')
        coll.create_index([(dt_field, -1)], name='dt_index')
        coll.create_index([(dt_field, -1), ("stockCode", 1)],
                          unique=True,
                          name='id_index')


def to_yahoo_ticker(code):
    if code[0] in ('0', '2', '3'):
        return f"{code}.SZ"
    else:
        return f"{code}.SS"


def financial_data():
    codes = read_all_stock_codes()
    logger.info(f'提取数据 股票数量 {len(codes)}')
    s = time.time()
    codes = ' '.join(list(map(to_yahoo_ticker, codes)))
    stock = Ticker(codes)
    data = retry_call(stock.get_modules, [ITEMS], tries=3, delay=3)
    duration = time.time() - s
    logger.info(f'完成提取 耗时 {duration:.2f}秒')
    return data


def parse(data):
    for t, info in data.items():
        code = t.split('.')[0]
        for name, value in info.items():
            value.pop('maxAge')
            yield code, name, value


def to_coll(data):
    db = get_db('yahoo')
    logger.info(f'刷新中......')
    s = time.time()
    for code, name, d in parse(data):
        for _, docs in d.items():
            create_index(name)
            coll = db[name]
            for doc in docs:
                doc.pop('maxAge')
                doc['stockCode'] = code
                doc['endDate'] = pd.to_datetime(doc['endDate'],
                                                errors='coerce')
                try:
                    coll.insert_one(doc)
                    logger.info(f'插入股票{code}项目{name:>40} 1 行')
                except DuplicateKeyError:
                    pass
    duration = time.time() - s
    logger.info(f'更新 耗时 {duration:.2f}秒')


def refresh():
    to_coll(financial_data())
