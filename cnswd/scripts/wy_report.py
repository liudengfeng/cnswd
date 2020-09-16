import re
import time
from functools import partial
from itertools import product
from multiprocessing import Manager, Pool

import pandas as pd
from numpy.random import shuffle
from toolz.dicttoolz import valfilter

from cnswd.mongodb import get_db
from cnswd.setting.constants import MARKET_START, MAX_WORKER
from cnswd.utils import make_logger
from cnswd.websource.tencent import get_recent_trading_stocks
from cnswd.websource.wy import fetch_financial_report

logger = make_logger('网易')

NAMES = {'zcfzb': '资产负债表', 'lrb': '利润表', 'xjllb': '现金流量表'}
START = MARKET_START.tz_localize(None)
DATE_KEY = '报告日期'


def create_index_for(collection):
    # 不存在索性信息时，创建索引
    if not collection.index_information():
        # collection.create_index([("公告日期", 1)])
        # collection.create_index([("股票代码", 1)])
        collection.create_index([(DATE_KEY, 1), ("股票代码", 1)])


def get_max_dt(collection, code):
    """最后日期"""
    pipe = [
        {
            '$match': {'股票代码': code}
        },
        {
            '$sort': {
                DATE_KEY: -1
            }
        },
        {
            '$project': {
                '_id': 0,
                DATE_KEY: 1
            }
        },
        {
            '$limit': 1
        },
    ]
    try:
        res = list(collection.aggregate(pipe))[0]
        dt = pd.Timestamp(res[DATE_KEY])
        return dt
    except (IndexError, ):
        return START


def _droped_null(doc):
    res = {}
    for k, v in doc.items():
        if not pd.isnull(doc[k]):
            res[k] = v
    return res


def _refresh(code, d):
    db = get_db('wy')
    for key, name in NAMES.items():
        if d.get((code, key), False):
            continue
        try:
            df = fetch_financial_report(code, key)
        except (ValueError,):
            continue
        df['股票代码'] = code
        df[DATE_KEY] = pd.to_datetime(df[DATE_KEY], errors='ignore')
        collection = db[name]
        create_index_for(collection)
        last_dt = get_max_dt(collection, code)
        df = df[df[DATE_KEY] > last_dt]
        if not df.empty:
            for doc in df.to_dict('records'):
                collection.insert_one(_droped_null(doc))
        logger.info(f"完成股票 {code} {name} 刷新")
        d[(code, key)] = True


def refresh():
    t = time.time()
    codes = get_recent_trading_stocks()
    shuffle(codes)
    # 单进程
    # d = {}
    # for _ in range(30):
    #     func = partial(_refresh, d=d)
    #     for code in codes:
    #         try:
    #             func(code)
    #             logger.info(code)
    #         except Exception as e:
    #             logger.error(f"{e}")
    #             time.sleep(30)
    # logger.info(f"股票数量 {len(codes)}, 用时 {time.time() - t:.2f}秒")
    # 多进程
    with Manager() as manager:
        d = manager.dict()
        for k in product(codes, NAMES.keys()):
            d[k] = False
        func = partial(_refresh, d=d)
        for _ in range(30):
            try:
                with Pool(MAX_WORKER) as pool:
                    list(pool.imap_unordered(func, codes))
            except Exception as e:
                logger.error(f"{e}")
                time.sleep(30)
        failed = valfilter(lambda x: x == False, d)
        print(f"失败数量：{len(failed)}")
        # print(f"失败项目 {failed} ")
    logger.info(f"股票数量 {len(codes)}, 用时 {time.time() - t:.2f}秒")
