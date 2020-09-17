import re
import time
from functools import partial
from itertools import product
from multiprocessing import Manager, Pool

import pandas as pd
from numpy.random import shuffle
from toolz import partition_all
from toolz.dicttoolz import valfilter

from cnswd.mongodb import get_db
from cnswd.setting.constants import MARKET_START, MAX_WORKER
from cnswd.utils import make_logger
from cnswd.websource.wy import fetch_company_info

from .base import get_stock_status

logger = make_logger('网易公司资料')

NAMES = ['公司简介', 'IPO资料']
START = MARKET_START.tz_localize(None)


def create_index_for(collection):
    # 不存在索性信息时，创建索引
    if not collection.index_information():
        collection.create_index([("股票代码", 1)])


def need_refresh(collection2, code):
    """是否需要刷新

    简单规则：
        如果已经存在IPO日期，且24小时内已经刷新 ❌ 否则 ✔
    """
    now = pd.Timestamp('now')
    doc = collection2.find_one({'股票代码': code})
    if doc:
        # 当更新间隔时间超过一天，且上市日期为空时才需要更新
        cond1 = now - doc['更新时间'] >= pd.Timedelta(days=1)
        cond2 = pd.isnull(doc.get('上市日期', None))
        return cond1 and cond2
    else:
        return True


def _droped_null(doc):
    res = {}
    for k, v in doc.items():
        if not pd.isnull(doc[k]):
            res[k] = v
    return res


def _refresh(batch, d):
    db = get_db('wy')
    collection1 = db[NAMES[0]]
    collection2 = db[NAMES[1]]
    for code in batch:
        # 首先检查状态，减少数据库查询
        if d.get(code, False):
            continue
        if not need_refresh(collection2, code):
            d[code] = True
            logger.info(f"股票 {code} 已经刷新")
            continue
        try:
            doc1, doc2 = fetch_company_info(code)
            doc1['股票代码'] = code
            doc1['更新时间'] = pd.Timestamp('now')
            doc2['股票代码'] = code
            doc2['更新时间'] = pd.Timestamp('now')
            collection1.insert_one(doc1)
            collection2.insert_one(doc2)
            d[code] = True
        except Exception as e:
            logger.error(f"{e}")
            continue
        logger.info(f"完成股票 {code} 刷新")


def refresh():
    t = time.time()
    codes = list(get_stock_status().keys())
    shuffle(codes)
    # 约4000只股票，每批300只
    batches = partition_all(300, codes)

    # 多进程
    with Manager() as manager:
        d = manager.dict()
        for k in codes:
            d[k] = False
        func = partial(_refresh, d=d)
        for _ in range(100):
            try:
                with Pool(MAX_WORKER) as pool:
                    list(pool.imap_unordered(func, batches))
            except Exception as e:
                logger.error(f"{e}")
                time.sleep(30)
        failed = valfilter(lambda x: x == False, d)
        logger.warning(f"失败数量：{len(failed)}")
        # print(f"失败项目 {failed} ")
    logger.info(f"股票数量 {len(codes)}, 用时 {time.time() - t:.2f}秒")
