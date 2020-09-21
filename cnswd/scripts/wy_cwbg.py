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
from cnswd.websource.wy import fetch_financial_report

from .base import get_stock_status

logger = make_logger('网易财务分析')

NAMES = {'zcfzb': '资产负债表', 'lrb': '利润表', 'xjllb': '现金流量表'}
# NAMES = {'xjllb': '现金流量表'}  # 删除
START = MARKET_START.tz_localize(None)
DATE_KEY = '报告日期'


def create_index_for(collection):
    # 不存在索性信息时，创建索引
    if not collection.index_information():
        collection.create_index([(DATE_KEY, 1), ("股票代码", 1)], unique=True)


def need_refresh(collection, code):
    """是否需要刷新

    简单规则：
        如果已经存在数据，且24小时内已经刷新 ❌ 否则 ✔
    """
    now = pd.Timestamp('now')
    doc = collection.find_one({'股票代码': code})
    if doc and now - doc['更新时间'] < pd.Timedelta(days=1):
        return False
    return True


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
        if pd.notnull(doc[k]) and doc[k] != '--':
            res[k] = v
    return res


def _refresh(batch, d):
    db = get_db('wy')
    for key, name in NAMES.items():
        collection = db[name]
        create_index_for(collection)
        for code in batch:
            # 首先检查状态，减少数据库查询
            if d.get((code, key), False):
                continue
            if not need_refresh(collection, code):
                d[(code, key)] = True
                logger.info(f"股票 {code} {name:>7} 已经刷新")
                continue
            try:
                # 此处下载股票全部历史数据
                df = fetch_financial_report(code, key)
            except (ValueError, KeyError, IndexError) as e:
                logger.error(f"股票 {code} {name:>7} 可忽略异常 {e}")
                # 网页不存在时发生，忽略
                # 标注为完成状态
                d[(code, key)] = True
                continue
            except Exception as e:
                logger.error(f"股票 {code} {name:>7} 失败 {e}")
                continue
            # 正常情形下运行以下代码
            df['股票代码'] = code
            df[DATE_KEY] = pd.to_datetime(df[DATE_KEY], errors='ignore')
            last_dt = get_max_dt(collection, code)
            # 只有新增数据才需要添加
            df = df[df[DATE_KEY] > last_dt]
            if not df.empty:
                for doc in df.to_dict('records'):
                    doc['更新时间'] = pd.Timestamp('now')
                    collection.insert_one(_droped_null(doc))
            logger.info(f"完成股票 {code} {name:>7} 刷新")
            d[(code, key)] = True


def refresh():
    t = time.time()
    codes = list(get_stock_status().keys())
    shuffle(codes)
    # 约4000只股票，每批300只
    batches = partition_all(300, codes)
    logger.info(f"股票数量 {len(codes)}")
    # 多进程
    with Manager() as manager:
        d = manager.dict()
        for k in product(codes, NAMES.keys()):
            d[k] = False
        func = partial(_refresh, d=d)
        for i in range(10):
            if all(d.values()):
                break
            logger.info(f"第{i+1}次尝试")
            # 异步导致失败
            with Pool(MAX_WORKER) as pool:
                pool.map(func, batches)
            time.sleep(30)
        failed = valfilter(lambda x: x == False, d)
        logger.warning(f"失败数量：{len(failed)}")
    logger.info(f"股票数量 {len(codes)}, 用时 {time.time() - t:.2f}秒")
