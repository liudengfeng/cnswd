"""网易财务分析 主要财务指标"""
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
from cnswd.websource.wy import fetch_financial_indicator

from .base import get_stock_status

logger = make_logger('网易主要财务指标')

TYPES = {'report': '按报告期', 'year': '按年度', 'season': '按单季度'}
NAMES = {'zycwzb': '主要财务指标', 'ylnl': '盈利能力',
         'chnl': '偿还能力', 'cznl': '成长能力', 'yynl': '营运能力'}
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
        if not pd.isnull(doc[k]):
            res[k] = v
    return res


def _refresh(batch, d):
    db = get_db('wy')
    for p, p_name in TYPES.items():
        for key, name in NAMES.items():
            collection_name = f'{p_name}_{name}'
            collection = db[collection_name]
            for code in batch:
                # 首先检查状态，减少数据库查询
                if d.get((code, p, key), False):
                    continue
                if not need_refresh(collection, code):
                    d[(code, p, key)] = True
                    logger.info(f"股票 {code} {p_name} {name:>7} 已经刷新")
                    continue
                try:
                    df = fetch_financial_indicator(code, p, key)
                    # 按报告日期降序排列
                    df.sort_values(DATE_KEY, ascending=False, inplace=True)
                except (ValueError, KeyError):
                    # 网页不存在时发生，忽略
                    # 标注为完成状态
                    d[(code, p, key)] = True
                    continue
                except Exception as e:
                    logger.error(f"股票 {code} {p_name} {name:>7} 失败 {e}")
                    continue
                # 正常情形下运行以下代码
                df['股票代码'] = code
                df[DATE_KEY] = pd.to_datetime(df[DATE_KEY], errors='ignore')
                create_index_for(collection)
                last_dt = get_max_dt(collection, code)
                df = df[df[DATE_KEY] > last_dt]
                if not df.empty:
                    for doc in df.to_dict('records'):
                        doc['更新时间'] = pd.Timestamp('now')
                        collection.insert_one(_droped_null(doc))
                logger.info(f"完成股票 {code} {p_name} {name:>7} 刷新")
                d[(code, p, key)] = True


def refresh():
    t = time.time()
    codes = list(get_stock_status().keys())
    shuffle(codes)
    # 约4000只股票，每批300只
    batches = partition_all(300, codes)

    # 多进程
    with Manager() as manager:
        d = manager.dict()
        for k in product(codes, TYPES.keys(), NAMES.keys()):
            d[k] = False
        func = partial(_refresh, d=d)
        for _ in range(10):
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
