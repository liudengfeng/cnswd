"""新浪融资融券"""
import random
import time

import pandas as pd
from pymongo.errors import DuplicateKeyError

from ..mongodb import get_db
from ..utils import make_logger
from ..websource.sina import fetch_margin

logger = make_logger('新浪融资融券')
START = pd.Timestamp('2010-03-31')
DATE_KEY = '交易日期'
# 降序排列
DATES = pd.date_range(START, pd.Timestamp('today') -
                      pd.Timedelta(days=1), freq='B')[::-1]


def create_index_for(collection):
    # 不存在索性信息时，创建索引
    if not collection.index_information():
        collection.create_index([(DATE_KEY, -1), ("股票代码", 1)], unique=True)


def need_refresh(collection):
    """是否需要刷新

    简单规则：
        如果已经存在数据，且24小时内已经刷新 ❌ 否则 ✔
    """
    now = pd.Timestamp('now')
    doc = collection.find_one({}, sort=[(DATE_KEY, -1)])
    if doc and now - doc['更新时间'] < pd.Timedelta(days=1):
        return False
    return True


def get_max_dt(collection):
    """最后日期"""
    doc = collection.find_one({}, sort=[(DATE_KEY, -1)])
    if doc:
        return pd.Timestamp(doc[DATE_KEY])
    else:
        return START


def _droped_null(doc):
    res = {}
    for k, v in doc.items():
        if doc[k] != '--':
            res[k] = v
    return res


def _refresh(df, collection, tdate):
    if not df.empty:
        td = pd.Timestamp(tdate)
        for doc in df.to_dict('records'):
            doc[DATE_KEY] = td
            doc['更新时间'] = pd.Timestamp('now')
            try:
                collection.insert_one(_droped_null(doc))
            except DuplicateKeyError:
                logger.info(f"已经刷新 {tdate}，提前退出")
                return True
    logger.info(f"完成 {tdate} 数据刷新")


def refresh():
    start_t = time.time()
    db = get_db('wy')
    collection = db['融资融券']
    create_index_for(collection)
    # last_dt = get_max_dt(collection)
    if not need_refresh(collection):
        logger.info("当日已经刷新，退出")
        return
    for dt in DATES:
        try:
            tdate = dt.strftime(r"%Y-%m-%d")
            df = fetch_margin(tdate)
            early_exit = _refresh(df, collection, tdate)
            if early_exit:
                break
        except Exception as e:
            logger.error(f"{e}")
            continue
        t = random.randint(10, 30) / 10
        time.sleep(t)

    logger.info(f"用时 {time.time() - start_t:.2f}秒")
