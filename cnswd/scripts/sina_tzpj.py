"""新浪投资评级"""
import random
import time

import pandas as pd
from pymongo.errors import DuplicateKeyError

from ..mongodb import get_db
from ..setting.constants import MARKET_START
from ..utils import make_logger
from ..websource.sina import fetch_rating

logger = make_logger('新浪投资评级')
START = MARKET_START.tz_localize(None)
DATE_KEY = '评级日期'
MAX_PAGES = 500


def create_index_for(collection):
    # 不存在索性信息时，创建索引
    if not collection.index_information():
        collection.create_index([(DATE_KEY, -1), ("股票代码", 1), ("分析师", 1)])


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
        if not pd.isnull(doc[k]):
            res[k] = v
    return res


def _refresh(df, page, collection, last_dt):
    if not df.empty:
        for doc in df.to_dict('records'):
            if doc[DATE_KEY] < last_dt:
                logger.info(f"{DATE_KEY} < {last_dt}，提前退出")
                return True
            doc['更新时间'] = pd.Timestamp('now')
            try:
                collection.insert_one(_droped_null(doc))
            except DuplicateKeyError:
                pass
    logger.info(f"完成第{page:>4}页数据刷新")


def refresh():
    start_t = time.time()
    db = get_db('wy')
    collection = db['投资评级']
    create_index_for(collection)
    last_dt = get_max_dt(collection)
    if not need_refresh(collection):
        logger.info("当日已经刷新，退出")
        return
    d = {}  # 按列存放完成状态
    for page in range(1, MAX_PAGES):
        if d.get(page, False):
            continue
        try:
            df = fetch_rating(page)
            early_exit = _refresh(df, page, collection, last_dt)
            if early_exit:
                break
            d[page] = True
        except Exception as e:
            logger.error(f"{e}")
            continue
        if df.empty:
            # 逐页提取，当数据为空时跳出循环
            d[page] = True
            break
        t = random.randint(10, 30) / 10
        time.sleep(t)

    logger.info(f"用时 {time.time() - start_t:.2f}秒")
