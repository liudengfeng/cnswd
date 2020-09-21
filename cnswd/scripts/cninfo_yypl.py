"""
巨潮资讯网财报预约披露

解决网易财务报告公告日期问题
"""
import random
import time
from requests.exceptions import ConnectionError
import akshare as ak
import pandas as pd
from pandas.tseries.offsets import QuarterEnd
from pymongo.errors import DuplicateKeyError

from ..mongodb import get_db
from ..utils import make_logger

logger = make_logger('巨潮财务报告预约披露')
offset = QuarterEnd(n=-1, startingMonth=3, normalize=True)
today = pd.Timestamp('today')

END_DATE = offset.apply(today)
START_DATE = pd.Timestamp('2006-12-31')

DB_NAME = 'wy'
COLLECTION_NAME = '预约披露'
DATE_KEY = '实际披露'
DATE_KEY_1 = '报告年度'
# 降序排列
DATES = pd.date_range(START_DATE, END_DATE, freq='Q')[::-1]


def _query_period(qd):
    if qd.quarter == 1:
        return f"{qd.year}一季"
    elif qd.quarter == 2:
        return f"{qd.year}半年报"
    elif qd.quarter == 3:
        return f"{qd.year}三季"
    else:
        return f"{qd.year}年报"


def create_index_for(collection):
    # 不存在索性信息时，创建索引
    if not collection.index_information():
        collection.create_index(
            [(DATE_KEY, -1), ("股票代码", 1), (DATE_KEY_1, 1)], unique=True)


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
        return START_DATE


def _refresh(df, collection, tdate, dt):
    if not df.empty:
        for doc in df.to_dict('records'):
            to_add = {}
            # 如果没有实际披露日期，不予采用
            try:
                to_add[DATE_KEY] = pd.to_datetime(
                    doc[DATE_KEY], errors='raise')
            except Exception:
                continue
            if pd.isna(to_add[DATE_KEY]):
                continue
            to_add['更新时间'] = pd.Timestamp('now')
            to_add['股票代码'] = doc['股票代码']
            to_add[DATE_KEY_1] = dt
            try:
                collection.insert_one(to_add)
            except DuplicateKeyError:
                # 最新一季度肯定存在重复，可以忽略继续
                if dt == END_DATE:
                    continue
                else:
                    logger.info(f"已经刷新 {to_add['股票代码']} {tdate}，提前退出")
                    return True
    logger.info(f"完成 {tdate} 数据刷新")


def refresh():
    start_t = time.time()
    db = get_db(DB_NAME)
    collection = db[COLLECTION_NAME]
    create_index_for(collection)
    # last_dt = get_max_dt(collection)
    if not need_refresh(collection):
        logger.info("当日已经刷新，退出")
        return
    d = {dt: False for dt in DATES}
    while not all(d.values()):
        for dt in d.keys():
            try:
                tdate = dt.strftime(r"%Y-%m-%d")
                period = _query_period(dt)
                df = ak.stock_report_disclosure(market="沪深", period=period)
                early_exit = _refresh(df, collection, tdate, dt)
                if early_exit:
                    d = {dt: True for dt in DATES}
                    break
                d[dt] = True
            except ValueError as e:
                # 产生值错误，代表所在季度没有数据
                logger.error(f"{e}")
                d = {dt: True for dt in DATES}
                break
            except (ConnectionResetError, ConnectionError):
                t = 60
                logger.info(f"休眠{t}秒")
                time.sleep(t)
            # 经常会中断，+休眠时长
            t = random.randint(10, 20)
            time.sleep(t)

    logger.info(f"用时 {time.time() - start_t:.2f}秒")
