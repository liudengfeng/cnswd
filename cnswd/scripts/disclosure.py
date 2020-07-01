import asyncio
import logging
import random
import time
from datetime import datetime

import aiohttp
import pandas as pd
from pymongo.errors import DuplicateKeyError
from retry.api import retry_call

from ..mongodb import get_db
from ..utils import make_logger
from ..websource.disclosures import fetch_disclosure

aiohttp_logger = logging.getLogger('aiohttp')
aiohttp_logger.setLevel(logging.CRITICAL)
collection_name = '公司公告'
logger = make_logger(collection_name)


def get_max(col_name, default):
    db = get_db()
    collection = db[collection_name]
    pipe = [
        {
            '$sort': {
                col_name: -1
            }
        },
        {
            '$project': {
                '_id': 0,
                col_name: 1
            }
        },
        {
            '$limit': 1
        },
    ]
    try:
        return list(collection.aggregate(pipe))[0][col_name]
    except (Exception, ):
        return default


def get_dates():
    """日期列表"""
    min_start = pd.Timestamp('2010-01-01')
    start = pd.Timestamp(get_max('公告时间', min_start))
    if start < min_start:
        start = min_start
    end = pd.Timestamp('now')
    if end.hour >= 16:
        end = end + pd.Timedelta(days=1)
    dates = pd.date_range(start, end)
    # dates = [d.strftime(r'%Y-%m-%d') for d in dates]
    return dates


def _append(docs):
    db = get_db()
    collection = db[collection_name]
    count = 0
    t = time.time()
    for doc in docs:
        try:
            collection.insert_one(doc)
            count += 1
        except DuplicateKeyError:
            pass
    duration = time.time() - t + 1e-6
    ratio = count / duration
    logger.info(
        f"Insert {count:>5} docs. write {ratio:.0f} docs/s keep {count}/{len(docs)}"
    )


async def refresh(init=False):
    """刷新"""
    if init:
        logger.warning("初始化公司公告......")
        if input("初始化将删除本地数据！！！请确认(y/n)") in ('y', 'yes'):
            db = get_db()
            collection = db[collection_name]
            collection.drop()
            create_index(collection)
    dates = get_dates()
    # 尽管理论上可以一次性获取期间全部数据，由于数据量大，容易产生故障
    # 比较稳妥的方式是分日读取
    for d in get_dates():
        # docs = await fetch_disclosure(d, d)
        try:
            docs = await retry_call(
                fetch_disclosure, [d, d],
                exceptions=(aiohttp.client_exceptions.ClientOSError, ),
                tries=3,
                delay=2,
                jitter=(1, 5),
                logger=logger)
            _append(docs)
        except Exception as e:
            logger.exception(e)


def create_index(collection):
    # collection.create_index([("公告时间", -1)], unique=True, name='id_index')
    collection.create_index([("公告时间", -1)], name='dt_index')
    collection.create_index([("公告编号", 1)], unique=True, name='id_index')


if __name__ == '__main__':
    # migrate()
    # refresh()
    test()
